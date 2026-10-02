import base64
import concurrent.futures
import os
import sqlite3
import subprocess
import uuid
from unittest.mock import Mock

import pytest

from mac_messages_mcp import attachment_send as sends

RECIPIENT = "+12025550124"  # Synthetic/mocked tests only; never invokes Messages.
PAYLOAD = base64.b64encode(b"harmless test fixture").decode()


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    monkeypatch.setenv("MAC_MESSAGES_OUTBOX_DIR", str(tmp_path / "outbox"))
    dbpath = tmp_path / "messages.sqlite3"
    with sqlite3.connect(dbpath) as db:
        db.executescript("""
        CREATE TABLE message (guid TEXT, error INTEGER, is_sent INTEGER, is_delivered INTEGER,
          date_delivered INTEGER, date_read INTEGER, service TEXT, is_from_me INTEGER);
        CREATE TABLE attachment (guid TEXT, transfer_state INTEGER, total_bytes INTEGER, transfer_name TEXT, filename TEXT);
        CREATE TABLE message_attachment_join (message_id INTEGER, attachment_id INTEGER);
        CREATE TABLE chat (style INTEGER);
        CREATE TABLE chat_message_join (chat_id INTEGER, message_id INTEGER);
        CREATE TABLE chat_handle_join (chat_id INTEGER, handle_id INTEGER);
        CREATE TABLE handle (id TEXT);
        INSERT INTO chat VALUES (45);
        INSERT INTO handle VALUES ('+12025550124');
        INSERT INTO chat_handle_join VALUES (1,1);
        """)

    def query(sql, params=()):
        with sqlite3.connect(dbpath) as db:
            db.row_factory = sqlite3.Row
            return [dict(row) for row in db.execute(sql, params)]

    def dispatch(recipient, path):
        with sqlite3.connect(dbpath) as db:
            mid = db.execute(
                "INSERT INTO message VALUES ('message-guid',0,1,0,0,0,'iMessage',1)"
            ).lastrowid
            aid = db.execute(
                "INSERT INTO attachment VALUES (?,5,?,?,?)",
                ("attachment-guid", path.stat().st_size, path.name, str(path)),
            ).lastrowid
            db.execute("INSERT INTO message_attachment_join VALUES (?,?)", (mid, aid))
            db.execute("INSERT INTO chat_message_join VALUES (1,?)", (mid,))
        return "accepted", None

    native = Mock(side_effect=dispatch)
    monkeypatch.setattr(sends, "query_messages_db", query)
    monkeypatch.setattr(sends, "_dispatch", native)
    # Catch any accidental live operation in this entire test file.
    monkeypatch.setattr(
        subprocess,
        "run",
        Mock(side_effect=AssertionError("Native calls must be mocked")),
    )
    return tmp_path, dbpath, native


def send(**kwargs):
    args = dict(
        recipient=RECIPIENT,
        request_id=str(uuid.uuid4()),
        filename="test.txt",
        content_base64=PAYLOAD,
    )
    args.update(kwargs)
    return sends.send_attachment(**args)


def test_native_file_and_evidence(sandbox):
    root, _, native = sandbox
    result = send()
    assert result["status"] == "sent"
    assert result["dispatch"] == "accepted"
    assert result["evidence"]["attachment_id"] == 1
    assert result["evidence"]["local_bytes_match"] is True
    assert result["evidence"]["message_guid"] == "message-guid"
    staged = native.call_args.args[1]
    assert staged.read_bytes() == b"harmless test fixture"
    assert staged.stat().st_mode & 0o777 == 0o600
    assert staged.parent.stat().st_mode & 0o777 == 0o700
    assert staged.name == result["request_id"] + "-test.txt"
    assert sends.attachment_send_status(result["request_id"])["status"] == "sent"
    native.assert_called_once()


def test_duplicate_restart_and_conflict(sandbox):
    request_id = str(uuid.uuid4())
    first = send(request_id=request_id)
    duplicate = send(request_id=request_id)
    assert duplicate["duplicate_suppressed"]
    assert duplicate["evidence"] == first["evidence"]
    with pytest.raises(ValueError, match="different content"):
        send(
            request_id=request_id, content_base64=base64.b64encode(b"changed").decode()
        )
    sandbox[2].assert_called_once()


def test_concurrent_duplicate_claims(sandbox):
    request_id = str(uuid.uuid4())
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: send(request_id=request_id), range(2)))
    assert sum(bool(r.get("duplicate_suppressed")) for r in results) == 1
    sandbox[2].assert_called_once()


@pytest.mark.parametrize(
    "recipient",
    [
        "Breland",
        "contact:1",
        "8033861737",
        "iMessage;+;chat123",
        "+12025550124\n",
        "Name <a@example.com>",
        "a@example.com\n",
    ],
)
def test_rejects_inexact_recipient(sandbox, recipient):
    with pytest.raises(ValueError):
        send(recipient=recipient)
    sandbox[2].assert_not_called()


@pytest.mark.parametrize(
    "filename",
    [
        "../bad.txt",
        "/tmp/bad.txt",
        "bad\\file",
        "bad:name",
        "bad\nname",
        ".",
        "",
        "x" * 181,
    ],
)
def test_rejects_unsafe_names(sandbox, filename):
    with pytest.raises(ValueError):
        send(filename=filename)
    sandbox[2].assert_not_called()


@pytest.mark.parametrize(
    "payload", ["", "bad!", "data:text/plain;base64,YQ==", "YQ==\n"]
)
def test_rejects_bad_base64(sandbox, payload):
    with pytest.raises(ValueError):
        send(content_base64=payload)
    sandbox[2].assert_not_called()


def test_input_caps_and_exclusive_sources(sandbox, monkeypatch):
    monkeypatch.setattr(sends, "MAX_INLINE_BYTES", 2)
    with pytest.raises(ValueError):
        send()
    with pytest.raises(ValueError, match="exactly one"):
        send(file_path="/tmp/x")
    with pytest.raises(ValueError, match="exactly one"):
        send(content_base64=None)
    sandbox[2].assert_not_called()


def test_local_file_copy_and_reject_symlink_fifo(sandbox):
    root, _, native = sandbox
    source = root / "source"
    source.write_bytes(b"local file")
    result = send(content_base64=None, file_path=str(source))
    assert result["total_bytes"] == 10
    assert native.call_args.args[1] != source
    link = root / "link"
    link.symlink_to(source)
    with pytest.raises(OSError):
        send(content_base64=None, file_path=str(link))
    fifo = root / "fifo"
    os.mkfifo(fifo)
    with pytest.raises(ValueError, match="regular file"):
        send(content_base64=None, file_path=str(fifo))
    native.assert_called_once()


def test_db_unavailable_prevents_dispatch(sandbox, monkeypatch):
    monkeypatch.setattr(sends, "query_messages_db", lambda *a: [{"error": "denied"}])
    with pytest.raises(ValueError, match="no attachment was dispatched"):
        send()
    sandbox[2].assert_not_called()


def test_uncertainty_is_durable_and_does_not_retry(sandbox, monkeypatch):
    sandbox[2].side_effect = lambda *a: ("uncertain", "timeout")
    monkeypatch.setattr(sends, "_evidence", lambda r: {"status": "unverified"})
    ticks = iter([0, 6])
    monkeypatch.setattr(sends.time, "monotonic", lambda: next(ticks))
    request_id = str(uuid.uuid4())
    result = send(request_id=request_id)
    assert result["status"] == "unverified" and not result["retry_safe"]
    assert send(request_id=request_id)["duplicate_suppressed"]
    sandbox[2].assert_called_once()


def test_crash_after_claim_does_not_resend(sandbox):
    sandbox[2].side_effect = RuntimeError("crash")
    request_id = str(uuid.uuid4())
    with pytest.raises(RuntimeError):
        send(request_id=request_id)
    assert sends.attachment_send_status(request_id)["dispatch"] == "uncertain"
    assert send(request_id=request_id)["duplicate_suppressed"]
    sandbox[2].assert_called_once()


def test_evidence_is_exact_and_distinguishes_delivery(sandbox):
    _, dbpath, _ = sandbox
    result = send()
    request_id = result["request_id"]
    with sqlite3.connect(dbpath) as db:
        db.execute("UPDATE message SET is_delivered=1, date_delivered=999")
    assert sends.attachment_send_status(request_id)["status"] == "delivered"
    with sqlite3.connect(dbpath) as db:
        db.execute("UPDATE message SET error=22")
    assert sends.attachment_send_status(request_id)["status"] == "failed"
    with sqlite3.connect(dbpath) as db:
        db.execute("UPDATE handle SET id='+12025550123'")
    assert sends.attachment_send_status(request_id)["status"] == "unverified"
    with sqlite3.connect(dbpath) as db:
        db.execute("UPDATE handle SET id=?", (RECIPIENT,))
        db.execute("UPDATE chat SET style=43")
    assert sends.attachment_send_status(request_id)["status"] == "unverified"


def test_status_never_initializes_outbox(sandbox):
    root, _, native = sandbox
    assert sends.attachment_send_status(str(uuid.uuid4()))["status"] == "not_found"
    assert not (root / "outbox").exists()
    native.assert_not_called()


def test_native_dispatch_uses_argv_once_and_timeout_has_no_fallback(
    tmp_path, monkeypatch
):
    native = Mock(side_effect=subprocess.TimeoutExpired("osascript", 15))
    monkeypatch.setattr(subprocess, "run", native)
    assert sends._dispatch(RECIPIENT, tmp_path / "file")[0] == "uncertain"
    native.assert_called_once()
    assert native.call_args.args[0] == [
        "/usr/bin/osascript",
        "-",
        RECIPIENT,
        str(tmp_path / "file"),
    ]
    assert "activate" not in sends.SEND_SCRIPT
    assert "System Events" not in sends.SEND_SCRIPT
    assert sends.SEND_SCRIPT.count("send attachmentFile") == 1


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE attachment SET transfer_name='unrelated.txt'",
        "UPDATE attachment SET total_bytes=9999",
        "UPDATE message SET is_from_me=0",
    ],
)
def test_never_correlates_unrelated_rows(sandbox, sql):
    result = send()
    with sqlite3.connect(sandbox[1]) as db:
        db.execute(sql)
    assert sends.attachment_send_status(result["request_id"])["status"] == "unverified"


def test_pending_and_ambiguous_evidence(sandbox):
    result = send()
    with sqlite3.connect(sandbox[1]) as db:
        db.execute("UPDATE message SET is_sent=0")
    assert sends.attachment_send_status(result["request_id"])["status"] == "pending"
    with sqlite3.connect(sandbox[1]) as db:
        db.execute("INSERT INTO message SELECT * FROM message")
        db.execute("INSERT INTO message_attachment_join VALUES (2,1)")
        db.execute("INSERT INTO chat_message_join VALUES (1,2)")
    assert sends.attachment_send_status(result["request_id"])["status"] == "ambiguous"
