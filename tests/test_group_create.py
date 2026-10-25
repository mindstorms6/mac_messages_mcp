"""Synthetic tests for at-most-once iMessage group creation.

Every native call is mocked.  These tests must never create a real chat or send
a real message.
"""

import asyncio
import concurrent.futures
import json
import sqlite3
import subprocess
import uuid
from pathlib import Path
from unittest.mock import Mock

import pytest

from mac_messages_mcp import group_create as groups
from mac_messages_mcp.server import mcp

RECIPIENTS = ["friend@example.com", "+12025550124"]
MULTI_RECIPIENTS = [
    "+12025550101",
    "+12025550102",
    "+12025550103",
    "+12025550104",
    "+12025550105",
    "+12025550106",
]
MESSAGE = "synthetic initial message"
CHAT_ID = "iMessage;+;chat-synthetic-new-group"
MESSAGE_ID = "message-synthetic-guid"


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    monkeypatch.setenv("MAC_MESSAGES_GROUP_OUTBOX_DIR", str(tmp_path / "group-outbox"))
    dbpath = tmp_path / "messages.sqlite3"
    with sqlite3.connect(dbpath) as db:
        db.executescript("""
        CREATE TABLE message (
          guid TEXT, error INTEGER, is_sent INTEGER, is_delivered INTEGER,
          date_delivered INTEGER, date_read INTEGER, service TEXT,
          is_from_me INTEGER, text TEXT, attributedBody BLOB);
        CREATE TABLE chat (guid TEXT, style INTEGER);
        CREATE TABLE handle (id TEXT);
        CREATE TABLE chat_message_join (chat_id INTEGER, message_id INTEGER);
        CREATE TABLE chat_handle_join (chat_id INTEGER, handle_id INTEGER);
        """)

    def query(sql, params=()):
        with sqlite3.connect(dbpath) as db:
            db.row_factory = sqlite3.Row
            return [dict(row) for row in db.execute(sql, params)]

    def add_group(
        recipients=RECIPIENTS,
        message=MESSAGE,
        chat_id=CHAT_ID,
        message_id=MESSAGE_ID,
        sent=1,
    ):
        with sqlite3.connect(dbpath) as db:
            cid = db.execute("INSERT INTO chat VALUES (?,43)", (chat_id,)).lastrowid
            for recipient in recipients:
                hid = db.execute(
                    "INSERT INTO handle VALUES (?)", (recipient,)
                ).lastrowid
                db.execute("INSERT INTO chat_handle_join VALUES (?,?)", (cid, hid))
            mid = db.execute(
                "INSERT INTO message VALUES (?,?,?,?,?,?,?,?,?,?)",
                (message_id, 0, sent, 0, 0, 0, "iMessage", 1, message, None),
            ).lastrowid
            db.execute("INSERT INTO chat_message_join VALUES (?,?)", (cid, mid))

    def dispatch(recipients, message):
        add_group(recipients=recipients, message=message)
        return "accepted", CHAT_ID, None

    native = Mock(side_effect=dispatch)
    monkeypatch.setattr(groups, "query_messages_db", query)
    monkeypatch.setattr(groups, "_check_imessage_availability", Mock(return_value=True))
    monkeypatch.setattr(groups, "_dispatch", native)
    return tmp_path, dbpath, native, add_group


def create(**kwargs):
    args = {
        "recipients": RECIPIENTS,
        "request_id": str(uuid.uuid4()),
        "message": MESSAGE,
    }
    args.update(kwargs)
    return groups.create_group_and_send(**args)


def test_returns_new_chat_and_message_ids(sandbox):
    root, _, native, _ = sandbox

    result = create(recipients=list(reversed(RECIPIENTS)))

    assert result["status"] == "sent"
    assert result["chat_id"] == CHAT_ID
    assert result["message_id"] == MESSAGE_ID
    assert result["evidence"]["message_rowid"] == 1
    assert result["recipients"] == sorted(RECIPIENTS)
    assert result["dispatch"] == "accepted"
    assert not result["retry_safe"]
    native.assert_called_once_with(sorted(RECIPIENTS), MESSAGE)
    ledger = root / "group-outbox/requests.sqlite3"
    assert ledger.exists()
    assert ledger.stat().st_mode & 0o777 == 0o600
    assert ledger.parent.stat().st_mode & 0o777 == 0o700
    assert (
        groups.group_creation_status(result["request_id"])["message_id"] == MESSAGE_ID
    )


def test_same_uuid_reconciles_without_another_dispatch(sandbox):
    request_id = str(uuid.uuid4())
    first = create(request_id=request_id)
    duplicate = create(request_id=request_id, recipients=list(reversed(RECIPIENTS)))

    assert duplicate["duplicate_suppressed"]
    assert duplicate["chat_id"] == first["chat_id"]
    assert duplicate["message_id"] == first["message_id"]
    sandbox[2].assert_called_once()

    # A protected retry is reconciliation-only even if later availability
    # checks would fail; it must not strand the request or dispatch again.
    groups._check_imessage_availability.return_value = False
    assert create(request_id=request_id)["duplicate_suppressed"]
    groups._check_imessage_availability.return_value = True

    with pytest.raises(
        groups.RequestConflictError, match="different recipients or message"
    ):
        create(request_id=request_id, message="different")
    sandbox[2].assert_called_once()


def test_six_recipient_retry_remains_reconciliation_only(sandbox):
    request_id = str(uuid.uuid4())

    first = create(request_id=request_id, recipients=MULTI_RECIPIENTS)
    duplicate = create(
        request_id=request_id, recipients=list(reversed(MULTI_RECIPIENTS))
    )

    assert first["status"] == "sent"
    assert duplicate["duplicate_suppressed"]
    assert duplicate["chat_id"] == first["chat_id"]
    assert duplicate["message_id"] == first["message_id"]
    sandbox[2].assert_called_once_with(sorted(MULTI_RECIPIENTS), MESSAGE)


def test_concurrent_same_uuid_dispatches_once(sandbox):
    request_id = str(uuid.uuid4())
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: create(request_id=request_id), range(2)))

    assert sum(bool(result.get("duplicate_suppressed")) for result in results) == 1
    sandbox[2].assert_called_once()


@pytest.mark.parametrize(
    "recipients",
    [
        ["friend@example.com"],
        ["friend@example.com", "FRIEND@example.com"],
        ["Breland", "+12025550124"],
        ["contact:1", "+12025550124"],
        ["iMessage;+;chat123", "+12025550124"],
        ["12345", "friend@example.com"],
        ["Name <friend@example.com>", "+12025550124"],
        ["friend@example.com\n", "+12025550124"],
    ],
)
def test_rejects_invalid_or_duplicate_recipients_before_dispatch(sandbox, recipients):
    with pytest.raises(ValueError):
        create(recipients=recipients)
    sandbox[2].assert_not_called()


@pytest.mark.parametrize("message", ["", "   ", "x" * 64_001])
def test_rejects_invalid_message_before_dispatch(sandbox, message):
    with pytest.raises(ValueError):
        create(message=message)
    sandbox[2].assert_not_called()


def test_requires_local_imessage_evidence_for_every_recipient(sandbox, monkeypatch):
    availability = Mock(side_effect=lambda recipient: recipient.endswith("example.com"))
    monkeypatch.setattr(groups, "_check_imessage_availability", availability)

    with pytest.raises(ValueError, match=r"\+12025550124"):
        create()

    assert availability.call_count == 2
    sandbox[2].assert_not_called()
    assert not (sandbox[0] / "group-outbox").exists()


def test_database_baseline_failure_prevents_dispatch(sandbox, monkeypatch):
    monkeypatch.setattr(
        groups, "query_messages_db", lambda *args: [{"error": "denied"}]
    )
    with pytest.raises(ValueError, match="no group was created"):
        create()
    sandbox[2].assert_not_called()


def test_timeout_is_durable_and_never_retries(sandbox, monkeypatch):
    sandbox[2].side_effect = lambda *args: ("uncertain", None, "timeout")
    ticks = iter([0, 6])
    monkeypatch.setattr(groups.time, "monotonic", lambda: next(ticks))
    request_id = str(uuid.uuid4())

    result = create(request_id=request_id)

    assert result["status"] == "unverified"
    assert result["dispatch"] == "uncertain"
    assert not result["retry_safe"]
    assert create(request_id=request_id)["duplicate_suppressed"]
    sandbox[2].assert_called_once()


def test_known_participant_coercion_exposes_safe_replacement(sandbox, monkeypatch):
    participant_list = ", ".join(
        f'participant id "service:{recipient}"' for recipient in MULTI_RECIPIENTS
    )
    sandbox[2].side_effect = lambda *args: (
        "uncertain",
        None,
        "execution error: Messages got an error: Can't make "
        f"{{{participant_list}}} into type participant. (-1700)",
    )
    ticks = iter([0, 6])
    monkeypatch.setattr(groups.time, "monotonic", lambda: next(ticks))
    request_id = str(uuid.uuid4())

    result = create(request_id=request_id, recipients=MULTI_RECIPIENTS)

    assert result["dispatch"] == "uncertain"
    assert result["status"] == "failed_before_creation"
    assert result["side_effects"] == "no_chat_or_message_created"
    assert result["replacement_request_safe"]
    assert result["replacement_requires_authorization"]
    assert not result["retry_safe"]
    assert "new canonical UUID" in result["next_step"]
    duplicate = create(request_id=request_id, recipients=MULTI_RECIPIENTS)
    assert duplicate["duplicate_suppressed"]
    sandbox[2].assert_called_once()


def test_other_native_errors_do_not_expose_replacement(sandbox, monkeypatch):
    sandbox[2].side_effect = lambda *args: (
        "uncertain",
        None,
        "execution error: Messages got an error: Can't make text into type "
        "participant. (-1700)",
    )
    ticks = iter([0, 6])
    monkeypatch.setattr(groups.time, "monotonic", lambda: next(ticks))

    result = create(recipients=MULTI_RECIPIENTS)

    assert result["status"] == "unverified"
    assert not result["replacement_request_safe"]
    assert not result["retry_safe"]


def test_matching_database_evidence_wins_over_coercion_classification(sandbox):
    _, _, native, add_group = sandbox
    participant_list = ", ".join(
        f'participant id "service:{recipient}"' for recipient in RECIPIENTS
    )

    def dispatch(recipients, message):
        add_group(recipients=recipients, message=message)
        return (
            "uncertain",
            None,
            "execution error: Messages got an error: Can't make "
            f"{{{participant_list}}} into type participant. (-1700)",
        )

    native.side_effect = dispatch
    result = create()

    assert result["status"] == "sent"
    assert not result["replacement_request_safe"]
    assert result["chat_id"] == CHAT_ID
    assert result["message_id"] == MESSAGE_ID


def test_crash_after_claim_is_reconciliation_only(sandbox):
    sandbox[2].side_effect = RuntimeError("synthetic crash")
    request_id = str(uuid.uuid4())

    with pytest.raises(RuntimeError, match="synthetic crash"):
        create(request_id=request_id)

    status = groups.group_creation_status(request_id)
    assert status["dispatch"] == "uncertain"
    assert status["status"] == "unverified"
    assert create(request_id=request_id)["duplicate_suppressed"]
    sandbox[2].assert_called_once()


def test_exact_participant_set_ignores_shared_participant_group(sandbox):
    _, _, native, add_group = sandbox

    def dispatch(recipients, message):
        add_group(
            recipients=[*recipients, "extra@example.com"],
            message=message,
            chat_id="iMessage;+;shared-participants-distractor",
            message_id="distractor-message",
        )
        add_group(recipients=recipients, message=message)
        return "accepted", CHAT_ID, None

    native.side_effect = dispatch
    result = create()

    assert result["status"] == "sent"
    assert result["chat_id"] == CHAT_ID
    assert result["message_id"] == MESSAGE_ID


def test_multiple_exact_new_groups_are_ambiguous(sandbox):
    _, _, native, add_group = sandbox

    def dispatch(recipients, message):
        add_group(recipients=recipients, message=message)
        add_group(
            recipients=recipients,
            message=message,
            chat_id="iMessage;+;second-new-group",
            message_id="second-message",
        )
        return "uncertain", None, "lost reply"

    native.side_effect = dispatch
    result = create()

    assert result["status"] == "ambiguous"
    assert "Multiple matching" in result["evidence_error"]
    assert not result["retry_safe"]


def test_failed_message_returns_ids_without_claiming_success(sandbox):
    _, dbpath, _, _ = sandbox
    result = create()
    with sqlite3.connect(dbpath) as db:
        db.execute("UPDATE message SET error=22, is_sent=0")

    status = groups.group_creation_status(result["request_id"])
    assert status["status"] == "failed"
    assert status["chat_id"] == CHAT_ID
    assert status["message_id"] == MESSAGE_ID


def test_status_lookup_does_not_initialize_outbox(sandbox):
    root, _, native, _ = sandbox
    result = groups.group_creation_status(str(uuid.uuid4()))
    assert result["status"] == "not_found"
    assert not (root / "group-outbox").exists()
    native.assert_not_called()


def test_native_script_is_single_imessage_create_and_send(tmp_path, monkeypatch):
    native = Mock(
        return_value=subprocess.CompletedProcess(
            args=[], returncode=0, stdout=CHAT_ID + "\n", stderr=""
        )
    )
    monkeypatch.setattr(subprocess, "run", native)

    dispatch, chat_id, error = groups._dispatch(MULTI_RECIPIENTS, MESSAGE)

    assert (dispatch, chat_id, error) == ("accepted", CHAT_ID, None)
    command = native.call_args.args[0]
    assert command[:2] == ["/usr/bin/osascript", "-"]
    assert command[3:] == MULTI_RECIPIENTS
    assert not Path(command[2]).exists()
    assert native.call_args.kwargs["input"] == groups.CREATE_GROUP_SCRIPT
    assert groups.CREATE_GROUP_SCRIPT.count("make new chat") == 1
    assert groups.CREATE_GROUP_SCRIPT.count("send messageText") == 1
    assert "make new chat at targetAccount" in groups.CREATE_GROUP_SCRIPT
    assert "{«class ICpt»:groupParticipants}" in groups.CREATE_GROUP_SCRIPT
    assert "{participants:groupParticipants}" not in groups.CREATE_GROUP_SCRIPT
    assert "service type = iMessage" in groups.CREATE_GROUP_SCRIPT
    assert "connection status = connected" in groups.CREATE_GROUP_SCRIPT
    assert "handle of targetParticipant" in groups.CREATE_GROUP_SCRIPT
    assert "SMS" not in groups.CREATE_GROUP_SCRIPT
    assert "RCS" not in groups.CREATE_GROUP_SCRIPT
    assert "activate" not in groups.CREATE_GROUP_SCRIPT
    assert "System Events" not in groups.CREATE_GROUP_SCRIPT


def test_tool_schema_documents_side_effect_ids_and_retry_contract():
    tools = {tool.name: tool for tool in asyncio.run(mcp.list_tools())}
    create_tool = tools["tool_create_group_and_send"]
    status_tool = tools["tool_get_group_creation_status"]

    properties = create_tool.input_schema["properties"]
    assert properties["recipients"]["minItems"] == 2
    assert properties["recipients"]["maxItems"] == 32
    assert "SAME ID" in properties["request_id"]["description"]
    assert "new full chat GUID" in create_tool.description
    assert "outgoing message GUID" in create_tool.description
    assert "never dispatches again" in " ".join(create_tool.description.split())
    assert (
        "never creates a group"
        in status_tool.input_schema["properties"]["request_id"]["description"]
    )
    assert "failed_before_creation" in status_tool.description
    assert "original UUID remains permanently closed" in " ".join(
        status_tool.description.split()
    )


def test_manifest_lists_group_creation_and_status_tools():
    manifest = json.loads(
        (Path(__file__).resolve().parents[1] / "manifest.json").read_text()
    )
    descriptions = {tool["name"]: tool["description"] for tool in manifest["tools"]}

    assert "durable request UUID" in descriptions["tool_create_group_and_send"]
    assert "chat and message GUIDs" in descriptions["tool_create_group_and_send"]
    assert "ever retrying" in descriptions["tool_get_group_creation_status"]
