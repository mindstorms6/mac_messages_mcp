"""Native attachment sends with durable at-most-once dispatch and exact evidence.

Only this module's MCP entrypoint may authorize dispatch. No UI automation,
network downloads, SMS fallback, retries, or writes to the Messages database.
"""

import base64
import binascii
import hashlib
import json
import os
import re
import sqlite3
import stat
import subprocess
import time
import uuid
from pathlib import Path

from .messages import query_messages_db
from .phone import to_dialable_e164

MAX_INLINE_BYTES = 3_000_000
MAX_FILE_BYTES = 20_000_000
SEND_SCRIPT = """on run argv
    set targetAddress to item 1 of argv
    set attachmentFile to (POSIX file (item 2 of argv)) as alias
    tell application "Messages"
        set targetAccount to first account whose service type = iMessage and enabled is true
        set targetParticipant to participant targetAddress of targetAccount
        if (handle of targetParticipant) is not targetAddress then error "Recipient handle mismatch"
        send attachmentFile to targetParticipant
    end tell
    return "accepted"
end run
"""


def _request_id(value):
    if not isinstance(value, str) or str(uuid.UUID(value)) != value:
        raise ValueError("request_id must be a canonical lowercase UUID")
    return value


def _recipient(value):
    if not isinstance(value, str):
        raise ValueError("recipient must be an exact E.164 phone number or email")
    if re.fullmatch(r"\+[1-9][0-9]{7,14}", value):
        # Explicit country code: do not consult the Mac's regional defaults.
        if to_dialable_e164(value, region="ZZ") == value:
            return value
    # Conservative plain email address; no display names or contact selectors.
    if (
        re.fullmatch(
            r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+", value
        )
        and len(value) <= 254
    ):
        return value
    raise ValueError(
        "recipient must be an exact E.164 phone number or plain email; names and groups are unsupported"
    )


def _filename(value):
    if not isinstance(value, str) or not value or value in {".", ".."}:
        raise ValueError("filename must be one nonempty basename")
    if (
        len(value.encode("utf-8")) > 180
        or any(c in value for c in "/\\:")
        or any(ord(c) < 32 or ord(c) == 127 for c in value)
    ):
        raise ValueError(
            "filename contains a separator/control character or exceeds 180 UTF-8 bytes"
        )
    return value


def _input(filename, content_base64, file_path):
    if (content_base64 is None) == (file_path is None):
        raise ValueError("Supply exactly one of content_base64 or file_path")
    if content_base64 is not None:
        if not isinstance(content_base64, str) or len(content_base64) > 4 * (
            (MAX_INLINE_BYTES + 2) // 3
        ):
            raise ValueError("Inline attachment exceeds 3,000,000 decoded bytes")
        try:
            data = base64.b64decode(content_base64, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValueError(
                "content_base64 must be strict base64, without a data URL prefix"
            ) from exc
        limit = MAX_INLINE_BYTES
    else:
        path = Path(file_path)
        if not path.is_absolute():
            raise ValueError(
                "file_path must be an absolute path on the MCP server host"
            )
        # Open without following a leaf symlink or blocking on a FIFO/device.
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_FILE_BYTES:
                raise ValueError(
                    "file_path must name a regular file of at most 20,000,000 bytes"
                )
            data = stream.read(MAX_FILE_BYTES + 1)
        limit = MAX_FILE_BYTES
    if not data or len(data) > limit:
        raise ValueError(f"Attachment must contain 1 to {limit} bytes")
    return _filename(filename), data


def _state_dir():
    return Path(
        os.environ.get(
            "MAC_MESSAGES_OUTBOX_DIR",
            str(Path.home() / "Library/Application Support/mac-messages-mcp/outbox"),
        )
    )


def _attachments_dir():
    return Path.home() / "Library/Messages/Attachments"


def _stage_attachment(request_id, filename, data):
    """Materialize where the native Messages agents already have read access.

    A successful Apple event does not grant imagent access to arbitrary files:
    staging in Application Support produced sandbox file-read-data denials,
    message error 25, and transfer_state 6. Both imagent and IMTransferAgent's
    shipped sandbox profiles allow ~/Library/Messages. Keep the durable ledger
    in Application Support, but place NEW transfer bytes in the attachment store.
    Never move or rewrite existing requests/files during an upgrade.
    """
    attachments = _attachments_dir()
    if attachments.is_symlink() or not attachments.is_dir():
        raise ValueError("The existing Messages attachment store is unavailable")
    root = attachments / "mac-messages-mcp"
    root.mkdir(mode=0o700, exist_ok=True)
    info = root.lstat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or info.st_mode & 0o077
    ):
        raise ValueError(
            "Attachment staging must be an owner-only directory, not a symlink"
        )
    # An existing request directory is never replaced, even if a previous
    # process crashed before committing its ledger claim.
    request_dir = root / request_id
    request_dir.mkdir(mode=0o700)
    staged = request_dir / filename
    fd = os.open(staged, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    return staged


def _store():
    root = _state_dir()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = root.lstat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or info.st_mode & 0o077
    ):
        raise ValueError("Outbox must be an owner-only directory, not a symlink")
    db_path = root / "requests.sqlite3"
    fd = os.open(db_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    os.close(fd)
    db = sqlite3.connect(db_path, timeout=10)
    db.row_factory = sqlite3.Row
    db.execute("""CREATE TABLE IF NOT EXISTS requests (
        request_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
        recipient TEXT NOT NULL, filename TEXT NOT NULL, sha256 TEXT NOT NULL,
        total_bytes INTEGER NOT NULL, baseline INTEGER NOT NULL,
        dispatch TEXT NOT NULL, created_at REAL NOT NULL, dispatch_error TEXT)""")
    return root, db


def _baseline():
    rows = query_messages_db("SELECT COALESCE(MAX(ROWID), 0) AS baseline FROM message")
    if not rows or "baseline" not in rows[0]:
        raise ValueError("Cannot read Messages database; no attachment was dispatched")
    return int(rows[0]["baseline"])


def _dispatch(recipient, path):
    try:
        result = subprocess.run(
            ["/usr/bin/osascript", "-", recipient, str(path)],
            input=SEND_SCRIPT,
            text=True,
            capture_output=True,
            timeout=15,
        )
        if result.returncode == 0 and result.stdout.strip() == "accepted":
            return "accepted", None
        # An error can occur after the side effect. Never retry or switch service.
        return (
            "uncertain",
            f"Native send exited {result.returncode}: {result.stderr[:1000]}",
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return "uncertain", str(exc)[:1000]


def _evidence(record):
    rows = query_messages_db(
        """
        SELECT DISTINCT m.ROWID AS message_id, m.guid AS message_guid,
            a.ROWID AS attachment_id, a.guid AS attachment_guid,
            m.error AS send_error, m.is_sent, m.is_delivered,
            m.date_delivered, m.date_read, m.service,
            a.transfer_state, a.total_bytes, a.transfer_name, a.filename AS file_path
        FROM message m
        JOIN message_attachment_join maj ON maj.message_id = m.ROWID
        JOIN attachment a ON a.ROWID = maj.attachment_id
        JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
        JOIN chat c ON c.ROWID = cmj.chat_id
        JOIN chat_handle_join chj ON chj.chat_id = c.ROWID
        JOIN handle h ON h.ROWID = chj.handle_id
        WHERE m.is_from_me = 1 AND m.ROWID > ? AND c.style = 45
          AND h.id = ? COLLATE NOCASE AND a.transfer_name = ?
          AND a.total_bytes = ?
        ORDER BY m.ROWID LIMIT 3
    """,
        (
            record["baseline"],
            record["recipient"],
            record["filename"],
            record["total_bytes"],
        ),
    )
    if rows and "error" in rows[0]:
        return {
            "status": "unverified",
            "evidence_error": "Messages database unavailable",
        }
    if len(rows) > 1:
        return {
            "status": "ambiguous",
            "evidence_error": "Multiple matching outgoing attachments; manual reconciliation required",
        }
    if not rows:
        return {"status": "unverified"}
    row = rows[0]
    # Verify local outgoing bytes when available, without claiming a remote
    # device downloaded them. Reject special files and bound the read.
    path = row.pop("file_path", None)
    try:
        fd = os.open(
            Path(path).expanduser(), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
        )
        with os.fdopen(fd, "rb") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise ValueError("not a regular attachment")
            data = stream.read(MAX_FILE_BYTES + 1)
        row["local_attachment_sha256"] = hashlib.sha256(data).hexdigest()
        row["local_bytes_match"] = (
            len(data) == record["total_bytes"]
            and row["local_attachment_sha256"] == record["sha256"]
        )
    except (OSError, TypeError, ValueError):
        row["local_bytes_match"] = None
    if row.get("send_error"):
        status = "failed"
    elif row.get("is_delivered") or row.get("date_delivered"):
        status = "delivered"
    elif row.get("is_sent"):
        status = "sent"
    else:
        status = "pending"
    return {"status": status, "evidence": row}


def _report(record):
    result = {
        key: record[key]
        for key in (
            "request_id",
            "recipient",
            "filename",
            "sha256",
            "total_bytes",
            "dispatch",
            "dispatch_error",
        )
    }
    result.update(_evidence(record))
    result["retry_safe"] = False
    result["receipt_scope"] = (
        "Delivery flags are Messages service evidence; they do not prove a particular device downloaded or a person opened the file."
    )
    return result


def attachment_send_status(request_id):
    request_id = _request_id(request_id)
    # A status lookup must not create state on a fresh installation.
    path = _state_dir() / "requests.sqlite3"
    if not path.exists():
        return {"status": "not_found", "request_id": request_id, "retry_safe": False}
    db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        record = db.execute(
            "SELECT * FROM requests WHERE request_id = ?", (request_id,)
        ).fetchone()
        if record is None:
            return {
                "status": "not_found",
                "request_id": request_id,
                "retry_safe": False,
            }
        return _report(dict(record))
    finally:
        db.close()


def send_attachment(
    recipient, request_id, filename, content_base64=None, file_path=None
):
    request_id, recipient = _request_id(request_id), _recipient(recipient)
    filename, data = _input(filename, content_base64, file_path)
    digest = hashlib.sha256(data).hexdigest()
    fingerprint = hashlib.sha256(
        json.dumps([recipient, filename, digest]).encode()
    ).hexdigest()
    sent_filename = f"{request_id}-{filename}"
    _, db = _store()
    try:
        # Serialize claims across processes. Persist the claim BEFORE the side effect.
        db.execute("BEGIN IMMEDIATE")
        previous = db.execute(
            "SELECT * FROM requests WHERE request_id = ?", (request_id,)
        ).fetchone()
        if previous:
            db.rollback()
            if previous["fingerprint"] != fingerprint:
                raise ValueError(
                    "request_id already belongs to different content or recipient"
                )
            result = _report(dict(previous))
            result["duplicate_suppressed"] = True
            return result
        baseline = _baseline()
        staged = _stage_attachment(request_id, sent_filename, data)
        record = dict(
            request_id=request_id,
            fingerprint=fingerprint,
            recipient=recipient,
            filename=sent_filename,
            sha256=digest,
            total_bytes=len(data),
            baseline=baseline,
            dispatch="uncertain",
            created_at=time.time(),
            dispatch_error=None,
        )
        db.execute(
            "INSERT INTO requests VALUES (:request_id,:fingerprint,:recipient,:filename,:sha256,:total_bytes,:baseline,:dispatch,:created_at,:dispatch_error)",
            record,
        )
        db.commit()
        record["dispatch"], record["dispatch_error"] = _dispatch(recipient, staged)
        db.execute(
            "UPDATE requests SET dispatch = ?, dispatch_error = ? WHERE request_id = ?",
            (record["dispatch"], record["dispatch_error"], request_id),
        )
        db.commit()
        # Retain the materialized file: Messages may ingest it asynchronously.
        deadline = time.monotonic() + 5
        while True:
            result = _report(record)
            if (
                result["status"] in {"sent", "delivered", "failed", "ambiguous"}
                or time.monotonic() >= deadline
            ):
                return result
            time.sleep(0.25)
    finally:
        db.close()
