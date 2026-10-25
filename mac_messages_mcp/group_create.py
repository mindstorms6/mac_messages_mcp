"""Create one new iMessage group with durable at-most-once dispatch.

The public operation validates every exact recipient before it persists a
request claim.  Once a claim exists, the same request UUID is reconciliation
only: it never creates another group or sends another message, even after a
timeout, exception, or process restart.
"""

import hashlib
import json
import os
import re
import sqlite3
import stat
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

from .messages import _check_imessage_availability, _row_text, query_messages_db
from .phone import canonical_handle, get_default_region, to_dialable_e164

MAX_RECIPIENTS = 32
MAX_MESSAGE_BYTES = 64_000
MAX_EVIDENCE_ROWS = 1_000

CREATE_GROUP_SCRIPT = """on run argv
    set messageFile to (POSIX file (item 1 of argv))
    set messageText to read messageFile as «class utf8»
    set targetAddresses to items 2 thru -1 of argv
    tell application "Messages"
        set targetAccount to first service whose service type = iMessage and enabled is true and connection status = connected
        set groupParticipants to {}
        repeat with targetAddress in targetAddresses
            set targetParticipant to participant (contents of targetAddress) of targetAccount
            if (handle of targetParticipant) is not (contents of targetAddress) then error "Recipient handle mismatch"
            set end of groupParticipants to targetParticipant
        end repeat
        -- Messages declares chat participants as a to-many element, not a
        -- property.  A make-properties record therefore cannot carry this
        -- list: "participants" compiles as singular pres, while raw ICpt is
        -- rejected as not being part of the chat properties record.  Pass the
        -- resolved list through make's separate contents (with data) channel.
        set newChat to make new chat at targetAccount with data groupParticipants
        send messageText to newChat
        return id of newChat
    end tell
end run
"""

_EMAIL = re.compile(
    r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+"
)


class RequestConflictError(ValueError):
    """The UUID already protects a different group creation/send."""


def _known_pre_creation_failure(record: dict) -> bool:
    """Recognize known participant coercions before ``make`` succeeded.

    The pre-fix script had exactly one create statement and passed the resolved
    participant list through invalid properties records.  The resulting -1700
    either names the singular ``participant`` target or rejects raw ``ICpt`` as
    a chat property.  Both are raised while evaluating ``make new chat``, before
    a chat object exists; the following ``send`` is therefore unreachable.
    """

    if record.get("dispatch") != "uncertain" or record.get("dispatch_chat_id"):
        return False
    error = record.get("dispatch_error")
    if not isinstance(error, str):
        return False
    normalized = error.replace("’", "'").replace("‘", "'")
    singular_participant = (
        "Can't make {participant id " in normalized
        and "} into type participant. (-1700)" in normalized
    )
    chat_properties_record = (
        "Can't make {«class ICpt»:{participant id " in normalized
        and "}} into type properties of chat. (-1700)" in normalized
    )
    return singular_participant or chat_properties_record


def _request_id(value: str) -> str:
    if not isinstance(value, str) or str(uuid.UUID(value)) != value:
        raise ValueError("request_id must be a canonical lowercase UUID")
    return value


def _recipients(values: list[str]) -> list[str]:
    if not isinstance(values, list) or not 2 <= len(values) <= MAX_RECIPIENTS:
        raise ValueError("recipients must contain 2 to 32 phone numbers or emails")
    region = get_default_region()
    normalized = []
    for value in values:
        if not isinstance(value, str) or value != value.strip() or not value:
            raise ValueError(
                "each recipient must be one exact phone number or plain email"
            )
        if len(value) <= 254 and _EMAIL.fullmatch(value):
            recipient = canonical_handle(value, region)
        else:
            recipient = to_dialable_e164(value, region)
        if recipient is None:
            raise ValueError(
                "each recipient must be a dialable phone number or plain email; names, contact:N selectors, groups, and short codes are unsupported"
            )
        normalized.append(recipient)
    if len(set(normalized)) != len(normalized):
        raise ValueError("recipients must be distinct after normalization")
    # Participant ordering does not change the group identity.  Stable sorting
    # lets a retry reorder the same addresses without conflicting with its UUID.
    return sorted(normalized)


def _message(value: str) -> tuple[str, str]:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("message must contain non-whitespace text")
    encoded = value.encode("utf-8")
    if len(encoded) > MAX_MESSAGE_BYTES:
        raise ValueError("message exceeds 64,000 UTF-8 bytes")
    return value, hashlib.sha256(encoded).hexdigest()


def _state_dir() -> Path:
    return Path(
        os.environ.get(
            "MAC_MESSAGES_GROUP_OUTBOX_DIR",
            str(
                Path.home()
                / "Library/Application Support/mac-messages-mcp/group-outbox"
            ),
        )
    )


def _store() -> sqlite3.Connection:
    root = _state_dir()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = root.lstat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or info.st_mode & 0o077
    ):
        raise ValueError("Group outbox must be an owner-only directory, not a symlink")
    path = root / "requests.sqlite3"
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    os.close(fd)
    db = sqlite3.connect(path, timeout=10)
    db.row_factory = sqlite3.Row
    db.execute("""CREATE TABLE IF NOT EXISTS requests (
        request_id TEXT PRIMARY KEY,
        fingerprint TEXT NOT NULL,
        recipients_json TEXT NOT NULL,
        message_sha256 TEXT NOT NULL,
        message_baseline INTEGER NOT NULL,
        chat_baseline INTEGER NOT NULL,
        dispatch TEXT NOT NULL,
        created_at REAL NOT NULL,
        dispatch_chat_id TEXT,
        dispatch_error TEXT)""")
    return db


def _baseline() -> tuple[int, int]:
    rows = query_messages_db("""SELECT
            COALESCE((SELECT MAX(ROWID) FROM message), 0) AS message_baseline,
            COALESCE((SELECT MAX(ROWID) FROM chat), 0) AS chat_baseline""")
    if not rows or "message_baseline" not in rows[0]:
        raise ValueError("Cannot read Messages database; no group was created")
    return int(rows[0]["message_baseline"]), int(rows[0]["chat_baseline"])


def _availability(recipients: list[str]) -> None:
    unavailable = [
        recipient
        for recipient in recipients
        if not _check_imessage_availability(recipient)
    ]
    if unavailable:
        joined = ", ".join(unavailable)
        raise ValueError(
            "No successful local iMessage evidence for: "
            f"{joined}. Verify each address with tool_check_imessage_availability first."
        )


def _dispatch(
    recipients: list[str], message: str
) -> tuple[str, str | None, str | None]:
    path = None
    try:
        fd, path = tempfile.mkstemp(prefix="mac-messages-group-", suffix=".txt")
        with os.fdopen(fd, "wb") as stream:
            stream.write(message.encode("utf-8"))
            stream.flush()
            os.fsync(stream.fileno())
        result = subprocess.run(
            ["/usr/bin/osascript", "-", path, *recipients],
            input=CREATE_GROUP_SCRIPT,
            text=True,
            capture_output=True,
            timeout=20,
        )
        chat_id = result.stdout.strip()
        if result.returncode == 0 and chat_id:
            return "accepted", chat_id, None
        # An error can occur after either creation or send.  It is never proof
        # that no side effect happened, so callers must only reconcile.
        detail = result.stderr.strip() or "Messages did not return a chat ID"
        return "uncertain", None, detail[:1000]
    except (OSError, subprocess.SubprocessError) as exc:
        return "uncertain", None, str(exc)[:1000]
    finally:
        if path:
            try:
                os.remove(path)
            except OSError:
                pass


def _participant_set(chat_rowid: int) -> tuple[set[str] | None, str | None]:
    rows = query_messages_db(
        """SELECT h.id
        FROM chat_handle_join chj
        JOIN handle h ON h.ROWID = chj.handle_id
        WHERE chj.chat_id = ?""",
        (chat_rowid,),
    )
    if rows and "error" in rows[0]:
        return None, "Messages database unavailable while reading participants"
    region = get_default_region()
    participants = set()
    for row in rows:
        normalized = canonical_handle(row.get("id"), region)
        if normalized is None:
            return None, "A group participant could not be normalized"
        participants.add(normalized)
    return participants, None


def _evidence(record: dict) -> dict:
    query = """SELECT
        m.ROWID AS message_rowid, m.guid AS message_id,
        m.error AS send_error, m.is_sent, m.is_delivered,
        m.date_delivered, m.date_read, m.service, m.text, m.attributedBody,
        c.ROWID AS chat_rowid, c.guid AS chat_id
      FROM message m
      JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
      JOIN chat c ON c.ROWID = cmj.chat_id
      WHERE m.is_from_me = 1 AND m.ROWID > ? AND c.ROWID > ? AND c.style = 43
      ORDER BY m.ROWID
      LIMIT ?"""
    rows = query_messages_db(
        query,
        (
            record["message_baseline"],
            record["chat_baseline"],
            MAX_EVIDENCE_ROWS + 1,
        ),
    )
    if rows and "message_rowid" not in rows[0]:
        return {
            "status": "unverified",
            "evidence_error": "Messages database unavailable",
        }
    if len(rows) > MAX_EVIDENCE_ROWS:
        return {
            "status": "ambiguous",
            "evidence_error": "Evidence window exceeded; manual reconciliation required",
        }

    recipients = set(json.loads(record["recipients_json"]))
    matches = []
    for row in rows:
        if (
            record.get("dispatch_chat_id")
            and row.get("chat_id") != record["dispatch_chat_id"]
        ):
            continue
        if row.get("service") not in ("iMessage", "iMessageLite"):
            continue
        body = _row_text(row)
        if (
            body is None
            or hashlib.sha256(body.encode("utf-8")).hexdigest()
            != record["message_sha256"]
        ):
            continue
        participants, error = _participant_set(int(row["chat_rowid"]))
        if error:
            return {"status": "unverified", "evidence_error": error}
        if participants == recipients:
            matches.append(row)

    if len(matches) > 1:
        return {
            "status": "ambiguous",
            "evidence_error": "Multiple matching new groups/messages; manual reconciliation required",
        }
    if not matches:
        return {"status": "unverified"}

    row = matches[0]
    if not row.get("chat_id") or not row.get("message_id"):
        return {
            "status": "unverified",
            "evidence_error": "Matching row lacks a stable chat or message ID",
        }
    row.pop("text", None)
    row.pop("attributedBody", None)
    if row.get("send_error"):
        status = "failed"
    elif row.get("is_delivered") or row.get("date_delivered"):
        status = "delivered"
    elif row.get("is_sent"):
        status = "sent"
    else:
        status = "pending"
    return {
        "status": status,
        "chat_id": row["chat_id"],
        "message_id": row["message_id"],
        "evidence": row,
    }


def _report(record: dict) -> dict:
    result = {
        key: record.get(key)
        for key in (
            "request_id",
            "message_sha256",
            "dispatch",
            "dispatch_chat_id",
            "dispatch_error",
        )
    }
    result["recipients"] = json.loads(record["recipients_json"])
    result.update(_evidence(record))
    result["retry_safe"] = False
    if result.get("status") == "unverified" and _known_pre_creation_failure(record):
        result.update(
            {
                "status": "failed_before_creation",
                "side_effects": "no_chat_or_message_created",
                "reconciliation_basis": (
                    "known pre-creation AppleScript participant-list coercion (-1700)"
                ),
                "replacement_request_safe": True,
                "replacement_requires_authorization": True,
                "next_step": (
                    "This request_id remains permanently closed. After explicit "
                    "authorization for a replacement operation, call "
                    "tool_create_group_and_send once with the same recipients and "
                    "message and a new canonical UUID."
                ),
            }
        )
    else:
        result["replacement_request_safe"] = False
        result["next_step"] = (
            "Reuse this request_id or query tool_get_group_creation_status; "
            "never retry with a new UUID after uncertainty."
        )
    return result


def group_creation_status(request_id: str) -> dict:
    request_id = _request_id(request_id)
    record = _read_record(request_id)
    if record is None:
        return {"status": "not_found", "request_id": request_id, "retry_safe": False}
    return _report(record)


def _read_record(request_id: str) -> dict | None:
    path = _state_dir() / "requests.sqlite3"
    if not path.exists():
        return None
    db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        record = db.execute(
            "SELECT * FROM requests WHERE request_id = ?", (request_id,)
        ).fetchone()
        return dict(record) if record is not None else None
    finally:
        db.close()


def create_group_and_send(recipients: list[str], request_id: str, message: str) -> dict:
    request_id = _request_id(request_id)
    recipients = _recipients(recipients)
    message, message_sha256 = _message(message)
    fingerprint = hashlib.sha256(
        json.dumps([recipients, message_sha256], separators=(",", ":")).encode()
    ).hexdigest()

    # A retry must remain reconciliation-only even if availability has changed
    # since the original attempt.  Read the durable claim before revalidating.
    previous = _read_record(request_id)
    if previous is not None:
        if previous["fingerprint"] != fingerprint:
            raise RequestConflictError(
                "request_id already belongs to different recipients or message"
            )
        result = _report(previous)
        result["duplicate_suppressed"] = True
        return result

    _availability(recipients)
    db = _store()
    try:
        # Serialize callers and commit an at-most-once claim before AppleScript.
        db.execute("BEGIN IMMEDIATE")
        previous = db.execute(
            "SELECT * FROM requests WHERE request_id = ?", (request_id,)
        ).fetchone()
        if previous:
            db.rollback()
            if previous["fingerprint"] != fingerprint:
                raise RequestConflictError(
                    "request_id already belongs to different recipients or message"
                )
            result = _report(dict(previous))
            result["duplicate_suppressed"] = True
            return result

        message_baseline, chat_baseline = _baseline()
        record = {
            "request_id": request_id,
            "fingerprint": fingerprint,
            "recipients_json": json.dumps(recipients, separators=(",", ":")),
            "message_sha256": message_sha256,
            "message_baseline": message_baseline,
            "chat_baseline": chat_baseline,
            "dispatch": "uncertain",
            "created_at": time.time(),
            "dispatch_chat_id": None,
            "dispatch_error": None,
        }
        db.execute(
            """INSERT INTO requests VALUES (
                :request_id, :fingerprint, :recipients_json, :message_sha256,
                :message_baseline, :chat_baseline, :dispatch, :created_at,
                :dispatch_chat_id, :dispatch_error)""",
            record,
        )
        db.commit()

        (
            record["dispatch"],
            record["dispatch_chat_id"],
            record["dispatch_error"],
        ) = _dispatch(recipients, message)
        db.execute(
            """UPDATE requests
               SET dispatch = ?, dispatch_chat_id = ?, dispatch_error = ?
               WHERE request_id = ?""",
            (
                record["dispatch"],
                record["dispatch_chat_id"],
                record["dispatch_error"],
                request_id,
            ),
        )
        db.commit()

        deadline = time.monotonic() + 5
        while True:
            result = _report(record)
            if (
                result["status"]
                in {
                    "sent",
                    "delivered",
                    "failed",
                    "failed_before_creation",
                    "ambiguous",
                }
                or time.monotonic() >= deadline
            ):
                return result
            time.sleep(0.25)
    finally:
        db.close()
