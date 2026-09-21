"""Read-only latest activity across all locally known chats for exact addresses."""

import json
import re
import sqlite3
from contextlib import closing
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .messages import (
    _connect_sqlite_readonly,
    _from_apple_ns,
    extract_body_from_attributed,
    get_messages_db_path,
)
from .phone import canonical_handle, get_default_region
from .untrusted import sanitize_untrusted_structure

_TABLES = ("message", "chat", "handle", "chat_handle_join", "chat_message_join")
_REACTIONS = {
    2000: "love",
    2001: "like",
    2002: "dislike",
    2003: "laugh",
    2004: "emphasize",
    2005: "question",
}


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    # Table names only come from fixed constants in this module.
    return {row[1] for row in connection.execute(f'PRAGMA table_info("{table}")')}


def _field(columns: set[str], alias: str, name: str) -> str:
    return f'{alias}."{name}"' if name in columns else "NULL"


def _text(value: Any, limit: int = 512) -> Any:
    return None if value is None else str(value)[:limit]


def get_latest_contact_activity(
    addresses: list[str], limit: int = 1, timezone: str = "America/Los_Angeles"
) -> dict[str, Any]:
    """Return data, not instructions; the MCP wrapper applies output protection."""
    if not isinstance(addresses, list) or not 1 <= len(addresses) <= 32:
        return {
            "status": "invalid_input",
            "error": "Supply 1 to 32 exact phone/email addresses, not contact names.",
        }
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 20:
        return {"status": "invalid_input", "error": "limit must be between 1 and 20."}
    try:
        zone = ZoneInfo(timezone)
    except (ZoneInfoNotFoundError, ValueError, TypeError):
        return {
            "status": "invalid_input",
            "error": "timezone must be an IANA timezone such as America/Los_Angeles.",
        }
    region = get_default_region()
    normalized = set()
    for address in addresses:
        if not isinstance(address, str) or len(address) > 320:
            return {
                "status": "invalid_input",
                "error": "Addresses must be strings of at most 320 characters.",
            }
        address = address.strip()
        if not (
            re.fullmatch(r"[^@\s<>]+@[^@\s<>]+", address)
            or re.fullmatch(r"\+?[0-9 ().-]+", address)
        ):
            return {
                "status": "invalid_input",
                "error": "Use exact phone/email addresses. Contact names and fuzzy/contact:N selections are not accepted; resolve ambiguity before calling.",
            }
        canonical = canonical_handle(address, region)
        if canonical is None:
            return {
                "status": "invalid_input",
                "error": "A phone address could not be normalized. Prefer E.164 with country code.",
            }
        normalized.add(canonical)
    result: dict[str, Any] = {
        "status": "not_found",
        "addresses": sorted(normalized),
        "normalization_region": region,
        "timezone": timezone,
        "latest_activity": None,
        "activities": [],
        "coverage": {
            "matched_handle_count": 0,
            "matched_chat_count": 0,
            "unmatched_addresses": [],
            "matched_chats_fully_searched": False,
            "activities_truncated": False,
            "output_truncated": False,
            "limit": limit,
            "scope": "Current local chat_handle_join membership only; historical membership and messages not downloaded to this Mac cannot be proven. Supply every known phone/email alias; aliases are not inferred from Contacts.",
            "eligibility": "Positive-date message/chat pairs; exclude system messages, nonzero item_type and retracted messages where those columns exist. Include all senders, outgoing messages, reactions and attachments. Not proof of delivery or a conversation exclusively with this contact.",
            "missing_optional_columns": [],
        },
    }
    coverage = result["coverage"]
    try:
        with closing(_connect_sqlite_readonly(get_messages_db_path())) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("BEGIN")  # Consistent read snapshot, never a write.
            schema = {table: _columns(connection, table) for table in _TABLES}
            required = {
                "message": {"date", "handle_id", "is_from_me"},
                "chat": {"chat_identifier"},
                "handle": {"id"},
                "chat_handle_join": {"chat_id", "handle_id"},
                "chat_message_join": {"chat_id", "message_id"},
            }
            missing = [
                f"{table}.{column}"
                for table, columns in required.items()
                for column in sorted(columns - schema[table])
            ]
            if missing:
                return {
                    **result,
                    "status": "unsupported_schema",
                    "error": "Missing required columns",
                    "missing_columns": missing,
                }
            msg, chat = schema["message"], schema["chat"]
            optional = (
                "guid",
                "text",
                "attributedBody",
                "cache_has_attachments",
                "associated_message_type",
                "associated_message_guid",
                "associated_message_emoji",
                "item_type",
                "is_system_message",
                "date_retracted",
            )
            coverage["missing_optional_columns"] = [
                f"message.{name}" for name in optional if name not in msg
            ]
            matches = [
                row
                for row in connection.execute("SELECT ROWID, id FROM handle")
                if canonical_handle(row["id"], region) in normalized
            ]
            found = {canonical_handle(row["id"], region) for row in matches}
            coverage["unmatched_addresses"] = sorted(normalized - found)
            coverage["matched_handle_count"] = len(matches)
            handle_ids = {row["ROWID"] for row in matches}
            connection.create_function(
                "is_contact_handle",
                1,
                lambda value: int(value in handle_ids),
                deterministic=True,
            )
            cte = "WITH matched_chats AS (SELECT DISTINCT chat_id FROM chat_handle_join WHERE is_contact_handle(handle_id)) "
            coverage["matched_chat_count"] = connection.execute(
                cte
                + "SELECT COUNT(*) FROM chat WHERE ROWID IN (SELECT chat_id FROM matched_chats)"
            ).fetchone()[0]
            coverage["matched_chats_fully_searched"] = True
            if not coverage["matched_chat_count"]:
                return result
            filters = ["m.date > 0"]
            for name in ("item_type", "is_system_message", "date_retracted"):
                if name in msg:
                    filters.append(f"COALESCE(m.{name}, 0) = 0")
            selects = [
                f'{_field(msg, "m", name)} AS "{name}"'
                for name in optional
                if name not in ("item_type", "is_system_message", "date_retracted")
            ]
            selects += [
                f'{_field(chat, "c", name)} AS "chat_{name}"'
                for name in ("guid", "display_name", "room_name", "style")
            ]
            query = (
                cte
                + "SELECT m.ROWID AS message_id, c.ROWID AS chat_id, c.chat_identifier, m.date, m.is_from_me, h.id AS sender, "
                + ", ".join(selects)
                + ", CASE WHEN m.date > 10000000000 THEN m.date ELSE m.date * 1000000000 END AS date_ns FROM message m JOIN chat_message_join j ON j.message_id=m.ROWID JOIN chat c ON c.ROWID=j.chat_id LEFT JOIN handle h ON h.ROWID=m.handle_id WHERE c.ROWID IN (SELECT chat_id FROM matched_chats) AND "
                + " AND ".join(filters)
                + " ORDER BY date_ns DESC, m.ROWID DESC, c.ROWID DESC LIMIT ?"
            )
            rows = connection.execute(query, (limit + 1,)).fetchall()
            coverage["activities_truncated"] = len(rows) > limit
            activities = [_activity(connection, row, msg, zone) for row in rows[:limit]]
            if not activities:
                result["status"] = "no_activity"
                return result
            result["status"] = "ok"
            coverage["ordering"] = (
                "Apple timestamp normalized to nanoseconds DESC, message ROWID DESC, chat ROWID DESC; limit counts message-chat pairs."
            )
            result["activities"], result["latest_activity"] = activities, activities[0]
            while (
                len(json.dumps(sanitize_untrusted_structure(result), ensure_ascii=True))
                > 80000
                and len(activities) > 1
            ):
                activities.pop()
                coverage["activities_truncated"] = coverage["output_truncated"] = True
            if (
                len(json.dumps(sanitize_untrusted_structure(result), ensure_ascii=True))
                > 80000
            ):
                # Even one unusually large record must never silently break the
                # JSON text fallback at the shared 100k-character boundary.
                return {
                    "status": "output_limit",
                    "error": "Detailed metadata exceeded the response budget. No partial JSON or fabricated summary is returned.",
                    "latest_activity": {
                        "message_id": activities[0]["message_id"],
                        "timestamp": activities[0]["timestamp"],
                        "chat_id": activities[0]["chat"]["id"],
                    },
                    "coverage": {
                        "matched_chat_count": coverage["matched_chat_count"],
                        "matched_chats_fully_searched": True,
                        "output_truncated": True,
                    },
                }
            return result
    except sqlite3.Error:
        coverage["matched_chats_fully_searched"] = False
        return {
            **result,
            "status": "database_error",
            "error": "Cannot read Messages database. Check Full Disk Access, database availability and schema; no messages were changed.",
        }


def _activity(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
    message_columns: set[str],
    zone: ZoneInfo,
) -> dict[str, Any]:
    participants = connection.execute(
        "SELECT DISTINCT h.id FROM chat_handle_join j JOIN handle h ON h.ROWID=j.handle_id WHERE j.chat_id=? ORDER BY h.id",
        (row["chat_id"],),
    ).fetchall()
    count = len(participants)
    group = (
        row["chat_style"] == 43
        or count > 1
        or bool(row["chat_room_name"])
        or str(row["chat_identifier"]).startswith("chat")
    )
    body = row["text"]
    body_status = "plain_text" if body is not None else "absent"
    if body is None and row["attributedBody"]:
        body = extract_body_from_attributed(row["attributedBody"])
        body_status = "decoded_attributed_body" if body is not None else "decode_failed"
    elif body is None and "attributedBody" not in message_columns:
        body_status = "unavailable_column"
    try:
        timestamp = _from_apple_ns(int(row["date"])).astimezone(zone).isoformat()
    except (OverflowError, ValueError, OSError):
        timestamp = None
    associated_type = row["associated_message_type"]
    reaction_kind = _REACTIONS.get(associated_type)
    action = "add" if reaction_kind else None
    if associated_type in range(3000, 3006):
        reaction_kind, action = _REACTIONS[associated_type - 1000], "remove"
    elif associated_type in (2006, 3006):
        reaction_kind, action = "custom_emoji", (
            "add" if associated_type == 2006 else "remove"
        )
    return {
        "message_id": row["message_id"],
        "message_guid": _text(row["guid"]),
        "timestamp": timestamp,
        "timestamp_status": "ok" if timestamp else "invalid",
        "apple_date_raw": row["date"],
        "chat": {
            "id": row["chat_id"],
            "guid": _text(row["chat_guid"]),
            "identifier": _text(row["chat_identifier"]),
            "name": _text(row["chat_display_name"]),
            "type": "group" if group else "direct",
            "participants": [_text(p[0], 320) for p in participants[:50]],
            "participant_count": count,
            "participants_truncated": count > 50,
            "participants_scope": "Local chat_handle_join handles; self is generally omitted.",
        },
        "sender": {
            "is_from_me": bool(row["is_from_me"]),
            "address": None if row["is_from_me"] else _text(row["sender"], 320),
            "kind": (
                "self"
                if row["is_from_me"]
                else "participant" if row["sender"] else "unknown"
            ),
        },
        "text": _text(body, 4000),
        "text_status": body_status,
        "text_truncated": body is not None and len(body) > 4000,
        "has_attachments": (
            None
            if row["cache_has_attachments"] is None
            else bool(row["cache_has_attachments"])
        ),
        "attachments": _attachments(connection, row["message_id"]),
        "reaction": {
            "associated_message_type": associated_type,
            "target_message_guid": _text(row["associated_message_guid"]),
            "emoji": _text(row["associated_message_emoji"]),
            "kind": reaction_kind,
            "action": action,
        },
    }


def _attachments(connection: sqlite3.Connection, message_id: int) -> dict[str, Any]:
    columns = _columns(connection, "attachment")
    joins = _columns(connection, "message_attachment_join")
    if not columns or not {"attachment_id", "message_id"} <= joins:
        return {"status": "unavailable_schema", "items": [], "truncated": False}
    fields = ("transfer_name", "mime_type", "uti", "total_bytes", "is_sticker")
    selects = ", ".join(
        f'{_field(columns, "a", field)} AS "{field}"' for field in fields
    )
    rows = connection.execute(
        "SELECT a.ROWID AS id, "
        + selects
        + " FROM attachment a JOIN message_attachment_join j ON j.attachment_id=a.ROWID WHERE j.message_id=? ORDER BY a.ROWID LIMIT 21",
        (message_id,),
    ).fetchall()
    return {
        "status": "ok",
        "items": [
            {
                "id": r["id"],
                "name": _text(r["transfer_name"], 256),
                "mime_type": _text(r["mime_type"], 256),
                "uti": _text(r["uti"], 256),
                "size_bytes_reported": r["total_bytes"],
                "is_sticker": (
                    None if r["is_sticker"] is None else bool(r["is_sticker"])
                ),
            }
            for r in rows[:20]
        ],
        "truncated": len(rows) > 20,
        "missing_columns": sorted(set(fields) - columns),
    }
