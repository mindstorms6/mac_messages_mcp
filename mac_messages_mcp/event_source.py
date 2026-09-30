"""Read-only observation of newly inserted Messages rows, including Tapbacks.

This is local observation, not proof of cross-device delivery/completeness.
No message body, attachment bytes, local filename, or contact name is emitted.
"""

import hashlib
import sqlite3
from contextlib import closing
from pathlib import Path

from .messages import _connect_sqlite_readonly, _from_apple_ns
from .untrusted import sanitize_untrusted_structure

REACTIONS = {
    0: "love",
    1: "like",
    2: "dislike",
    3: "laugh",
    4: "emphasize",
    5: "question",
    6: "custom_emoji",
    7: "sticker",
}


class MessageSource:
    def __init__(self, path: str):
        self.path = str(Path(path).expanduser().resolve())

    def identity(self) -> str:
        stat = Path(self.path).stat()
        return f"{stat.st_dev}:{stat.st_ino}"

    def maximum(self) -> int:
        with closing(_connect_sqlite_readonly(self.path)) as db:
            return db.execute("SELECT COALESCE(MAX(ROWID), 0) FROM message").fetchone()[
                0
            ]

    def rows(self, after: int, pending: list[int], limit: int = 250) -> list[dict]:
        with closing(_connect_sqlite_readonly(self.path)) as db:
            db.row_factory = sqlite3.Row
            db.execute("BEGIN")
            columns = {r[1] for r in db.execute("PRAGMA table_info(message)")}
            required = {
                "guid",
                "date",
                "handle_id",
                "is_from_me",
                "associated_message_type",
                "associated_message_guid",
            }
            if not required <= columns:
                raise RuntimeError("Messages schema does not support event observation")
            names = sorted(
                required
                | (
                    {
                        "associated_message_emoji",
                        "cache_has_attachments",
                        "item_type",
                        "is_system_message",
                    }
                    & columns
                )
            )
            fields = ",".join('m."' + name + '"' for name in names)
            # Pending rows are retried separately so delayed chat joins are not
            # permanently lost when later rows have already been observed.
            selected = list(
                db.execute(
                    f"SELECT m.ROWID AS row_id,{fields} FROM message m WHERE m.ROWID>? ORDER BY m.ROWID LIMIT ?",
                    (after, limit),
                )
            )
            for offset in range(0, len(pending), 500):
                batch = pending[offset : offset + 500]
                marks = ",".join("?" for _ in batch)
                selected += list(
                    db.execute(
                        f"SELECT m.ROWID AS row_id,{fields} FROM message m WHERE m.ROWID IN ({marks})",
                        batch,
                    )
                )
            result = []
            for row in selected:
                data = dict(row)
                chats = list(
                    db.execute(
                        "SELECT DISTINCT c.ROWID,c.guid FROM chat c JOIN chat_message_join j ON j.chat_id=c.ROWID WHERE j.message_id=? ORDER BY c.ROWID LIMIT 129",
                        (row["row_id"],),
                    )
                )
                sender = db.execute(
                    "SELECT id FROM handle WHERE ROWID=?", (row["handle_id"],)
                ).fetchone()
                data["chats"] = [{"id": c[0], "guid": c[1]} for c in chats]
                data["sender"] = sender[0] if sender else None
                result.append(data)
            return result


def shape_event(row: dict, source_id: str) -> dict | None:
    kind = row["associated_message_type"] or 0
    # Apple uses 1000 for older sticker Tapbacks and 2007/3007 for newer
    # sticker add/remove rows. See imessage-exporter's Message::tapback.
    reaction = kind == 1000 or 2000 <= kind < 4000
    if not reaction and (
        kind != 0 or row.get("item_type") or row.get("is_system_message")
    ):
        return None
    if not row["guid"] or len(row["chats"]) > 128:
        raise ValueError("Invalid message identity or excessive chat associations")
    name = (
        "reaction.removed"
        if kind >= 3000
        else "reaction.added" if reaction else "message.created"
    )
    data = {
        "message_id": row["row_id"],
        "message_guid": row["guid"],
        "chats": row["chats"],
        "sender": row["sender"],
        "direction": "outgoing" if row["is_from_me"] else "incoming",
        "has_attachments": bool(row.get("cache_has_attachments")),
        "reaction": None,
    }
    if reaction:
        data["reaction"] = {
            "action": "remove" if kind >= 3000 else "add",
            "kind": (
                "sticker" if kind == 1000 else REACTIONS.get(kind % 1000, "unknown")
            ),
            "apple_type": kind,
            "target_message_guid": row["associated_message_guid"],
            "emoji": row.get("associated_message_emoji"),
        }
    event_id = (
        "msg_"
        + hashlib.sha256(f"{source_id}:{row['guid']}:{name}".encode()).hexdigest()
    )
    return {
        "eventId": event_id,
        "name": name,
        "timestamp": _from_apple_ns(int(row["date"])).isoformat(),
        "data": {"untrusted-mcp-output": sanitize_untrusted_structure(data)},
        "cursor": None,
    }
