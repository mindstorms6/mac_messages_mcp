"""Mark a conversation read through Messages, never by writing to chat.db."""

import sqlite3
import subprocess
import threading
import time
from contextlib import closing
from urllib.parse import urlencode

from .messages import _connect_sqlite_readonly, get_messages_db_path

_MARK_READ_LOCK = threading.Lock()
_VERIFY_ATTEMPTS = 20
_VERIFY_INTERVAL = 0.25


def _unread_count(chat_rowid: int, through_rowid: int) -> int:
    with closing(_connect_sqlite_readonly(get_messages_db_path())) as connection:
        return int(
            connection.execute(
                "SELECT COUNT(*) FROM message m "
                "WHERE m.is_from_me = 0 AND m.is_read = 0 AND m.ROWID <= ? "
                "AND EXISTS (SELECT 1 FROM chat_message_join cmj "
                "WHERE cmj.message_id = m.ROWID AND cmj.chat_id = ?)",
                (through_rowid, chat_rowid),
            ).fetchone()[0]
        )


def mark_read(chat_id: str) -> str:
    """Open an exact existing chat and verify its incoming read state locally.

    Messages owns the mutation and any read receipts/iCloud synchronization.
    The sms: message-guid deep link is an undocumented macOS surface, so a
    successful launch alone is never treated as proof that unread messages
    changed. No fuzzy contact matching or database writes are performed.
    """
    if not isinstance(chat_id, str) or not chat_id.strip():
        return "Error: chat_id must be a non-empty exact chat GUID or identifier."
    chat_id = chat_id.strip()

    with _MARK_READ_LOCK:
        try:
            with closing(
                _connect_sqlite_readonly(get_messages_db_path())
            ) as connection:
                # A GUID takes precedence; a handle may match multiple services.
                chats = connection.execute(
                    "SELECT ROWID, guid FROM chat WHERE guid = ?", (chat_id,)
                ).fetchall()
                if not chats:
                    chats = connection.execute(
                        "SELECT ROWID, guid FROM chat "
                        "WHERE chat_identifier = ? OR room_name = ?",
                        (chat_id, chat_id),
                    ).fetchall()
                if not chats:
                    return "Error: No conversation matches that exact chat ID."
                if len(chats) != 1:
                    return (
                        "Error: Ambiguous chat identifier. Retry with one exact "
                        "chat GUID, including its service prefix: "
                        + ", ".join(str(chat[1]) for chat in chats)
                    )
                chat_rowid, chat_guid = chats[0]
                latest = connection.execute(
                    "SELECT m.guid FROM message m JOIN chat_message_join cmj "
                    "ON cmj.message_id = m.ROWID WHERE cmj.chat_id = ? "
                    "AND m.guid IS NOT NULL AND m.guid != '' "
                    "ORDER BY m.date DESC, m.ROWID DESC LIMIT 1",
                    (chat_rowid,),
                ).fetchone()
                if latest is None:
                    return (
                        "Error: Conversation has no message to open; nothing changed."
                    )
                through_rowid = connection.execute(
                    "SELECT COALESCE(MAX(message_id), 0) FROM chat_message_join "
                    "WHERE chat_id = ?",
                    (chat_rowid,),
                ).fetchone()[0]

            before = _unread_count(chat_rowid, through_rowid)
            # Pass an encoded URL as a single argv value, never a shell command.
            url = "sms://open?" + urlencode({"message-guid": latest[0]})
            try:
                opened = subprocess.run(
                    ["/usr/bin/open", "-a", "/System/Applications/Messages.app", url],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
            except (OSError, subprocess.TimeoutExpired) as error:
                return (
                    f"Error: Could not open Messages; read state unconfirmed: {error}"
                )
            if opened.returncode != 0:
                return (
                    "Error: Messages could not open the conversation; read state "
                    f"unconfirmed: {opened.stderr.strip()}"
                )

            if before == 0:
                return (
                    f"Opened conversation {chat_guid}. It already had no unread "
                    "incoming messages in the local database. A manually set unread "
                    "badge and cross-device synchronization are not independently verified."
                )
            for attempt in range(_VERIFY_ATTEMPTS):
                remaining = _unread_count(chat_rowid, through_rowid)
                if remaining == 0:
                    return (
                        f"Marked conversation {chat_guid} read: verified {before} "
                        "previously unread incoming message(s) are now read locally. "
                        "Messages is open on this conversation. Read receipts follow "
                        "your Messages settings; cross-device sync is not verified."
                    )
                if attempt + 1 < _VERIFY_ATTEMPTS:
                    time.sleep(_VERIFY_INTERVAL)
            return (
                f"Error: Messages opened, but marking {chat_guid} read was not "
                f"confirmed: {remaining} of {before} incoming message(s) remain unread. "
                "No database writes or fallback UI clicks were attempted. Ensure "
                "Messages is signed in in an unlocked GUI session. This macOS "
                "version may not support the conversation deep link."
            )
        except sqlite3.Error as error:
            return (
                "Error: Cannot resolve or verify the conversation in chat.db; "
                "read state unconfirmed. The host application needs Full Disk "
                f"Access. Database error: {error}"
            )
