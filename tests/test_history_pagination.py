"""Exercise real SQLite queries, including history beyond the old 10k cap."""

import asyncio
import re
import sqlite3
from unittest.mock import patch

import pytest

from mac_messages_mcp.messages import fuzzy_search_messages, get_recent_messages
from mac_messages_mcp.pagination import parse_date
from mac_messages_mcp.server import mcp


@pytest.fixture
def history(tmp_path):
    path = tmp_path / "history.db"
    with sqlite3.connect(path) as conn:
        conn.executescript("""
            CREATE TABLE message (date INTEGER, text TEXT, attributedBody BLOB,
                is_from_me INTEGER, handle_id INTEGER, cache_roomnames TEXT);
            CREATE TABLE handle (ROWID INTEGER PRIMARY KEY, id TEXT);
            CREATE TABLE chat (ROWID INTEGER PRIMARY KEY, guid TEXT, chat_identifier TEXT, room_name TEXT, display_name TEXT);
            CREATE TABLE chat_message_join (chat_id INTEGER, message_id INTEGER);
            INSERT INTO handle (id) VALUES ('a@example.com'), ('b@example.com');
            INSERT INTO chat (guid,chat_identifier,room_name,display_name) VALUES ('iMessage;+;chat1', 'chat1', 'chat1', 'Test');
        """)
        date = parse_date("2025-01-01")
        conn.executemany(
            "INSERT INTO message VALUES (?, ?, NULL, 0, 1, NULL)",
            [(date, f"message-{i}") for i in range(10005)],
        )
        conn.execute("UPDATE message SET text='rejected' WHERE ROWID=1")
        conn.execute("INSERT INTO chat_message_join VALUES (1, 1)")
    with (
        patch("mac_messages_mcp.messages.get_messages_db_path", return_value=str(path)),
        patch("mac_messages_mcp.messages.get_chat_mapping", return_value={}),
        patch("mac_messages_mcp.messages.get_contact_name", return_value="Contact"),
        patch(
            "mac_messages_mcp.messages._attachments_for_message_ids", return_value={}
        ),
    ):
        yield path


def next_cursor(result):
    token = re.search(r"next_cursor=([A-Za-z0-9_=-]+)", result).group(1)
    return None if token == "null" else token


def test_empty_match_pages_reach_oldest_after_10000(history):
    cursor = None
    scanned = 0
    for _ in range(102):
        result = fuzzy_search_messages(
            "rejected", hours=0, threshold=0.99, cursor=cursor
        )
        scanned += int(re.search(r"Scanned (\d+)", result).group(1))
        cursor = next_cursor(result)
        if cursor is None:
            break
        assert "Score:" not in result
    assert scanned == 10005
    assert "Score: 1.00" in result
    assert "has_more=false" in result


def test_recent_tied_dates_no_duplicates_and_newer_insert(history):
    first = get_recent_messages(hours=0, limit=2)
    assert "message-10004" in first and "message-10003" in first
    with sqlite3.connect(history) as c:
        c.execute(
            "INSERT INTO message VALUES (?, 'new arrival', NULL, 0, 1, NULL)",
            (parse_date("2025-01-02"),),
        )
    second = get_recent_messages(hours=0, limit=2, cursor=next_cursor(first))
    assert "message-10002" in second and "message-10001" in second
    assert "message-10004" not in second and "new arrival" not in second


def test_dates_chat_and_contact_filters(history):
    result = fuzzy_search_messages(
        "rejected", hours=0, chat_id="chat1", after="2025-01-01", before="2025-01-02"
    )
    assert "Score: 1.00" in result and "Scanned 1 messages" in result
    result = fuzzy_search_messages("rejected", hours=0, before="2025-01-01")
    assert "Scanned 0 messages" in result and "has_more=false" in result
    result = fuzzy_search_messages("rejected", hours=0, contact="b@example.com")
    assert "Scanned 0 messages" in result
    result = get_recent_messages(hours=0, contact="b@example.com")
    assert "No messages found" in result


def test_invalid_and_mismatched_cursors_are_rejected(history):
    first = fuzzy_search_messages("rejected", hours=0)
    assert "Error:" in fuzzy_search_messages(
        "different", hours=0, cursor=next_cursor(first)
    )
    for cursor in ["invalid", "x" * 3000, "W10="]:
        assert "Error:" in get_recent_messages(hours=0, cursor=cursor)
    assert "Error:" in get_recent_messages(hours=0, limit=0)
    assert "Error:" in get_recent_messages(hours=0, before="not-a-date")


def test_seconds_timestamps_and_true_fuzzy_matching(history):
    with sqlite3.connect(history) as c:
        c.execute("DELETE FROM message")
        c.execute(
            "INSERT INTO message VALUES (?, 'rejectd', NULL, 0, 1, NULL)",
            (parse_date("2025-01-01") // 10**9,),
        )
    result = fuzzy_search_messages("rejected", hours=0, threshold=0.6)
    assert "Score:" in result and "rejectd" in result
    assert "2024-12-31" in result or "2025-01-01" in result


def test_mcp_schemas_expose_continuation():
    tools = {tool.name: tool for tool in asyncio.run(mcp.list_tools())}
    for name in ("tool_get_recent_messages", "tool_fuzzy_search_messages"):
        assert {"cursor", "before", "after", "limit", "contact", "chat_id"} <= set(
            tools[name].inputSchema["properties"]
        )
