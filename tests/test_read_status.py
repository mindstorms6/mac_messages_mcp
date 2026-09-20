"""Read-state verification and safe targeting without controlling real Messages."""

import asyncio
import sqlite3
import subprocess
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import pytest

from mac_messages_mcp.read_status import mark_read
from mac_messages_mcp.server import mcp, tool_mark_read


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "chat.db"
    with sqlite3.connect(path) as connection:
        connection.executescript(
            "CREATE TABLE chat (guid TEXT, chat_identifier TEXT, room_name TEXT);"
            "CREATE TABLE message (guid TEXT, date INTEGER, is_from_me INTEGER, "
            "is_read INTEGER);"
            "CREATE TABLE chat_message_join (chat_id INTEGER, message_id INTEGER);"
            "INSERT INTO chat VALUES ('iMessage;-;+15551234567', '+15551234567', NULL);"
            "INSERT INTO chat VALUES ('iMessage;+;chat123', 'chat123', 'chat123');"
            "INSERT INTO message VALUES ('message-1', 1, 0, 0);"
            "INSERT INTO message VALUES ('message-2', 2, 1, 0);"
            "INSERT INTO message VALUES ('message-3', 3, 0, 0);"
            "INSERT INTO chat_message_join VALUES (1, 1), (1, 2), (2, 3);"
        )
    with patch(
        "mac_messages_mcp.read_status.get_messages_db_path", return_value=str(path)
    ):
        yield path


@pytest.fixture
def launch():
    with patch("mac_messages_mcp.read_status.subprocess.run") as run:
        run.return_value = subprocess.CompletedProcess([], 0, "", "")
        with patch("mac_messages_mcp.read_status._VERIFY_ATTEMPTS", 2):
            with patch("mac_messages_mcp.read_status.time.sleep"):
                yield run


def test_verified_read_only_affects_target(db, launch):
    def messages_app(*args, **kwargs):
        with sqlite3.connect(db) as connection:
            connection.execute("UPDATE message SET is_read = 1 WHERE ROWID = 1")
            # A new arrival is outside the verification snapshot.
            connection.execute("INSERT INTO message VALUES ('later', 4, 0, 0)")
            connection.execute("INSERT INTO chat_message_join VALUES (1, 4)")
        return subprocess.CompletedProcess([], 0, "", "")

    launch.side_effect = messages_app
    result = mark_read("+15551234567")
    assert "verified 1 previously unread" in result
    assert "cross-device sync is not verified" in result
    argv = launch.call_args.args[0]
    assert argv[:3] == ["/usr/bin/open", "-a", "/System/Applications/Messages.app"]
    assert parse_qs(urlsplit(argv[3]).query) == {"message-guid": ["message-2"]}
    with sqlite3.connect(db) as connection:
        assert connection.execute(
            "SELECT is_read FROM message WHERE ROWID=3"
        ).fetchone() == (0,)


def test_launch_alone_is_not_success_and_never_writes_db(db, launch):
    assert "not confirmed: 1 of 1" in mark_read("chat123")
    with sqlite3.connect(db) as connection:
        assert connection.execute("SELECT SUM(is_read) FROM message").fetchone() == (0,)


def test_ambiguous_identifier_requires_guid(db, launch):
    with sqlite3.connect(db) as connection:
        connection.execute(
            "INSERT INTO chat VALUES ('SMS;-;+15551234567', '+15551234567', NULL)"
        )
    result = mark_read("+15551234567")
    assert "Ambiguous" in result
    assert "SMS;-;+15551234567" in result
    launch.assert_not_called()
    assert "not confirmed" in mark_read("iMessage;-;+15551234567")
    launch.assert_called_once()


@pytest.mark.parametrize("identifier", ["", "  ", None, "unknown", "' OR 1=1 --"])
def test_invalid_target_never_launches(db, launch, identifier):
    assert mark_read(identifier).startswith("Error:")
    launch.assert_not_called()


def test_empty_conversation_does_not_launch(db, launch):
    with sqlite3.connect(db) as connection:
        connection.execute("DELETE FROM chat_message_join WHERE chat_id=1")
    assert "no message to open" in mark_read("+15551234567")
    launch.assert_not_called()


def test_already_read_is_not_claimed_as_changed(db, launch):
    with sqlite3.connect(db) as connection:
        connection.execute("UPDATE message SET is_read=1")
    result = mark_read("chat123")
    assert "already had no unread" in result
    assert "manually set unread badge" in result
    launch.assert_called_once()


@pytest.mark.parametrize(
    "failure", [OSError("no GUI"), subprocess.TimeoutExpired("open", 10)]
)
def test_launch_exception_is_unconfirmed(db, launch, failure):
    launch.side_effect = failure
    assert "read state unconfirmed" in mark_read("chat123")


def test_launch_nonzero_is_error(db, launch):
    launch.return_value = subprocess.CompletedProcess([], 1, "", "not permitted")
    assert "unconfirmed: not permitted" in mark_read("chat123")


def test_db_failure_does_not_launch(db, launch):
    with patch(
        "mac_messages_mcp.read_status._connect_sqlite_readonly",
        side_effect=sqlite3.OperationalError("denied"),
    ):
        assert "read state unconfirmed" in mark_read("chat123")
    launch.assert_not_called()


def test_verification_error_after_open_is_not_success(db, launch):
    with patch(
        "mac_messages_mcp.read_status._unread_count",
        side_effect=[1, sqlite3.OperationalError("locked")],
    ):
        assert "read state unconfirmed" in mark_read("chat123")
    launch.assert_called_once()


def test_deep_link_guid_is_encoded_not_executed(db, launch):
    malicious = 'message&addresses=other;$(touch /tmp/should-not-exist)"'
    with sqlite3.connect(db) as connection:
        connection.execute("UPDATE message SET guid=? WHERE ROWID=3", (malicious,))
    mark_read("chat123")
    assert parse_qs(urlsplit(launch.call_args.args[0][3]).query) == {
        "message-guid": [malicious]
    }
    assert "shell" not in launch.call_args.kwargs


def test_mcp_tool_is_registered_and_not_readonly(db, launch):
    tools = asyncio.run(mcp.list_tools())
    tool = next(tool for tool in tools if tool.name == "tool_mark_read")
    assert tool.inputSchema["required"] == ["chat_id"]
    assert tool.annotations.readOnlyHint is False
    assert tool.annotations.idempotentHint is True
    assert "<untrusted-mcp-output>" in tool_mark_read(None, "unknown")
