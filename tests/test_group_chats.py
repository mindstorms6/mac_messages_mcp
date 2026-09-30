"""Regression tests for existing named and unnamed group-chat resolution."""

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from mac_messages_mcp.messages import _find_chat_by_identifier, send_message
from mac_messages_mcp.server import mcp, tool_get_chats

BARE_ID = "opaque-existing-group"
FULL_GUID = f"any;+;{BARE_ID}"


def _group_row(rowid=2367, guid=FULL_GUID, display_name=None):
    return {
        "ROWID": rowid,
        "guid": guid,
        "chat_identifier": BARE_ID,
        "room_name": BARE_ID,
        "display_name": display_name,
    }


@patch("mac_messages_mcp.server.query_messages_db")
def test_get_chats_discovers_unnamed_existing_group(mock_query):
    mock_query.return_value = [_group_row()]

    result = tool_get_chats(ctx=MagicMock())

    assert "named and unnamed" in result
    assert "Unnamed group" in result
    assert f"ID: {FULL_GUID}" in result
    assert f"Identifier: {BARE_ID}" in result
    sql = mock_query.call_args.args[0]
    assert "style = 43" in sql
    assert "display_name IS NOT NULL" not in sql


@pytest.mark.parametrize("identifier", [BARE_ID, FULL_GUID])
@patch("mac_messages_mcp.messages.query_messages_db")
def test_find_chat_accepts_bare_and_full_identifiers(mock_query, identifier):
    mock_query.return_value = [_group_row()]

    result = _find_chat_by_identifier(identifier)

    assert result["ROWID"] == 2367
    sql, params = mock_query.call_args.args
    assert "guid IN" in sql
    assert "style = 43" in sql
    assert BARE_ID in params
    if identifier == FULL_GUID:
        assert FULL_GUID in params


@patch("mac_messages_mcp.messages.query_messages_db")
def test_bare_identifier_rejects_ambiguous_groups(mock_query):
    mock_query.return_value = [
        _group_row(rowid=1, guid=f"any;+;{BARE_ID}"),
        _group_row(rowid=2, guid=f"iMessage;+;{BARE_ID}"),
    ]

    with pytest.raises(ValueError, match="exact full GUID"):
        _find_chat_by_identifier(BARE_ID)


@patch("mac_messages_mcp.messages.query_messages_db")
def test_full_guid_wins_over_bare_identifier_collision(mock_query):
    exact = _group_row(rowid=1, guid=FULL_GUID)
    mock_query.return_value = [
        exact,
        _group_row(rowid=2, guid=f"iMessage;+;{BARE_ID}"),
    ]

    assert _find_chat_by_identifier(FULL_GUID) == exact


@patch("mac_messages_mcp.messages._send_message_to_recipient")
@patch("mac_messages_mcp.messages._find_chat_by_identifier")
def test_group_send_resolves_to_canonical_existing_guid(mock_find, mock_send):
    mock_find.return_value = _group_row()
    mock_send.return_value = "sent"

    result = send_message(BARE_ID, "synthetic body", group_chat=True)

    assert result == "sent"
    mock_send.assert_called_once_with(
        FULL_GUID, "synthetic body", "Unnamed group", group_chat=True
    )


@patch("mac_messages_mcp.messages._send_message_to_recipient")
@patch("mac_messages_mcp.messages._find_chat_by_identifier", return_value=None)
def test_group_send_rejects_unknown_id_without_dispatch(_find, mock_send):
    result = send_message("not-a-real-group", "synthetic body", group_chat=True)

    assert "Existing group chat not found" in result
    mock_send.assert_not_called()


def test_group_tool_schemas_describe_existing_named_and_unnamed_contract():
    tools = {tool.name: tool for tool in asyncio.run(mcp.list_tools())}

    assert "named and unnamed" in tools["tool_get_chats"].description
    send_properties = tools["tool_send_message"].input_schema["properties"]
    assert "full GUID preferred" in send_properties["recipient"]["description"]
    assert "never creates" in send_properties["group_chat"]["description"]
    for name in ("tool_get_recent_messages", "tool_fuzzy_search_messages"):
        description = tools[name].input_schema["properties"]["chat_id"]["description"]
        assert "full GUID" in description
        assert "bare Identifier" in description
