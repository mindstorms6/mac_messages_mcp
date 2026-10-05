import json
import os
import select
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests.test_events import database, insert, params

META = {
    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
    "io.modelcontextprotocol/clientCapabilities": {},
}


class Peer:
    def __init__(self, process):
        self.process = process
        self.sequence = 0

    def request(self, method, params=None, modern=True):
        self.sequence += 1
        request = {
            "jsonrpc": "2.0",
            "id": self.sequence,
            "method": method,
            "params": dict(params or {}),
        }
        if modern:
            request["params"]["_meta"] = META
        self.process.stdin.write(json.dumps(request) + "\n")
        self.process.stdin.flush()
        ready, _, _ = select.select([self.process.stdout], [], [], 15)
        assert ready, f"MCP response timed out for {method}"
        line = self.process.stdout.readline()
        assert line, "MCP process exited unexpectedly"
        response = json.loads(line)
        assert response["id"] == self.sequence
        return response


@pytest.fixture
def peer(tmp_path, database, request):
    deliveries = tmp_path / "deliveries.jsonl"
    options = getattr(request, "param", True)
    if isinstance(options, dict):
        if options.get("missing_database"):
            database.rename(database.with_suffix(".waiting"))
        if options.get("changed_source"):
            from mac_messages_mcp.event_source import MessageSource
            from mac_messages_mcp.events import EventEngine

            engine = EventEngine(str(tmp_path / "state"), MessageSource(str(database)))
            with engine.db:
                engine._set("source_identity", "different-database")
            engine.close()
    env = dict(
        os.environ,
        MAC_MESSAGES_EVENTS="1" if getattr(request, "param", True) else "0",
        MAC_MESSAGES_EVENTS_DB=str(database),
        MAC_MESSAGES_EVENTS_STATE_DIR=str(tmp_path / "state"),
        MAC_MESSAGES_EVENTS_INTERVAL="0.25",
        TEST_DELIVERIES=str(deliveries),
    )
    with open(tmp_path / "server.log", "w") as errors:
        process = subprocess.Popen(
            [sys.executable, "-m", "tests.event_server_fixture"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=errors,
            text=True,
            env=env,
        )
        try:
            yield Peer(process), deliveries
        finally:
            process.stdin.close()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            process.stdout.close()


def test_modern_wire_discovery_subscribe_delivery_unsubscribe(peer, database):
    client, deliveries = peer
    discovery = client.request("server/discover")["result"]
    assert discovery["supportedVersions"] == ["2026-07-28"]
    assert discovery["capabilities"]["events"] == {}
    listed = client.request("events/list")["result"]
    assert {e["name"] for e in listed["events"]} == {
        "message.created",
        "reaction.added",
        "reaction.removed",
    }
    tools = client.request("tools/list")["result"]["tools"]
    assert {
        "tool_mark_read",
        "tool_get_latest_contact_activity",
        "tool_get_attachment",
    } <= {t["name"] for t in tools}
    request = params()
    subscription = client.request("events/subscribe", request)["result"]
    assert subscription["cursor"] is None and subscription["truncated"] is False
    assert (
        client.request("events/subscribe", request)["result"]["id"]
        == subscription["id"]
    )
    insert(database, 2, attached=True)
    deadline = time.monotonic() + 10
    emitted = []
    while time.monotonic() < deadline:
        emitted = [json.loads(line) for line in deliveries.read_text().splitlines()]
        if any("eventId" in e["body"] for e in emitted):
            break
        time.sleep(0.05)
    event = next(e for e in emitted if "eventId" in e["body"])
    assert event["headers"]["X-MCP-Subscription-Id"] == subscription["id"]
    assert event["headers"]["webhook-id"] == event["body"]["eventId"]
    assert event["body"]["data"]["untrusted-mcp-output"]["message_id"] == 2
    del request["delivery"]["secret"]
    assert (
        client.request("events/unsubscribe", request)["result"]["resultType"]
        == "complete"
    )
    status = client.request(
        "tools/call", {"name": "tool_event_status", "arguments": {}}
    )["result"]
    assert status["structuredContent"]["subscriptions"] == 0


def test_legacy_tools_still_work_and_events_rejected(peer):
    client, _ = peer
    initialized = client.request(
        "initialize",
        {
            "protocolVersion": "2025-11-25",
            "capabilities": {},
            "clientInfo": {"name": "legacy-test", "version": "1"},
        },
        modern=False,
    )
    assert initialized["result"]["protocolVersion"] == "2025-11-25"
    assert "events" not in initialized["result"]["capabilities"]
    tools = client.request("tools/list", modern=False)["result"]["tools"]
    assert any(t["name"] == "tool_get_recent_messages" for t in tools)
    assert client.request("events/list", modern=False)["error"]["code"] == -32601


def test_invalid_params_and_cursor_errors_on_wire(peer):
    client, _ = peer
    assert client.request("events/list", {"cursor": "bad"})["error"]["code"] == -32602
    assert (
        client.request("events/subscribe", params(ttlMs=-5))["error"]["code"] == -32602
    )
    assert (
        client.request("events/subscribe", {"principal": "other"})["error"]["code"]
        == -32602
    )


@pytest.mark.parametrize("peer", [False], indirect=True)
def test_events_disabled_by_default(peer):
    client, _ = peer
    assert "events" not in client.request("server/discover")["result"]["capabilities"]
    assert client.request("events/list")["error"]["code"] == -32601
    result = client.request(
        "tools/call", {"name": "tool_event_status", "arguments": {}}
    )["result"]
    assert result["structuredContent"] == {"enabled": False}


@pytest.mark.parametrize("peer", [{"missing_database": True}], indirect=True)
def test_missing_database_does_not_stop_tools_and_recovers(peer, database):
    client, _ = peer
    tools = client.request("tools/list")["result"]["tools"]
    assert any(t["name"] == "tool_get_recent_messages" for t in tools)
    status = client.request(
        "tools/call", {"name": "tool_event_status", "arguments": {}}
    )["result"]["structuredContent"]
    assert status == {
        "enabled": False,
        "configured": True,
        "retrying": True,
        "last_error": "FileNotFoundError",
    }
    assert client.request("events/subscribe", params())["error"]["code"] == -32603
    database.with_suffix(".waiting").rename(database)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        status = client.request(
            "tools/call", {"name": "tool_event_status", "arguments": {}}
        )["result"]["structuredContent"]
        if status["enabled"]:
            break
        time.sleep(0.05)
    assert status["enabled"] and status["last_error"] is None
    assert client.request("events/subscribe", params())["result"]["id"]


@pytest.mark.parametrize("peer", [{"changed_source": True}], indirect=True)
def test_replaced_database_pauses_events_but_keeps_tools_available(peer):
    client, _ = peer
    assert client.request("tools/list")["result"]["tools"]
    assert client.request("events/list")["result"]["events"]
    assert client.request("events/subscribe", params())["error"]["code"] == -32603
    status = client.request(
        "tools/call", {"name": "tool_event_status", "arguments": {}}
    )["result"]["structuredContent"]
    assert status["last_error"] == "SourceChangedError"
    assert status["enabled"] is False


def test_attachment_tools_wire_schema_and_safe_rejection(peer):
    client, _ = peer
    tools = {t["name"]: t for t in client.request("tools/list")["result"]["tools"]}
    send = tools["tool_send_attachment"]
    assert set(send["inputSchema"]["required"]) == {
        "recipient",
        "request_id",
        "filename",
    }
    assert {"file_path", "content_base64"} <= set(send["inputSchema"]["properties"])
    assert send["annotations"]["readOnlyHint"] is False
    assert send["annotations"]["idempotentHint"] is True
    assert (
        tools["tool_get_attachment_send_status"]["annotations"]["readOnlyHint"] is True
    )
    result = client.request(
        "tools/call",
        {
            "name": "tool_send_attachment",
            "arguments": {
                "recipient": "invalid name",
                "request_id": "bfca0d57-f613-4a46-b92e-149fddae53c5",
                "filename": "fixture.txt",
                "content_base64": "YQ==",
            },
        },
    )["result"]
    assert result["isError"] is True
    assert result["structuredContent"]["untrusted-mcp-output"]["retry_safe"] is False
