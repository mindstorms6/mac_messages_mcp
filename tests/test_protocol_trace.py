import asyncio
import json
import os
import select
import stat
import subprocess
import sys

import pytest

from mac_messages_mcp.protocol_trace import (
    ProtocolMetadataTrace,
    _TracingJSONAdapter,
    _TracingSendStream,
)
from tests.test_events import database


class Adapter:
    def __init__(self, result):
        self.result = result
        self.raw = None

    def validate_json(self, raw, *args, **kwargs):
        self.raw = raw
        return self.result


class Message:
    def __init__(self, value):
        self.value = value

    def model_dump(self, **kwargs):
        return self.value


class Envelope:
    def __init__(self, value):
        self.message = Message(value)


class SendStream:
    def __init__(self):
        self.values = []
        self.closed = False

    async def send(self, value):
        self.values.append(value)

    async def aclose(self):
        self.closed = True


def entries(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_request_trace_is_allowlisted_and_validator_receives_exact_frame(tmp_path):
    path = tmp_path / "protocol.jsonl"
    trace = ProtocolMetadataTrace(path)
    result = object()
    adapter = Adapter(result)
    wrapped = _TracingJSONAdapter(adapter, trace)
    raw = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": "private-request-id",
            "method": "events/subscribe",
            "params": {
                "name": "message.created",
                "arguments": {"chat_id": "private-chat"},
                "delivery": {
                    "url": "https://callback.invalid/private-token",
                    "secret": "whsec_private",
                },
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                    "io.modelcontextprotocol/clientCapabilities": {
                        "events": {},
                        "tools": {},
                        "private-capability-secret": {},
                    },
                    "private": "must-not-appear",
                },
            },
        }
    )

    assert wrapped.validate_json(raw) is result
    assert adapter.raw is raw
    trace.close()

    text = path.read_text()
    assert "private-request-id" not in text
    assert "private-chat" not in text
    assert "callback.invalid" not in text
    assert "whsec_private" not in text
    assert "must-not-appear" not in text
    assert "private-capability-secret" not in text
    request = entries(path)[-1]
    assert request == {
        "direction": "request",
        "has_meta": True,
        "has_request_id": True,
        "meta_client_capabilities_present": True,
        "meta_client_capability_names": ["events", "tools"],
        "meta_protocol_version_present": True,
        "meta_protocol_version": "2026-07-28",
        "method": "events/subscribe",
        "timestamp": request["timestamp"],
        "trace_id": 1,
    }


def test_unknown_method_and_capability_names_are_never_logged(tmp_path):
    path = tmp_path / "protocol.jsonl"
    trace = ProtocolMetadataTrace(path)
    adapter = _TracingJSONAdapter(Adapter(None), trace)
    adapter.validate_json(
        json.dumps(
            {
                "jsonrpc": "2.0",
                "id": "caller-private-id",
                "method": "secret/method-name",
                "params": {
                    "private": {"nested": "private-body"},
                    "_meta": {
                        "io.modelcontextprotocol/protocolVersion": "not-a-date-secret",
                        "io.modelcontextprotocol/clientCapabilities": {
                            "secret-capability-name": {}
                        },
                    },
                },
            }
        )
    )
    trace.close()

    text = path.read_text()
    for secret in (
        "caller-private-id",
        "secret/method-name",
        "private-body",
        "not-a-date-secret",
        "secret-capability-name",
    ):
        assert secret not in text
    request = entries(path)[-1]
    assert request["method"] == "unknown"
    assert request["meta_protocol_version_present"] is True
    assert request["meta_protocol_version"] is None
    assert request["meta_client_capability_names"] == []


def test_response_trace_is_bounded_and_forwards_same_object(tmp_path):
    path = tmp_path / "protocol.jsonl"
    trace = ProtocolMetadataTrace(path)
    adapter = _TracingJSONAdapter(Adapter(None), trace)
    adapter.validate_json(
        json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 7,
                "method": "server/discover",
                "params": {
                    "_meta": {"io.modelcontextprotocol/protocolVersion": "2026-07-28"}
                },
            }
        )
    )
    envelope = Envelope(
        {
            "jsonrpc": "2.0",
            "id": 7,
            "result": {
                "resultType": "complete",
                "supportedVersions": ["2026-07-28"],
                "capabilities": {"events": {}, "tools": {}},
                "_meta": {"private": "must-not-appear"},
            },
        }
    )
    inner = SendStream()
    wrapped = _TracingSendStream(inner, trace)

    asyncio.run(wrapped.send(envelope))
    asyncio.run(wrapped.aclose())
    trace.close()

    assert inner.values == [envelope]
    assert inner.values[0] is envelope
    assert inner.closed is True
    text = path.read_text()
    assert "must-not-appear" not in text
    response = entries(path)[-1]
    assert response["method"] == "server/discover"
    assert response["outcome"] == "result"
    assert response["supported_versions"] == ["2026-07-28"]
    assert response["result_capability_names"] == ["events", "tools"]


def test_error_trace_records_code_not_message_or_data(tmp_path):
    path = tmp_path / "protocol.jsonl"
    trace = ProtocolMetadataTrace(path)
    adapter = _TracingJSONAdapter(Adapter(None), trace)
    adapter.validate_json(
        json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 9,
                "method": "events/list",
                "params": {},
            }
        )
    )
    envelope = Envelope(
        {
            "jsonrpc": "2.0",
            "id": 9,
            "error": {
                "code": -32601,
                "message": "secret message",
                "data": {"url": "https://callback.invalid/private"},
            },
        }
    )
    inner = SendStream()

    asyncio.run(_TracingSendStream(inner, trace).send(envelope))
    trace.close()

    text = path.read_text()
    assert "secret message" not in text
    assert "callback.invalid" not in text
    response = entries(path)[-1]
    assert response["method"] == "events/list"
    assert response["outcome"] == "error"
    assert response["error_code"] == -32601


def test_trace_rotates_and_skips_large_frames(tmp_path):
    path = tmp_path / "protocol.jsonl"
    trace = ProtocolMetadataTrace(path, max_bytes=300, backup_count=1)
    for request_id in range(20):
        trace.record_request(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "tools/list",
                    "params": {},
                }
            )
        )
    trace.record_request(" " * (256 * 1024 + 1))
    trace.close()

    assert path.exists()
    assert path.with_name(path.name + ".1").exists()
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(path.with_name(path.name + ".1").stat().st_mode) == 0o600
    combined = path.read_text() + path.with_name(path.name + ".1").read_text()
    assert "frame_too_large" in combined


def test_opt_in_trace_wraps_real_stdio_without_changing_protocol(tmp_path, database):
    trace_path = tmp_path / "protocol.jsonl"
    environment = dict(
        os.environ,
        MAC_MESSAGES_EVENTS="1",
        MAC_MESSAGES_EVENTS_DB=str(database),
        MAC_MESSAGES_EVENTS_STATE_DIR=str(tmp_path / "state"),
        MAC_MESSAGES_EVENTS_INTERVAL="0.25",
        MAC_MESSAGES_PROTOCOL_TRACE_FILE=str(trace_path),
        TEST_DELIVERIES=str(tmp_path / "deliveries.jsonl"),
    )
    server_log = tmp_path / "server.log"
    with open(server_log, "w") as errors:
        process = subprocess.Popen(
            [sys.executable, "-m", "tests.event_server_fixture"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=errors,
            text=True,
            env=environment,
        )
        try:
            request = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "server/discover",
                "params": {
                    "_meta": {
                        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                        "io.modelcontextprotocol/clientCapabilities": {"events": {}},
                    }
                },
            }
            original = json.dumps(request)
            process.stdin.write(original + "\n")
            process.stdin.flush()
            ready, _, _ = select.select([process.stdout], [], [], 15)
            if not ready:
                process.kill()
                process.wait()
                pytest.fail(
                    "MCP response timed out; "
                    f"returncode={process.returncode}; "
                    f"stderr={server_log.read_text()!r}; "
                    f"trace={trace_path.read_text()!r}"
                )
            response_line = process.stdout.readline()
            response = json.loads(response_line)
            assert response["id"] == 1
            assert response["result"]["supportedVersions"] == ["2026-07-28"]
            assert response["result"]["capabilities"]["events"] == {}
        finally:
            process.stdin.close()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            process.stdout.close()

    traced = entries(trace_path)
    request_entry = next(
        entry
        for entry in traced
        if entry.get("direction") == "request"
        and entry.get("method") == "server/discover"
    )
    response_entry = next(
        entry
        for entry in traced
        if entry.get("direction") == "response"
        and entry.get("method") == "server/discover"
    )
    assert request_entry["meta_protocol_version"] == "2026-07-28"
    assert request_entry["meta_client_capability_names"] == ["events"]
    assert response_entry["supported_versions"] == ["2026-07-28"]
    assert response_entry["result_capability_names"] == [
        "events",
        "prompts",
        "resources",
        "tools",
    ]
    assert stat.S_IMODE(trace_path.stat().st_mode) == 0o600


def test_trace_preserves_legacy_then_modern_rejection(tmp_path, database):
    trace_path = tmp_path / "protocol.jsonl"
    environment = dict(
        os.environ,
        MAC_MESSAGES_EVENTS="1",
        MAC_MESSAGES_EVENTS_DB=str(database),
        MAC_MESSAGES_EVENTS_STATE_DIR=str(tmp_path / "state"),
        MAC_MESSAGES_EVENTS_INTERVAL="0.25",
        MAC_MESSAGES_PROTOCOL_TRACE_FILE=str(trace_path),
        TEST_DELIVERIES=str(tmp_path / "deliveries.jsonl"),
    )
    server_log = tmp_path / "server.log"
    with open(server_log, "w") as errors:
        process = subprocess.Popen(
            [sys.executable, "-m", "tests.event_server_fixture"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=errors,
            text=True,
            env=environment,
        )
        try:
            requests = [
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2025-11-25",
                        "capabilities": {},
                        "clientInfo": {"name": "legacy-test", "version": "1"},
                    },
                },
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "server/discover",
                    "params": {
                        "_meta": {
                            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                            "io.modelcontextprotocol/clientCapabilities": {
                                "events": {}
                            },
                        }
                    },
                },
            ]
            responses = []
            for request in requests:
                process.stdin.write(json.dumps(request) + "\n")
                process.stdin.flush()
                ready, _, _ = select.select([process.stdout], [], [], 15)
                if not ready:
                    process.kill()
                    process.wait()
                    pytest.fail(
                        "MCP response timed out; "
                        f"returncode={process.returncode}; "
                        f"stderr={server_log.read_text()!r}; "
                        f"trace={trace_path.read_text()!r}"
                    )
                responses.append(json.loads(process.stdout.readline()))

            assert responses[0]["result"]["protocolVersion"] == "2025-11-25"
            assert responses[1]["error"]["code"] == -32600
        finally:
            process.stdin.close()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            process.stdout.close()

    traced = entries(trace_path)
    modern_request = next(
        entry
        for entry in traced
        if entry.get("direction") == "request"
        and entry.get("method") == "server/discover"
    )
    modern_response = next(
        entry
        for entry in traced
        if entry.get("direction") == "response"
        and entry.get("method") == "server/discover"
    )
    assert modern_request["meta_protocol_version"] == "2026-07-28"
    assert modern_request["meta_client_capability_names"] == ["events"]
    assert modern_response["outcome"] == "error"
    assert modern_response["error_code"] == -32600
