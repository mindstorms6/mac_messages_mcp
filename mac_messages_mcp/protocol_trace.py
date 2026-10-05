"""Bounded, metadata-only tracing for the stdio MCP protocol boundary.

The trace is deliberately opt-in and records a fixed allowlist of protocol
metadata. It never records raw JSON-RPC payloads, request IDs, tool arguments,
messages, callback URLs, signing secrets, headers, or authentication material.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from collections import OrderedDict
from datetime import datetime, timezone
from io import TextIOWrapper
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import anyio
from mcp.server import stdio as stdio_transport
from mcp.shared.message import SessionMessage

LOG = logging.getLogger(__name__)
TRACE_MAX_BYTES = 256 * 1024
TRACE_BACKUP_COUNT = 2
TRACE_PARSE_LIMIT = 256 * 1024
TRACE_CORRELATION_LIMIT = 256
PROTOCOL_META_KEY = "io.modelcontextprotocol/protocolVersion"
CAPABILITIES_META_KEY = "io.modelcontextprotocol/clientCapabilities"
_PROTOCOL_VERSION = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_KNOWN_METHODS = frozenset(
    {
        "completion/complete",
        "elicitation/create",
        "events/list",
        "events/subscribe",
        "events/unsubscribe",
        "initialize",
        "logging/setLevel",
        "notifications/initialized",
        "ping",
        "prompts/get",
        "prompts/list",
        "resources/list",
        "resources/read",
        "resources/subscribe",
        "resources/templates/list",
        "resources/unsubscribe",
        "roots/list",
        "sampling/createMessage",
        "server/discover",
        "tools/call",
        "tools/list",
    }
)
_KNOWN_CAPABILITIES = frozenset(
    {
        "completions",
        "elicitation",
        "events",
        "experimental",
        "logging",
        "prompts",
        "resources",
        "roots",
        "sampling",
        "tasks",
        "tools",
    }
)
_KNOWN_RESULT_TYPES = frozenset({"accepted", "complete", "inputRequired"})


def _method_name(value: Any) -> str:
    return value if value in _KNOWN_METHODS else "unknown"


def _safe_protocol_version(value: Any) -> str | None:
    return (
        value if isinstance(value, str) and _PROTOCOL_VERSION.fullmatch(value) else None
    )


def _capability_names(value: Any) -> list[str]:
    if not isinstance(value, dict):
        return []
    return sorted(name for name in value if name in _KNOWN_CAPABILITIES)


def _result_type(value: Any) -> str | None:
    return value if value in _KNOWN_RESULT_TYPES else None


class _OwnerOnlyRotatingFileHandler(RotatingFileHandler):
    """Create every trace generation with owner-only permissions."""

    def _open(self) -> TextIOWrapper:
        return open(
            self.baseFilename,
            mode="a",
            encoding=self.encoding,
            errors=self.errors,
            opener=lambda path, flags: os.open(path, flags, 0o600),
        )

    def doRollover(self) -> None:
        super().doRollover()
        for suffix in range(0, self.backupCount + 1):
            path = self.baseFilename if suffix == 0 else f"{self.baseFilename}.{suffix}"
            if os.path.exists(path):
                os.chmod(path, 0o600)


def _request_key(value: Any) -> bytes:
    try:
        encoded = json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("ascii")
    except (TypeError, ValueError):
        encoded = repr(type(value)).encode("ascii", "replace")
    return hashlib.sha256(encoded).digest()


class ProtocolMetadataTrace:
    """Write a small rotating JSONL trace containing protocol metadata only."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        max_bytes: int = TRACE_MAX_BYTES,
        backup_count: int = TRACE_BACKUP_COUNT,
    ) -> None:
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.path.parent, 0o700)
        descriptor = os.open(self.path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        os.close(descriptor)
        os.chmod(self.path, 0o600)
        self._handler = _OwnerOnlyRotatingFileHandler(
            self.path,
            mode="a",
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        self._handler.setFormatter(logging.Formatter("%(message)s"))
        self._requests: OrderedDict[bytes, tuple[int, str]] = OrderedDict()
        self._sequence = 0
        self._write(
            {
                "direction": "system",
                "event": "trace_started",
                "max_bytes": max_bytes,
                "backup_count": backup_count,
            }
        )

    def close(self) -> None:
        self._handler.close()

    def _write(self, values: dict[str, Any]) -> None:
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            **values,
        }
        record = logging.LogRecord(
            name="mac_messages_mcp.protocol_trace",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=json.dumps(entry, sort_keys=True, separators=(",", ":")),
            args=(),
            exc_info=None,
        )
        self._handler.handle(record)
        self._handler.flush()

    def record_request(self, raw: str | bytes) -> None:
        """Inspect a raw frame without retaining it or changing validation."""
        try:
            size = len(raw.encode("utf-8")) if isinstance(raw, str) else len(raw)
            if size > TRACE_PARSE_LIMIT:
                self._write(
                    {
                        "direction": "request",
                        "event": "metadata_skipped",
                        "reason": "frame_too_large",
                        "byte_length": size,
                    }
                )
                return
            value = json.loads(raw)
            if not isinstance(value, dict):
                return
            raw_method = value.get("method")
            if not isinstance(raw_method, str):
                return
            method = _method_name(raw_method)
            self._sequence += 1
            trace_id = self._sequence
            request_id = value.get("id")
            if request_id is not None:
                key = _request_key(request_id)
                self._requests[key] = (trace_id, method)
                self._requests.move_to_end(key)
                while len(self._requests) > TRACE_CORRELATION_LIMIT:
                    self._requests.popitem(last=False)

            params = value.get("params")
            params = params if isinstance(params, dict) else {}
            raw_meta = params.get("_meta")
            meta = raw_meta if isinstance(raw_meta, dict) else {}
            meta_capabilities = meta.get(CAPABILITIES_META_KEY)
            initialize_capabilities = params.get("capabilities")
            entry: dict[str, Any] = {
                "direction": "request",
                "trace_id": trace_id,
                "method": method,
                "has_request_id": request_id is not None,
                "has_meta": "_meta" in params,
                "meta_protocol_version_present": PROTOCOL_META_KEY in meta,
                "meta_protocol_version": _safe_protocol_version(
                    meta.get(PROTOCOL_META_KEY)
                ),
                "meta_client_capabilities_present": CAPABILITIES_META_KEY in meta,
                "meta_client_capability_names": _capability_names(meta_capabilities),
            }
            if method == "initialize":
                entry.update(
                    {
                        "initialize_protocol_version": _safe_protocol_version(
                            params.get("protocolVersion")
                        ),
                        "initialize_capability_names": _capability_names(
                            initialize_capabilities
                        ),
                    }
                )
            self._write(entry)
        except Exception:
            # Diagnostics must never change framing, validation, or availability.
            return

    def record_response(self, session_message: SessionMessage) -> None:
        """Record only allowlisted result/error metadata for a response."""
        try:
            value = session_message.message.model_dump(
                by_alias=True, exclude_unset=True
            )
            if not isinstance(value, dict) or "id" not in value:
                return
            request = self._requests.pop(_request_key(value.get("id")), None)
            if request is None:
                return
            trace_id, method = request
            entry: dict[str, Any] = {
                "direction": "response",
                "trace_id": trace_id,
                "method": method,
            }
            error = value.get("error")
            if isinstance(error, dict):
                code = error.get("code")
                entry["outcome"] = "error"
                entry["error_code"] = code if isinstance(code, int) else None
                self._write(entry)
                return

            result = value.get("result")
            entry["outcome"] = "result"
            if isinstance(result, dict):
                entry["result_protocol_version"] = _safe_protocol_version(
                    result.get("protocolVersion")
                )
                versions = result.get("supportedVersions")
                entry["supported_versions"] = (
                    [
                        version
                        for raw in versions[:8]
                        if (version := _safe_protocol_version(raw)) is not None
                    ]
                    if isinstance(versions, list)
                    else []
                )
                entry["result_capability_names"] = _capability_names(
                    result.get("capabilities")
                )
                result_type = _result_type(result.get("resultType"))
                if result_type is not None:
                    entry["result_type"] = result_type
                events = result.get("events")
                if isinstance(events, list):
                    entry["event_count"] = len(events)

            self._write(entry)
        except Exception:
            # Diagnostics must never affect the response or its original object.
            return


class _TracingJSONAdapter:
    def __init__(self, inner: Any, trace: ProtocolMetadataTrace) -> None:
        self._inner = inner
        self._trace = trace

    def validate_json(self, raw: str | bytes, *args: Any, **kwargs: Any) -> Any:
        self._trace.record_request(raw)
        return self._inner.validate_json(raw, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class _TracingSendStream:
    def __init__(self, inner: Any, trace: ProtocolMetadataTrace) -> None:
        self._inner = inner
        self._trace = trace

    async def send(self, value: SessionMessage) -> None:
        self._trace.record_response(value)
        await self._inner.send(value)

    async def aclose(self) -> None:
        await self._inner.aclose()

    def close(self) -> None:
        self._inner.close()

    def clone(self) -> _TracingSendStream:
        return _TracingSendStream(self._inner.clone(), self._trace)

    async def __aenter__(self) -> _TracingSendStream:
        await self._inner.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: Any,
        exc_value: Any,
        traceback: Any,
    ) -> bool | None:
        return await self._inner.__aexit__(exc_type, exc_value, traceback)


async def _run_traced_stdio(server: Any, trace: ProtocolMetadataTrace) -> None:
    original_adapter = stdio_transport.types.jsonrpc_message_adapter
    stdio_transport.types.jsonrpc_message_adapter = _TracingJSONAdapter(
        original_adapter, trace
    )
    try:
        async with stdio_transport.stdio_server() as (read_stream, write_stream):
            await server._lowlevel_server.run(
                read_stream,
                _TracingSendStream(write_stream, trace),
                server._lowlevel_server.create_initialization_options(),
            )
    finally:
        stdio_transport.types.jsonrpc_message_adapter = original_adapter
        trace.close()


def run_stdio_with_protocol_trace(server: Any, path: str) -> None:
    """Run stdio with opt-in metadata tracing, falling back safely if setup fails."""
    try:
        trace = ProtocolMetadataTrace(path)
    except Exception as exc:
        LOG.warning("Protocol metadata trace unavailable (%s)", type(exc).__name__)
        server.run()
        return
    LOG.info(
        "Protocol metadata trace enabled at %s (max_bytes=%d backups=%d)",
        trace.path,
        TRACE_MAX_BYTES,
        TRACE_BACKUP_COUNT,
    )
    anyio.run(_run_traced_stdio, server, trace)
