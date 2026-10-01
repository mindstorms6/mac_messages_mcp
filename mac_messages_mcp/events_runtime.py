"""MCP Events wire adapter and process lifecycle.

The shipped entry point is stdio, whose principal is the local process owner.
A tunnel must preserve that single-owner authorization boundary. HTTP/multi-user
embedding is deliberately rejected until an authenticated principal resolver and
ongoing revocation checks are supplied; clientInfo is NEVER an identity source.
"""

import asyncio
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from mcp.server.context import CallNext, HandlerResult, ServerRequestContext
from mcp.shared.exceptions import MCPError

from . import __version__
from .event_source import MessageSource
from .event_webhooks import WebhookError
from .events import EventEngine, catalog

LOG = logging.getLogger(__name__)
VERSION = "2026-07-28"


class EventsMiddleware:
    def __init__(self):
        self.engine: EventEngine | None = None
        self.configured = False
        self.startup_error: str | None = None

    async def __call__(
        self, ctx: ServerRequestContext[Any, Any], call_next: CallNext
    ) -> HandlerResult:
        engine = self.engine
        if engine is None and not self.configured:
            return await call_next(ctx)
        if ctx.method == "server/discover":
            result = await call_next(ctx)
            if ctx.request is None and isinstance(result, dict):
                result["capabilities"]["events"] = {}
            return result
        if ctx.method not in ("events/list", "events/subscribe", "events/unsubscribe"):
            return await call_next(ctx)
        if ctx.protocol_version != VERSION:
            raise MCPError(-32601, "Events require MCP protocol 2026-07-28")
        if ctx.request is not None:
            raise MCPError(
                -32012, "Events are available only on the single-owner stdio transport"
            )
        params = dict(ctx.params or {})
        params.pop("_meta", None)
        allowed = (
            {"cursor"}
            if ctx.method == "events/list"
            else {"name", "arguments", "delivery"}
        )
        if ctx.method == "events/subscribe":
            allowed |= {"cursor", "ttlMs"}
        if set(params) - allowed:
            raise MCPError(-32602, "Unknown event parameters")
        try:
            if ctx.method == "events/list":
                if params.get("cursor") is not None:
                    raise ValueError("Event catalog does not have another page")
                result = catalog()
            elif engine is None:
                raise RuntimeError("Event service is initializing")
            elif ctx.method == "events/subscribe":
                result = await asyncio.to_thread(engine.subscribe, params)
            else:
                result = await asyncio.to_thread(engine.unsubscribe, params)
        except WebhookError as exc:
            raise MCPError(
                -32015, "Callback verification failed", {"reason": str(exc)}
            ) from None
        except (ValueError, TypeError, KeyError):
            raise MCPError(
                -32602, "Invalid event name, filters, delivery, secret, TTL or cursor"
            ) from None
        except Exception:
            # Do not leak message data, callback query tokens, secrets or paths.
            raise MCPError(
                -32603, "Event service unavailable; inspect local event status"
            ) from None
        return {
            "resultType": "complete",
            **result,
            "_meta": {
                "io.modelcontextprotocol/serverInfo": {
                    "name": "MessageBridge",
                    "version": __version__,
                }
            },
        }


events_middleware = EventsMiddleware()


def _initialize_engine(directory: str, source: MessageSource) -> EventEngine | None:
    try:
        engine = EventEngine(directory, source)
    except Exception as exc:
        error = type(exc).__name__
        if events_middleware.startup_error != error:
            LOG.warning(
                "Event initialization paused (%s); tools remain available", error
            )
        events_middleware.startup_error = error
        return None
    events_middleware.startup_error = None
    events_middleware.engine = engine
    return engine


def _worker(
    directory: str, source: MessageSource, stopped: threading.Event, interval: float
) -> None:
    last_scan = 0.0
    while not stopped.is_set():
        engine = events_middleware.engine
        if engine is None:
            engine = _initialize_engine(directory, source)
            if engine is None:
                stopped.wait(max(1.0, interval))
                continue
        try:
            # Drain first so a full queue can recover without bypassing bounds.
            engine.deliver(limit=1)
            if time.monotonic() - last_scan >= interval:
                engine.scan()
                last_scan = time.monotonic()
            engine.last_error = None
        except Exception as exc:
            # Exception text may include local DB paths/remote data; only log type.
            error = type(exc).__name__
            if engine.last_error != error:
                LOG.warning("Event worker paused this iteration (%s)", error)
            engine.last_error = error
        stopped.wait(engine.delivery_delay(interval))


@asynccontextmanager
async def events_lifespan(server: Any):
    if os.environ.get("MAC_MESSAGES_EVENTS", "0") != "1":
        yield {}
        return
    from .messages import get_messages_db_path

    interval = float(os.environ.get("MAC_MESSAGES_EVENTS_INTERVAL", "2"))
    if not 0.25 <= interval <= 60:
        raise ValueError("Event polling interval must be between 0.25 and 60 seconds")
    directory = os.environ.get(
        "MAC_MESSAGES_EVENTS_STATE_DIR",
        str(Path.home() / "Library/Application Support/mac-messages-mcp/events"),
    )
    source = MessageSource(
        os.environ.get("MAC_MESSAGES_EVENTS_DB", get_messages_db_path())
    )
    events_middleware.configured = True
    _initialize_engine(directory, source)
    stopped = threading.Event()
    worker = threading.Thread(
        target=_worker,
        args=(directory, source, stopped, interval),
        name="messages-events",
        daemon=True,
    )
    worker.start()
    try:
        yield {"events": events_middleware.engine}
    finally:
        stopped.set()
        await asyncio.to_thread(worker.join)
        engine = events_middleware.engine
        events_middleware.engine = None
        events_middleware.configured = False
        events_middleware.startup_error = None
        if engine is not None:
            engine.close()


def event_status() -> dict:
    engine = events_middleware.engine
    if engine is None and events_middleware.configured:
        return {
            "enabled": False,
            "configured": True,
            "retrying": True,
            "last_error": events_middleware.startup_error,
        }
    return {"enabled": engine is not None, **(engine.status() if engine else {})}
