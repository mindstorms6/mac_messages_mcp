import base64
import hashlib
import hmac
import json
import os
import socket
import sqlite3
from pathlib import Path

import jsonschema
import pytest

from mac_messages_mcp.event_source import MessageSource
from mac_messages_mcp.event_webhooks import (
    WebhookError,
    WebhookSender,
    callback_parts,
    public_address,
    secret_bytes,
    signed_headers,
)
from mac_messages_mcp.events import NAMES, PAYLOAD_SCHEMA, EventEngine, catalog

KEY = "whsec_" + base64.b64encode(b"a" * 32).decode()
KEY2 = "whsec_" + base64.b64encode(b"b" * 32).decode()
URL = "https://callbacks.example/events"


class FakeSender(WebhookSender):
    def __init__(self):
        self.calls = []
        self.status = 204
        self.verify_ok = True

    def post(self, url, body, headers):
        self.calls.append((url, body, headers))
        data = json.loads(body)
        if data.get("type") == "verification":
            return (
                200,
                json.dumps(
                    {"challenge": data["challenge"] if self.verify_ok else "wrong"}
                ).encode(),
            )
        return self.status, b""

    @property
    def deliveries(self):
        return [c for c in self.calls if "eventId" in json.loads(c[1])]


@pytest.fixture
def database(tmp_path):
    path = tmp_path / "chat.db"
    with sqlite3.connect(path) as db:
        db.executescript("""
            CREATE TABLE message (guid TEXT, date INTEGER, handle_id INTEGER,
                is_from_me INTEGER, associated_message_type INTEGER,
                associated_message_guid TEXT, associated_message_emoji TEXT,
                cache_has_attachments INTEGER, text TEXT, item_type INTEGER DEFAULT 0);
            CREATE TABLE chat (guid TEXT);
            CREATE TABLE handle (id TEXT);
            CREATE TABLE chat_message_join (chat_id INTEGER,message_id INTEGER);
            INSERT INTO chat VALUES ('chat-one'),('chat-group');
            INSERT INTO handle VALUES ('+15555550100'),('friend@example.com');
        """)
    insert(path, 1)
    return path


def insert(path, row_id, kind=0, outgoing=False, chat=1, sender=1, attached=False):
    with sqlite3.connect(path) as db:
        db.execute(
            "INSERT INTO message (ROWID,guid,date,handle_id,is_from_me,associated_message_type,associated_message_guid,associated_message_emoji,cache_has_attachments,text) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                row_id,
                f"message-{row_id}",
                800000000000000000 + row_id,
                sender,
                outgoing,
                kind,
                "p:0/message-1" if kind else None,
                "🍀" if kind in (2006, 3006) else None,
                attached,
                "PRIVATE BODY NOT FOR NOTIFICATIONS",
            ),
        )
        if chat:
            db.execute("INSERT INTO chat_message_join VALUES (?,?)", (chat, row_id))


def params(name="message.created", arguments=None, key=KEY, **kwargs):
    return {
        "name": name,
        "arguments": arguments or {},
        "delivery": {"mode": "webhook", "url": URL, "secret": key},
        **kwargs,
    }


@pytest.fixture
def engine(tmp_path, database):
    sender = FakeSender()
    now = [1800000000.0]
    instance = EventEngine(
        str(tmp_path / "state"),
        MessageSource(str(database)),
        sender=sender,
        clock=lambda: now[0],
    )
    instance.test_now = now
    yield instance
    instance.close()


def test_all_messages_and_reactions_metadata_only(engine, database):
    for name in NAMES:
        engine.subscribe(params(name))
    insert(database, 2)
    insert(database, 3, outgoing=True, chat=2, sender=2, attached=True)
    insert(database, 4, kind=2001)
    insert(database, 5, kind=3001)
    insert(database, 6, kind=2006)
    insert(database, 7, kind=3006)
    insert(database, 8, kind=2007, attached=True)
    insert(database, 9, kind=1000, attached=True)
    engine.scan()
    engine.deliver()
    events = [json.loads(c[1]) for c in engine.sender.deliveries]
    assert len(events) == 8
    assert [e["name"] for e in events].count("message.created") == 2
    attached = next(
        e for e in events if e["data"]["untrusted-mcp-output"]["message_id"] == 3
    )
    assert attached["data"]["untrusted-mcp-output"]["has_attachments"] is True
    for event in events:
        jsonschema.validate(event["data"], PAYLOAD_SCHEMA)
        assert "PRIVATE BODY" not in json.dumps(event)
        assert event["cursor"] is None
    engine.scan()
    engine.deliver()
    assert len(engine.sender.deliveries) == 8
    with sqlite3.connect(database) as db:
        assert (
            db.execute("SELECT text FROM message WHERE ROWID=1").fetchone()[0]
            == "PRIVATE BODY NOT FOR NOTIFICATIONS"
        )


def test_filters_and_idempotent_refresh(engine, database):
    request = params(
        arguments={
            "chat_guids": ["chat-group"],
            "sender_addresses": ["friend@example.com"],
            "direction": "incoming",
        }
    )
    first = engine.subscribe(request)
    engine.test_now[0] += 5
    assert engine.subscribe(request)["id"] == first["id"]
    assert engine.status()["subscriptions"] == 1
    insert(database, 2)
    insert(database, 3, chat=2, sender=2)
    insert(database, 4, chat=2, sender=2, outgoing=True)
    engine.scan()
    engine.deliver()
    assert len(engine.sender.deliveries) == 1
    assert (
        json.loads(engine.sender.deliveries[0][1])["data"]["untrusted-mcp-output"][
            "message_id"
        ]
        == 3
    )


def test_delayed_chat_join(engine, database):
    engine.subscribe(params())
    insert(database, 2, chat=None)
    insert(database, 3)
    engine.scan()
    assert engine.status()["pending_chat_associations"] == 1
    with sqlite3.connect(database) as db:
        db.execute("INSERT INTO chat_message_join VALUES (1,2)")
    engine.scan()
    engine.deliver()
    assert len(engine.sender.deliveries) == 2
    assert engine.status()["pending_chat_associations"] == 0


def test_restart_keeps_queue_and_checkpoint(tmp_path, database):
    directory = str(tmp_path / "state")
    sender = FakeSender()
    first = EventEngine(directory, MessageSource(str(database)), sender=sender)
    first.subscribe(params())
    insert(database, 2)
    first.scan()
    first.close()
    second = EventEngine(directory, MessageSource(str(database)), sender=sender)
    try:
        second.scan()
        second.deliver()
        assert len(sender.deliveries) == 1
        insert(database, 3)
        second.scan()
        second.deliver()
        assert len(sender.deliveries) == 2
    finally:
        second.close()


def test_retry_same_body_id_new_signature(engine, database):
    engine.subscribe(params())
    insert(database, 2)
    engine.scan()
    engine.sender.status = 503
    engine.deliver()
    assert engine.status()["queued"] == 1
    engine.test_now[0] += 100
    engine.sender.status = 204
    engine.deliver()
    a, b = engine.sender.deliveries
    assert a[1] == b[1]
    assert a[2]["webhook-id"] == b[2]["webhook-id"]
    assert a[2]["webhook-signature"] != b[2]["webhook-signature"]
    assert engine.status()["queued"] == 0


@pytest.mark.parametrize("status", [400, 401, 403, 410, 413, 301])
def test_permanent_failures_not_retried(engine, database, status):
    engine.subscribe(params())
    insert(database, 2)
    engine.scan()
    engine.sender.status = status
    engine.deliver()
    assert engine.status()["queued"] == 0
    assert engine.status()["subscriptions"] == (0 if status == 410 else 1)


def test_expiry_and_unsubscribe_cancel_queue(engine, database):
    engine.subscribe(params(ttlMs=10))
    insert(database, 2)
    engine.scan()
    engine.test_now[0] += 1
    engine.deliver()
    assert not engine.sender.deliveries
    engine.subscribe(params())
    insert(database, 3)
    engine.scan()
    request = params()
    del request["delivery"]["secret"]
    assert engine.unsubscribe(request) == {}
    assert engine.unsubscribe(request) == {}
    engine.deliver()
    assert engine.status()["queued"] == 0
    assert not engine.sender.deliveries


def test_secret_rotation_and_failed_refresh(engine, database):
    first = engine.subscribe(params())
    engine.sender.verify_ok = False
    with pytest.raises(WebhookError):
        engine.subscribe(params(key=KEY2))
    assert engine.db.execute("SELECT secret FROM subscriptions").fetchone()[0] == KEY
    engine.sender.verify_ok = True
    assert engine.subscribe(params(key=KEY2))["id"] == first["id"]
    insert(database, 2)
    engine.scan()
    engine.deliver()
    assert len(engine.sender.deliveries[-1][2]["webhook-signature"].split()) == 2
    engine.test_now[0] += 301
    insert(database, 3)
    engine.scan()
    engine.deliver()
    assert len(engine.sender.deliveries[-1][2]["webhook-signature"].split()) == 1


@pytest.mark.parametrize(
    "change",
    [
        {"ttlMs": -1},
        {"ttlMs": True},
        {"cursor": "old"},
        {"name": "unknown"},
        {"arguments": {"direction": "invalid"}},
        {"arguments": {"chat_guids": []}},
        {"arguments": {"sender_addresses": [123]}},
        {"arguments": {"new_filter": True}},
    ],
)
def test_invalid_subscriptions_fail_before_network(engine, change):
    request = params()
    request.update(change)
    with pytest.raises((ValueError, TypeError)):
        engine.subscribe(request)
    assert not engine.sender.calls


def test_single_worker_and_private_state(engine):
    with pytest.raises(RuntimeError, match="Another event worker"):
        EventEngine(str(engine.directory), engine.source)
    assert os.stat(engine.directory).st_mode & 0o077 == 0
    assert os.stat(engine.directory / "events.sqlite3").st_mode & 0o077 == 0


def test_source_rewind_fails_closed(engine, database):
    engine.subscribe(params())
    insert(database, 2)
    engine.scan()
    with sqlite3.connect(database) as db:
        db.execute("DELETE FROM message WHERE ROWID=2")
    with pytest.raises(RuntimeError, match="rewound"):
        engine.deliver()
    assert not engine.sender.deliveries


@pytest.mark.parametrize(
    "key",
    [
        "bad",
        "whsec_??",
        "whsec_" + base64.b64encode(b"x" * 23).decode(),
        "whsec_" + base64.b64encode(b"x" * 65).decode(),
    ],
)
def test_invalid_keys(key):
    with pytest.raises(ValueError):
        secret_bytes(key)


def test_standard_webhooks_signature():
    body = b'{"example":true}'
    result = signed_headers(body, "event_1", "sub_1", [KEY], 12345.9)
    expected = base64.b64encode(
        hmac.new(b"a" * 32, b"event_1.12345." + body, hashlib.sha256).digest()
    ).decode()
    assert result["webhook-signature"] == "v1," + expected
    assert result["webhook-timestamp"] == "12345"


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com",
        "https://user:pass@example.com",
        "https://example.com:444",
        "https://example.com/#fragment",
        "https://example.com/\r\nheader",
        "https://[fe80::1%en0]/",
        "https://example.com\\evil",
    ],
)
def test_invalid_callback_url(url):
    with pytest.raises(ValueError):
        callback_parts(url)


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "10.0.0.1",
        "169.254.169.254",
        "100.64.0.1",
        "::1",
        "fd00::1",
        "::ffff:8.8.8.8",
        "64:ff9b::808:808",
        "224.0.0.1",
        "0.0.0.0",
    ],
)
def test_ssrf_rejected(monkeypatch, address):
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *a, **k: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, 443))],
    )
    with pytest.raises(WebhookError, match="non_public"):
        public_address("callback.example")


def test_mixed_dns_rejected(monkeypatch):
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *a, **k: [(2, 1, 6, "", (ip, 443)) for ip in ["8.8.8.8", "127.0.0.1"]],
    )
    with pytest.raises(WebhookError):
        public_address("callback.example")


def test_catalog_schemas():
    for event in catalog()["events"]:
        jsonschema.Draft202012Validator.check_schema(event["inputSchema"])
        jsonschema.Draft202012Validator.check_schema(event["payloadSchema"])


def test_backpressure_preserves_unobserved_rows(engine, database, monkeypatch):
    monkeypatch.setattr("mac_messages_mcp.events.MAX_QUEUE", 2)
    engine.subscribe(params())
    for row_id in range(2, 7):
        insert(database, row_id)
    for _ in range(3):
        engine.scan()
        assert engine.status()["queued"] <= 2
        engine.deliver()
    assert len(engine.sender.deliveries) == 5
    assert engine.status()["counters"]["backpressure"] == 2
    assert engine._get("highwater") == "6"


def test_pending_fanout_backpressure(engine, database, monkeypatch):
    monkeypatch.setattr("mac_messages_mcp.events.MAX_QUEUE", 2)
    engine.subscribe(params())
    engine.subscribe(params(arguments={"direction": "incoming"}))
    insert(database, 2, chat=None)
    insert(database, 3)
    engine.scan()
    engine.deliver()
    with sqlite3.connect(database) as db:
        db.execute("INSERT INTO chat_message_join VALUES (1,2)")
    insert(database, 4)
    engine.scan()
    engine.deliver()
    engine.scan()
    engine.deliver()
    assert len(engine.sender.deliveries) == 6
    assert engine.status()["pending_chat_associations"] == 0


def test_retry_budget_is_bounded(engine, database):
    engine.subscribe(params())
    insert(database, 2)
    engine.scan()
    engine.sender.status = 503
    for _ in range(10):
        engine.deliver()
        engine.test_now[0] += 1000
    assert len(engine.sender.deliveries) == 8
    assert engine.status()["counters"]["failed_deliveries"] == 1
    assert engine.status()["queued"] == 0


def test_queue_age_is_bounded(engine, database):
    engine.subscribe(params(ttlMs=7 * 86400000))
    insert(database, 2)
    engine.scan()
    engine.test_now[0] += 86401
    engine.deliver()
    assert not engine.sender.deliveries
    assert engine.status()["counters"]["failed_deliveries"] == 1


def test_invalid_and_system_rows_do_not_block_other_events(engine, database):
    engine.subscribe(params())
    insert(database, 2)
    insert(database, 3, chat=None)
    insert(database, 4)
    with sqlite3.connect(database) as db:
        db.execute("UPDATE message SET date='invalid' WHERE ROWID=2")
        db.execute("UPDATE message SET item_type=1 WHERE ROWID=3")
    engine.scan()
    engine.deliver()
    assert len(engine.sender.deliveries) == 1
    assert engine.status()["counters"]["invalid_events"] == 1
    assert engine.status()["pending_chat_associations"] == 0


def test_deleted_pending_rows_are_pruned(engine, database):
    engine.subscribe(params())
    insert(database, 2, chat=None)
    insert(database, 3)
    engine.scan()
    with sqlite3.connect(database) as db:
        db.execute("DELETE FROM message WHERE ROWID=2")
    engine.scan()
    assert engine.status()["pending_chat_associations"] == 0


def test_filter_order_and_defaults_do_not_duplicate(engine):
    first = engine.subscribe(params(arguments={"chat_guids": ["b", "a"]}))
    second = engine.subscribe(
        params(arguments={"chat_guids": ["a", "b"], "direction": "all"})
    )
    assert first["id"] == second["id"]


def test_new_subscription_does_not_receive_unscanned_history(engine, database):
    insert(database, 2)
    engine.subscribe(params())
    insert(database, 3)
    engine.scan()
    engine.deliver()
    assert len(engine.sender.deliveries) == 1
    assert (
        json.loads(engine.sender.deliveries[0][1])["data"]["untrusted-mcp-output"][
            "message_id"
        ]
        == 3
    )


def test_no_secrets_in_status(engine):
    engine.subscribe(params())
    status = json.dumps(engine.status())
    assert KEY not in status and URL not in status


def test_public_connection_is_pinned_and_redirect_not_followed(monkeypatch):
    from mac_messages_mcp import event_webhooks as webhooks

    seen = []

    class Connection:
        def __init__(self, host, address):
            seen.append((host, address))

        def request(self, method, path, **kwargs):
            seen.append((method, path))

        def getresponse(self):
            return self

        status = 302

        def read(self, limit):
            assert limit == 8193
            return b"redirect"

        def close(self):
            seen.append("closed")

    monkeypatch.setattr(webhooks, "_PinnedHTTPS", Connection)
    monkeypatch.setattr(
        socket, "getaddrinfo", lambda *a, **k: [(2, 1, 6, "", ("8.8.8.8", 443))]
    )
    assert (
        WebhookSender().post("https://callback.example/events?a=b", b"{}", {})[0] == 302
    )
    assert seen == [("callback.example", "8.8.8.8"), ("POST", "/events?a=b"), "closed"]


def test_connection_preserves_tls_hostname(monkeypatch):
    from unittest.mock import Mock

    from mac_messages_mcp.event_webhooks import _PinnedHTTPS

    sock = Mock()
    monkeypatch.setattr(
        socket,
        "create_connection",
        lambda address, **kwargs: (
            sock
            if address == ("8.8.8.8", 443)
            else pytest.fail("Unvalidated destination")
        ),
    )
    connection = _PinnedHTTPS("callback.example", "8.8.8.8")
    context = Mock()
    connection._context = context
    connection.connect()
    context.wrap_socket.assert_called_once_with(
        sock, server_hostname="callback.example"
    )


def test_http_events_fail_closed(engine):
    import asyncio
    from types import SimpleNamespace

    from mcp.shared.exceptions import MCPError

    from mac_messages_mcp.events_runtime import EventsMiddleware

    middleware = EventsMiddleware()
    middleware.engine = engine
    context = SimpleNamespace(
        method="events/subscribe",
        protocol_version="2026-07-28",
        request=object(),
        params=params(),
    )
    with pytest.raises(MCPError) as caught:
        asyncio.run(middleware(context, None))
    assert caught.value.code == -32012
    assert not engine.sender.calls


def test_plugin_metadata_is_host_independent():
    root = Path(__file__).parents[1]
    plugin = json.loads((root / "plugin.json").read_text())
    mcp = json.loads((root / "mcp.json").read_text())
    assert plugin["name"] == "mac-messages"
    server = mcp["mcpServers"]["messages"]
    assert server["command"] == "mac-messages-mcp"
    assert server["env"]["MAC_MESSAGES_EVENTS_STATE_DIR"] == "${PLUGIN_DATA}/events"
