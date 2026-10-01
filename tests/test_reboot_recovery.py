"""Reboot identity, legacy checkpoint migration, and replacement boundaries."""

from types import SimpleNamespace

import pytest

from mac_messages_mcp.event_source import MessageSource
from mac_messages_mcp.events import EventEngine, SourceChangedError
from tests.test_events import FakeSender, database, insert, params


def boot_stat(monkeypatch, database):
    from pathlib import Path

    original = Path.stat
    state = {"device": 100, "birth": 1771053986.625}

    def stat(path, *args, **kwargs):
        value = original(path, *args, **kwargs)
        if str(path) == str(database):
            return SimpleNamespace(
                st_dev=state["device"],
                st_ino=value.st_ino,
                st_birthtime=state["birth"],
            )
        return value

    monkeypatch.setattr(Path, "stat", stat)
    return state


def test_reboot_preserves_subscriptions_queue_and_event_ids(
    tmp_path, database, monkeypatch
):
    boot = boot_stat(monkeypatch, database)
    directory = str(tmp_path / "state")
    source = MessageSource(str(database))
    sender = FakeSender()
    first = EventEngine(directory, source, sender=sender)
    sid = first.subscribe(params())["id"]
    insert(database, 2)
    first.scan()
    before = first.db.execute("SELECT event_id,body FROM outbox").fetchone()
    first.close()
    boot["device"] = 200
    second = EventEngine(directory, MessageSource(str(database)), sender=sender)
    try:
        assert second.status()["subscriptions"] == 1
        assert second.db.execute("SELECT id FROM subscriptions").fetchone()[0] == sid
        assert tuple(
            second.db.execute("SELECT event_id,body FROM outbox").fetchone()
        ) == tuple(before)
        second.deliver()
        second.scan()
        second.deliver()
        assert len(sender.deliveries) == 1
        insert(database, 3)
        second.scan()
        second.deliver()
        assert len(sender.deliveries) == 2
    finally:
        second.close()


def test_reused_inode_with_different_birthtime_fails_closed(
    tmp_path, database, monkeypatch
):
    boot = boot_stat(monkeypatch, database)
    directory = str(tmp_path / "state")
    engine = EventEngine(directory, MessageSource(str(database)))
    engine.close()
    boot["birth"] += 1
    with pytest.raises(SourceChangedError):
        EventEngine(directory, MessageSource(str(database)))


@pytest.mark.parametrize("active", [False, True])
@pytest.mark.parametrize("rebooted", [False, True])
def test_legacy_migration_is_conservative(
    tmp_path, database, monkeypatch, active, rebooted
):
    boot = boot_stat(monkeypatch, database)
    directory = str(tmp_path / "state")
    source = MessageSource(str(database))
    engine = EventEngine(directory, source, sender=FakeSender())
    if active:
        engine.subscribe(params())
        insert(database, 2)
        engine.scan()
    legacy = source.legacy_identity()
    with engine.db:
        engine._set("source", legacy)
        engine.db.execute("DELETE FROM metadata WHERE key='source_identity'")
    before = engine.status()
    highwater = engine._get("highwater")
    engine.close()
    if rebooted:
        boot["device"] += 1
    if active and rebooted:
        with pytest.raises(SourceChangedError):
            EventEngine(directory, source)
        return
    migrated = EventEngine(directory, source, sender=FakeSender())
    try:
        assert migrated._get("source") == legacy
        assert migrated._get("source_identity") == source.identity()
        assert migrated._get("highwater") == highwater
        assert migrated.status() == before
    finally:
        migrated.close()
