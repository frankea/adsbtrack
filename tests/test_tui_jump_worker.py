"""Tests for jump.py's conversion to the fetch/filter worker pattern (Task A3).

Before this fix, JumpToHex queried the DB directly on the event loop from
on_mount and on_input_changed -- the one view module under tui/views/ that
hadn't been moved onto the same @work(thread=True) + cached-rows pattern
as aircraft.py/events.py/flights.py/status.py/map.py (see
test_tui_workers.py's docstring for why exit_on_error=False matters).
These prove the palette now fetches the aircraft list exactly once
through a worker, filters the cached rows in-memory on every keystroke,
and still degrades to a notify() instead of crashing on a DB failure.
"""

from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime

import pytest

pytest.importorskip("textual")  # tui extra: pyproject [project.optional-dependencies].tui

from textual.widgets import DataTable, Input  # noqa: E402

import adsbtrack.tui.views.jump as jump_module  # noqa: E402
from adsbtrack.db import Database  # noqa: E402
from adsbtrack.models import Flight  # noqa: E402
from adsbtrack.tui.app import AdsbtrackApp  # noqa: E402
from adsbtrack.tui.views.jump import JumpToHex  # noqa: E402


async def _settle(app, pilot) -> None:
    """Poll until no worker on the app is PENDING/RUNNING. Copied from
    test_tui_workers.py -- see that module's docstring for why this
    avoids app.workers.wait_for_complete()."""
    for _ in range(500):
        active = [w for w in app.workers if w.state.name in ("PENDING", "RUNNING")]
        if not active:
            # A worker that just finished delivers its UI updates via
            # messages that may not have been pumped yet, and those
            # handlers can start follow-on workers: flush, then re-check.
            await pilot.pause()
            if not [w for w in app.workers if w.state.name in ("PENDING", "RUNNING")]:
                return
            continue
        await pilot.pause()
        await asyncio.sleep(0.01)
    raise AssertionError("workers did not settle in time")


def _seed_two_aircraft(db_path) -> None:
    with Database(db_path) as db:
        db.insert_flight(
            Flight(
                icao="aaa111",
                takeoff_time=datetime(2026, 3, 1, 12, 0, tzinfo=UTC),
                takeoff_lat=40.0,
                takeoff_lon=-74.0,
                takeoff_date="2026-03-01",
            )
        )
        db.insert_flight(
            Flight(
                icao="bbb222",
                takeoff_time=datetime(2026, 3, 2, 9, 0, tzinfo=UTC),
                takeoff_lat=41.0,
                takeoff_lon=-75.0,
                takeoff_date="2026-03-02",
            )
        )
        db.conn.execute(
            "INSERT INTO aircraft_registry (icao, registration, type_code, description) VALUES (?, ?, ?, ?)",
            ("aaa111", "N111AA", "B738", "BOEING 737-800"),
        )
        db.conn.execute(
            "INSERT INTO aircraft_registry (icao, registration, type_code, description) VALUES (?, ?, ?, ?)",
            ("bbb222", "N222BB", "A388", "AIRBUS A-380-800"),
        )
        db.refresh_aircraft_stats("aaa111")
        db.refresh_aircraft_stats("bbb222")
        db.commit()


def test_jump_fetches_aircraft_via_worker_exactly_once(tmp_path, monkeypatch):
    db_path = tmp_path / "jump_worker.db"
    _seed_two_aircraft(db_path)

    calls: list[str] = []
    real_search = jump_module.search_aircraft

    def _counting_search(db, query, **kwargs):
        calls.append(query)
        return real_search(db, query, **kwargs)

    monkeypatch.setattr(jump_module, "search_aircraft", _counting_search)

    async def scenario() -> None:
        app = AdsbtrackApp(db_path)
        async with app.run_test() as pilot:
            await _settle(app, pilot)
            app.push_screen(JumpToHex())
            await pilot.pause()  # let compose/mount (and on_mount's fetch worker) start
            screen = app.screen
            assert isinstance(screen, JumpToHex)
            await _settle(app, pilot)
            assert not screen.loading, "loading must clear once the fetch worker succeeds"

            table = app.screen.query_one("#jump-results", DataTable)
            assert table.row_count == 2

            for ch in "aaa111":
                await pilot.press(ch)
            await pilot.pause()
            assert table.row_count == 1

            # Only the initial full-list fetch should have hit the DB --
            # every keystroke re-filters the cached rows in memory rather
            # than issuing a fresh query.
            assert calls == [""], f"expected exactly one DB fetch, got {calls!r}"

    asyncio.run(scenario())


def test_jump_loading_is_set_while_fetch_is_in_flight(tmp_path, monkeypatch):
    """Deterministic (non-racy) proof that loading is True while the fetch
    worker is still running: the patched search_aircraft blocks on a
    threading.Event until the test has observed loading=True, then the
    test releases it and confirms loading clears."""
    db_path = tmp_path / "jump_loading.db"
    _seed_two_aircraft(db_path)

    release = threading.Event()
    real_search = jump_module.search_aircraft

    def _blocking_search(db, query, **kwargs):
        release.wait(timeout=5)
        return real_search(db, query, **kwargs)

    monkeypatch.setattr(jump_module, "search_aircraft", _blocking_search)

    async def scenario() -> None:
        app = AdsbtrackApp(db_path)
        async with app.run_test() as pilot:
            await _settle(app, pilot)
            app.push_screen(JumpToHex())
            screen = app.screen
            assert isinstance(screen, JumpToHex)
            await pilot.pause()
            assert screen.loading, "loading should be set while the fetch worker is still running"
            release.set()
            await _settle(app, pilot)
            assert not screen.loading, "loading must clear once the fetch worker succeeds"

    asyncio.run(scenario())


def test_jump_worker_error_clears_loading_and_notifies(tmp_path, monkeypatch):
    db_path = tmp_path / "jump_worker_error.db"
    _seed_two_aircraft(db_path)

    def _boom(*args, **kwargs):
        raise RuntimeError("simulated DB failure")

    monkeypatch.setattr(jump_module, "search_aircraft", _boom)

    async def scenario() -> None:
        app = AdsbtrackApp(db_path)
        async with app.run_test() as pilot:
            await _settle(app, pilot)
            notify_calls: list[tuple[tuple, dict]] = []
            monkeypatch.setattr(app, "notify", lambda *a, **kw: notify_calls.append((a, kw)))

            await app.run_action("jump")
            await pilot.pause()
            screen = app.screen
            await _settle(app, pilot)

            assert not screen.loading, "loading must clear on the ERROR branch"
            assert notify_calls, "a failed fetch must notify the user instead of crashing silently"
            (args, kwargs) = notify_calls[0]
            assert "failed to load aircraft" in args[0]
            assert kwargs.get("severity") == "error"

            # The app (and modal) must still be alive and responsive --
            # exit_on_error=True would have crashed run_test() by now.
            await pilot.press("escape")
            await pilot.pause()

    asyncio.run(scenario())


def test_jump_select_still_posts_correct_icao(tmp_path):
    db_path = tmp_path / "jump_select.db"
    _seed_two_aircraft(db_path)

    async def scenario() -> None:
        app = AdsbtrackApp(db_path)
        async with app.run_test() as pilot:
            await _settle(app, pilot)
            await app.run_action("jump")
            await pilot.pause()
            await _settle(app, pilot)

            for ch in "bbb222":
                await pilot.press(ch)
            await pilot.pause()

            await pilot.press("enter")
            await pilot.pause()
            await _settle(app, pilot)

            assert app._current_icao == "bbb222"

    asyncio.run(scenario())


def test_jump_input_widget_id_unchanged(tmp_path):
    """Sanity check that the conversion didn't change widget ids the rest
    of the app (or other tests) rely on."""
    db_path = tmp_path / "jump_ids.db"
    _seed_two_aircraft(db_path)

    async def scenario() -> None:
        app = AdsbtrackApp(db_path)
        async with app.run_test() as pilot:
            await _settle(app, pilot)
            await app.run_action("jump")
            await pilot.pause()
            await _settle(app, pilot)
            assert app.screen.query_one("#jump-input", Input) is not None
            assert app.screen.query_one("#jump-results", DataTable) is not None

    asyncio.run(scenario())
