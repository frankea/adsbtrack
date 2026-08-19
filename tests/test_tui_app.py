"""Textual Pilot smoke tests for the whole TUI app.

test_tui_workers.py already covers the worker error-handling path in
detail (and explains why no pytest-asyncio-style plugin is needed: no
such plugin is installed in this project, so async Pilot sessions are
driven with ``asyncio.run()`` from a plain sync test). This module
reuses that same harness pattern to answer a different question: does
the app actually boot, and can a user reach every view through its
real keybinding without the app raising or getting stuck?

Two scenarios:

* ``test_pilot_visits_all_six_views`` -- boots against a seeded tmp DB,
  selects an aircraft (unlocking the ICAO-scoped views), then presses
  each of the six view keybindings (1-6) in turn and asserts the
  ContentSwitcher actually switched and that view's main widget is
  mounted and queryable.
* ``test_events_filter_narrows_row_count`` -- seeds two aircraft with
  an emergency-squawk flight each, opens the events view (which needs
  no aircraft selection), and drives the filter Input via real
  keypresses to prove Task 14/15's filter-then-worker wiring still
  narrows the visible row count.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

pytest.importorskip("textual")  # tui extra: pyproject [project.optional-dependencies].tui

from textual.widgets import ContentSwitcher, DataTable, Static  # noqa: E402

from adsbtrack.db import Database  # noqa: E402
from adsbtrack.models import Flight  # noqa: E402
from adsbtrack.tui.app import AdsbtrackApp  # noqa: E402
from adsbtrack.tui.views.aircraft import AircraftOpenFlights, AircraftView  # noqa: E402
from adsbtrack.tui.views.events import EventsView  # noqa: E402
from adsbtrack.tui.views.flights import FlightsView  # noqa: E402
from adsbtrack.tui.views.map import MapCanvas, MapView  # noqa: E402
from adsbtrack.tui.views.spoof import SpoofView  # noqa: E402
from adsbtrack.tui.views.status import StatusView  # noqa: E402


async def _settle(app, pilot) -> None:
    """Poll until no worker on the app is PENDING/RUNNING.

    Copied from test_tui_workers.py: deliberately avoids
    ``app.workers.wait_for_complete()`` -- it calls ``worker.wait()`` on
    every worker, which raises for one that was legitimately superseded
    via exclusive+group cancellation. Polling ``worker.state`` sidesteps
    that without treating cancellation as a failure.
    """
    for _ in range(500):
        active = [w for w in app.workers if w.state.name in ("PENDING", "RUNNING")]
        if not active:
            return
        await pilot.pause()
        await asyncio.sleep(0.01)
    raise AssertionError("workers did not settle in time")


def _seed_one_aircraft(db_path) -> None:
    """One aircraft with one confirmed flight -- enough to unlock
    flights/map/status once selected."""
    with Database(db_path) as db:
        db.insert_flight(
            Flight(
                icao="aaa111",
                takeoff_time=datetime(2026, 3, 1, 12, 0, tzinfo=UTC),
                takeoff_lat=40.0,
                takeoff_lon=-74.0,
                takeoff_date="2026-03-01",
                landing_time=datetime(2026, 3, 1, 14, 0, tzinfo=UTC),
                landing_type="confirmed",
                callsign="UAL1",
                destination_icao="KBOS",
                origin_icao="KEWR",
                duration_minutes=120.0,
                max_altitude=35000,
                cruise_gs_kt=430,
                landing_confidence=0.9,
                mission_type="transport",
            )
        )
        db.conn.execute(
            "INSERT INTO aircraft_registry (icao, registration, type_code, description) VALUES (?, ?, ?, ?)",
            ("aaa111", "N111AA", "B738", "BOEING 737-800"),
        )
        db.insert_trace_day(
            "aaa111",
            "2026-03-01",
            {"timestamp": 1772280000.0, "trace": [[0, 40.0, -74.0, 5000, 210, None, None, None, {}, "adsb_icao"]]},
            source="adsbx",
        )
        db.refresh_aircraft_stats("aaa111")
        db.commit()


def _seed_two_emergency_aircraft(db_path) -> None:
    """Two aircraft, each with one flight carrying an emergency squawk and
    no destination airport, so the "all aircraft" events view has four rows
    (an emergency_squawk + an off_airport_landing event per aircraft) to
    filter down from."""
    with Database(db_path) as db:
        db.insert_flight(
            Flight(
                icao="aaa111",
                takeoff_time=datetime(2026, 3, 1, 12, 0, tzinfo=UTC),
                takeoff_lat=40.0,
                takeoff_lon=-74.0,
                takeoff_date="2026-03-01",
                landing_time=datetime(2026, 3, 1, 14, 0, tzinfo=UTC),
                landing_type="confirmed",
                callsign="UAL1",
                emergency_squawk="7700",
            )
        )
        db.insert_flight(
            Flight(
                icao="ccc333",
                takeoff_time=datetime(2026, 3, 2, 9, 0, tzinfo=UTC),
                takeoff_lat=41.0,
                takeoff_lon=-75.0,
                takeoff_date="2026-03-02",
                landing_time=datetime(2026, 3, 2, 10, 0, tzinfo=UTC),
                landing_type="confirmed",
                callsign="DAL22",
                emergency_squawk="7500",
            )
        )
        db.refresh_aircraft_stats("aaa111")
        db.refresh_aircraft_stats("ccc333")
        db.commit()


def test_pilot_visits_all_six_views(tmp_path):
    """Boot the app and reach every one of the six main views by pressing
    its real keybinding, asserting no crash and that each view's main
    widget is mounted.

    Bindings, per adsbtrack/tui/app.py: 1=aircraft, 2=flights, 3=events,
    4=spoof, 5=map, 6=status (7=f=ops is not one of the six view keys).
    flights/map/status require an aircraft selection first, so this
    drives that selection the same way AircraftView really produces it:
    posting the AircraftOpenFlights message the row-selection handler
    posts in production.
    """
    db_path = tmp_path / "views.db"
    _seed_one_aircraft(db_path)

    async def scenario() -> None:
        app = AdsbtrackApp(db_path)
        async with app.run_test() as pilot:
            await _settle(app, pilot)

            switcher = app.query_one(ContentSwitcher)

            # 1: aircraft (already the initial view, but press it anyway
            # to exercise the real binding).
            await pilot.press("1")
            await _settle(app, pilot)
            assert switcher.current == "view-aircraft"
            aircraft_view = app.query_one(AircraftView)
            assert app.query_one("#aircraft-table", DataTable) is not None

            # Select the seeded aircraft the way a real row-selection
            # does: post the same message AircraftView.on_data_table_row_selected
            # posts, which the App handles by setting the shared ICAO and
            # jumping to the flights view.
            aircraft_view.post_message(AircraftOpenFlights("aaa111"))
            await pilot.pause()
            await _settle(app, pilot)
            assert switcher.current == "view-flights"
            assert app.query_one("#flights-table", DataTable) is not None

            # 2: flights (now unlocked)
            await pilot.press("2")
            await _settle(app, pilot)
            assert switcher.current == "view-flights"
            assert app.query_one(FlightsView) is not None
            assert app.query_one("#flights-table", DataTable) is not None

            # 3: events (works with or without a selection)
            await pilot.press("3")
            await _settle(app, pilot)
            assert switcher.current == "view-events"
            assert app.query_one(EventsView) is not None
            assert app.query_one("#events-table", DataTable) is not None

            # 4: spoof
            await pilot.press("4")
            await _settle(app, pilot)
            assert switcher.current == "view-spoof"
            assert app.query_one(SpoofView) is not None
            assert app.query_one("#spoof-table", DataTable) is not None

            # 5: map
            await pilot.press("5")
            await _settle(app, pilot)
            assert switcher.current == "view-map"
            assert app.query_one(MapView) is not None
            assert app.query_one(MapCanvas) is not None

            # 6: status
            await pilot.press("6")
            await _settle(app, pilot)
            assert switcher.current == "view-status"
            assert app.query_one(StatusView) is not None
            assert app.query_one("#status-body", Static) is not None

            # App is still alive and responsive after the full tour.
            await pilot.press("1")
            await pilot.pause()
            assert switcher.current == "view-aircraft"

    asyncio.run(scenario())


def test_events_filter_narrows_row_count(tmp_path):
    """Typing into the events filter Input must narrow the visible row
    count without a fresh DB query (Task 14's cache-and-refilter path),
    proving the filter-input wiring still works after Task 15 moved the
    fetch itself into a thread worker."""
    db_path = tmp_path / "events_filter.db"
    _seed_two_emergency_aircraft(db_path)

    async def scenario() -> None:
        app = AdsbtrackApp(db_path)
        async with app.run_test() as pilot:
            await _settle(app, pilot)

            await pilot.press("3")
            await _settle(app, pilot)

            table = app.query_one("#events-table", DataTable)
            unfiltered_count = table.row_count
            assert unfiltered_count == 4, "both aircraft's two events each should be present unfiltered"

            events_view = app.query_one(EventsView)
            events_view.focus_filter()
            await pilot.pause()

            for ch in "aaa111":
                await pilot.press(ch)
            await pilot.pause()

            filtered_count = table.row_count
            assert filtered_count < unfiltered_count
            assert filtered_count == 2, "only aaa111's two events should remain"

            # Clearing the filter restores the full set again, proving
            # the narrowing wasn't a one-way fluke from re-querying.
            for _ in range(len("aaa111")):
                await pilot.press("backspace")
            await pilot.pause()
            assert table.row_count == unfiltered_count

    asyncio.run(scenario())
