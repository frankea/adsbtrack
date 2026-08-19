"""Tests proving a typed filter needle survives a background refresh
(Task A4).

Before this fix, aircraft.py / events.py / flights.py's
``on_worker_state_changed`` SUCCESS branch called ``self._apply_filter("")``
unconditionally -- discarding whatever the user had typed into the filter
Input while a background refresh (``refresh_data()``) was in flight. The
Input kept showing the typed needle even though the table snapped back to
the unfiltered set, breaking the invariant "table contents always match
what the filter box shows". Each test here: types a needle, narrows the
table, calls ``refresh_data()`` directly (simulating a background
re-fetch), and asserts the table stays narrowed by that same needle.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

pytest.importorskip("textual")  # tui extra: pyproject [project.optional-dependencies].tui

from textual.widgets import DataTable  # noqa: E402

from adsbtrack.db import Database  # noqa: E402
from adsbtrack.models import Flight  # noqa: E402
from adsbtrack.tui.app import AdsbtrackApp  # noqa: E402
from adsbtrack.tui.views.aircraft import AircraftView  # noqa: E402
from adsbtrack.tui.views.events import EventsView  # noqa: E402
from adsbtrack.tui.views.flights import FlightsView  # noqa: E402


async def _settle(app, pilot) -> None:
    """Poll until no worker on the app is PENDING/RUNNING. Copied from
    test_tui_workers.py -- see that module's docstring for why this
    avoids app.workers.wait_for_complete()."""
    for _ in range(500):
        active = [w for w in app.workers if w.state.name in ("PENDING", "RUNNING")]
        if not active:
            return
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
                landing_time=datetime(2026, 3, 1, 14, 0, tzinfo=UTC),
                landing_type="confirmed",
                callsign="UAL1",
                destination_icao="KBOS",
                origin_icao="KEWR",
                duration_minutes=120.0,
            )
        )
        db.insert_flight(
            Flight(
                icao="bbb222",
                takeoff_time=datetime(2026, 3, 2, 9, 0, tzinfo=UTC),
                takeoff_lat=41.0,
                takeoff_lon=-75.0,
                takeoff_date="2026-03-02",
                landing_time=datetime(2026, 3, 2, 10, 0, tzinfo=UTC),
                landing_type="confirmed",
                callsign="DAL22",
                destination_icao="KLAX",
                origin_icao="KORD",
                duration_minutes=60.0,
            )
        )
        db.refresh_aircraft_stats("aaa111")
        db.refresh_aircraft_stats("bbb222")
        db.commit()


def _seed_two_emergency_aircraft(db_path) -> None:
    """Two aircraft, each with one emergency-squawk flight and no
    destination airport, so the "all aircraft" events view has four rows
    (an emergency_squawk + an off_airport_landing per aircraft)."""
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


def _seed_two_flights_one_aircraft(db_path) -> None:
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
            )
        )
        db.insert_flight(
            Flight(
                icao="aaa111",
                takeoff_time=datetime(2026, 3, 2, 9, 0, tzinfo=UTC),
                takeoff_lat=41.0,
                takeoff_lon=-75.0,
                takeoff_date="2026-03-02",
                landing_time=datetime(2026, 3, 2, 10, 0, tzinfo=UTC),
                landing_type="confirmed",
                callsign="DAL22",
                destination_icao="KLAX",
                origin_icao="KORD",
                duration_minutes=60.0,
            )
        )
        db.refresh_aircraft_stats("aaa111")
        db.commit()


def test_aircraft_filter_survives_refresh(tmp_path):
    db_path = tmp_path / "aircraft_filter_persistence.db"
    _seed_two_aircraft(db_path)

    async def scenario() -> None:
        app = AdsbtrackApp(db_path)
        async with app.run_test() as pilot:
            await _settle(app, pilot)

            view = app.query_one(AircraftView)
            table = app.query_one("#aircraft-table", DataTable)
            unfiltered = table.row_count
            assert unfiltered == 2

            view.focus_filter()
            await pilot.pause()
            for ch in "aaa111":
                await pilot.press(ch)
            await pilot.pause()
            assert table.row_count == 1, "typing should narrow the table first"

            view.refresh_data()
            await _settle(app, pilot)

            assert table.row_count == 1, "a background refresh must not discard the typed filter"
            assert view._filter.input_widget.value == "aaa111", "the Input must still show what was typed"

    asyncio.run(scenario())


def test_events_filter_survives_refresh(tmp_path):
    db_path = tmp_path / "events_filter_persistence.db"
    _seed_two_emergency_aircraft(db_path)

    async def scenario() -> None:
        app = AdsbtrackApp(db_path)
        async with app.run_test() as pilot:
            await _settle(app, pilot)

            await pilot.press("3")
            await _settle(app, pilot)

            view = app.query_one(EventsView)
            table = app.query_one("#events-table", DataTable)
            unfiltered = table.row_count
            assert unfiltered == 4

            view.focus_filter()
            await pilot.pause()
            for ch in "aaa111":
                await pilot.press(ch)
            await pilot.pause()
            assert table.row_count == 2, "typing should narrow the table first"

            view.refresh_data()
            await _settle(app, pilot)

            assert table.row_count == 2, "a background refresh must not discard the typed filter"
            assert view._filter.input_widget.value == "aaa111", "the Input must still show what was typed"

    asyncio.run(scenario())


def test_flights_filter_survives_refresh(tmp_path):
    db_path = tmp_path / "flights_filter_persistence.db"
    _seed_two_flights_one_aircraft(db_path)

    async def scenario() -> None:
        app = AdsbtrackApp(db_path)
        async with app.run_test() as pilot:
            await _settle(app, pilot)
            app._open_icao("aaa111")
            await _settle(app, pilot)

            view = app.query_one(FlightsView)
            table = app.query_one("#flights-table", DataTable)
            unfiltered = table.row_count
            assert unfiltered == 2

            view.focus_filter()
            await pilot.pause()
            for ch in "ual1":
                await pilot.press(ch)
            await pilot.pause()
            assert table.row_count == 1, "typing should narrow the table first"

            view.refresh_data()
            await _settle(app, pilot)

            assert table.row_count == 1, "a background refresh must not discard the typed filter"
            assert view._filter.input_widget.value == "ual1", "the Input must still show what was typed"

    asyncio.run(scenario())
