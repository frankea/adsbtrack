"""Combined headless-boot smoke test for the PR #16 port.

Boots the real app once against one seeded DB and walks all four
TUI-visible features from the port in a single session: data-driven
aircraft flags, the events column order, the spoof view's expandable
detail row, and the map's first-flight route crumb. Each feature also
has focused unit/integration tests elsewhere (test_tui_queries.py,
test_tui_views_filtering.py, test_tui_events_view.py,
test_tui_spoof_view.py, test_tui_map.py, test_tui_widgets.py); this
test exists to prove they all still cooperate when exercised together
through real keypresses against one running app, not in isolation.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import pytest

pytest.importorskip("textual")  # tui extra: pyproject [project.optional-dependencies].tui

from textual.widgets import DataTable  # noqa: E402

from adsbtrack.db import Database  # noqa: E402
from adsbtrack.models import Flight  # noqa: E402
from adsbtrack.tui.app import AdsbtrackApp  # noqa: E402
from adsbtrack.tui.views.aircraft import AircraftOpenFlights  # noqa: E402
from adsbtrack.tui.views.events import EventsView  # noqa: E402
from adsbtrack.tui.views.map import MapView  # noqa: E402
from adsbtrack.tui.views.spoof import SpoofView  # noqa: E402


async def _settle(app, pilot) -> None:
    """Poll until no worker on the app is PENDING/RUNNING (see test_tui_app.py)."""
    for _ in range(200):
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


def _seed(db_path) -> None:
    with Database(db_path) as db:
        # aaa111: hovering helicopter with a manual type override, and a
        # route (KSPG -> KHKY) on the flight day for the map crumb.
        db.insert_flight(
            Flight(
                icao="aaa111",
                takeoff_time=datetime(2026, 3, 1, 12, 0, tzinfo=UTC),
                takeoff_lat=27.77,
                takeoff_lon=-82.63,
                takeoff_date="2026-03-01",
                landing_time=datetime(2026, 3, 1, 13, 0, tzinfo=UTC),
                landing_type="confirmed",
                callsign="RESCUE1",
                origin_icao="KSPG",
                destination_icao="KHKY",
                duration_minutes=60.0,
                max_altitude=2500,
                max_hover_secs=340,
                emergency_squawk="7700",
                mission_type="medevac",
            )
        )
        db.conn.execute(
            "INSERT INTO aircraft_registry (icao, registration, type_code, description) VALUES (?, ?, ?, ?)",
            ("aaa111", "N911HH", "B407", "BELL 407"),
        )
        db.upsert_hex_crossref({"icao": "aaa111", "type_code": "AS350", "type_description": "AIRBUS AS350"})
        trace = [
            [0, 27.77, -82.63, 500, 60, None, None, None, {}, "adsb_icao"],
            [30, 27.80, -82.60, 800, 65, None, None, None, {}, "adsb_icao"],
        ]
        db.insert_trace_day("aaa111", "2026-03-01", {"timestamp": 1772366400.0, "trace": trace}, source="adsbx")
        db.insert_spoofed_broadcast(
            icao="aaa111",
            takeoff_time="2026-03-02T00:49:47+00:00",
            landing_time="2026-03-02T01:41:52+00:00",
            takeoff_date="2026-03-02",
            callsign="GHOST1",
            takeoff_lat=27.77,
            takeoff_lon=-82.63,
            landing_lat=27.90,
            landing_lon=-82.50,
            max_altitude=250,
            data_points=350,
            sources="adsbfi,adsbx",
            origin_icao=None,
            destination_icao=None,
            reason="bimodal_integrity",
            reason_detail=json.dumps({"v2_samples": 350, "v2_sil0_pct": 25.1, "v2_nic0_pct": 27.1}),
        )
        db.refresh_aircraft_stats("aaa111")
        db.commit()


def test_pr16_port_features_cooperate_in_one_session(tmp_path):
    db_path = tmp_path / "pr16_smoke.db"
    _seed(db_path)

    async def scenario() -> None:
        app = AdsbtrackApp(db_path)
        async with app.run_test() as pilot:
            await _settle(app, pilot)

            # --- aircraft: data-driven HOVER/TYP flags + scope crumb prefix ---
            aircraft_table = app.query_one("#aircraft-table", DataTable)
            row = aircraft_table.get_row_at(0)
            flags_cell = row[-1].plain
            assert "HOVER" in flags_cell
            assert "TYP" in flags_cell
            aircraft_view = app.query_one("#view-aircraft")
            assert aircraft_view._header._crumb_prefix == "›"

            # --- events: TIME/ICAO/CALLSIGN/EVENT/SUMMARY column order ---
            aircraft_view.post_message(AircraftOpenFlights("aaa111"))
            await pilot.pause()
            await _settle(app, pilot)
            await pilot.press("3")
            await _settle(app, pilot)
            events_table = app.query_one("#events-table", DataTable)
            labels = [c.label.plain for c in events_table.ordered_columns]
            assert labels == ["TIME", "ICAO", "CALLSIGN", "EVENT", "SUMMARY"]
            assert app.query_one(EventsView)._header._crumb_prefix == "›"

            # --- spoof: expandable detail row ---
            await pilot.press("4")
            await _settle(app, pilot)
            spoof_view = app.query_one(SpoofView)
            spoof_table = app.query_one("#spoof-table", DataTable)
            assert spoof_table.row_count == 1
            spoof_table.move_cursor(row=0)
            spoof_view.toggle_detail()
            assert spoof_view._detail.display is True
            assert spoof_table.get_row_at(0)[-1].plain == "−"
            assert spoof_view._header._crumb_prefix == "›"

            # --- map: first-flight route crumb ---
            await pilot.press("5")
            await _settle(app, pilot)
            map_view = app.query_one(MapView)
            crumb_text = map_view._header._build().plain
            assert "KSPG > KHKY" in crumb_text

    asyncio.run(scenario())
