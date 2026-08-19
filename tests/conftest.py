"""Shared pytest fixtures for the adsbtrack test suite."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from adsbtrack.db import Database
from adsbtrack.models import Flight


@pytest.fixture
def seeded_db(tmp_path):
    """DB with two aircraft: one clean, one with a rejected spoof broadcast."""
    db_path = tmp_path / "tui.db"
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
        db.insert_spoofed_broadcast(
            icao="bbb222",
            takeoff_time="2026-04-21T00:49:47.580000+00:00",
            landing_time="2026-04-21T01:41:52.140000+00:00",
            takeoff_date="2026-04-21",
            callsign="EK01",
            takeoff_lat=25.25,
            takeoff_lon=55.38,
            landing_lat=27.14,
            landing_lon=55.55,
            max_altitude=250,
            data_points=350,
            sources="adsbfi,adsbx",
            origin_icao=None,
            destination_icao=None,
            reason="bimodal_integrity",
            reason_detail=json.dumps(
                {
                    "date": "2026-04-21",
                    "v2_samples": 350,
                    "v2_sil0_pct": 25.14,
                    "v2_nic0_pct": 27.14,
                    "sources": ["adsbfi", "adsbx"],
                    "source_rates": [["adsbfi", 26.04], ["adsbx", 24.31]],
                }
            ),
        )
        db.conn.execute(
            "INSERT INTO aircraft_registry (icao, registration, type_code, description) VALUES (?, ?, ?, ?)",
            ("bbb222", "A6-EEN", "A388", "AIRBUS A-380-800"),
        )
        db.refresh_aircraft_stats("aaa111")
        db.commit()
    return db_path
