"""Tests for adsbtrack.mcp -- read-only MCP query surface.

Tests target the pure `_query_*` functions directly with a fixture DB.
The MCP stdio protocol itself is not exercised; the decorated tool
wrappers in serve() are thin JSON-dumping bindings over the pure
functions, so a failure there is a wiring bug visible immediately when
the server starts.

Coverage pins:
- happy path per tool (returns expected shape)
- empty-result path (hex not in DB, registration not resolvable)
- truncation path (total_matching > limit, truncated flag set,
  returned_count matches limit)
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from adsbtrack.db import Database
from adsbtrack.mcp import (
    _query_aircraft_stats,
    _query_events,
    _query_flights,
    _query_gaps,
    _registry_lookup,
)
from adsbtrack.models import Flight

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _seed_flights(db: Database, icao: str, count: int, start_year: int = 2024) -> None:
    """Insert `count` flights for the given icao, spread across days."""
    for i in range(count):
        db.insert_flight(
            Flight(
                icao=icao,
                takeoff_time=datetime(start_year, 6, 1 + (i % 28), 12, 0, 0, tzinfo=UTC),
                takeoff_lat=40.0 + i * 0.01,
                takeoff_lon=-74.0 - i * 0.01,
                takeoff_date=f"{start_year}-06-{1 + (i % 28):02d}",
                landing_time=datetime(start_year, 6, 1 + (i % 28), 14, 0, 0, tzinfo=UTC),
                landing_type="confirmed",
                callsign="TEST123",
                max_altitude=30000 + i,
                cruise_gs_kt=450,
                mission_type="transport",
                destination_icao="KBOS",
                origin_icao="KJFK",
            )
        )
    db.commit()


@pytest.fixture
def populated_db(tmp_path):
    db_path = tmp_path / "mcp.db"
    with Database(db_path) as db:
        _seed_flights(db, "abc123", 3)
        # aircraft_registry + aircraft_stats for abc123
        db.conn.execute(
            "INSERT INTO aircraft_registry (icao, registration, type_code, owner_operator, last_updated) "
            "VALUES (?, ?, ?, ?, ?)",
            ("abc123", "N12345", "GLF5", "Test Operator", "2026-04-10T00:00:00Z"),
        )
        db.conn.execute(
            "INSERT INTO aircraft_stats (icao, registration, type_code, total_flights, total_hours) "
            "VALUES (?, ?, ?, ?, ?)",
            ("abc123", "N12345", "GLF5", 3, 6.0),
        )
        # hex_crossref
        db.conn.execute(
            "INSERT INTO hex_crossref (icao, registration, type_code, source) VALUES (?, ?, ?, ?)",
            ("abc123", "N12345", "GLF5", "mictronics"),
        )
        # Event-triggering flight: emergency squawk
        db.insert_flight(
            Flight(
                icao="abc123",
                takeoff_time=datetime(2024, 7, 1, 12, 0, 0, tzinfo=UTC),
                takeoff_lat=40.0,
                takeoff_lon=-74.0,
                takeoff_date="2024-07-01",
                landing_type="confirmed",
                callsign="EMERG01",
                emergency_squawk="7700",
                destination_icao="KBOS",
            )
        )
        db.commit()
    return db_path


# ---------------------------------------------------------------------------
# _query_aircraft_stats
# ---------------------------------------------------------------------------


def test_query_aircraft_stats_happy_path(populated_db):
    with Database(populated_db) as db:
        result = _query_aircraft_stats(db, "abc123")
    assert result["hex"] == "abc123"
    assert "stats" in result
    assert result["stats"]["registration"] == "N12345"
    assert result["stats"]["total_flights"] == 3


def test_query_aircraft_stats_case_insensitive_hex(populated_db):
    with Database(populated_db) as db:
        result = _query_aircraft_stats(db, "ABC123")
    assert "stats" in result
    assert result["hex"] == "abc123"


def test_query_aircraft_stats_not_found(populated_db):
    with Database(populated_db) as db:
        result = _query_aircraft_stats(db, "ffffff")
    assert result["error"] == "not_found"
    assert "hex" in result
    assert "message" in result
    # Message should point at remediation
    assert "fetch" in result["message"].lower() or "extract" in result["message"].lower()


# ---------------------------------------------------------------------------
# _query_flights
# ---------------------------------------------------------------------------


def test_query_flights_happy_path(populated_db):
    with Database(populated_db) as db:
        result = _query_flights(db, "abc123")
    assert result["hex"] == "abc123"
    assert result["total_matching"] == 4  # 3 regular + 1 emergency
    assert result["returned_count"] == 4
    assert result["truncated"] is False
    # Newest first
    timestamps = [f["takeoff_time"] for f in result["flights"]]
    assert timestamps == sorted(timestamps, reverse=True)
    # Summary columns present, raw blob columns NOT (verify we're not
    # sending back all 90+ flight columns)
    first = result["flights"][0]
    assert "takeoff_time" in first
    assert "max_altitude" in first
    assert "path_length_km" not in first  # not in the summary projection


def test_query_flights_truncation(populated_db):
    """limit=2 on a 4-flight aircraft must set truncated=True and
    returned_count=2 and return the 2 newest."""
    with Database(populated_db) as db:
        result = _query_flights(db, "abc123", limit=2)
    assert result["total_matching"] == 4
    assert result["returned_count"] == 2
    assert result["truncated"] is True
    assert len(result["flights"]) == 2


def test_query_flights_date_range_filter(populated_db):
    """from_date filter must narrow the total_matching count."""
    with Database(populated_db) as db:
        result = _query_flights(db, "abc123", from_date="2024-07-01")
    # Only the July emergency flight is in range
    assert result["total_matching"] == 1
    assert result["flights"][0]["callsign"] == "EMERG01"


def test_query_flights_not_found(populated_db):
    with Database(populated_db) as db:
        result = _query_flights(db, "ffffff")
    assert result["total_matching"] == 0
    assert result["returned_count"] == 0
    assert result["truncated"] is False
    assert result["flights"] == []


def test_query_flights_limit_clamped_to_200(populated_db):
    """limit > 200 is clamped, not rejected."""
    with Database(populated_db) as db:
        result = _query_flights(db, "abc123", limit=99999)
    # 4 flights; limit=200 means all returned, not truncated
    assert result["returned_count"] == 4
    assert result["truncated"] is False


# ---------------------------------------------------------------------------
# _query_events
# ---------------------------------------------------------------------------


def test_query_events_happy_path(populated_db):
    with Database(populated_db) as db:
        result = _query_events(db, "abc123")
    assert result["hex"] == "abc123"
    # Seeded one emergency_squawk event
    assert result["total_matching"] == 1
    assert result["events"][0]["event_type"] == "emergency_squawk"
    assert result["events"][0]["severity"] == "emergency"


def test_query_events_bad_since_date(populated_db):
    with Database(populated_db) as db:
        result = _query_events(db, "abc123", since_date="not-a-date")
    assert result["error"] == "bad_input"
    assert "since_date" in result["message"].lower() or "iso" in result["message"].lower()


def test_query_events_not_found(populated_db):
    with Database(populated_db) as db:
        result = _query_events(db, "ffffff")
    assert result["total_matching"] == 0
    assert result["events"] == []


# ---------------------------------------------------------------------------
# _query_gaps
# ---------------------------------------------------------------------------


def test_query_gaps_not_found(populated_db):
    """Seeded flights have no trace_days (just flight summaries); detect_gaps
    returns [] because there's no trace to walk. Empty-result path."""
    with Database(populated_db) as db:
        result = _query_gaps(db, "abc123")
    assert result["total_matching"] == 0
    assert result["returned_count"] == 0
    assert result["truncated"] is False
    assert result["gaps"] == []


# ---------------------------------------------------------------------------
# _registry_lookup
# ---------------------------------------------------------------------------


def test_registry_lookup_by_hex(populated_db):
    with Database(populated_db) as db:
        result = _registry_lookup(db, "abc123")
    assert result["hex"] == "abc123"
    assert "aircraft_registry" in result
    assert result["aircraft_registry"]["registration"] == "N12345"
    assert "hex_crossref" in result


def test_registry_lookup_by_hex_uppercase(populated_db):
    with Database(populated_db) as db:
        result = _registry_lookup(db, "ABC123")
    assert "aircraft_registry" in result
    assert result["hex"] == "abc123"


def test_registry_lookup_by_registration(populated_db):
    with Database(populated_db) as db:
        result = _registry_lookup(db, "N12345")
    assert "aircraft_registry" in result
    assert result["aircraft_registry"]["icao"] == "abc123"


def test_registry_lookup_unknown_reg(populated_db):
    with Database(populated_db) as db:
        result = _registry_lookup(db, "G-NOPE")
    assert result["error"] == "not_found"
    assert "message" in result


def test_registry_lookup_unknown_hex(populated_db):
    with Database(populated_db) as db:
        result = _registry_lookup(db, "ffffff")
    assert result["error"] == "not_found"
    assert result["hex"] == "ffffff"
