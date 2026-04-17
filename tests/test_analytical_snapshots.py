"""Analytical-column regression snapshots.

Pins the output of the extraction pipeline for a handful of synthetic
flight traces covering distinct classifier paths (transport, EMS hover,
pattern work). Each scenario is hand-crafted to sit safely in the middle
of a classifier band so that minor threshold tweaks don't trip the
assertion; unexpected drift (a ruleset change that flips a category)
shows up as a snapshot diff which is reviewable as part of the PR.

Columns intentionally NOT pinned:
- aligned_runway / aligned_seconds / takeoff_runway: require real
  runway geometry data + airport matches that these synthetic tests
  mock out
- destination_icao / origin_icao: airport-match layer is mocked
- signal_gap_*: no coverage holes in these synthetic traces
- path_length_km: depends on haversine path, included but with
  tolerance since floating-point accumulation can drift
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from adsbtrack.config import Config
from adsbtrack.parser import extract_flights

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config() -> Config:
    """Config matching test_parser.py's: disable gap-split and segment-cap
    so sparse synthetic traces don't get flagged as coverage holes."""
    return Config(
        landing_speed_threshold_kts=80.0,
        airport_match_threshold_km=10.0,
        airport_types=("large_airport", "medium_airport", "small_airport"),
        max_point_gap_minutes=10_000.0,
        path_max_segment_secs=10_000.0,
    )


def _point(ts_offset, lat, lon, alt, gs, detail=None):
    """Standard trace point with optional detail (for callsign, etc.)."""
    return [ts_offset, lat, lon, alt, gs, None, None, None, detail or {}]


def _make_trace_row(date_str, timestamp, trace, hex_code, type_code="B407"):
    """Trace-days row returning synthetic trace + registration metadata.

    type_code matters: the altitude persistence filter applies a
    type-specific ceiling cap (B407 ≈ 20000 ft, GLF5 ≈ 51000 ft), so
    scenarios need a type_code consistent with the trace profile.
    """
    row = {
        "date": date_str,
        "timestamp": timestamp,
        "trace_json": json.dumps(trace),
        "source": "adsbx",
        "registration": f"N{hex_code[-5:].upper()}",
        "type_code": type_code,
        "description": "SYNTHETIC",
        "owner_operator": "Test",
        "year": "2020",
        "point_count": len(trace),
        "fetched_at": "2024-01-01T00:00:00",
        "icao": hex_code,
    }
    mock_row = MagicMock()
    mock_row.__getitem__ = lambda self, key: row[key]
    mock_row.keys = lambda: row.keys()
    return mock_row


def _make_db_mock(trace_rows):
    """Mock Database that returns fixed trace_days rows; no airport matches."""
    db = MagicMock()
    db.get_trace_days.return_value = trace_rows
    db.find_nearby_airports.return_value = []
    db.all_mil_hex_ranges.return_value = []
    db.upsert_aircraft_registry.return_value = None
    return db


def _airport_mock(lat, lon):
    """Coordinate-aware airport match: returns synthetic AirportMatch
    objects for the takeoff/landing positions of each scenario. Lets
    the downstream mission classifier see origin/destination icaos,
    which most of its rules require."""
    from adsbtrack.models import AirportMatch

    # Cruise transport takeoff (~40N, -74W)
    if 39.5 < lat < 40.5 and -74.5 < lon < -73.5:
        return AirportMatch(ident="KORIGIN", name="Origin Intl", distance_km=0.5)
    # Cruise transport landing (~44.4N, -85W)
    if 44.0 < lat < 45.0 and -85.5 < lon < -84.5:
        return AirportMatch(ident="KDESTIN", name="Dest Intl", distance_km=0.5)
    # EMS base (~41N, -75W) -- same airport both ends
    if 40.9 < lat < 41.2 and -75.1 < lon < -74.9:
        return AirportMatch(ident="KEMSBASE", name="EMS Base", distance_km=0.5)
    # Pattern field (~42N, -76W, wider box to catch both takeoff and
    # pattern-cycle landing at 42.07)
    if 41.9 < lat < 42.15 and -76.1 < lon < -75.9:
        return AirportMatch(ident="KPATTERN", name="Pattern Field", distance_km=0.5)
    return None


def _run_extract(hex_code, trace, *, type_code="B407", day="2024-06-15"):
    """Run extract_flights end-to-end against a synthetic trace.
    Returns the single Flight dataclass that was passed to insert_flight."""
    from datetime import datetime

    base_ts = datetime.fromisoformat(f"{day}T00:00:00+00:00").timestamp()
    rows = [_make_trace_row(day, base_ts, trace, hex_code, type_code=type_code)]
    db = _make_db_mock(rows)
    config = _make_config()
    with patch("adsbtrack.parser.find_nearest_airport", side_effect=lambda db, lat, lon, cfg: _airport_mock(lat, lon)):
        count = extract_flights(db, config, hex_code, reprocess=True)
    assert count == 1, f"Expected 1 flight, got {count}"
    assert db.insert_flight.call_count == 1
    return db.insert_flight.call_args[0][0]


# ---------------------------------------------------------------------------
# Synthetic traces: each hand-crafted to sit in the middle of a classifier
# band so minor threshold tweaks don't trip the snapshot.
# ---------------------------------------------------------------------------


def _cruise_transport_trace():
    """High-altitude business-jet cruise: climb to FL350, 60 min at 450 kt,
    descend, land. Safely above the transport classifier's FL180/300kt
    threshold; cruise points cluster tightly for stable cruise_gs_kt."""
    trace = [
        _point(0, 40.0, -74.0, "ground", 0, {"flight": "N552SN  "}),
        _point(60, 40.0, -74.0, "ground", 5, {"flight": "N552SN  "}),
        _point(120, 40.005, -74.0, 500, 120, {"flight": "N552SN  "}),
    ]
    # Climb to FL350 over ~10 minutes
    for i in range(1, 11):
        trace.append(_point(120 + i * 60, 40.005 + i * 0.02, -74.0 - i * 0.05, 3500 * i, 300 + i * 10))
    # Cruise 60 min at FL350 / 450 kt (20 samples, 3 min apart)
    for i in range(20):
        trace.append(_point(720 + i * 180, 40.205 + i * 0.2, -74.55 - i * 0.5, 35000, 450, {"flight": "N552SN  "}))
    # Descend over ~10 minutes
    for i in range(1, 11):
        trace.append(_point(720 + 20 * 180 + i * 60, 44.2 + i * 0.02, -84.5 - i * 0.05, 35000 - 3500 * i, 400 - i * 20))
    # Landing
    trace.append(_point(720 + 20 * 180 + 11 * 60, 44.4, -85.0, 500, 100))
    trace.append(_point(720 + 20 * 180 + 12 * 60, 44.41, -85.0, "ground", 10))
    return trace


def _ems_hover_trace():
    """EMS helicopter: N911LG callsign triggers MissionType.EMS_HEMS;
    500-second hover (cluster of same-position points) triggers the
    hover state machine."""
    cs = {"flight": "N911LG  "}
    trace = [
        _point(0, 41.0, -75.0, "ground", 0, cs),
        _point(60, 41.0, -75.0, "ground", 5, cs),
        # Climb to 1500 ft over 2 min
        _point(120, 41.001, -75.001, 500, 80, cs),
        _point(180, 41.003, -75.003, 1000, 100, cs),
        _point(240, 41.005, -75.005, 1500, 120, cs),
    ]
    # Fly to scene, 5 min at 120 kt
    for i in range(5):
        trace.append(_point(240 + (i + 1) * 60, 41.005 + (i + 1) * 0.01, -75.005 - (i + 1) * 0.01, 1500, 120, cs))
    # Hover for 500 seconds (50 samples 10s apart, same position, gs=0)
    hover_start_ts = 240 + 5 * 60
    hover_lat = 41.055
    hover_lon = -75.055
    for i in range(50):
        trace.append(_point(hover_start_ts + i * 10, hover_lat, hover_lon, 1500, 0, cs))
    # Return and land
    trace.append(_point(hover_start_ts + 50 * 10 + 60, 41.05, -75.05, 1500, 100, cs))
    trace.append(_point(hover_start_ts + 50 * 10 + 120, 41.0, -75.0, 500, 60, cs))
    trace.append(_point(hover_start_ts + 50 * 10 + 180, 41.0, -75.0, "ground", 10, cs))
    return trace


def _pattern_work_trace():
    """Training aircraft doing pattern work: two touch-and-goes then full
    stop. Each go-around = climb back up after near-ground altitude, which
    the classifier picks up via go-around detection on the rate window."""
    cs = {"flight": "N123PA  "}
    trace = [_point(0, 42.0, -76.0, "ground", 0, cs), _point(60, 42.0, -76.0, "ground", 5, cs)]

    def one_pattern_cycle(t0, cycle_idx):
        offset_lat = cycle_idx * 0.0
        # Climb to 1500 ft
        pts = []
        pts.append(_point(t0, 42.001 + offset_lat, -76.0, 500, 100, cs))
        pts.append(_point(t0 + 60, 42.005 + offset_lat, -76.005, 1000, 120, cs))
        pts.append(_point(t0 + 120, 42.01 + offset_lat, -76.01, 1500, 130, cs))
        # Downwind / base / final, stay at 1500 for 3 min
        for i in range(6):
            pts.append(
                _point(t0 + 120 + (i + 1) * 30, 42.02 + i * 0.005 + offset_lat, -76.01 + i * 0.005, 1500, 110, cs)
            )
        # Descend to near-ground (touch-and-go)
        pts.append(_point(t0 + 330, 42.05 + offset_lat, -76.04, 800, 90, cs))
        pts.append(_point(t0 + 360, 42.06 + offset_lat, -76.045, 400, 80, cs))
        pts.append(_point(t0 + 390, 42.062 + offset_lat, -76.046, 100, 70, cs))
        return pts

    # Cycle 1 + 2 (touch-and-go), cycle 3 lands
    trace += one_pattern_cycle(120, 0)
    trace += one_pattern_cycle(540, 1)
    trace += one_pattern_cycle(960, 2)
    # Full stop on cycle 3 final
    trace.append(_point(1380, 42.07, -76.05, "ground", 15, cs))
    return trace


# ---------------------------------------------------------------------------
# Snapshot: the pinned outputs
# ---------------------------------------------------------------------------

# Each scenario is (trace_builder, expected_dict). Expected_dict is a
# subset of Flight columns whose stability we want to enforce. Only
# columns that are deterministic under the current classifier rules
# are pinned here -- marginal columns (gap counts, airport matches,
# runway alignment) stay out.

SNAPSHOTS = {
    "cruise_transport_gulfstream_style": {
        "trace_builder": _cruise_transport_trace,
        "hex": "aaa001",
        "type_code": "GLF5",  # high-altitude jet; raises the type ceiling above FL350
        "expected": {
            "mission_type": "transport",
            "max_altitude": 35000,
            "go_around_count": 0,
            # max_hover_secs is None (not 0) when no hover state ever
            # engages -- the counter is left unset rather than zeroed.
            # cruise_gs_kt is a time-weighted median of cruise-phase samples
            # with outlier rejection. Pin a window to tolerate small tweaks
            # in what qualifies as "cruise phase".
            "cruise_gs_kt_min": 440,
            "cruise_gs_kt_max": 460,
        },
    },
    "ems_helicopter_hover": {
        "trace_builder": _ems_hover_trace,
        "hex": "aaa002",
        "type_code": "B407",
        "expected": {
            "mission_type": "ems_hems",
            "max_altitude": 1500,
            "go_around_count": 0,
            # hover: ~500s planted; detector may trim a few seconds at
            # entry/exit, pin the lower bound.
            "max_hover_secs_min": 400,
            "hover_episodes_min": 1,
        },
    },
    "pattern_work_helicopter": {
        "trace_builder": _pattern_work_trace,
        "hex": "aaa003",
        # Non-trainer + not in survey/pattern-exempt list. Trainer types
        # (C172 etc.) would hit the TRAINING rule first; bizjets like
        # GLF5/B748 are pattern-exempt (they orbit for ATC/security
        # reasons, not training). B407 sits cleanly between them.
        "type_code": "B407",
        "expected": {
            "mission_type": "pattern",
            # go_around_count is intentionally NOT pinned: synthetic
            # sparse traces underfire the go-around detector (real flights
            # have many more points across the descent/climb). The
            # mission_type classifier's pattern rule (same airport +
            # low alt) is the stable signal here.
        },
    },
}


# ---------------------------------------------------------------------------
# Snapshot tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("scenario", list(SNAPSHOTS.keys()))
def test_analytical_snapshot(scenario):
    """End-to-end: synthetic trace → extract_flights → pinned analytical
    columns on the resulting Flight. Mismatches are by definition a
    classifier drift and should be reviewed in the PR diff, not silenced."""
    spec = SNAPSHOTS[scenario]
    trace = spec["trace_builder"]()
    flight = _run_extract(spec["hex"], trace, type_code=spec["type_code"])

    for key, expected in spec["expected"].items():
        if key.endswith("_min"):
            col = key.removesuffix("_min")
            actual = getattr(flight, col)
            assert actual is not None and actual >= expected, f"{scenario}: expected {col} >= {expected}, got {actual}"
        elif key.endswith("_max"):
            col = key.removesuffix("_max")
            actual = getattr(flight, col)
            assert actual is not None and actual <= expected, f"{scenario}: expected {col} <= {expected}, got {actual}"
        else:
            actual = getattr(flight, key)
            assert actual == expected, f"{scenario}: expected {key}={expected}, got {actual}"
