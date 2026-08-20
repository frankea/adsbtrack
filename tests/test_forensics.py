"""Tests for adsbtrack.forensics -- day-level trace fragment/integrity/squawk forensics.

Pure functions over decoded trace rows (readsb point-list format), so every
test here drives the module with synthetic data -- no DB involved. The CLI
smoke test for the `inspect` command lives in tests/test_cli.py alongside
the other command tests.
"""

from __future__ import annotations

import pytest

from adsbtrack.forensics import (
    DEFAULT_FRAGMENT_GAP_SECS,
    FragmentSummary,
    callsign_timeline,
    closest_approach,
    split_fragments,
    squawk_timeline,
    summarize_fragments,
)
from adsbtrack.geo import haversine_km

BASE_TS = 1_700_000_000.0


def _point(time_offset, lat, lon, alt, gs=None, detail=None):
    """Build a single trace point in the standard readsb layout, matching
    tests/test_parser.py's _make_trace_point shape."""
    return [time_offset, lat, lon, alt, gs, None, None, None, detail or {}]


# ---------------------------------------------------------------------------
# split_fragments
# ---------------------------------------------------------------------------


def test_split_fragments_breaks_on_gap():
    trace = [
        _point(0, 40.0, -74.0, 1000, gs=100),
        _point(60, 40.01, -74.0, 1000, gs=100),
        _point(120, 40.02, -74.0, 1000, gs=100),
        _point(1000, 40.5, -74.5, 5000, gs=200),  # gap of 880s > 300s
    ]
    fragments = split_fragments(trace, BASE_TS, DEFAULT_FRAGMENT_GAP_SECS)

    assert len(fragments) == 2
    assert len(fragments[0]) == 3
    assert len(fragments[1]) == 1
    assert fragments[0][0] == (BASE_TS + 0, trace[0])
    assert fragments[0][-1] == (BASE_TS + 120, trace[2])
    assert fragments[1][0] == (BASE_TS + 1000, trace[3])


def test_split_fragments_skips_non_list_rows():
    trace = [
        _point(0, 40.0, -74.0, 1000),
        "not a point",
        None,
        _point(60, 40.01, -74.0, 1000),
    ]
    fragments = split_fragments(trace, BASE_TS, DEFAULT_FRAGMENT_GAP_SECS)

    assert len(fragments) == 1
    assert len(fragments[0]) == 2


def test_split_fragments_empty_trace():
    assert split_fragments([], BASE_TS, DEFAULT_FRAGMENT_GAP_SECS) == []


# ---------------------------------------------------------------------------
# summarize_fragments
# ---------------------------------------------------------------------------


def test_summarize_fragments_counts_integrity_and_identity():
    detail = {"version": 2, "sil": 0, "nic": 0, "flight": "TEST1", "squawk": "7700"}
    trace = [
        _point(0, 40.0, -74.0, "ground", gs=0, detail=detail),
        _point(60, 40.01, -74.0, 1000, gs=100, detail=detail),
        _point(120, 40.02, -74.0, 2000, gs=150, detail=detail),
    ]

    summaries = summarize_fragments("adsbx", BASE_TS, trace, DEFAULT_FRAGMENT_GAP_SECS)

    assert len(summaries) == 1
    frag = summaries[0]
    assert isinstance(frag, FragmentSummary)
    assert frag.source == "adsbx"
    assert frag.n_points == 3
    assert frag.start_ts == BASE_TS
    assert frag.end_ts == BASE_TS + 120
    assert frag.start_lat == 40.0
    assert frag.start_lon == -74.0
    assert frag.end_lat == 40.02
    assert frag.end_lon == -74.0
    assert frag.v2_samples == 3
    assert frag.v2_sil0 == 3
    assert frag.v2_nic0 == 3
    assert frag.callsigns == ["TEST1"]
    assert frag.squawks == ["7700"]
    # "ground" counts as 0 for alt_min and is excluded from alt_max.
    assert frag.alt_min == 0
    assert frag.alt_max == 2000
    assert frag.gs_min == 0
    assert frag.gs_max == 150


def test_summarize_fragments_position_source_from_index_9_or_detail_type():
    trace = [
        [0, 40.0, -74.0, 1000, 100, None, None, None, {}, "adsb_icao"],
        [60, 40.01, -74.0, 1000, 100, None, None, None, {"type": "mlat"}],
    ]
    summaries = summarize_fragments("adsbx", BASE_TS, trace, DEFAULT_FRAGMENT_GAP_SECS)

    assert len(summaries) == 1
    assert summaries[0].position_sources == {"adsb_icao": 1, "mlat": 1}


def test_summarize_fragments_no_integrity_or_altitude_data():
    """No version-2 detail and no numeric/ground altitude: counts and
    identity sets stay empty/zero rather than raising."""
    trace = [_point(0, 40.0, -74.0, None), _point(60, 40.01, -74.0, None)]
    summaries = summarize_fragments("adsbx", BASE_TS, trace, DEFAULT_FRAGMENT_GAP_SECS)

    assert len(summaries) == 1
    frag = summaries[0]
    assert frag.v2_samples == 0
    assert frag.callsigns == []
    assert frag.squawks == []
    assert frag.alt_min is None
    assert frag.alt_max is None
    assert frag.gs_min is None
    assert frag.gs_max is None


def test_summarize_fragments_splits_into_multiple_summaries():
    trace = [
        _point(0, 40.0, -74.0, 1000),
        _point(1000, 41.0, -75.0, 2000),  # gap > default
    ]
    summaries = summarize_fragments("adsbx", BASE_TS, trace, DEFAULT_FRAGMENT_GAP_SECS)
    assert len(summaries) == 2
    assert summaries[0].n_points == 1
    assert summaries[1].n_points == 1


# ---------------------------------------------------------------------------
# squawk_timeline / callsign_timeline
# ---------------------------------------------------------------------------


def test_squawk_timeline_reports_change_points_only():
    trace = [
        _point(0, 40.0, -74.0, 1000, detail={"squawk": "1200"}),
        _point(60, 40.0, -74.0, 1000, detail={"squawk": "1200"}),
        _point(120, 40.0, -74.0, 1000, detail={"squawk": "7700"}),
        _point(180, 40.0, -74.0, 1000, detail={"squawk": "7700"}),
        _point(240, 40.0, -74.0, 1000, detail={"squawk": "1200"}),
    ]

    timeline = squawk_timeline(BASE_TS, trace)

    assert timeline == [
        (BASE_TS + 0, "1200"),
        (BASE_TS + 120, "7700"),
        (BASE_TS + 240, "1200"),
    ]


def test_squawk_timeline_ignores_points_without_squawk():
    trace = [
        _point(0, 40.0, -74.0, 1000, detail={"squawk": "1200"}),
        _point(60, 40.0, -74.0, 1000, detail={}),
        _point(120, 40.0, -74.0, 1000, detail={"squawk": "1200"}),
    ]
    assert squawk_timeline(BASE_TS, trace) == [(BASE_TS + 0, "1200")]


def test_callsign_timeline_reports_change_points_only():
    trace = [
        _point(0, 40.0, -74.0, 1000, detail={"flight": "UAL123"}),
        _point(60, 40.0, -74.0, 1000, detail={"flight": "UAL123 "}),
        _point(120, 40.0, -74.0, 1000, detail={"flight": "DAL456"}),
    ]

    timeline = callsign_timeline(BASE_TS, trace)

    assert timeline == [
        (BASE_TS + 0, "UAL123"),
        (BASE_TS + 120, "DAL456"),
    ]


# ---------------------------------------------------------------------------
# closest_approach
# ---------------------------------------------------------------------------


def test_closest_approach_finds_minimum_distance_point():
    target_lat, target_lon = 40.5, -74.5
    trace = [
        _point(0, 40.0, -74.0, 1000),
        _point(60, 40.3, -74.3, 2000),
        _point(120, 40.49, -74.49, 3000),  # closest
        _point(180, 41.0, -75.0, 4000),
    ]

    result = closest_approach(BASE_TS, trace, target_lat, target_lon)

    assert result is not None
    dist_km, ts, alt = result
    expected_dist = haversine_km(target_lat, target_lon, 40.49, -74.49)
    assert dist_km == pytest.approx(expected_dist)
    assert ts == BASE_TS + 120
    assert alt == 3000


def test_closest_approach_empty_trace_returns_none():
    assert closest_approach(BASE_TS, [], 40.0, -74.0) is None
