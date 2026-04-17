"""Tests for adsbtrack.gaps - within-flight ADS-B gap detection and classification.

The classifier is intentionally conservative: a gap is only tagged as
`likely_transponder_off` when multiple strong positive signals agree
(high altitude AND good ADS-B coverage on both sides AND close to a
known airport). Everything ambiguous falls back to `unknown`. Low
altitude or far-from-airport gaps are tagged `coverage_hole`.

A gap-analysis tool that confidently mislabels is worse than no tool;
these tests pin the conservative discrimination contract.
"""

from __future__ import annotations

import pytest

from adsbtrack.config import Config
from adsbtrack.db import Database
from adsbtrack.gaps import (
    Gap,
    _classify_gap,
    _source_category,
    _source_mix,
    detect_gaps,
)

# ---------------------------------------------------------------------------
# _source_category: normalize raw readsb type tags to adsb / mlat / other
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("adsb_icao", "adsb"),
        ("adsb_icao_nt", "adsb"),
        ("adsb_other", "adsb"),
        ("mlat", "mlat"),
        ("mode_s", "other"),
        ("adsr_icao", "other"),
        ("tisb_icao", "other"),
        ("tisb_trackfile", "other"),
        ("tisb_other", "other"),
        (None, "other"),
        ("", "other"),
        ("unknown_future_tag", "other"),
    ],
)
def test_source_category_normalization(raw, expected):
    assert _source_category(raw) == expected


# ---------------------------------------------------------------------------
# _source_mix: count source categories over N surrounding points
# ---------------------------------------------------------------------------


def test_source_mix_all_adsb():
    points = [{"position_source": "adsb_icao"} for _ in range(10)]
    assert _source_mix(points) == {"adsb": 10, "mlat": 0, "other": 0}


def test_source_mix_mixed():
    points = [
        {"position_source": "adsb_icao"},
        {"position_source": "adsb_icao"},
        {"position_source": "mlat"},
        {"position_source": "mode_s"},
        {"position_source": None},
    ]
    assert _source_mix(points) == {"adsb": 2, "mlat": 1, "other": 2}


def test_source_mix_empty_input():
    assert _source_mix([]) == {"adsb": 0, "mlat": 0, "other": 0}


# ---------------------------------------------------------------------------
# _classify_gap: the rule matrix. Each case pins intent.
# ---------------------------------------------------------------------------


def test_classify_no_altitude_is_unknown():
    cls, reason = _classify_gap(
        alt_ft=None,
        pre_adsb_frac=1.0,
        post_adsb_frac=1.0,
        nearest_airport_nm=50.0,
        duration_secs=600.0,
    )
    assert cls == "unknown"
    assert "altitude" in reason.lower()


def test_classify_low_altitude_is_coverage_hole():
    """Below 3000 ft MSL: line-of-sight to receivers fails in most
    terrain. Even strong pre/post ADS-B coverage doesn't override this --
    low-altitude gaps are genuinely almost always coverage, not
    transponder events. Preserve the rule by pinning it."""
    cls, reason = _classify_gap(
        alt_ft=1500,
        pre_adsb_frac=1.0,
        post_adsb_frac=1.0,
        nearest_airport_nm=5.0,
        duration_secs=600.0,
    )
    assert cls == "coverage_hole"
    assert "1500" in reason or "low altitude" in reason.lower()


def test_classify_high_alt_good_coverage_near_airport_is_transponder_off():
    """The only combination that earns `likely_transponder_off`:
    FL150+ AND >=70% ADS-B on both sides AND <200nm from an airport."""
    cls, reason = _classify_gap(
        alt_ft=35000,
        pre_adsb_frac=1.0,
        post_adsb_frac=0.9,
        nearest_airport_nm=50.0,
        duration_secs=900.0,
    )
    assert cls == "likely_transponder_off"
    assert "FL350" in reason or "35000" in reason


def test_classify_high_alt_good_coverage_far_from_airport_is_coverage_hole():
    """Strong ADS-B pre/post but far from airports (>300nm): remote or
    over-ocean. Not transponder-off."""
    cls, reason = _classify_gap(
        alt_ft=38000,
        pre_adsb_frac=0.95,
        post_adsb_frac=0.95,
        nearest_airport_nm=450.0,
        duration_secs=1200.0,
    )
    assert cls == "coverage_hole"


def test_classify_high_alt_weak_adsb_is_coverage_hole():
    """Mostly-MLAT pre/post: ADS-B wasn't really working even at the
    edges, so the gap itself is just more of the same -- coverage
    issue, not a transponder event."""
    cls, reason = _classify_gap(
        alt_ft=35000,
        pre_adsb_frac=0.1,
        post_adsb_frac=0.2,
        nearest_airport_nm=100.0,
        duration_secs=600.0,
    )
    assert cls == "coverage_hole"


def test_classify_mid_alt_mixed_signals_is_unknown():
    """FL100 (10000 ft), medium pre-ADS-B coverage, mid-range airport
    distance. Ambiguous: could be climbing out through line-of-sight
    shadow, could be transponder off. Default to unknown."""
    cls, reason = _classify_gap(
        alt_ft=10000,
        pre_adsb_frac=0.6,
        post_adsb_frac=0.5,
        nearest_airport_nm=150.0,
        duration_secs=400.0,
    )
    assert cls == "unknown"


def test_classify_no_airport_found_is_coverage_hole():
    """nearest_airport_nm=None means the airport search radius found
    nothing. That's a remote area. Not transponder_off territory."""
    cls, reason = _classify_gap(
        alt_ft=37000,
        pre_adsb_frac=0.9,
        post_adsb_frac=0.9,
        nearest_airport_nm=None,
        duration_secs=1000.0,
    )
    assert cls == "coverage_hole"


def test_classify_fl150_boundary_just_below_is_not_transponder_off():
    """Rule requires alt >= 15000. 14900 should NOT trigger
    transponder_off even with perfect other signals."""
    cls, reason = _classify_gap(
        alt_ft=14900,
        pre_adsb_frac=1.0,
        post_adsb_frac=1.0,
        nearest_airport_nm=50.0,
        duration_secs=600.0,
    )
    assert cls != "likely_transponder_off"


def test_classify_fl150_boundary_at_threshold_is_transponder_off():
    """Exactly 15000 ft with all other signals green should trigger."""
    cls, reason = _classify_gap(
        alt_ft=15000,
        pre_adsb_frac=0.8,
        post_adsb_frac=0.8,
        nearest_airport_nm=100.0,
        duration_secs=600.0,
    )
    assert cls == "likely_transponder_off"


@pytest.mark.parametrize(
    "pre_frac, post_frac, expected",
    [
        # Step-function at 0.70 (the _ADSB_STRONG_FRAC threshold, with >=).
        # pre-side boundary, post fixed at 0.80 (safely above).
        (0.69, 0.80, "unknown"),
        (0.70, 0.80, "likely_transponder_off"),
        (0.71, 0.80, "likely_transponder_off"),
        # post-side boundary, pre fixed at 0.80.
        (0.80, 0.69, "unknown"),
        (0.80, 0.70, "likely_transponder_off"),
        (0.80, 0.71, "likely_transponder_off"),
        # Symmetric just-above on both sides.
        (0.70, 0.70, "likely_transponder_off"),
        # Symmetric just-below on both sides (one side short enough to
        # fail the `weak-both-sides` coverage_hole rule which triggers
        # when BOTH sides < 0.3; here they're 0.69 so we fall through
        # to unknown, not coverage_hole).
        (0.69, 0.69, "unknown"),
    ],
)
def test_classify_adsb_threshold_boundary(pre_frac, post_frac, expected):
    """Pin the step-function behavior of the ADS-B fraction threshold.

    Below 0.70 on either side downgrades from `likely_transponder_off`
    to `unknown` (the conservative bucket). At exactly 0.70 the rule
    triggers because the check is `>=`. Both sides must pass; failing
    on either axis is enough to downgrade. The 0.69/0.70/0.71 sweep
    catches off-by-one or > / >= drift in the classifier."""
    cls, _ = _classify_gap(
        alt_ft=30000,
        pre_adsb_frac=pre_frac,
        post_adsb_frac=post_frac,
        nearest_airport_nm=50.0,
        duration_secs=600.0,
    )
    assert cls == expected


# ---------------------------------------------------------------------------
# detect_gaps: integration with a fixture DB holding a hand-crafted trace
# ---------------------------------------------------------------------------


def _trace_point(ts_offset, lat, lon, alt, gs, source="adsb_icao"):
    """Build a 10-element trace point matching the adsbx layout."""
    return [ts_offset, lat, lon, alt, gs, 0.0, 0, 0, {}, source]


@pytest.fixture
def populated_db(tmp_path):
    """Single-day trace with a deliberate 600-second gap at FL350 near KSAT."""
    db_path = tmp_path / "gaps.db"
    base_ts = 1700000000.0  # arbitrary epoch for the day
    # Day trace:
    #   - 30 points of climb/cruise from ~34N -98W (San Antonio area)
    #   - gap of 600 seconds
    #   - 20 more cruise points continuing east
    trace = []
    for i in range(30):
        # climb 0-600s, cruise at 35000 from there
        alt = min(35000, 1000 + i * 1200)
        lat = 29.5 + i * 0.01
        lon = -98.5 + i * 0.02
        trace.append(_trace_point(i * 30.0, lat, lon, alt, 400.0, "adsb_icao"))
    # Gap: next point is 600 seconds later
    gap_end_offset = 30 * 30.0 + 600.0
    for i in range(20):
        lat = 29.80 + i * 0.01
        lon = -97.86 + i * 0.02
        trace.append(_trace_point(gap_end_offset + i * 30.0, lat, lon, 35000, 400.0, "adsb_icao"))

    with Database(db_path) as db:
        # Seed the airports table with KSAT so the airport-distance lookup works.
        db.conn.execute(
            """INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type, iata_code, municipality)
               VALUES ('KSAT', 'San Antonio Intl', 29.533699, -98.469803, 'large_airport', 'SAT', 'San Antonio')"""
        )
        db.insert_trace_day(
            "abc123",
            "2023-11-14",
            {"timestamp": base_ts, "trace": trace, "r": "N12345", "t": "C172"},
            source="adsbx",
        )
        db.commit()
    return db_path


def test_detect_gaps_finds_the_planted_gap(populated_db):
    with Database(populated_db) as db:
        gaps = detect_gaps(db, "abc123", min_gap_secs=300, config=Config())

    assert len(gaps) == 1
    gap = gaps[0]
    assert isinstance(gap, Gap)
    assert gap.icao == "abc123"
    assert 550 < gap.duration_secs < 650  # 600s planted, allow some slack
    assert gap.start_alt_ft == 35000
    assert gap.end_alt_ft == 35000
    assert gap.classification == "likely_transponder_off"
    # Planted near KSAT (<200nm), FL350, all-ADSB pre and post -- every
    # positive signal. Conservative classifier should commit.
    assert "FL" in gap.classification_reason or "35000" in gap.classification_reason


def test_detect_gaps_ignores_sub_threshold_gaps(populated_db):
    """With min_gap_secs=1200 the planted 600s gap should be skipped."""
    with Database(populated_db) as db:
        gaps = detect_gaps(db, "abc123", min_gap_secs=1200, config=Config())
    assert gaps == []


def test_detect_gaps_returns_empty_for_unknown_icao(populated_db):
    with Database(populated_db) as db:
        gaps = detect_gaps(db, "ffffff", min_gap_secs=300, config=Config())
    assert gaps == []


def _inter_day_populated_db(tmp_path):
    """Two single-point 'days' separated by 3 days: this simulates an
    aircraft that flew, parked for 3 days, then flew again. The
    3-day 'gap' between the last point of day 1 and the first point
    of day 4 is NOT a within-flight signal loss -- it's just parking.
    detect_gaps should filter it out by default.
    """
    db_path = tmp_path / "parked.db"
    base_ts = 1700000000.0
    trace_day1 = [_trace_point(0.0, 29.5, -98.5, 1000, 100.0)]
    trace_day4 = [_trace_point(0.0, 29.5, -98.5, 1000, 100.0)]
    with Database(db_path) as db:
        db.insert_trace_day("parked1", "2023-11-14", {"timestamp": base_ts, "trace": trace_day1}, source="adsbx")
        db.insert_trace_day(
            "parked1",
            "2023-11-17",
            {"timestamp": base_ts + 3 * 86400, "trace": trace_day4},
            source="adsbx",
        )
        db.commit()
    return db_path


def test_detect_gaps_filters_between_flight_parked_gaps(tmp_path):
    """3-day gap between two trace points is >>> Config.max_point_gap_minutes
    (30 min default). Default detect_gaps should filter it out as
    between-flight. Opting in via include_between_flight=True surfaces it."""
    db_path = _inter_day_populated_db(tmp_path)
    with Database(db_path) as db:
        default_gaps = detect_gaps(db, "parked1", min_gap_secs=300, config=Config())
        all_gaps = detect_gaps(
            db,
            "parked1",
            min_gap_secs=300,
            config=Config(),
            include_between_flight=True,
        )
    assert default_gaps == [], f"between-flight gap leaked into default output: {default_gaps}"
    assert len(all_gaps) == 1
    assert all_gaps[0].duration_secs > 30 * 60
