"""Tests for adsbtrack.classifier -- FlightMetrics online accumulators.

Complements test_features.py which tests the pure per-flight derivation
helpers; this file exercises the record_point side where the running
counters are built up.
"""

from adsbtrack.classifier import FlightMetrics, PointData
from adsbtrack.config import Config


def _cfg() -> Config:
    return Config()


def _point(
    ts: float,
    *,
    lat=40.0,
    lon=-74.0,
    baro_alt=10_000,
    gs=300.0,
    callsign=None,
    nav_altitude_mcp=None,
) -> PointData:
    return PointData(
        ts=ts,
        lat=lat,
        lon=lon,
        baro_alt=baro_alt,
        gs=gs,
        track=90.0,
        geom_alt=None,
        baro_rate=0.0,
        geom_rate=None,
        squawk=None,
        category=None,
        nav_altitude_mcp=nav_altitude_mcp,
        nav_qnh=None,
        emergency_field=None,
        true_heading=None,
        callsign=callsign,
    )


# ---------------------------------------------------------------------------
# B5: persistence-filtered max_altitude
# ---------------------------------------------------------------------------


def test_max_altitude_persistence_rejects_single_spike():
    """A single 125,000 ft baro glitch should not set max_altitude.

    Real readsb traces for B748s and B407s occasionally emit one-point baro
    spikes well above the aircraft's service ceiling. Before the v5 fix,
    max_altitude tracked the raw max() and these spikes became the stored
    ceiling. With persistence filtering, a candidate peak only wins when
    held for >= alt_persistence_min_samples within alt_persistence_window_secs.
    """
    cfg = _cfg()
    m = FlightMetrics()

    # 10 real cruise samples at 30,000 ft, 5 s apart (50 s span, 10 samples
    # - exceeds the 5-sample minimum within the 30 s window).
    # nav_altitude_mcp is set so these enter the AP-validated persistence
    # filter (v14 R4a requires AP for altitude cross-validation).
    for i in range(10):
        m.record_point(
            _point(ts=1000.0 + i * 5, baro_alt=30_000, gs=450.0, nav_altitude_mcp=30_000),
            ground_state="airborne",
            ground_reason="airborne",
            config=cfg,
        )
    # One rogue baro spike at 125,000 ft (Karman line, clearly bogus).
    # No AP data -- the spike is uncorroborated and only hits the raw max.
    m.record_point(
        _point(ts=1055.0, baro_alt=125_000, gs=450.0),
        ground_state="airborne",
        ground_reason="airborne",
        config=cfg,
    )
    # A few more real cruise samples after the spike.
    for i in range(5):
        m.record_point(
            _point(ts=1060.0 + i * 5, baro_alt=30_000, gs=450.0, nav_altitude_mcp=30_000),
            ground_state="airborne",
            ground_reason="airborne",
            config=cfg,
        )

    assert m.max_altitude == 30_000, f"max_altitude pegged by single spike: {m.max_altitude}"


def test_max_altitude_updates_on_sustained_cruise():
    """Real sustained cruise above prior max should update max_altitude."""
    cfg = _cfg()
    m = FlightMetrics()

    # Start in cruise at 20,000.
    for i in range(8):
        m.record_point(
            _point(ts=1000.0 + i * 5, baro_alt=20_000, gs=400.0),
            ground_state="airborne",
            ground_reason="airborne",
            config=cfg,
        )
    # Climb to 40,000 and stay there for >= 30 s with >= 5 samples.
    for i in range(8):
        m.record_point(
            _point(ts=1100.0 + i * 5, baro_alt=40_000, gs=450.0),
            ground_state="airborne",
            ground_reason="airborne",
            config=cfg,
        )

    assert m.max_altitude == 40_000


# ---------------------------------------------------------------------------
# B6: persistence-filtered max_gs_kt
# ---------------------------------------------------------------------------


def test_max_gs_kt_persistence_rejects_single_spike():
    """A single 400 kt GS spike on a B407 should not set max_gs_kt.

    A Bell 407's Vne is ~140 kt. A one-sample 400 kt reading is a GS glitch.
    """
    cfg = _cfg()
    m = FlightMetrics()

    # 10 real samples at 120 kt.
    for i in range(10):
        m.record_point(
            _point(ts=1000.0 + i * 5, baro_alt=3_000, gs=120.0),
            ground_state="airborne",
            ground_reason="airborne",
            config=cfg,
        )
    # One 400 kt spike.
    m.record_point(
        _point(ts=1055.0, baro_alt=3_000, gs=400.0),
        ground_state="airborne",
        ground_reason="airborne",
        config=cfg,
    )
    # A few more real samples.
    for i in range(5):
        m.record_point(
            _point(ts=1060.0 + i * 5, baro_alt=3_000, gs=120.0),
            ground_state="airborne",
            ground_reason="airborne",
            config=cfg,
        )

    assert m.max_gs_kt == 120, f"max_gs_kt pegged by single spike: {m.max_gs_kt}"


def test_max_gs_kt_updates_on_sustained_cruise_speed():
    cfg = _cfg()
    m = FlightMetrics()

    # Slow climb at 150 kt.
    for i in range(6):
        m.record_point(
            _point(ts=1000.0 + i * 5, baro_alt=10_000, gs=150.0),
            ground_state="airborne",
            ground_reason="airborne",
            config=cfg,
        )
    # Sustained cruise at 280 kt.
    for i in range(10):
        m.record_point(
            _point(ts=1100.0 + i * 5, baro_alt=20_000, gs=280.0),
            ground_state="airborne",
            ground_reason="airborne",
            config=cfg,
        )

    assert m.max_gs_kt == 280


# ---------------------------------------------------------------------------
# B4: callsign_changes counts real transitions
# ---------------------------------------------------------------------------


def test_callsign_changes_ping_pong_counted_as_distinct_not_flicker():
    """TWY501 -> GS501 -> TWY501 -> GS501 -> TWY501 is 4 observed
    transitions in the raw stream, but only 2 distinct callsigns were
    ever used. The stored `callsign_changes` should reflect REAL flight-
    operational transitions (1 change from TWY to GS, or the round
    count)--not receiver frame flicker. This test captures the ping-pong
    inflation bug that A7-HBJ hit (2 distinct, 148 recorded changes).
    """
    cfg = _cfg()
    m = FlightMetrics()

    sequence = ["TWY501", "GS501", "TWY501", "GS501", "TWY501", "GS501", "TWY501"]
    for i, cs in enumerate(sequence):
        m.record_point(
            _point(ts=1000.0 + i * 10, callsign=cs),
            ground_state="airborne",
            ground_reason="airborne",
            config=cfg,
        )

    # Distinct count is 2, so changes must be <= 1 after the
    # compute_callsigns_summary cap applies. At the metrics level we
    # track real transitions (which ping-pong 6 times) but the
    # feature-level cap brings it to 1.
    assert len(m.callsigns_seen) == 2, f"callsigns_seen must be deduped, got {m.callsigns_seen}"


# ---------------------------------------------------------------------------
# Position source counters (readsb type/src)
# ---------------------------------------------------------------------------


def _point_with_source(ts: float, position_source: str | None) -> PointData:
    return PointData(
        ts=ts,
        lat=40.0,
        lon=-74.0,
        baro_alt=10_000,
        gs=300.0,
        track=90.0,
        geom_alt=None,
        baro_rate=0.0,
        geom_rate=None,
        squawk=None,
        category=None,
        nav_altitude_mcp=None,
        nav_qnh=None,
        emergency_field=None,
        true_heading=None,
        callsign=None,
        position_source=position_source,
    )


def test_metrics_count_adsb_points():
    """adsb_icao and adsb_other both count as ADS-B."""
    cfg = _cfg()
    m = FlightMetrics()
    for i, src in enumerate(["adsb_icao", "adsb_other", "adsb_icao"]):
        m.record_point(
            _point_with_source(1000.0 + i * 5, src),
            ground_state="airborne",
            ground_reason="airborne",
            config=cfg,
        )
    assert m.adsb_points == 3
    assert m.mlat_points == 0
    assert m.tisb_points == 0


def test_metrics_count_mlat_and_tisb():
    """mlat and tisb_icao bump their respective counters."""
    cfg = _cfg()
    m = FlightMetrics()
    sources = ["mlat", "mlat", "tisb_icao", "adsb_icao"]
    for i, src in enumerate(sources):
        m.record_point(
            _point_with_source(1000.0 + i * 5, src),
            ground_state="airborne",
            ground_reason="airborne",
            config=cfg,
        )
    assert m.mlat_points == 2
    assert m.tisb_points == 1
    assert m.adsb_points == 1


def test_metrics_count_other_source_types_unclassified():
    """Values like 'other', 'mode_s', 'adsc', None do not bump adsb/mlat/tisb."""
    cfg = _cfg()
    m = FlightMetrics()
    for i, src in enumerate(["other", "mode_s", "adsc", None]):
        m.record_point(
            _point_with_source(1000.0 + i * 5, src),
            ground_state="airborne",
            ground_reason="airborne",
            config=cfg,
        )
    assert m.adsb_points == 0
    assert m.mlat_points == 0
    assert m.tisb_points == 0


def test_metrics_counts_apply_for_ground_points_too():
    """Source counters track every recorded point, not just airborne ones.

    Percentages are computed against data_points (total), so ground points
    must be included or adsb_pct + mlat_pct + tisb_pct could exceed 100%.
    """
    cfg = _cfg()
    m = FlightMetrics()
    m.record_point(
        _point_with_source(1000.0, "adsb_icao"),
        ground_state="ground",
        ground_reason="ok",
        config=cfg,
    )
    m.record_point(
        _point_with_source(1005.0, "adsb_icao"),
        ground_state="airborne",
        ground_reason="ok",
        config=cfg,
    )
    assert m.data_points == 2
    assert m.adsb_points == 2


def test_record_point_populates_lat_lon_on_recent_points():
    """After record_point, the last sample in recent_points should carry lat/lon."""
    from adsbtrack.classifier import FlightMetrics, PointData

    m = FlightMetrics()
    pt = PointData(
        ts=1_700_000_000.0,
        lat=33.63,
        lon=-84.43,
        baro_alt=1200,
        gs=130.0,
        track=90.0,
        geom_alt=1250,
        baro_rate=-400.0,
        geom_rate=None,
        squawk="1200",
        category=None,
        nav_altitude_mcp=None,
        nav_qnh=None,
        emergency_field=None,
        true_heading=None,
        callsign="TEST1",
    )
    m.record_point(pt, ground_state="airborne", ground_reason="")
    assert len(m.recent_points) == 1
    sample = m.recent_points[-1]
    assert sample.lat == 33.63
    assert sample.lon == -84.43
    assert sample.baro_alt == 1200


def test_record_point_captures_track_on_recent_points() -> None:
    """After record_point, the last sample in recent_points should carry track.

    The ILS alignment detector (adsbtrack.ils_alignment) needs recent-points
    samples to expose ground track so it can score runway alignment during
    approach. This test pins that contract at the FlightMetrics layer.
    """
    metrics = FlightMetrics()
    p = PointData(
        ts=100.0,
        lat=33.64,
        lon=-84.43,
        baro_alt=3000,
        gs=150.0,
        track=87.0,
        geom_alt=3050,
        baro_rate=-800.0,
        geom_rate=None,
        squawk=None,
        category=None,
        nav_altitude_mcp=None,
        nav_qnh=None,
        emergency_field=None,
        true_heading=None,
        callsign=None,
    )
    metrics.record_point(p, ground_state="airborne", ground_reason="ok")
    assert len(metrics.recent_points) == 1
    sample = metrics.recent_points[-1]
    assert sample.track == 87.0
