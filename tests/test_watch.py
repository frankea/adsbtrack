"""Tests for adsbtrack.watch -- the pure alert-evaluation core for `watch`.

Every test follows the same shape the module is built around: seed a "pre"
world, call snapshot_state(), mutate the DB to simulate a fetch/extract run,
then call evaluate() with the earlier snapshot. That before/after split is
what the alert core is actually testing -- a row that already existed at
snapshot time must never re-fire on a later run.
"""

from datetime import UTC, date, datetime, timedelta

import pytest

from adsbtrack.config import Config
from adsbtrack.db import Database
from adsbtrack.models import Flight
from adsbtrack.watch import WatchAlert, WatchState, evaluate, snapshot_state

ICAO = "aaa111"


@pytest.fixture
def db(tmp_path):
    database = Database(tmp_path / "test.db")
    yield database
    database.close()


def _trace_data(timestamp: float = 1700000000.0) -> dict:
    return {
        "timestamp": timestamp,
        "trace": [[0, 40.0, -74.0, 5000, 200, None, None, None, {}]],
    }


def _flight(
    icao: str,
    takeoff_time: datetime,
    *,
    callsign: str | None = None,
    had_emergency: int | None = None,
    emergency_squawk: str | None = None,
    squawks_observed: str | None = None,
) -> Flight:
    return Flight(
        icao=icao,
        takeoff_time=takeoff_time,
        takeoff_lat=40.0,
        takeoff_lon=-74.0,
        takeoff_date=takeoff_time.date().isoformat(),
        callsign=callsign,
        had_emergency=had_emergency,
        emergency_squawk=emergency_squawk,
        squawks_observed=squawks_observed,
    )


def _spoof(
    db: Database,
    icao: str,
    takeoff_time: datetime,
    detected_at: str,
    *,
    callsign: str | None = "EK123",
    reason: str = "bimodal_integrity",
    reason_detail: str | None = None,
) -> None:
    db.insert_spoofed_broadcast(
        icao=icao,
        takeoff_time=takeoff_time.isoformat(),
        landing_time=None,
        takeoff_date=takeoff_time.date().isoformat(),
        callsign=callsign,
        takeoff_lat=40.0,
        takeoff_lon=-74.0,
        landing_lat=None,
        landing_lon=None,
        max_altitude=1000,
        data_points=10,
        sources="adsbx",
        origin_icao=None,
        destination_icao=None,
        reason=reason,
        reason_detail=reason_detail,
    )
    # insert_spoofed_broadcast always stamps detected_at with "now", so
    # tests that need a specific detected_at (before/after run_started_at)
    # have to override it directly after the insert.
    db.conn.execute(
        "UPDATE spoofed_broadcasts SET detected_at = ? WHERE icao = ? AND takeoff_time = ?",
        (detected_at, icao, takeoff_time.isoformat()),
    )


def test_first_run_baseline_returns_no_alerts(db):
    # pre snapshot on an empty db -- has_any_trace is False.
    pre = snapshot_state(db, ICAO)
    assert pre.has_any_trace is False

    # Seed data (including an emergency flight and a spoof row) after the
    # snapshot, simulating a first-ever fetch backfilling history.
    db.insert_trace_day(ICAO, "2026-01-01", _trace_data())
    db.insert_flight(_flight(ICAO, datetime(2026, 1, 1, 12, tzinfo=UTC), had_emergency=1, emergency_squawk="7700"))
    _spoof(db, ICAO, datetime(2026, 1, 1, 13, tzinfo=UTC), datetime.now(UTC).isoformat())
    db.commit()

    alerts = evaluate(db, ICAO, pre, datetime.now(UTC).isoformat(), Config())
    assert alerts == []


def test_reactivation_fires_after_dormancy_gap(db):
    last_day = date(2026, 1, 1)
    db.insert_trace_day(ICAO, last_day.isoformat(), _trace_data())
    db.commit()
    pre = snapshot_state(db, ICAO)
    assert pre.last_data_day == last_day.isoformat()

    new_day = last_day + timedelta(days=100)
    db.insert_trace_day(ICAO, new_day.isoformat(), _trace_data())
    # Observation evidence (I2): an earlier run already asked about a day
    # inside the gap and logged the answer, strictly before run_started_at.
    mid_gap_day = last_day + timedelta(days=50)
    db.insert_fetch_log(ICAO, mid_gap_day.isoformat(), 404, source="adsbx")
    db.commit()

    run_started_at = datetime.now(UTC).isoformat()
    alerts = evaluate(db, ICAO, pre, run_started_at, Config())

    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.kind == "reactivation"
    assert alert.icao == ICAO
    assert alert.detail == {
        "dormant_since": last_day.isoformat(),
        "reactivated_on": new_day.isoformat(),
        "gap_days": 100,
    }


def test_reactivation_respects_dormancy_floor(db):
    last_day = date(2026, 1, 1)
    db.insert_trace_day(ICAO, last_day.isoformat(), _trace_data())
    db.commit()
    pre = snapshot_state(db, ICAO)

    new_day = last_day + timedelta(days=5)
    db.insert_trace_day(ICAO, new_day.isoformat(), _trace_data())
    db.commit()

    config = Config()
    assert config.watch_dormancy_days == 30
    alerts = evaluate(db, ICAO, pre, datetime.now(UTC).isoformat(), config)
    assert alerts == []


def test_reactivation_suppressed_without_observation_evidence(db):
    """A dormancy gap with no fetch_log row logged inside it before this run
    started must not fire (I2): without evidence someone actually asked and
    got nothing during the gap, this looks identical to watch's first-ever
    look at a hex that already had sporadic history from `fetch`."""
    last_day = date(2026, 1, 1)
    db.insert_trace_day(ICAO, last_day.isoformat(), _trace_data())
    db.commit()
    pre = snapshot_state(db, ICAO)

    new_day = last_day + timedelta(days=100)
    db.insert_trace_day(ICAO, new_day.isoformat(), _trace_data())
    db.commit()

    alerts = evaluate(db, ICAO, pre, datetime.now(UTC).isoformat(), Config())
    assert alerts == []


def test_emergency_fires_only_for_new_flights(db):
    db.insert_trace_day(ICAO, "2026-01-01", _trace_data())
    anchor_takeoff = datetime(2026, 1, 1, 10, tzinfo=UTC)
    db.insert_flight(_flight(ICAO, anchor_takeoff, callsign="N1"))
    db.commit()
    pre = snapshot_state(db, ICAO)
    assert pre.max_flight_takeoff_time == anchor_takeoff.isoformat()

    # A "boundary" emergency flight that incremental extraction re-wrote
    # with a fresh id -- its takeoff_time is still <= the pre snapshot, so
    # it must not fire. The later emergency flight is genuinely new.
    boundary_takeoff = anchor_takeoff - timedelta(hours=1)
    later_takeoff = anchor_takeoff + timedelta(hours=1)
    db.insert_flight(_flight(ICAO, boundary_takeoff, callsign="BOUND", had_emergency=1, emergency_squawk="7700"))
    db.insert_flight(_flight(ICAO, later_takeoff, callsign="LATER", had_emergency=1, emergency_squawk="7700"))
    db.commit()

    alerts = evaluate(db, ICAO, pre, datetime.now(UTC).isoformat(), Config())

    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.kind == "emergency"
    assert alert.icao == ICAO
    assert alert.detail["takeoff_time"] == later_takeoff.isoformat()
    assert alert.detail["callsign"] == "LATER"
    assert alert.detail["emergency_squawk"] == "7700"


def test_emergency_baselines_when_no_prior_flights_exist(db):
    """A hex with trace history but zero flights extracted yet at snapshot
    time (e.g. this run's incremental extract hit parser.py's
    incremental-refusal fallback and silently upgraded to a full reprocess,
    writing years of flights in one pass) must not flood on every
    historical emergency once flights appear -- baseline instead (M7)."""
    db.insert_trace_day(ICAO, "2026-01-01", _trace_data())
    db.commit()
    pre = snapshot_state(db, ICAO)
    assert pre.has_any_trace is True
    assert pre.max_flight_takeoff_time is None

    db.insert_flight(
        _flight(ICAO, datetime(2020, 1, 1, 10, tzinfo=UTC), callsign="OLD", had_emergency=1, emergency_squawk="7700")
    )
    db.insert_flight(_flight(ICAO, datetime(2026, 1, 1, 10, tzinfo=UTC), callsign="RECENT"))
    db.commit()

    alerts = evaluate(db, ICAO, pre, datetime.now(UTC).isoformat(), Config())
    assert alerts == []


def test_spoof_fires_only_for_new_identity_keys(db):
    """Spoof suppression is keyed on (takeoff_time, reason) identity, not
    row presence or detected_at (C2) -- a full reprocess that deletes and
    re-inserts spoofed_broadcasts wholesale (stamping a fresh detected_at on
    content that never actually changed) must not re-fire the same row."""
    db.insert_trace_day(ICAO, "2026-01-01", _trace_data())
    old_takeoff = datetime(2026, 1, 1, 1, tzinfo=UTC)
    _spoof(db, ICAO, old_takeoff, datetime.now(UTC).isoformat(), callsign="OLD")
    db.commit()

    pre = snapshot_state(db, ICAO)
    assert pre.spoof_keys == {(old_takeoff.isoformat(), "bimodal_integrity")}

    # Simulate a full reprocess rewriting the SAME row: delete + re-insert,
    # stamping a fresh detected_at, identical (takeoff_time, reason).
    db.conn.execute(
        "DELETE FROM spoofed_broadcasts WHERE icao = ? AND takeoff_time = ?",
        (ICAO, old_takeoff.isoformat()),
    )
    _spoof(db, ICAO, old_takeoff, datetime.now(UTC).isoformat(), callsign="OLD")

    # A genuinely new spoof row, different takeoff_time.
    new_takeoff = datetime(2026, 1, 2, 2, tzinfo=UTC)
    _spoof(db, ICAO, new_takeoff, datetime.now(UTC).isoformat(), callsign="NEW")
    db.commit()

    alerts = evaluate(db, ICAO, pre, datetime.now(UTC).isoformat(), Config())

    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.kind == "spoof"
    assert alert.icao == ICAO
    assert alert.detail["callsign"] == "NEW"


def test_snapshot_state_shapes(db):
    empty = snapshot_state(db, ICAO)
    assert empty == WatchState(
        has_any_trace=False, last_data_day=None, max_flight_takeoff_time=None, spoof_keys=frozenset()
    )

    db.insert_trace_day(ICAO, "2026-01-01", _trace_data())
    db.insert_trace_day(ICAO, "2026-01-05", _trace_data())
    earlier = datetime(2026, 1, 1, 8, tzinfo=UTC)
    latest = datetime(2026, 1, 5, 9, tzinfo=UTC)
    db.insert_flight(_flight(ICAO, earlier))
    db.insert_flight(_flight(ICAO, latest))
    db.commit()

    seeded = snapshot_state(db, ICAO)
    assert seeded == WatchState(
        has_any_trace=True,
        last_data_day="2026-01-05",
        max_flight_takeoff_time=latest.isoformat(),
        spoof_keys=frozenset(),
    )

    spoof_takeoff = datetime(2026, 1, 5, 10, tzinfo=UTC)
    _spoof(db, ICAO, spoof_takeoff, datetime.now(UTC).isoformat(), reason="bimodal_integrity")
    db.commit()
    with_spoof = snapshot_state(db, ICAO)
    assert with_spoof.spoof_keys == {(spoof_takeoff.isoformat(), "bimodal_integrity")}


def test_watch_alert_and_state_are_plain_dataclasses():
    alert = WatchAlert(kind="emergency", icao=ICAO, summary="x", detail={})
    assert alert.kind == "emergency"
    state = WatchState(has_any_trace=True, last_data_day=None, max_flight_takeoff_time=None, spoof_keys=frozenset())
    assert state.has_any_trace is True
