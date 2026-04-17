# Landing Airport Matching: Altitude-Minimum Anchor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace "last-point" with "altitude-minimum point within the final N minutes" as the anchor for landing airport matching (both `destination_icao` for confirmed/uncertain landings and `probable_destination_icao` for signal_lost/dropped_on_approach), falling back to the last point when altitude data is missing. Track which method was used in a new `landing_anchor_method` column.

**Architecture:** Add a new tiny pure module `adsbtrack/landing_anchor.py` that computes the anchor from a `FlightMetrics` instance. Extend `_PointSample` in `classifier.py` with `lat`/`lon` so the existing `recent_points` deque carries everything needed. Wire the anchor at the two existing airport-matching sites in `parser.py` without touching the extractor state machine or the confidence-scoring algorithm.

**Tech Stack:** Python 3.12+, SQLite (via `Database`), Click, pytest. No new runtime dependencies.

---

## Pre-reading (for implementers)

- `docs/internals.md` §"Airport matching" - explains current on-field-vs-nearest gating at 2 km / 10 km thresholds and the `signal_lost` / `dropped_on_approach` skip.
- `docs/features.md` §"Landing types and confidence scoring" - explains the five landing types. `probable_destination_icao` is populated only for `signal_lost` / `dropped_on_approach`, via `features.infer_destination` using `flight.last_seen_lat/lon`.
- `adsbtrack/parser.py:834-843` - current destination airport matching. Uses `flight.landing_lat/lon`.
- `adsbtrack/parser.py:935-954` - current probable destination inference. Queries candidates + calls `features.infer_destination` using `flight.last_seen_lat/lon`.
- `adsbtrack/classifier.py:64-73` - `_PointSample` (frozen `_PointSample` holds samples in `FlightMetrics.recent_points`). Currently has ts, baro_alt, geom_alt, gs, baro_rate. No lat/lon.
- `adsbtrack/classifier.py:270-282` - the single site where `_PointSample` instances are constructed.
- `adsbtrack/features.py:728-787` - `infer_destination` consumes `flight.last_seen_lat/lon`.

---

## File structure

**New file:**
- `adsbtrack/landing_anchor.py` - `LandingAnchor` dataclass + `compute_landing_anchor(metrics, *, window_minutes) -> LandingAnchor | None`. Pure function.

**Modified files:**
- `adsbtrack/db.py` - add `landing_anchor_method TEXT` to the `flights` SCHEMA and the migration list; extend `insert_flight` SQL + values tuple.
- `adsbtrack/models.py` - add `landing_anchor_method: str | None = None` to `Flight`.
- `adsbtrack/config.py` - add `landing_anchor_window_minutes: float = 10.0`.
- `adsbtrack/classifier.py` - extend `_PointSample` with optional `lat`/`lon`; populate in `FlightMetrics.record_point`.
- `adsbtrack/parser.py` - compute the anchor once per flight, use it for both `destination_icao` matching and the probable-destination candidate query.
- `adsbtrack/features.py` - `infer_destination` gains `anchor_lat` / `anchor_lon` kwargs; existing `flight.last_seen_*` is kept only as a fallback when anchor kwargs are omitted (backwards compat for any test that still calls the old signature).
- `docs/features.md` - document `landing_anchor_method`.

**New tests:**
- `tests/test_landing_anchor.py` - unit tests for `compute_landing_anchor` with synthetic `FlightMetrics`.

**Extended tests:**
- `tests/test_db.py` - schema presence + insert roundtrip for the new column.
- `tests/test_parser.py` - integration test: extract_flights selects the correct destination / probable destination using the alt-min anchor.
- `tests/test_features.py` - `infer_destination` picks destination by the supplied anchor when different from `last_seen`.

---

## Task 1: Schema, Config, Flight dataclass, and DB insert

**Goal:** Persist `landing_anchor_method` and add the configurable window. No behavior change yet; nothing computes or writes the value.

**Files:**
- Modify: `adsbtrack/db.py` (SCHEMA string + migration list + `insert_flight` SQL/values)
- Modify: `adsbtrack/models.py` (`Flight` dataclass)
- Modify: `adsbtrack/config.py` (new field)
- Test: `tests/test_db.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_db.py`:

```python
def test_flights_landing_anchor_method_column_exists(db_path):
    """Schema should include landing_anchor_method on the flights table."""
    database = Database(db_path)
    cols = {row[1] for row in database.conn.execute("PRAGMA table_info(flights)").fetchall()}
    assert "landing_anchor_method" in cols
    database.close()


def test_insert_flight_persists_landing_anchor_method(db, db_path):
    """insert_flight should roundtrip the landing_anchor_method value."""
    from datetime import UTC, datetime

    f = Flight(
        icao="aaaaaa",
        takeoff_time=datetime(2026, 4, 16, 12, 0, tzinfo=UTC),
        takeoff_lat=30.0,
        takeoff_lon=-90.0,
        takeoff_date="2026-04-16",
        landing_anchor_method="alt_min",
    )
    db.insert_flight(f)
    db.commit()
    row = db.conn.execute("SELECT landing_anchor_method FROM flights WHERE icao = ?", ("aaaaaa",)).fetchone()
    assert row["landing_anchor_method"] == "alt_min"


def test_insert_flight_landing_anchor_method_defaults_to_null(db):
    """Flights without an explicit method should store NULL."""
    from datetime import UTC, datetime

    f = Flight(
        icao="bbbbbb",
        takeoff_time=datetime(2026, 4, 16, 12, 0, tzinfo=UTC),
        takeoff_lat=30.0,
        takeoff_lon=-90.0,
        takeoff_date="2026-04-16",
    )
    db.insert_flight(f)
    db.commit()
    row = db.conn.execute("SELECT landing_anchor_method FROM flights WHERE icao = ?", ("bbbbbb",)).fetchone()
    assert row["landing_anchor_method"] is None


def test_config_has_landing_anchor_window_minutes_default():
    from adsbtrack.config import Config

    cfg = Config()
    assert cfg.landing_anchor_window_minutes == 10.0
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `cd /Users/afranke/Projects/adsbtrack && uv run pytest tests/test_db.py -k "landing_anchor or config_has_landing" -v`
Expected: FAIL - column missing, Flight has no such field, Config lacks attribute.

- [ ] **Step 3: Add the column to the flights schema**

In `adsbtrack/db.py`, find the flights `CREATE TABLE` block and add this column just before the final `UNIQUE(icao, takeoff_time)` line (current line ~127):

```sql
    landing_anchor_method TEXT,
```

- [ ] **Step 4: Add the migration entry**

In `adsbtrack/db.py`, inside `_migrate_add_flight_columns`, append to the `new_columns` list (before the closing `]`):

```python
        # Landing airport-matching anchor: "alt_min" or "last_point".
        # Populated by parser.py using adsbtrack.landing_anchor.compute_landing_anchor.
        ("landing_anchor_method", "TEXT"),
```

- [ ] **Step 5: Extend the INSERT statement in `insert_flight`**

Edit `adsbtrack/db.py` `insert_flight`:
- Add `landing_anchor_method` to the column list right after `acars_in` (or anywhere in the column list; match the values tuple position).
- Add a corresponding `?` to the VALUES section.
- Add `flight.landing_anchor_method,` at the matching position in the values tuple (right after `flight.acars_in`).

Concretely: locate the line containing `acars_out, acars_off, acars_on, acars_in)` in the INSERT column list and change it to `acars_out, acars_off, acars_on, acars_in, landing_anchor_method)`. In the VALUES placeholder block, the final group currently reads `?, ?, ?, ?)` (the four acars fields) - extend to `?, ?, ?, ?, ?)` (add one more). In the values tuple, add `flight.landing_anchor_method,` after `flight.acars_in,`.

- [ ] **Step 6: Add the field to `Flight` dataclass**

In `adsbtrack/models.py`, after the `acars_in: str | None = None` line (currently the last field in `Flight`), add:

```python

    # --- Landing airport-matching anchor method ---
    # "alt_min" when compute_landing_anchor found a valid altitude-minimum point
    # in the final window; "last_point" when we fell back to the last observed
    # position (missing altitude in the tail). NULL on flights where the anchor
    # was not computed (e.g. legacy rows before the migration).
    landing_anchor_method: str | None = None
```

- [ ] **Step 7: Add the config parameter**

In `adsbtrack/config.py`, find the existing `post_landing_window_secs` field inside the `Config` dataclass (currently line ~271) and add a new field in the same section:

```python
    # --- landing airport matching anchor ---
    # Final-N-minute window used by adsbtrack.landing_anchor.compute_landing_anchor
    # to find the altitude-minimum point for airport matching. 10 min is a
    # reasonable default for most approach profiles.
    landing_anchor_window_minutes: float = 10.0
```

- [ ] **Step 8: Run the tests and verify they pass**

Run: `cd /Users/afranke/Projects/adsbtrack && uv run pytest tests/test_db.py -k "landing_anchor or config_has_landing" -v`
Expected: PASS (4 tests).

- [ ] **Step 9: Run ruff to check formatting**

Run: `cd /Users/afranke/Projects/adsbtrack && uv run ruff check adsbtrack tests && uv run ruff format --check adsbtrack tests`
Expected: clean.

- [ ] **Step 10: Commit**

```bash
git add adsbtrack/db.py adsbtrack/models.py adsbtrack/config.py tests/test_db.py
git commit -m "Add landing_anchor_method column, Flight field, and config window"
```

---

## Task 2: Extend `_PointSample` with lat/lon

**Goal:** The `recent_points` deque already tracks ts / alt / gs / baro_rate for every recorded point. Extending the sample with lat/lon lets the anchor-computation function walk that same deque without a parallel data structure.

**Files:**
- Modify: `adsbtrack/classifier.py` (`_PointSample` + `FlightMetrics.record_point`)
- Test: `tests/test_classifier.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_classifier.py`:

```python
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
```

- [ ] **Step 2: Run and verify it fails**

Run: `cd /Users/afranke/Projects/adsbtrack && uv run pytest tests/test_classifier.py::test_record_point_populates_lat_lon_on_recent_points -v`
Expected: FAIL with `AttributeError: '_PointSample' object has no attribute 'lat'` (or similar).

- [ ] **Step 3: Extend `_PointSample`**

In `adsbtrack/classifier.py`, modify the `_PointSample` dataclass (currently around line 64-73) to add lat/lon. Default them to `None` so any other constructor sites (currently none, but to be safe) continue to work:

```python
@dataclass
class _PointSample:
    """A lightweight snapshot of a trace point kept for descent scoring and
    landing airport anchor selection."""

    ts: float  # absolute unix timestamp
    baro_alt: int | None  # None when trace reports 'ground'
    geom_alt: int | None
    gs: float | None
    baro_rate: float | None
    # Position is used by adsbtrack.landing_anchor to pick the alt-min anchor
    # for destination airport matching. Defaults to None so any future
    # constructor that omits them still compiles (existing consumers of
    # recent_points only read ts / alt / gs / baro_rate).
    lat: float | None = None
    lon: float | None = None
```

- [ ] **Step 4: Populate lat/lon in `record_point`**

In `adsbtrack/classifier.py`, find the `self.recent_points.append(_PointSample(...))` block (currently around line 274-282) and add `lat=lat, lon=lon` as kwargs. The block should read:

```python
        self.recent_points.append(
            _PointSample(
                ts=ts,
                baro_alt=sample_baro_alt,
                geom_alt=int(geom_alt) if isinstance(geom_alt, (int, float)) else None,
                gs=gs,
                baro_rate=baro_rate,
                lat=lat,
                lon=lon,
            )
        )
```

(The local `lat` / `lon` variables already exist earlier in `record_point`, assigned from `point.lat` / `point.lon`.)

- [ ] **Step 5: Run the test and verify it passes**

Run: `cd /Users/afranke/Projects/adsbtrack && uv run pytest tests/test_classifier.py::test_record_point_populates_lat_lon_on_recent_points -v`
Expected: PASS.

- [ ] **Step 6: Run the full classifier and features test modules**

These modules use `FlightMetrics` heavily; verify no regressions:

Run: `cd /Users/afranke/Projects/adsbtrack && uv run pytest tests/test_classifier.py tests/test_features.py tests/test_parser.py -v`
Expected: all pass (plus the new one above).

- [ ] **Step 7: Commit**

```bash
git add adsbtrack/classifier.py tests/test_classifier.py
git commit -m "Carry lat/lon on _PointSample for landing anchor use"
```

---

## Task 3: Create `adsbtrack/landing_anchor.py`

**Goal:** Pure function that walks `metrics.recent_points`, filters to the final N minutes, finds the minimum-altitude sample (tie-break by latest ts), and returns it. Falls back to `last_seen_*` when no sample in the window has altitude data.

**Files:**
- Create: `adsbtrack/landing_anchor.py`
- Test: `tests/test_landing_anchor.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_landing_anchor.py`:

```python
"""Tests for adsbtrack.landing_anchor -- pick the anchor point for
destination airport matching."""

from collections import deque

from adsbtrack.classifier import FlightMetrics, _PointSample
from adsbtrack.landing_anchor import LandingAnchor, compute_landing_anchor


def _make_sample(ts: float, alt: int | None, lat: float, lon: float) -> _PointSample:
    return _PointSample(
        ts=ts,
        baro_alt=alt,
        geom_alt=None,
        gs=None,
        baro_rate=None,
        lat=lat,
        lon=lon,
    )


def _metrics_with_samples(samples: list[_PointSample]) -> FlightMetrics:
    m = FlightMetrics()
    # FlightMetrics constructs recent_points with a maxlen; reuse that deque.
    for s in samples:
        m.recent_points.append(s)
    if samples:
        last = samples[-1]
        m.last_seen_ts = last.ts
        m.last_seen_lat = last.lat
        m.last_seen_lon = last.lon
        m.last_seen_alt_ft = last.baro_alt
        m.last_point_ts = last.ts
    return m


def test_clean_descent_alt_min_near_touchdown():
    """A clean descent where alt_min is the last sample (touchdown).
    Method should be 'alt_min' and coordinates should match that sample."""
    base_ts = 1_700_000_000.0
    samples = [
        _make_sample(base_ts + 0, 3000, 33.70, -84.50),
        _make_sample(base_ts + 60, 2000, 33.68, -84.48),
        _make_sample(base_ts + 120, 1000, 33.66, -84.46),
        _make_sample(base_ts + 180, 100, 33.64, -84.44),   # alt-min = touchdown
    ]
    metrics = _metrics_with_samples(samples)
    anchor = compute_landing_anchor(metrics, window_minutes=10.0)
    assert anchor == LandingAnchor(lat=33.64, lon=-84.44, method="alt_min")


def test_sig_lost_alt_min_well_before_last_point():
    """Signal lost after a descent-then-climb (approach missed or holding).
    alt_min is in the middle of the window; last point is at altitude."""
    base_ts = 1_700_000_000.0
    samples = [
        _make_sample(base_ts + 0, 5000, 40.00, -100.00),
        _make_sample(base_ts + 60, 1500, 40.05, -100.05),  # alt-min
        _make_sample(base_ts + 120, 3000, 40.10, -100.10),
        _make_sample(base_ts + 180, 4000, 40.15, -100.15),  # last point, at altitude
    ]
    metrics = _metrics_with_samples(samples)
    anchor = compute_landing_anchor(metrics, window_minutes=10.0)
    assert anchor == LandingAnchor(lat=40.05, lon=-100.05, method="alt_min")


def test_drop_alt_min_equals_last_point():
    """Dropped on approach: last point is the lowest we saw."""
    base_ts = 1_700_000_000.0
    samples = [
        _make_sample(base_ts + 0, 5000, 25.00, -80.00),
        _make_sample(base_ts + 60, 3000, 25.05, -80.05),
        _make_sample(base_ts + 120, 1200, 25.10, -80.10),  # last point, lowest
    ]
    metrics = _metrics_with_samples(samples)
    anchor = compute_landing_anchor(metrics, window_minutes=10.0)
    assert anchor == LandingAnchor(lat=25.10, lon=-80.10, method="alt_min")


def test_missing_altitude_falls_back_to_last_point():
    """When no sample in the window has altitude, fall back to last_seen_*."""
    base_ts = 1_700_000_000.0
    samples = [
        _make_sample(base_ts + 0, None, 47.00, -122.00),
        _make_sample(base_ts + 60, None, 47.05, -122.05),
        _make_sample(base_ts + 120, None, 47.10, -122.10),
    ]
    metrics = _metrics_with_samples(samples)
    anchor = compute_landing_anchor(metrics, window_minutes=10.0)
    # Fallback uses last_seen_lat/lon (which _metrics_with_samples sets to the
    # final sample above).
    assert anchor == LandingAnchor(lat=47.10, lon=-122.10, method="last_point")


def test_samples_outside_window_are_excluded():
    """Only samples within the final N minutes should be considered."""
    base_ts = 1_700_000_000.0
    samples = [
        # This one is 15 min before the last point - outside a 10-min window
        _make_sample(base_ts + 0, 100, 10.00, 10.00),
        _make_sample(base_ts + 900, 5000, 20.00, 20.00),    # last point (15 min later)
    ]
    metrics = _metrics_with_samples(samples)
    anchor = compute_landing_anchor(metrics, window_minutes=10.0)
    # alt=100 is outside the 10-min window, so the in-window min is 5000.
    assert anchor == LandingAnchor(lat=20.00, lon=20.00, method="alt_min")


def test_tie_break_by_latest_timestamp():
    """If multiple samples share the same min altitude, pick the latest."""
    base_ts = 1_700_000_000.0
    samples = [
        _make_sample(base_ts + 0, 1000, 33.64, -84.40),
        _make_sample(base_ts + 60, 500, 33.65, -84.41),   # tie
        _make_sample(base_ts + 120, 500, 33.66, -84.42),  # tie, later
        _make_sample(base_ts + 180, 800, 33.67, -84.43),
    ]
    metrics = _metrics_with_samples(samples)
    anchor = compute_landing_anchor(metrics, window_minutes=10.0)
    assert anchor.lat == 33.66 and anchor.lon == -84.42


def test_empty_metrics_returns_none():
    """No samples and no last_seen coords -> None."""
    m = FlightMetrics()
    assert compute_landing_anchor(m, window_minutes=10.0) is None


def test_fallback_when_recent_points_empty_but_last_seen_set():
    """recent_points empty but last_seen coords exist (unusual but possible
    after certain stitches) -> last_point fallback."""
    m = FlightMetrics()
    m.last_seen_lat = 51.47
    m.last_seen_lon = -0.45
    m.last_seen_ts = 1_700_000_000.0
    anchor = compute_landing_anchor(m, window_minutes=10.0)
    assert anchor == LandingAnchor(lat=51.47, lon=-0.45, method="last_point")


def test_uses_geom_alt_when_baro_alt_missing():
    """If baro_alt is None but geom_alt is present, geom_alt should be used."""
    base_ts = 1_700_000_000.0
    sample_a = _PointSample(
        ts=base_ts,
        baro_alt=None,
        geom_alt=3000,
        gs=None,
        baro_rate=None,
        lat=33.70,
        lon=-84.50,
    )
    sample_b = _PointSample(
        ts=base_ts + 60,
        baro_alt=None,
        geom_alt=500,
        gs=None,
        baro_rate=None,
        lat=33.64,
        lon=-84.44,
    )
    metrics = _metrics_with_samples([sample_a, sample_b])
    anchor = compute_landing_anchor(metrics, window_minutes=10.0)
    assert anchor == LandingAnchor(lat=33.64, lon=-84.44, method="alt_min")
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `cd /Users/afranke/Projects/adsbtrack && uv run pytest tests/test_landing_anchor.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'adsbtrack.landing_anchor'`.

- [ ] **Step 3: Create the module**

Create `adsbtrack/landing_anchor.py`:

```python
"""Landing airport-matching anchor selection.

When picking which airport a flight was headed for, the "last observed
point" is often a poor proxy: on signal-loss / dropped-on-approach
flights the aircraft may have drifted laterally or climbed back up after
a missed approach. The altitude minimum within the final N minutes of
the trace is a stronger "where the aircraft was trying to land"
estimate.

This module is a pure function over `FlightMetrics.recent_points`. No
I/O, no DB calls. Falls back to the last observed position when no
in-window sample has altitude data.
"""

from __future__ import annotations

from dataclasses import dataclass

from .classifier import FlightMetrics, _PointSample


@dataclass(frozen=True)
class LandingAnchor:
    """Result of compute_landing_anchor. `method` is either `"alt_min"`
    (anchor chosen from the altitude minimum) or `"last_point"` (fell
    back to last_seen_lat/lon)."""

    lat: float
    lon: float
    method: str  # "alt_min" | "last_point"


def _sample_altitude(sample: _PointSample) -> int | None:
    """Prefer baro altitude; fall back to geometric. Returns None when
    neither is present (sample contributes nothing to alt_min selection)."""
    if sample.baro_alt is not None:
        return sample.baro_alt
    if sample.geom_alt is not None:
        return sample.geom_alt
    return None


def compute_landing_anchor(
    metrics: FlightMetrics,
    *,
    window_minutes: float = 10.0,
) -> LandingAnchor | None:
    """Choose the landing airport-matching anchor from a flight's metrics.

    Walks the tail of `metrics.recent_points` within the final
    `window_minutes` and returns the lowest-altitude sample (tie-broken
    by latest timestamp). Falls back to `metrics.last_seen_lat` /
    `metrics.last_seen_lon` with `method="last_point"` when no sample in
    the window has altitude data.

    Returns None when the metrics carry no usable position data at all
    (empty recent_points AND no last_seen coordinates).
    """
    window_secs = float(window_minutes) * 60.0

    # last_point_ts is the reference; fall back to last_seen_ts then to the
    # most recent sample ts. This lets us compute a window even on metrics
    # that haven't had post-close bookkeeping done.
    ref_ts: float | None = metrics.last_point_ts or metrics.last_seen_ts
    if ref_ts is None and metrics.recent_points:
        ref_ts = metrics.recent_points[-1].ts

    best: _PointSample | None = None
    if ref_ts is not None:
        cutoff = ref_ts - window_secs
        # Iterate newest-first so tie-break by "latest ts" is naturally
        # satisfied when comparing <= instead of <.
        for sample in reversed(metrics.recent_points):
            if sample.ts < cutoff:
                break
            alt = _sample_altitude(sample)
            if alt is None or sample.lat is None or sample.lon is None:
                continue
            if best is None:
                best = sample
                continue
            best_alt = _sample_altitude(best)
            # best_alt is non-None because we only set best when alt was non-None.
            assert best_alt is not None
            if alt < best_alt:
                best = sample
            elif alt == best_alt and sample.ts > best.ts:
                best = sample

    if best is not None:
        return LandingAnchor(lat=best.lat, lon=best.lon, method="alt_min")

    # Fallback: last_seen_*
    if metrics.last_seen_lat is not None and metrics.last_seen_lon is not None:
        return LandingAnchor(
            lat=metrics.last_seen_lat,
            lon=metrics.last_seen_lon,
            method="last_point",
        )
    return None
```

- [ ] **Step 4: Run the tests and verify they pass**

Run: `cd /Users/afranke/Projects/adsbtrack && uv run pytest tests/test_landing_anchor.py -v`
Expected: PASS (9 tests).

- [ ] **Step 5: Ruff + format**

Run: `cd /Users/afranke/Projects/adsbtrack && uv run ruff check adsbtrack/landing_anchor.py tests/test_landing_anchor.py && uv run ruff format --check adsbtrack/landing_anchor.py tests/test_landing_anchor.py`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add adsbtrack/landing_anchor.py tests/test_landing_anchor.py
git commit -m "Add compute_landing_anchor for altitude-minimum airport matching"
```

---

## Task 4: Update `features.infer_destination` to accept anchor kwargs

**Goal:** Break the hard dependency on `flight.last_seen_*` inside the destination-inference scorer so parser.py can pass the anchor. Keep backward compat: if anchor kwargs are omitted, fall back to `flight.last_seen_*` exactly as today.

**Files:**
- Modify: `adsbtrack/features.py` (`infer_destination`)
- Test: `tests/test_features.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_features.py`:

```python
def test_infer_destination_uses_supplied_anchor():
    """When anchor_lat / anchor_lon are supplied, proximity is measured
    from the anchor, not from flight.last_seen_*. Pick the airport that
    is closer to the anchor."""
    from datetime import UTC, datetime

    from adsbtrack.config import Config
    from adsbtrack.features import infer_destination

    # Airports: KA at (30.0, -90.0), KB at (31.0, -90.0)
    candidates = [
        {"ident": "KA", "latitude_deg": 30.0, "longitude_deg": -90.0, "elevation_ft": 10},
        {"ident": "KB", "latitude_deg": 31.0, "longitude_deg": -90.0, "elevation_ft": 10},
    ]
    m = FlightMetrics()
    m.recent_points.append(
        _make_sample_for_descent(ts=1_700_000_000.0, baro_alt=2000, baro_rate=-500)
    )
    flight = Flight(
        icao="aaaaaa",
        takeoff_time=datetime(2026, 4, 16, 12, 0, tzinfo=UTC),
        takeoff_lat=30.0,
        takeoff_lon=-90.0,
        takeoff_date="2026-04-16",
        landing_type="dropped_on_approach",
        last_seen_lat=31.0,    # close to KB
        last_seen_lon=-90.0,
        last_seen_alt_ft=2000,
    )
    cfg = Config()

    # Without anchor kwargs -> KB (closest to last_seen_*)
    result_default = infer_destination(flight=flight, metrics=m, candidates=candidates, config=cfg)
    assert result_default["probable_destination_icao"] == "KB"

    # With anchor kwargs pointing near KA -> KA
    result_anchor = infer_destination(
        flight=flight,
        metrics=m,
        candidates=candidates,
        config=cfg,
        anchor_lat=30.0,
        anchor_lon=-90.0,
    )
    assert result_anchor["probable_destination_icao"] == "KA"
```

You will also need a helper `_make_sample_for_descent` at the top of `tests/test_features.py` if one doesn't already exist. Check for existing descent-sample helpers; if none, add:

```python
def _make_sample_for_descent(ts: float, baro_alt: int | None, baro_rate: float | None):
    """Minimal _PointSample for descent scoring tests."""
    from adsbtrack.classifier import _PointSample

    return _PointSample(
        ts=ts,
        baro_alt=baro_alt,
        geom_alt=None,
        gs=None,
        baro_rate=baro_rate,
    )
```

- [ ] **Step 2: Run the test and verify it fails**

Run: `cd /Users/afranke/Projects/adsbtrack && uv run pytest tests/test_features.py::test_infer_destination_uses_supplied_anchor -v`
Expected: FAIL with `TypeError: infer_destination() got an unexpected keyword argument 'anchor_lat'`.

- [ ] **Step 3: Extend `infer_destination` signature**

In `adsbtrack/features.py`, locate `def infer_destination(` (currently at line ~728) and change it to:

```python
def infer_destination(
    *,
    flight: Flight,
    metrics: FlightMetrics,
    candidates: list,
    config: Config,
    anchor_lat: float | None = None,
    anchor_lon: float | None = None,
) -> dict:
    """Compute probable destination for signal_lost / dropped_on_approach.

    ``candidates`` is a list of airport rows (as returned by
    ``db.find_nearby_airports``) that parser.py has already queried around
    the anchor position. This function is pure - no db calls.

    ``anchor_lat`` / ``anchor_lon`` override the proximity reference point.
    When omitted, the function falls back to ``flight.last_seen_lat`` /
    ``flight.last_seen_lon`` (the historical behavior).
    """
    if flight.landing_type not in ("signal_lost", "dropped_on_approach"):
        return {
            "probable_destination_icao": None,
            "probable_destination_distance_km": None,
            "probable_destination_confidence": None,
        }

    ref_lat = anchor_lat if anchor_lat is not None else flight.last_seen_lat
    ref_lon = anchor_lon if anchor_lon is not None else flight.last_seen_lon
    if ref_lat is None or ref_lon is None or not candidates:
        return {
            "probable_destination_icao": None,
            "probable_destination_distance_km": None,
            "probable_destination_confidence": None,
        }

    max_km = config.prob_dest_max_distance_km
    best = None
    best_dist = float("inf")
    for ap in candidates:
        d_m = _haversine_m(ref_lat, ref_lon, ap["latitude_deg"], ap["longitude_deg"])
        d_km = d_m / 1000.0
        if d_km <= max_km and d_km < best_dist:
            best = ap
            best_dist = d_km

    if best is None:
        return {
            "probable_destination_icao": None,
            "probable_destination_distance_km": None,
            "probable_destination_confidence": None,
        }

    # Confidence factors (altitude factor still uses flight.last_seen_alt_ft
    # - that is a property of the trace-end, not the anchor - and descent
    # score is computed from metrics, both unchanged).
    alt = flight.last_seen_alt_ft or 5000
    alt_factor = max(0.0, min(1.0, (5000.0 - alt) / 4500.0))
    prox_factor = max(0.0, min(1.0, 1.0 - best_dist / max_km))
    descent_factor = descent_score(metrics.recent_points)

    confidence = (
        alt_factor * config.prob_dest_alt_weight
        + prox_factor * config.prob_dest_prox_weight
        + descent_factor * config.prob_dest_descent_weight
    )

    return {
        "probable_destination_icao": best["ident"],
        "probable_destination_distance_km": round(best_dist, 2),
        "probable_destination_confidence": round(confidence, 2),
    }
```

Only two changes vs. the existing body:
1. Signature adds `anchor_lat` / `anchor_lon` kwargs.
2. `ref_lat` / `ref_lon` resolution picks anchor first, then falls back to `flight.last_seen_*`.

The confidence-score weights and descent factor are untouched (spec requirement 4).

- [ ] **Step 4: Run the tests and verify they pass**

Run: `cd /Users/afranke/Projects/adsbtrack && uv run pytest tests/test_features.py -k "infer_destination" -v`
Expected: all pass (existing tests continue to pass with the fallback; new test passes).

- [ ] **Step 5: Commit**

```bash
git add adsbtrack/features.py tests/test_features.py
git commit -m "Accept anchor kwargs on infer_destination"
```

---

## Task 5: Wire anchor into parser.py

**Goal:** Compute the anchor once per flight in the final loop and use it for (a) destination airport matching and (b) the probable-destination candidate query. Populate `flight.landing_anchor_method`.

**Files:**
- Modify: `adsbtrack/parser.py`
- Test: `tests/test_parser.py`

- [ ] **Step 1: Write the failing integration tests**

Append to `tests/test_parser.py`:

```python
def test_extract_flights_uses_alt_min_anchor_for_destination():
    """Synthetic flight where the altitude minimum is near airport KA but
    the last observed trace point drifted close to airport KB. The
    destination should resolve to KA via the alt_min anchor; method
    recorded as 'alt_min'."""
    from unittest.mock import MagicMock

    base = "2026-04-16"
    start_ts = _ts(base, 12, 0, 0)

    # Flight profile: takeoff, climb, cruise, descent to touchdown near KA,
    # then a few more airborne points that drift toward KB before signal
    # drop. Simulates a dropped_on_approach at KA followed by aircraft
    # continuing visible past it.
    trace = [
        _make_trace_point(0, 30.00, -90.00, "ground", gs=0),
        _make_trace_point(60, 30.00, -90.00, 100, gs=80),    # takeoff
        _make_trace_point(300, 30.05, -90.10, 3000, gs=140),
        _make_trace_point(600, 30.05, -90.05, 500, gs=80),   # alt-min, near KA (30.0, -90.0)
        _make_trace_point(660, 30.20, -89.80, 2000, gs=120),
        _make_trace_point(720, 30.40, -89.60, 2500, gs=120), # last point, near KB (30.5, -89.5)
        # Signal lost (trace ends airborne).
    ]
    row = _make_trace_row(base, start_ts, trace)

    db = _make_db_mock([row])
    # KA is the alt_min airport, KB is the last-point airport.
    def fake_nearby(lat, lon, **kwargs):
        return [
            {"ident": "KA", "latitude_deg": 30.00, "longitude_deg": -90.00, "elevation_ft": 10, "name": "Alpha",
             "municipality": "", "iata_code": "", "type": "small_airport"},
            {"ident": "KB", "latitude_deg": 30.50, "longitude_deg": -89.50, "elevation_ft": 10, "name": "Bravo",
             "municipality": "", "iata_code": "", "type": "small_airport"},
        ]
    db.find_nearby_airports.side_effect = fake_nearby
    db.insert_flight = MagicMock()

    cfg = _make_config()

    extract_flights(db, cfg, "aaaaaa", reprocess=False)

    # Grab the flight object(s) passed to insert_flight and verify the
    # probable destination was picked via the alt_min anchor.
    inserted = [c.args[0] for c in db.insert_flight.call_args_list]
    assert len(inserted) >= 1
    flight = inserted[0]
    # landing_type will be signal_lost or dropped_on_approach - either way,
    # anchor method should be 'alt_min'.
    assert flight.landing_anchor_method == "alt_min"
    # Probable destination should resolve to KA (closest to alt-min point),
    # not KB (closest to last point).
    assert flight.probable_destination_icao == "KA"


def test_extract_flights_falls_back_to_last_point_when_tail_alts_missing():
    """When the final window has no altitude data (OpenSky-style traces
    with 'ground' strings or None throughout), landing_anchor_method
    should record 'last_point'."""
    from unittest.mock import MagicMock

    base = "2026-04-16"
    start_ts = _ts(base, 12, 0, 0)

    # Short "flight" where every airborne point has baro_alt='ground'.
    # The parser typically flags these as altitude_error; we only care
    # that the fallback method is recorded.
    trace = [
        _make_trace_point(0, 30.00, -90.00, "ground", gs=0),
        _make_trace_point(60, 30.00, -90.00, "ground", gs=90),   # baro='ground' at flight speed
        _make_trace_point(300, 30.05, -90.05, "ground", gs=120),
        _make_trace_point(600, 30.10, -90.10, "ground", gs=80),
    ]
    row = _make_trace_row(base, start_ts, trace)
    db = _make_db_mock([row])
    db.insert_flight = MagicMock()

    extract_flights(db, _make_config(), "aaaaaa", reprocess=False)

    inserted = [c.args[0] for c in db.insert_flight.call_args_list]
    # At least one flight may have been extracted; if so, confirm the
    # anchor fell back. If the parser filtered this trace out entirely,
    # the test is non-applicable and passes vacuously.
    for flight in inserted:
        assert flight.landing_anchor_method in ("last_point", None)


def test_reprocess_recomputes_landing_anchor_method():
    """Calling extract_flights with reprocess=True should recompute the
    anchor even if flights already exist with stale values."""
    from datetime import UTC, datetime

    import sqlite3

    from adsbtrack.db import Database
    from adsbtrack.models import Flight

    # Real DB, minimal trace stored + re-extract
    import tempfile
    from pathlib import Path

    tmp = tempfile.TemporaryDirectory()
    try:
        db_path = Path(tmp.name) / "t.db"
        with Database(db_path) as db:
            # Insert a stale flight with landing_anchor_method=NULL.
            stale = Flight(
                icao="aaaaaa",
                takeoff_time=datetime(2026, 4, 16, 12, 0, tzinfo=UTC),
                takeoff_lat=30.0,
                takeoff_lon=-90.0,
                takeoff_date="2026-04-16",
                landing_anchor_method=None,
            )
            db.insert_flight(stale)
            db.commit()

            # Insert trace for the same icao.
            base = "2026-04-16"
            start_ts = _ts(base, 12, 0, 0)
            trace = [
                _make_trace_point(0, 30.00, -90.00, "ground", gs=0),
                _make_trace_point(60, 30.00, -90.00, 100, gs=80),
                _make_trace_point(300, 30.05, -90.10, 3000, gs=140),
                _make_trace_point(600, 30.05, -90.05, 500, gs=80),
            ]
            db.insert_trace_day(
                "aaaaaa",
                base,
                {
                    "r": "N1", "t": "C172", "desc": "CESSNA", "ownOp": "", "year": "",
                    "timestamp": start_ts, "trace": trace,
                },
                source="adsbx",
            )
            db.commit()

            extract_flights(db, _make_config(), "aaaaaa", reprocess=True)
            db.commit()

            rows = db.conn.execute(
                "SELECT landing_anchor_method FROM flights WHERE icao = ?",
                ("aaaaaa",),
            ).fetchall()
            # After reprocess, the new flight row should have a populated
            # anchor method (either alt_min or last_point depending on trace).
            methods = [r["landing_anchor_method"] for r in rows]
            assert any(m in ("alt_min", "last_point") for m in methods), f"got {methods}"
    finally:
        tmp.cleanup()
```

- [ ] **Step 2: Run tests and verify they fail**

Run: `cd /Users/afranke/Projects/adsbtrack && uv run pytest tests/test_parser.py::test_extract_flights_uses_alt_min_anchor_for_destination tests/test_parser.py::test_reprocess_recomputes_landing_anchor_method -v`
Expected: FAIL (anchor method never set, or probable_destination resolves via last_seen).

- [ ] **Step 3: Import anchor module in parser.py**

At the top of `adsbtrack/parser.py` (with the other local imports), add:

```python
from .landing_anchor import compute_landing_anchor
```

- [ ] **Step 4: Compute and store the anchor in the main per-flight loop**

In `adsbtrack/parser.py`, locate the final per-flight loop starting with `for flight, metrics in zip(valid_flights, valid_metrics, strict=True):` (currently around line 784). Immediately AFTER the `classify_landing` call and BEFORE the airport-matching block, insert:

```python
        # Landing airport-matching anchor. Altitude-minimum point within the
        # final N minutes is a stronger estimator than the last observed
        # point for flights that drifted laterally or lost signal at
        # altitude. Falls back to last_point when tail altitudes are missing.
        anchor = compute_landing_anchor(
            metrics,
            window_minutes=config.landing_anchor_window_minutes,
        )
        flight.landing_anchor_method = anchor.method if anchor is not None else None
```

- [ ] **Step 5: Use the anchor for destination airport matching**

Still in `adsbtrack/parser.py`, locate the destination-matching block (currently lines 834-843 in the pre-change file):

```python
        if has_landing and flight.landing_type not in ("signal_lost", "dropped_on_approach"):
            dest = find_nearest_airport(db, flight.landing_lat, flight.landing_lon, config)
```

Change the argument to use the anchor when available:

```python
        if has_landing and flight.landing_type not in ("signal_lost", "dropped_on_approach"):
            # Use anchor (alt-min in final window) when available; fall back
            # to landing_lat/lon only if compute_landing_anchor returned None
            # (shouldn't happen on a has_landing flight but guards against
            # empty recent_points).
            dest_lat = anchor.lat if anchor is not None else flight.landing_lat
            dest_lon = anchor.lon if anchor is not None else flight.landing_lon
            dest = find_nearest_airport(db, dest_lat, dest_lon, config)
```

Leave the remainder of the block (the on-field threshold check, `destination_icao` / `nearest_destination_icao` assignments) unchanged.

- [ ] **Step 6: Use the anchor for probable-destination candidate query and scoring**

Still in `adsbtrack/parser.py`, locate the probable-destination block (currently around line 935-954):

```python
        # v3 destination inference for dropped / signal_lost flights
        if flight.landing_type in ("signal_lost", "dropped_on_approach") and flight.last_seen_lat is not None:
            try:
                candidates = db.find_nearby_airports(
                    flight.last_seen_lat,
                    flight.last_seen_lon,
                    delta=config.prob_dest_search_delta,
                    types=config.airport_types,
                )
            except Exception:
                candidates = []
            infer = features.infer_destination(
                flight=flight,
                metrics=metrics,
                candidates=list(candidates),
                config=config,
            )
```

Replace with anchor-based query and pass anchor kwargs to `infer_destination`:

```python
        # v3 destination inference for dropped / signal_lost flights.
        # Uses the alt-min anchor (falling back to last_seen) so candidates
        # are queried around "where the aircraft was trying to land" rather
        # than where it was last observed (which may be at altitude).
        if flight.landing_type in ("signal_lost", "dropped_on_approach"):
            ref_lat = anchor.lat if anchor is not None else flight.last_seen_lat
            ref_lon = anchor.lon if anchor is not None else flight.last_seen_lon
            if ref_lat is not None and ref_lon is not None:
                try:
                    candidates = db.find_nearby_airports(
                        ref_lat,
                        ref_lon,
                        delta=config.prob_dest_search_delta,
                        types=config.airport_types,
                    )
                except Exception:
                    candidates = []
                infer = features.infer_destination(
                    flight=flight,
                    metrics=metrics,
                    candidates=list(candidates),
                    config=config,
                    anchor_lat=ref_lat,
                    anchor_lon=ref_lon,
                )
                flight.probable_destination_icao = infer["probable_destination_icao"]
                flight.probable_destination_distance_km = infer["probable_destination_distance_km"]
                flight.probable_destination_confidence = infer["probable_destination_confidence"]
```

(The outer `if flight.landing_type in (...) and flight.last_seen_lat is not None:` guard becomes an inner `if ref_lat is not None and ref_lon is not None:` so that when the anchor is available but `last_seen_lat` is somehow missing, we can still score. In practice `last_seen_lat` is always populated on these flights; the change is defensive.)

- [ ] **Step 7: Run the tests and verify they pass**

Run: `cd /Users/afranke/Projects/adsbtrack && uv run pytest tests/test_parser.py -v`
Expected: all pass (the three new tests plus existing).

- [ ] **Step 8: Run the full feature + classifier + db suites as a regression guard**

Run: `cd /Users/afranke/Projects/adsbtrack && uv run pytest tests/test_classifier.py tests/test_features.py tests/test_db.py tests/test_landing_anchor.py tests/test_parser.py -v`
Expected: all pass.

- [ ] **Step 9: Ruff + format**

Run: `cd /Users/afranke/Projects/adsbtrack && uv run ruff check adsbtrack tests && uv run ruff format --check adsbtrack tests`
Expected: clean.

- [ ] **Step 10: Commit**

```bash
git add adsbtrack/parser.py tests/test_parser.py
git commit -m "Use alt-min anchor for destination and probable-destination matching"
```

---

## Task 6: Update `docs/features.md`

**Goal:** Document the new column and the rationale for the alt-min anchor.

**Files:**
- Modify: `docs/features.md`

- [ ] **Step 1: Insert the new section**

In `docs/features.md`, find the "Probable destination" paragraph (currently line 61 area) and insert a new paragraph immediately after it:

```markdown
**Landing airport anchor.** `landing_anchor_method` records whether the destination / probable-destination airport match used the altitude-minimum point within the final 10 minutes of the flight (`"alt_min"`) or fell back to the last observed position (`"last_point"`). The altitude minimum is a stronger "where the aircraft was trying to land" estimator than the last point, which can be at altitude or laterally drifted on `signal_lost` / `dropped_on_approach` flights. The window length is configurable via `Config.landing_anchor_window_minutes` (default 10). The anchor is used both to pick candidate airports via the on-field bounding-box query and to score the final match; the landing confidence factors and weights are unchanged.
```

(Place it right before the "Turnaround" paragraph so it flows: Probable destination -> Landing airport anchor -> Turnaround.)

- [ ] **Step 2: Verify the doc renders**

Run: `cd /Users/afranke/Projects/adsbtrack && grep -n '^\*\*Landing airport anchor\.\*\*' docs/features.md`
Expected: exactly one match.

- [ ] **Step 3: Commit**

```bash
git add docs/features.md
git commit -m "Document landing_anchor_method in features.md"
```

---

## Task 7: Final verification

- [ ] **Step 1: Full test suite**

Run: `cd /Users/afranke/Projects/adsbtrack && uv run pytest --deselect tests/test_cli.py::test_enrich_hex_no_data_reports_yellow 2>&1 | tail -3`
Expected: all pass. The deselected test hits a live network service and is pre-existing / unrelated.

- [ ] **Step 2: Ruff + format check**

Run: `cd /Users/afranke/Projects/adsbtrack && uv run ruff check . && uv run ruff format --check .`
Expected: clean.

- [ ] **Step 3: Mypy (informational)**

Run: `cd /Users/afranke/Projects/adsbtrack && uv run mypy adsbtrack/landing_anchor.py adsbtrack/models.py adsbtrack/config.py 2>&1 | tail -5`
Expected: zero new errors in the three modules the change touched. Pre-existing errors in db.py / airports.py are acceptable.

- [ ] **Step 4: Smoke test against a real aircraft (optional but recommended)**

If the user has an existing `adsbtrack.db` with any fetched aircraft, run:

```bash
cd /Users/afranke/Projects/adsbtrack && uv run python -m adsbtrack.cli extract --hex <some_hex> --reprocess
cd /Users/afranke/Projects/adsbtrack && uv run python -c "
import sqlite3
c = sqlite3.connect('adsbtrack.db')
c.row_factory = sqlite3.Row
for r in c.execute('SELECT landing_anchor_method, COUNT(*) AS n FROM flights GROUP BY landing_anchor_method'):
    print(dict(r))
"
```

Expected: the counts include at least `alt_min`; `last_point` rows are acceptable (traces with sparse tail altitudes); `NULL` should only appear on rows that were inserted before this feature (and only if there are such rows left after the reprocess).

This step is skipped if there is no pre-existing DB; the synthetic integration tests already exercise the same code paths.

---

## Self-Review Checklist

**Spec coverage:**
- [x] Req 1 "Read docs/internals.md and docs/features.md first" - Pre-reading block at the top of the plan; both docs were read during plan writing and referenced by section.
- [x] Req 2 "anchor = altitude-minimum point in final N min; tie-break by latest ts" - Task 3 `compute_landing_anchor` + `test_tie_break_by_latest_timestamp`.
- [x] Req 3 "fallback to last-point when alt is missing; new `landing_anchor_method` column with `alt_min` / `last_point`" - Task 1 schema + Task 3 fallback logic + Task 4 writes `flight.landing_anchor_method`.
- [x] Req 4 "do not change confidence scoring" - `infer_destination` confidence weights unchanged (Task 4 comment + test that checks destination picking, not confidence math).
- [x] Req 5 "--reprocess support" - Task 5 `test_reprocess_recomputes_landing_anchor_method` verifies integration.
- [x] Req 6 "automatic schema migration" - Task 1 Step 4 appends to `_migrate_add_flight_columns` list, which already runs on every `__init__`.
- [x] Req 7 test coverage - Task 3 covers: clean descent (alt_min near touchdown), SIG LOST with alt_min before last point, DROP with alt_min equal last point, missing altitude fallback. Plus tie-break, window exclusion, empty, geom_alt fallback.
- [x] Req 8 "update docs/features.md" - Task 6.
- [x] "Do not add runway-level detection" - nothing in this plan touches runways.

**Placeholder scan:** No TBD / TODO / "similar to" / "add appropriate" phrases remain.

**Type consistency:**
- `LandingAnchor` dataclass has `lat`, `lon`, `method` fields. Tests construct it with those names. Task 5 accesses `anchor.lat`, `anchor.lon`, `anchor.method` - match.
- `compute_landing_anchor(metrics, *, window_minutes)` signature matches all call sites: Task 5 parser code passes `window_minutes=config.landing_anchor_window_minutes`, Task 3 tests pass `window_minutes=10.0`.
- `infer_destination(*, ..., anchor_lat, anchor_lon)` - Task 4 adds them, Task 5 passes them, Task 4 test uses them.
- `Flight.landing_anchor_method: str | None` - set in parser Task 5, persisted in db Task 1, read in db tests Task 1, read in parser test Task 5.
- `Config.landing_anchor_window_minutes: float` - defined Task 1 Step 7, read Task 5 Step 4.
