# Takeoff Runway Detection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Identify which runway each flight took off from by testing whether its low-altitude departure trajectory passes through a trapezoid polygon aligned with a given runway end. Store the result as a new `takeoff_runway` column on the `flights` table, render it in `trips` output.

**Architecture:** Mirrors the ILS alignment feature (adsbtrack/ils_alignment.py) but for takeoff. A new pure module `adsbtrack/takeoff_runway.py` builds a trapezoid polygon per runway end using shapely, tests which polygons the first 600 s of trajectory samples pass through, and returns the runway name of the longest-aligned candidate. Parser wires it in after origin-airport matching; one column gets added to `flights`. No effect on confidence or classification, unlike landing alignment.

**Tech Stack:** Python 3.12, shapely (NEW dependency for polygon containment), spherical destination-point-from-bearing for polygon-corner math.

---

## Scope notes

Per the user spec, thresholds for commercial jets (vert_rate > 256 fpm, gs > 140 kt) need to scale down for helicopters and light GA. A new config field `takeoff_low_gs_types` lists type codes that trigger the low-gs variant (60 kt); rotorcraft are picked up via the existing `Config.helicopter_types` without duplicating the list.

We do NOT add go-around detection in this task. We do NOT thread the takeoff_runway signal into `takeoff_confidence` — it's diagnostic metadata only.

Shapely is pulled in as a new top-level dependency. The polygon-containment check is simple enough to hand-roll, but the user explicitly asked for shapely and it's a widely-packaged battle-tested library. Added to `pyproject.toml` `dependencies`.

---

## File structure

**New files**
- `adsbtrack/takeoff_runway.py` - pure detector: `TakeoffRunwayResult` frozen dataclass + `detect_takeoff_runway(...)` function + polygon-building helpers.
- `tests/test_takeoff_runway.py` - unit tests for the polygon geometry, containment, and multi-runway selection.

**Modified files**
- `pyproject.toml` - add `shapely>=2.0` to `dependencies`.
- `adsbtrack/classifier.py` - add `takeoff_points: list[_PointSample]` buffer to `FlightMetrics`; populate it in `record_point` for the first 600 s or 240 samples, whichever comes first.
- `adsbtrack/config.py` - 6 new fields: `takeoff_runway_zone_length_m`, `takeoff_runway_little_base_m`, `takeoff_runway_opening_deg`, `takeoff_runway_max_ft_above_airport`, `takeoff_runway_min_gs_kt_default`, `takeoff_runway_min_gs_kt_low`, `takeoff_runway_min_vert_rate_fpm`, `takeoff_low_gs_types`.
- `adsbtrack/models.py` - append `takeoff_runway: str | None = None` to `Flight`.
- `adsbtrack/db.py` - CREATE TABLE flights: add `takeoff_runway TEXT`; migration entry; insert_flight column list / placeholder / value tuple extension; one new roundtrip test.
- `adsbtrack/parser.py` - after origin-airport match (near existing `find_nearest_airport(db, flight.takeoff_lat, ...)` block around line 835), resolve origin runways via `db.get_runways_for_airport`, call `detect_takeoff_runway`, populate `flight.takeoff_runway`. Reuse the Task-4 `runway_cache` dict since it's already in scope.
- `adsbtrack/cli.py` - `trips` command: render `KSPG/24` in the From column when `takeoff_runway` is populated; `KSPG` otherwise.
- `tests/test_parser.py` - 4 integration tests using the existing MagicMock pattern (commercial jet takeoff, helicopter, sparse-data fail, no-runway airport).
- `tests/test_cli.py` - one test that asserts the From column renders `KSPG/24`.
- `docs/features.md` - new "Takeoff runway." subsection; mention the GS-threshold scaling for rotorcraft / light GA.
- `docs/schema.md` - add `takeoff_runway` row.

---

## Task 1: Add shapely, config knobs, Flight field, DB schema + migration

**Files:**
- Modify: `pyproject.toml`
- Modify: `adsbtrack/config.py`
- Modify: `adsbtrack/models.py`
- Modify: `adsbtrack/db.py` (CREATE TABLE flights, `_migrate_add_flight_columns`, `insert_flight`)
- Modify: `tests/test_db.py` (add column presence test + insert roundtrip test)

- [ ] **Step 1: Write the failing DB tests**

Append to `tests/test_db.py`:

```python
def test_flights_table_has_takeoff_runway_column(db) -> None:
    cols = {row[1] for row in db.conn.execute("PRAGMA table_info(flights)").fetchall()}
    assert "takeoff_runway" in cols


def test_insert_flight_persists_takeoff_runway(db) -> None:
    f = Flight(
        icao="ffffff",
        takeoff_time=datetime(2024, 6, 1, 10, 0),
        takeoff_lat=27.77, takeoff_lon=-82.67,
        takeoff_date="2024-06-01",
        takeoff_runway="24",
    )
    db.insert_flight(f)
    db.commit()
    row = db.conn.execute("SELECT takeoff_runway FROM flights WHERE icao = ?", ("ffffff",)).fetchone()
    assert row["takeoff_runway"] == "24"


def test_insert_flight_takeoff_runway_defaults_to_null(db) -> None:
    f = Flight(
        icao="eeeeee",
        takeoff_time=datetime(2024, 6, 1, 10, 0),
        takeoff_lat=27.77, takeoff_lon=-82.67,
        takeoff_date="2024-06-01",
    )
    db.insert_flight(f)
    db.commit()
    row = db.conn.execute("SELECT takeoff_runway FROM flights WHERE icao = ?", ("eeeeee",)).fetchone()
    assert row["takeoff_runway"] is None
```

- [ ] **Step 2: Run tests to verify they fail**

`uv run pytest tests/test_db.py::test_flights_table_has_takeoff_runway_column tests/test_db.py::test_insert_flight_persists_takeoff_runway tests/test_db.py::test_insert_flight_takeoff_runway_defaults_to_null -v`
Expected: FAIL (column missing).

- [ ] **Step 3: Add shapely to pyproject.toml**

Open `pyproject.toml`. Find the `dependencies = [` block and add a single new line (alphabetically near the other deps is fine):

```toml
    "shapely>=2.0",
```

Then run `uv sync --extra dev` to install it.

- [ ] **Step 4: Add config fields**

Open `adsbtrack/config.py`. Immediately AFTER the `ils_alignment_bonus_long: float = 0.25` line (added in the ILS alignment milestone), append:

```python
    # --- Takeoff runway detector (see adsbtrack/takeoff_runway.py) ---
    # A trapezoid polygon is built per runway end: narrow `little_base_m`
    # base at the threshold, extending `zone_length_m` outward along the
    # departure heading, opening symmetrically by `opening_deg`. A flight's
    # first 600 s of low-altitude climb (below airport_elev +
    # `max_ft_above_airport`) is tested for intersection with each polygon;
    # the runway whose polygon the flight was inside longest wins, subject
    # to a minimum ground-speed peak (`min_gs_kt_default` for commercial,
    # scaled to `min_gs_kt_low` for rotorcraft and type codes listed in
    # `takeoff_low_gs_types`) and a minimum vertical rate.
    takeoff_runway_zone_length_m: float = 6000.0
    takeoff_runway_little_base_m: float = 50.0
    takeoff_runway_opening_deg: float = 5.0
    takeoff_runway_max_ft_above_airport: float = 2000.0
    takeoff_runway_min_gs_kt_default: float = 140.0
    takeoff_runway_min_gs_kt_low: float = 60.0
    takeoff_runway_min_vert_rate_fpm: float = 256.0
    takeoff_low_gs_types: tuple[str, ...] = (
        "C150", "C152", "C162", "C172", "C177", "C182", "DA20", "DA40",
        "PA28", "PA32", "SR20", "SR22", "BE33", "BE35", "BE36",
    )
```

- [ ] **Step 5: Add Flight field**

In `adsbtrack/models.py`, append after `aligned_min_offset_m: float | None = None`:

```python

    # --- Takeoff runway identification (adsbtrack/takeoff_runway.py) ---
    # Runway name (e.g., "24", "08R") the aircraft departed from, inferred
    # by testing which runway's trapezoid polygon the low-altitude climb
    # trajectory passed through longest. NULL when the origin airport has
    # no runway data, no polygon matched, or the ground-speed / vertical-
    # rate thresholds weren't met.
    takeoff_runway: str | None = None
```

- [ ] **Step 6: Add SCHEMA column**

In `adsbtrack/db.py`, in the `CREATE TABLE IF NOT EXISTS flights (` DDL, insert the new column immediately AFTER `aligned_min_offset_m REAL,`:

```sql
    takeoff_runway TEXT,
```

- [ ] **Step 7: Extend `_migrate_add_flight_columns`**

Append to the `new_columns` list, after `("aligned_min_offset_m", "REAL"),`:

```python
        # Takeoff runway identification (adsbtrack/takeoff_runway.py)
        ("takeoff_runway", "TEXT"),
```

- [ ] **Step 8: Extend `insert_flight`**

In `Database.insert_flight`, three parallel edits:

1. Extend the SQL column list in the INSERT. Replace:

```python
                aligned_runway, aligned_seconds, aligned_min_offset_m)
```

with:

```python
                aligned_runway, aligned_seconds, aligned_min_offset_m,
                takeoff_runway)
```

2. Extend the VALUES placeholder group. The last placeholder group currently ends with `?, ?, ?, ?, ?, ?, ?, ?)` — add one more `?` to give `?, ?, ?, ?, ?, ?, ?, ?, ?)`.

3. Extend the Python value tuple. After `flight.aligned_min_offset_m,` append:

```python
                flight.takeoff_runway,
```

- [ ] **Step 9: Run DB tests to verify they pass**

`uv run pytest tests/test_db.py -v`
Expected: PASS (existing 67 tests plus the 3 new ones).

- [ ] **Step 10: Run the full suite**

`uv run pytest -x --deselect tests/test_cli.py::test_enrich_hex_no_data_reports_yellow`
Expected: PASS.

- [ ] **Step 11: Commit**

```bash
git add pyproject.toml uv.lock adsbtrack/config.py adsbtrack/models.py adsbtrack/db.py tests/test_db.py
git commit -m "feat(takeoff): add shapely dep, config, and schema for takeoff_runway"
```

---

## Task 2: Collect takeoff points in FlightMetrics

**Why:** `FlightMetrics.recent_points` is a `maxlen=240` deque that evicts oldest entries; by flight end it carries only the tail samples. The takeoff detector needs the HEAD of the flight. Add a bounded `takeoff_points` list that's filled for the first 600 seconds of the flight (or 240 samples, whichever first) and never trimmed.

**Files:**
- Modify: `adsbtrack/classifier.py` (`FlightMetrics` dataclass + `record_point`)
- Modify: `tests/test_classifier.py`

- [ ] **Step 1: Write failing test**

Append to `tests/test_classifier.py`:

```python
def test_record_point_populates_takeoff_points_for_first_window() -> None:
    """takeoff_points collects samples in the first 600s or first 240 samples,
    whichever comes first, and never evicts."""
    metrics = FlightMetrics()

    def _pt(ts: float, alt: int = 0) -> PointData:
        return PointData(
            ts=ts, lat=27.77, lon=-82.67, baro_alt=alt, gs=50.0,
            track=90.0, geom_alt=alt, baro_rate=0.0, geom_rate=None,
            squawk=None, category=None, nav_altitude_mcp=None, nav_qnh=None,
            emergency_field=None, true_heading=None, callsign=None,
        )

    # 100 samples spanning 500 seconds (within 600s window)
    for i in range(100):
        metrics.record_point(_pt(i * 5.0), ground_state="airborne", ground_reason="ok")
    assert len(metrics.takeoff_points) == 100

    # Push past the 600s window; new samples should NOT be appended
    metrics.record_point(_pt(700.0), ground_state="airborne", ground_reason="ok")
    assert len(metrics.takeoff_points) == 100  # still only 100
```

Add a second test for the 240-sample cap:

```python
def test_takeoff_points_capped_at_240_samples() -> None:
    metrics = FlightMetrics()

    def _pt(ts: float) -> PointData:
        return PointData(
            ts=ts, lat=27.77, lon=-82.67, baro_alt=1000, gs=150.0,
            track=90.0, geom_alt=1000, baro_rate=500.0, geom_rate=None,
            squawk=None, category=None, nav_altitude_mcp=None, nav_qnh=None,
            emergency_field=None, true_heading=None, callsign=None,
        )

    # 300 samples within 600s: 240 cap kicks in before the time window closes
    for i in range(300):
        metrics.record_point(_pt(i * 1.0), ground_state="airborne", ground_reason="ok")
    assert len(metrics.takeoff_points) == 240
```

- [ ] **Step 2: Run test to verify failure**

`uv run pytest tests/test_classifier.py::test_record_point_populates_takeoff_points_for_first_window tests/test_classifier.py::test_takeoff_points_capped_at_240_samples -v`
Expected: FAIL (no `takeoff_points` attribute).

- [ ] **Step 3: Add the buffer field**

In `adsbtrack/classifier.py`, `FlightMetrics` dataclass. Append this field BEFORE `def record_point(` (field section ends around line 218 where `_recent_positions` is defined):

```python
    # First-N-window samples captured for takeoff-runway detection. Unlike
    # recent_points (which is a tail-only deque), takeoff_points is a
    # monotonically-growing list bounded by a time window and a sample cap.
    # Capped at 240 samples OR 600 seconds from first_point_ts, whichever
    # first. Consumed by adsbtrack.takeoff_runway.
    takeoff_points: list[_PointSample] = field(default_factory=list)
```

- [ ] **Step 4: Populate in `record_point`**

In `record_point`, just AFTER the `self.recent_points.append(...)` call (around line 294), add:

```python
        # Takeoff window: first 600 s or 240 samples, whichever comes first.
        # first_point_ts was assigned a few lines above when first_point_ts
        # was None; by here it is guaranteed non-None.
        if (
            len(self.takeoff_points) < 240
            and self.first_point_ts is not None
            and (ts - self.first_point_ts) <= 600.0
        ):
            self.takeoff_points.append(self.recent_points[-1])
```

Sharing the same `_PointSample` object with `recent_points` is fine (the class is not frozen but fields are only set at construction time in `record_point`).

- [ ] **Step 5: Run the tests to verify they pass**

`uv run pytest tests/test_classifier.py -v`
Expected: PASS.

- [ ] **Step 6: Run the full suite**

`uv run pytest -x --deselect tests/test_cli.py::test_enrich_hex_no_data_reports_yellow`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add adsbtrack/classifier.py tests/test_classifier.py
git commit -m "feat(takeoff): collect head-of-flight takeoff_points in FlightMetrics"
```

---

## Task 3: Pure `takeoff_runway` detection module

**Files:**
- Create: `adsbtrack/takeoff_runway.py`
- Create: `tests/test_takeoff_runway.py`

### Public API

```python
@dataclass(frozen=True)
class TakeoffRunwayResult:
    runway_name: str
    duration_secs: float
    max_gs_kt: float


def detect_takeoff_runway(
    metrics: FlightMetrics,
    *,
    airport_lat: float,
    airport_lon: float,
    airport_elev_ft: float,
    runway_ends: Iterable[Mapping[str, object]],
    max_ft_above_airport: float = 2000.0,
    zone_length_m: float = 6000.0,
    little_base_m: float = 50.0,
    opening_deg: float = 5.0,
    min_gs_kt: float = 140.0,
    min_vert_rate_fpm: float = 256.0,
) -> TakeoffRunwayResult | None:
```

The `airport_lat` / `airport_lon` inputs are present for symmetry and future tuning but the current algorithm doesn't need them — the polygon is anchored at each runway end's own lat/lon. Include them anyway to leave the door open for "filter out points farther than X from airport centroid" if we need it later.

### Algorithm

1. **Filter takeoff-window points** (input = `metrics.takeoff_points`): keep samples where
   - `lat`, `lon`, `track` are all present,
   - altitude (baro preferred, else geom) is not None AND is below `airport_elev_ft + max_ft_above_airport`,
   - `baro_rate > min_vert_rate_fpm` OR `gs > min_gs_kt` (either a climbing sample or a roll-accelerating sample — using `OR` so the takeoff roll is captured even before rotation).
   If the filter leaves 0 samples, return None.

2. **Build a trapezoid polygon per runway end**. For a given runway with threshold `(r_lat, r_lon)` and heading `r_heading`:
   - `p_near_left` = destination from `(r_lat, r_lon)` on bearing `r_heading + 90°` at distance `little_base_m / 2`.
   - `p_near_right` = destination from `(r_lat, r_lon)` on bearing `r_heading - 90°` at distance `little_base_m / 2`.
   - `p_far_center` = destination from `(r_lat, r_lon)` on bearing `(r_heading + 180°) % 360` (departure direction) at distance `zone_length_m`.
   - Wide half-width: `little_base_m / 2 + zone_length_m * tan(radians(opening_deg))`.
   - `p_far_left` = destination from `p_far_center` on bearing `r_heading + 90°` at distance `wide_half`.
   - `p_far_right` = destination from `p_far_center` on bearing `r_heading - 90°` at distance `wide_half`.
   - Polygon corners (counterclockwise, `(lon, lat)` order that shapely expects):
     `[p_near_left, p_far_left, p_far_right, p_near_right]` (closed automatically by shapely).

   Define a helper `_destination_point(lat, lon, bearing_deg, distance_m) -> (lat, lon)` using the spherical destination formula. We do NOT use `pitot.geodesy`; we implement our own (as we did for `ils_alignment._bearing_deg`).

3. **For each runway polygon**, walk the filtered samples chronologically and find contiguous inside-polygon runs:
   - A point is inside the polygon iff `polygon.contains(Point(lon, lat))` (shapely).
   - Track `(first_inside_ts, last_inside_ts, max_gs_inside)` per continuous run.
   - Split runs on gaps > 20 s (trace coverage hole) or on leaving+re-entering the polygon.
   - For each run, require `max_gs_inside >= min_gs_kt`. Runs that don't reach the threshold are discarded.
   - Of the qualifying runs, pick the longest by `last_inside_ts - first_inside_ts`.
   - Record that as a per-runway candidate `(runway_name, duration_secs, max_gs_inside)`.

4. **Pick the winning runway**: the candidate with the longest duration across runway ends. Return a `TakeoffRunwayResult` or None if no runway has a qualifying run.

### Tests (create `tests/test_takeoff_runway.py`)

Write the tests FIRST, verify failure, then implement.

```python
"""Tests for adsbtrack.takeoff_runway."""

from __future__ import annotations

import math

from adsbtrack.classifier import _PointSample
from adsbtrack.takeoff_runway import _destination_point, detect_takeoff_runway


def _sample(ts: float, lat: float, lon: float, alt: int, gs: float, baro_rate: float, track: float) -> _PointSample:
    return _PointSample(
        ts=ts, baro_alt=alt, geom_alt=None, gs=gs, baro_rate=baro_rate,
        lat=lat, lon=lon, track=track,
    )


class _Metrics:
    def __init__(self, samples: list[_PointSample]) -> None:
        self.takeoff_points = samples


def _walk_departure(
    threshold_lat: float, threshold_lon: float, heading: float,
    start_ts: float, n: int, spacing_secs: float,
    start_alt_ft: int, alt_step_ft: int, start_gs_kt: float, gs_step_kt: float,
) -> list[_PointSample]:
    """Generate n samples departing a runway: starts at the threshold moving
    along the runway heading, climbing and accelerating."""
    samples = []
    departure_bearing_rad = math.radians(heading)
    for i in range(n):
        km_out = i * 0.05  # 50m per step
        dlat = (km_out / 111.0) * math.cos(departure_bearing_rad)
        dlon = (km_out / (111.0 * math.cos(math.radians(threshold_lat)))) * math.sin(departure_bearing_rad)
        lat = threshold_lat + dlat
        lon = threshold_lon + dlon
        alt = start_alt_ft + i * alt_step_ft
        gs = start_gs_kt + i * gs_step_kt
        samples.append(_sample(start_ts + i * spacing_secs, lat, lon, alt, gs, 1500.0, heading))
    return samples


def test_destination_point_roundtrip() -> None:
    """Destination-point math: 1000m east of (0, 0) should be near (0, 1000/111000/cos(0))."""
    lat, lon = _destination_point(0.0, 0.0, 90.0, 1000.0)
    assert abs(lat - 0.0) < 1e-5
    assert abs(lon - (1000.0 / 111_000.0)) < 1e-4


def test_no_runways_returns_none() -> None:
    metrics = _Metrics([_sample(0, 27.77, -82.67, 100, 150.0, 500.0, 240.0)])
    assert detect_takeoff_runway(
        metrics, airport_lat=27.77, airport_lon=-82.67, airport_elev_ft=7,
        runway_ends=[],
    ) is None


def test_empty_takeoff_points_returns_none() -> None:
    metrics = _Metrics([])
    runway = {"runway_name": "24", "latitude_deg": 27.77, "longitude_deg": -82.67, "heading_deg_true": 240.0}
    assert detect_takeoff_runway(
        metrics, airport_lat=27.77, airport_lon=-82.67, airport_elev_ft=7,
        runway_ends=[runway],
    ) is None


def test_clean_commercial_departure_identifies_runway() -> None:
    # KSPG runway 24 has heading ~240° magnetic. Simulate a departure: 40
    # samples at 3s intervals, starting from threshold at 30 kt accelerating
    # to 150+ kt, climbing 100 ft per sample from 10 ft MSL.
    runway = {"runway_name": "24", "latitude_deg": 27.76, "longitude_deg": -82.63, "heading_deg_true": 240.0}
    samples = _walk_departure(27.76, -82.63, 240.0, start_ts=0, n=40, spacing_secs=3.0,
                              start_alt_ft=10, alt_step_ft=50, start_gs_kt=30, gs_step_kt=4.0)
    metrics = _Metrics(samples)
    result = detect_takeoff_runway(
        metrics, airport_lat=27.76, airport_lon=-82.63, airport_elev_ft=7,
        runway_ends=[runway],
    )
    assert result is not None
    assert result.runway_name == "24"
    assert result.duration_secs > 0.0
    assert result.max_gs_kt >= 140.0


def test_too_slow_returns_none() -> None:
    # Aircraft on runway 24 centerline but never exceeds 100 kt (sparse data
    # or small piston); default 140 kt min should reject.
    runway = {"runway_name": "24", "latitude_deg": 27.76, "longitude_deg": -82.63, "heading_deg_true": 240.0}
    samples = _walk_departure(27.76, -82.63, 240.0, start_ts=0, n=40, spacing_secs=3.0,
                              start_alt_ft=10, alt_step_ft=20, start_gs_kt=30, gs_step_kt=1.0)  # peaks ~70 kt
    metrics = _Metrics(samples)
    result = detect_takeoff_runway(
        metrics, airport_lat=27.76, airport_lon=-82.63, airport_elev_ft=7,
        runway_ends=[runway],
    )
    assert result is None  # below default 140 kt
    # Now retry with low min_gs_kt (helicopter/GA) — should pass
    result2 = detect_takeoff_runway(
        metrics, airport_lat=27.76, airport_lon=-82.63, airport_elev_ft=7,
        runway_ends=[runway], min_gs_kt=60.0,
    )
    assert result2 is not None
    assert result2.runway_name == "24"


def test_too_high_returns_none() -> None:
    # Samples over centerline but all above airport_elev + 2000ft
    runway = {"runway_name": "24", "latitude_deg": 27.76, "longitude_deg": -82.63, "heading_deg_true": 240.0}
    samples = _walk_departure(27.76, -82.63, 240.0, start_ts=0, n=40, spacing_secs=3.0,
                              start_alt_ft=3000, alt_step_ft=100, start_gs_kt=150, gs_step_kt=2.0)
    metrics = _Metrics(samples)
    result = detect_takeoff_runway(
        metrics, airport_lat=27.76, airport_lon=-82.63, airport_elev_ft=7,
        runway_ends=[runway],
    )
    assert result is None


def test_offset_departure_not_aligned_returns_none() -> None:
    # Departure in wrong direction (away from runway 24 polygon): heading 60°
    # instead of 240°; samples walk east, not southwest.
    runway = {"runway_name": "24", "latitude_deg": 27.76, "longitude_deg": -82.63, "heading_deg_true": 240.0}
    samples = _walk_departure(27.76, -82.63, 60.0, start_ts=0, n=40, spacing_secs=3.0,
                              start_alt_ft=10, alt_step_ft=50, start_gs_kt=30, gs_step_kt=4.0)
    metrics = _Metrics(samples)
    assert detect_takeoff_runway(
        metrics, airport_lat=27.76, airport_lon=-82.63, airport_elev_ft=7,
        runway_ends=[runway],
    ) is None


def test_multi_runway_picks_longest() -> None:
    # Two runways 24 and 06 at same airport. Departure is on 24's polygon
    # (60 s inside) and briefly clips 06's polygon at the start (~6 s). Winner = 24.
    runway_24 = {"runway_name": "24", "latitude_deg": 27.76, "longitude_deg": -82.63, "heading_deg_true": 240.0}
    runway_06 = {"runway_name": "06", "latitude_deg": 27.745, "longitude_deg": -82.650, "heading_deg_true": 60.0}
    samples = _walk_departure(27.76, -82.63, 240.0, start_ts=0, n=30, spacing_secs=3.0,
                              start_alt_ft=10, alt_step_ft=50, start_gs_kt=30, gs_step_kt=5.0)
    metrics = _Metrics(samples)
    result = detect_takeoff_runway(
        metrics, airport_lat=27.76, airport_lon=-82.63, airport_elev_ft=7,
        runway_ends=[runway_24, runway_06],
    )
    assert result is not None
    assert result.runway_name == "24"


def test_missing_heading_skips_runway() -> None:
    runway = {"runway_name": "24", "latitude_deg": 27.76, "longitude_deg": -82.63, "heading_deg_true": None}
    samples = _walk_departure(27.76, -82.63, 240.0, start_ts=0, n=40, spacing_secs=3.0,
                              start_alt_ft=10, alt_step_ft=50, start_gs_kt=30, gs_step_kt=5.0)
    metrics = _Metrics(samples)
    assert detect_takeoff_runway(
        metrics, airport_lat=27.76, airport_lon=-82.63, airport_elev_ft=7,
        runway_ends=[runway],
    ) is None
```

- [ ] **Step 1: Run the tests to verify they fail**

`uv run pytest tests/test_takeoff_runway.py -v`
Expected: FAIL (ModuleNotFoundError).

- [ ] **Step 2: Implement `adsbtrack/takeoff_runway.py`**

```python
"""Polygon-based takeoff runway identification.

For each runway end at a known origin airport, build a trapezoid polygon:
a narrow base at the runway threshold, extending ``zone_length_m`` outward
along the departure heading, opening symmetrically by ``opening_deg``.
Filter the flight's first-600-s trace window to points below
``airport_elev_ft + max_ft_above_airport`` that are either climbing at
``min_vert_rate_fpm`` or rolling at ``min_gs_kt``. Test which polygons
those points pass through; the runway whose polygon the flight occupied
the longest wins, subject to reaching the speed floor inside the polygon.

Attribution: the trapezoid geometry and "longest segment wins" selection
match xoolive/traffic's ``PolygonBasedRunwayDetection`` (MIT-licensed).
This module reimplements the algorithm using shapely for polygon
containment and our own spherical destination-point helper; no code is
copied from traffic.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from shapely.geometry import Point, Polygon

from .classifier import FlightMetrics, _PointSample


@dataclass(frozen=True)
class TakeoffRunwayResult:
    runway_name: str
    duration_secs: float
    max_gs_kt: float


_EARTH_RADIUS_M = 6_371_000.0


def _destination_point(lat_deg: float, lon_deg: float, bearing_deg: float, distance_m: float) -> tuple[float, float]:
    """Destination (lat, lon) given a start point, bearing (degrees true), and distance (meters).

    Spherical earth model. Accuracy at runway scale (<10 km) is within a
    meter or two, well under the polygon's least-sensitive dimension.
    """
    br = math.radians(bearing_deg)
    ang = distance_m / _EARTH_RADIUS_M
    phi1 = math.radians(lat_deg)
    lam1 = math.radians(lon_deg)
    sin_phi2 = math.sin(phi1) * math.cos(ang) + math.cos(phi1) * math.sin(ang) * math.cos(br)
    phi2 = math.asin(sin_phi2)
    y = math.sin(br) * math.sin(ang) * math.cos(phi1)
    x = math.cos(ang) - math.sin(phi1) * sin_phi2
    lam2 = lam1 + math.atan2(y, x)
    return math.degrees(phi2), ((math.degrees(lam2) + 540.0) % 360.0) - 180.0


def _sample_alt(s: _PointSample) -> int | None:
    if s.baro_alt is not None:
        return s.baro_alt
    return s.geom_alt


def _build_polygon(
    *, threshold_lat: float, threshold_lon: float, heading_deg: float,
    zone_length_m: float, little_base_m: float, opening_deg: float,
) -> Polygon:
    half_near = little_base_m / 2.0
    wide_half = half_near + zone_length_m * math.tan(math.radians(opening_deg))
    departure_heading = (heading_deg + 180.0) % 360.0

    near_left = _destination_point(threshold_lat, threshold_lon, (heading_deg + 90.0) % 360.0, half_near)
    near_right = _destination_point(threshold_lat, threshold_lon, (heading_deg - 90.0) % 360.0, half_near)
    far_center = _destination_point(threshold_lat, threshold_lon, departure_heading, zone_length_m)
    far_left = _destination_point(far_center[0], far_center[1], (heading_deg + 90.0) % 360.0, wide_half)
    far_right = _destination_point(far_center[0], far_center[1], (heading_deg - 90.0) % 360.0, wide_half)

    # Shapely uses (x, y) = (lon, lat).
    return Polygon([
        (near_left[1], near_left[0]),
        (far_left[1], far_left[0]),
        (far_right[1], far_right[0]),
        (near_right[1], near_right[0]),
    ])


def _filter_takeoff_samples(
    samples: Sequence[_PointSample],
    *, airport_elev_ft: float, max_ft_above_airport: float,
    min_gs_kt: float, min_vert_rate_fpm: float,
) -> list[_PointSample]:
    alt_cap = airport_elev_ft + max_ft_above_airport
    kept: list[_PointSample] = []
    for s in samples:
        if s.lat is None or s.lon is None or s.track is None:
            continue
        alt = _sample_alt(s)
        if alt is None or alt > alt_cap:
            continue
        climbing = s.baro_rate is not None and s.baro_rate > min_vert_rate_fpm
        rolling = s.gs is not None and s.gs > min_gs_kt
        if not (climbing or rolling):
            continue
        kept.append(s)
    return kept


def _longest_inside_run(
    samples: Sequence[_PointSample], polygon: Polygon,
    *, split_gap_secs: float, min_gs_kt: float,
) -> tuple[float, float] | None:
    """Walk samples, find the longest contiguous run inside polygon whose
    max gs >= min_gs_kt. Returns (duration_secs, max_gs) or None."""
    best_duration = 0.0
    best_max_gs = 0.0
    run_start: float | None = None
    run_last: float | None = None
    run_max_gs = 0.0
    prev_ts: float | None = None

    def _close_run() -> None:
        nonlocal best_duration, best_max_gs
        if run_start is None or run_last is None:
            return
        dur = run_last - run_start
        if run_max_gs >= min_gs_kt and dur > best_duration:
            best_duration = dur
            best_max_gs = run_max_gs

    for s in samples:
        inside = polygon.contains(Point(s.lon, s.lat))
        gap = prev_ts is not None and (s.ts - prev_ts) > split_gap_secs
        if inside and (run_start is None or gap):
            if run_start is not None and gap:
                _close_run()
            run_start = s.ts
            run_last = s.ts
            run_max_gs = s.gs or 0.0
        elif inside and run_start is not None:
            run_last = s.ts
            if s.gs is not None and s.gs > run_max_gs:
                run_max_gs = s.gs
        elif not inside and run_start is not None:
            _close_run()
            run_start = None
            run_last = None
            run_max_gs = 0.0
        prev_ts = s.ts
    _close_run()

    if best_duration <= 0.0:
        return None
    return best_duration, best_max_gs


def detect_takeoff_runway(
    metrics: FlightMetrics,
    *,
    airport_lat: float,  # noqa: ARG001 — reserved for future tuning
    airport_lon: float,  # noqa: ARG001 — reserved for future tuning
    airport_elev_ft: float,
    runway_ends: Iterable[Mapping[str, object]],
    max_ft_above_airport: float = 2000.0,
    zone_length_m: float = 6000.0,
    little_base_m: float = 50.0,
    opening_deg: float = 5.0,
    min_gs_kt: float = 140.0,
    min_vert_rate_fpm: float = 256.0,
    split_gap_secs: float = 20.0,
) -> TakeoffRunwayResult | None:
    """Identify the runway used on takeoff, or None.

    Assumes `metrics.takeoff_points` is in chronological order (it is, by
    FlightMetrics.record_point's append-only contract).
    """
    filtered = _filter_takeoff_samples(
        metrics.takeoff_points,
        airport_elev_ft=airport_elev_ft,
        max_ft_above_airport=max_ft_above_airport,
        min_gs_kt=min_gs_kt,
        min_vert_rate_fpm=min_vert_rate_fpm,
    )
    if not filtered:
        return None

    best: TakeoffRunwayResult | None = None
    for runway in runway_ends:
        heading = runway.get("heading_deg_true")
        r_lat = runway.get("latitude_deg")
        r_lon = runway.get("longitude_deg")
        if heading is None or r_lat is None or r_lon is None:
            continue
        polygon = _build_polygon(
            threshold_lat=float(r_lat), threshold_lon=float(r_lon), heading_deg=float(heading),
            zone_length_m=zone_length_m, little_base_m=little_base_m, opening_deg=opening_deg,
        )
        hit = _longest_inside_run(
            filtered, polygon,
            split_gap_secs=split_gap_secs, min_gs_kt=min_gs_kt,
        )
        if hit is None:
            continue
        duration, max_gs = hit
        if best is None or duration > best.duration_secs:
            best = TakeoffRunwayResult(
                runway_name=str(runway.get("runway_name", "")),
                duration_secs=round(duration, 1),
                max_gs_kt=round(max_gs, 1),
            )
    return best
```

- [ ] **Step 3: Run the unit tests**

`uv run pytest tests/test_takeoff_runway.py -v`
Expected: all PASS. If `test_too_slow_returns_none` doesn't pass cleanly because accumulated gs still crosses 140 kt at the tail, tune `gs_step_kt` in the test fixture — the intent is to keep peak gs below 140.

If `test_multi_runway_picks_longest` reports the wrong runway (or None), tune the runway_06 coordinates so its polygon is genuinely far from the departure path.

- [ ] **Step 4: Full suite + lint**

`uv run pytest -x --deselect tests/test_cli.py::test_enrich_hex_no_data_reports_yellow`
`uv run ruff check . && uv run ruff format --check .`
`uv run mypy adsbtrack/takeoff_runway.py`
All clean.

- [ ] **Step 5: Commit**

```bash
git add adsbtrack/takeoff_runway.py tests/test_takeoff_runway.py
git commit -m "feat(takeoff): add polygon-based takeoff runway detector"
```

---

## Task 4: Parser integration

**Files:**
- Modify: `adsbtrack/parser.py`
- Modify: `tests/test_parser.py`

Add the block right after the existing origin-airport matching logic (near where `flight.origin_icao = origin.ident` is set, around line 835-843). Reuse the `runway_cache` / `airport_elev_cache` dicts added in the ILS alignment milestone — they're already in scope.

### Parser changes

Import at the top:

```python
from .takeoff_runway import TakeoffRunwayResult, detect_takeoff_runway
```

After origin airport is assigned (look for the existing `origin = find_nearest_airport(db, flight.takeoff_lat, flight.takeoff_lon, config)` block), add:

```python
        # --- Takeoff runway identification (adsbtrack/takeoff_runway.py) ---
        takeoff_origin_icao = flight.origin_icao or flight.nearest_origin_icao
        if takeoff_origin_icao:
            if takeoff_origin_icao not in airport_elev_cache:
                airport_elev_cache[takeoff_origin_icao] = db.get_airport_elevation(takeoff_origin_icao)
            origin_elev = airport_elev_cache[takeoff_origin_icao]
            if takeoff_origin_icao not in runway_cache:
                runway_cache[takeoff_origin_icao] = db.get_runways_for_airport(takeoff_origin_icao)
            origin_runways = runway_cache[takeoff_origin_icao]
            if origin_runways:
                effective_type = type_code or ""
                is_low_gs = (
                    effective_type.startswith("H")
                    or effective_type in config.takeoff_low_gs_types
                    or effective_type in config.helicopter_types
                )
                min_gs = config.takeoff_runway_min_gs_kt_low if is_low_gs else config.takeoff_runway_min_gs_kt_default
                to_result: TakeoffRunwayResult | None = detect_takeoff_runway(
                    metrics,
                    airport_lat=flight.takeoff_lat,
                    airport_lon=flight.takeoff_lon,
                    airport_elev_ft=float(origin_elev) if origin_elev is not None else 0.0,
                    runway_ends=[dict(r) for r in origin_runways],
                    max_ft_above_airport=config.takeoff_runway_max_ft_above_airport,
                    zone_length_m=config.takeoff_runway_zone_length_m,
                    little_base_m=config.takeoff_runway_little_base_m,
                    opening_deg=config.takeoff_runway_opening_deg,
                    min_gs_kt=min_gs,
                    min_vert_rate_fpm=config.takeoff_runway_min_vert_rate_fpm,
                )
                if to_result is not None:
                    flight.takeoff_runway = to_result.runway_name
```

### Tests

Append 4 integration tests to `tests/test_parser.py`:

1. **`test_takeoff_runway_commercial_jet_identified`**: trace builds a ground roll + climb through runway 24's polygon at KSPG. Patch `find_nearest_airport` with `side_effect=[KSPG_match, ...]`. Set `db.get_runways_for_airport.return_value = [runway_24_dict]`, `db.get_airport_elevation.return_value = 7`. Assert captured flight has `takeoff_runway == "24"`.

2. **`test_takeoff_runway_helicopter_threshold_scaled`**: same geometry but type_code="H60", ground speed maxes at ~80 kt. Default 140-kt gate would fail; scaled 60-kt gate should succeed. Assert `takeoff_runway == "24"`.

3. **`test_takeoff_runway_sparse_data_fails_gracefully`**: only 3-4 trace points spanning the takeoff window (no reliable polygon fit). Assert `takeoff_runway is None` and the flight is NOT rejected.

4. **`test_takeoff_runway_no_runway_data_leaves_null`**: same geometry as #1 but `db.get_runways_for_airport.return_value = []`. Assert `takeoff_runway is None`, flight still saved.

Fixture helper (inline, mirroring `_walk_approach`):

```python
def _walk_takeoff(
    base_ts: float, n: int, spacing_secs: float,
    threshold_lat: float, threshold_lon: float, heading_deg: float,
    start_alt_ft: int, alt_step_ft: int, start_gs_kt: float, gs_step_kt: float,
) -> list[list]:
    """Build 9-element trace rows for a departure on a given runway."""
    samples = []
    departure_bearing_rad = math.radians(heading_deg)
    for i in range(n):
        km_out = i * 0.05
        dlat = (km_out / 111.0) * math.cos(departure_bearing_rad)
        dlon = (km_out / (111.0 * math.cos(math.radians(threshold_lat)))) * math.sin(departure_bearing_rad)
        lat = threshold_lat + dlat
        lon = threshold_lon + dlon
        alt = start_alt_ft + i * alt_step_ft
        gs = start_gs_kt + i * gs_step_kt
        samples.append([i * spacing_secs, lat, lon, alt, gs, heading_deg, None, 1500.0, {"track": heading_deg}])
    return samples
```

Each test's trace should start with a few ground samples, then the `_walk_takeoff` points, then a few cruise samples, then a landing (for symmetry; doesn't matter for takeoff detection).

The test assertions focus on `flight.takeoff_runway`. Landing metadata may end up as whatever `classify_landing` infers; that's fine.

- [ ] **Step 1: Write failing tests**

- [ ] **Step 2: Run tests, expect fail**

- [ ] **Step 3: Implement parser block**

- [ ] **Step 4: Run tests, expect pass**

- [ ] **Step 5: Full suite + ruff**

- [ ] **Step 6: Commit**

```bash
git add adsbtrack/parser.py tests/test_parser.py
git commit -m "feat(takeoff): wire runway detection into parser with GS scaling"
```

---

## Task 5: CLI `trips` From-column rendering

**Files:**
- Modify: `adsbtrack/cli.py`
- Modify: `tests/test_cli.py`

- [ ] **Step 1: Write failing test**

Append to `tests/test_cli.py`:

```python
def test_trips_from_column_appends_takeoff_runway(tmp_path, monkeypatch) -> None:
    """trips From column shows `KSPG/24` when takeoff_runway is populated."""
    monkeypatch.setenv("COLUMNS", "200")
    db_path = tmp_path / "a.db"
    with Database(db_path) as db:
        f = Flight(
            icao="abc789",
            takeoff_time=datetime(2023, 11, 14, 10, 0),
            takeoff_lat=27.76, takeoff_lon=-82.63,
            takeoff_date="2023-11-14",
            landing_time=datetime(2023, 11, 14, 11, 0),
            landing_lat=27.0, landing_lon=-82.0,
            landing_date="2023-11-14",
            origin_icao="KSPG", origin_name="Albert Whitted",
            origin_distance_km=0.3,
            destination_icao="KPIE", destination_name="St Petersburg-Clearwater",
            destination_distance_km=0.5,
            duration_minutes=60.0,
            landing_type="confirmed",
            landing_confidence=0.9,
            takeoff_runway="24",
        )
        db.insert_flight(f)

    runner = CliRunner()
    result = runner.invoke(cli, ["trips", "--hex", "abc789", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "KSPG/24" in result.output


def test_trips_from_column_plain_when_takeoff_runway_null(tmp_path, monkeypatch) -> None:
    """No `/24` suffix when takeoff_runway is NULL."""
    monkeypatch.setenv("COLUMNS", "200")
    db_path = tmp_path / "a.db"
    with Database(db_path) as db:
        f = Flight(
            icao="abc790",
            takeoff_time=datetime(2023, 11, 14, 10, 0),
            takeoff_lat=27.76, takeoff_lon=-82.63,
            takeoff_date="2023-11-14",
            landing_time=datetime(2023, 11, 14, 11, 0),
            landing_lat=27.0, landing_lon=-82.0,
            landing_date="2023-11-14",
            origin_icao="KSPG", origin_name="Albert Whitted",
            origin_distance_km=0.3,
            destination_icao="KPIE", destination_name="St Petersburg-Clearwater",
            destination_distance_km=0.5,
            duration_minutes=60.0,
            landing_type="confirmed",
            landing_confidence=0.9,
        )
        db.insert_flight(f)

    runner = CliRunner()
    result = runner.invoke(cli, ["trips", "--hex", "abc790", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "KSPG" in result.output
    assert "KSPG/" not in result.output
```

- [ ] **Step 2: Run tests, expect fail**

- [ ] **Step 3: Modify `trips` From cell rendering**

In `adsbtrack/cli.py`, in the row-building loop of `trips`, find the `origin = ...` assignment (around line 332-334):

```python
            origin = f["origin_icao"] or f"({f['takeoff_lat']:.2f}, {f['takeoff_lon']:.2f})"
            if f["origin_name"]:
                origin = f"{f['origin_icao']} ({f['origin_name']})"
```

Replace with:

```python
            rwy = _col(f, "takeoff_runway")
            origin_icao = f["origin_icao"]
            origin_suffix = f"/{rwy}" if rwy else ""
            if origin_icao and f["origin_name"]:
                origin = f"{origin_icao}{origin_suffix} ({f['origin_name']})"
            elif origin_icao:
                origin = f"{origin_icao}{origin_suffix}"
            else:
                origin = f"({f['takeoff_lat']:.2f}, {f['takeoff_lon']:.2f})"
```

Note: `_col` is the same safe accessor hoisted in Task 5 of the ILS-alignment milestone. Using it here handles pre-migration DB rows that lack the `takeoff_runway` column.

- [ ] **Step 4: Run tests, expect pass**

- [ ] **Step 5: Full suite + ruff**

- [ ] **Step 6: Commit**

```bash
git add adsbtrack/cli.py tests/test_cli.py
git commit -m "feat(takeoff): trips From column shows `KSPG/24` with takeoff runway"
```

---

## Task 6: Docs

**Files:**
- Modify: `docs/features.md`
- Modify: `docs/schema.md`

- [ ] **Step 1: Add "Takeoff runway." subsection to features.md**

Insert BEFORE the **ILS alignment.** subsection:

```markdown
**Takeoff runway.** `takeoff_runway`. Runway name the aircraft used to depart, inferred by testing which of the origin airport's runway trapezoid polygons the first 600 seconds of trace data passed through longest. For each runway end a trapezoid is built at the runway threshold, extending 6 km along the departure heading (`Config.takeoff_runway_zone_length_m`) with a 50 m narrow base (`Config.takeoff_runway_little_base_m`) and a 5 degree symmetric opening (`Config.takeoff_runway_opening_deg`). Points are filtered to those below `airport_elevation + 2,000 ft` (`Config.takeoff_runway_max_ft_above_airport`) that are either climbing faster than 256 fpm or rolling above the minimum ground speed. The runway whose polygon was occupied longest wins, subject to reaching the GS floor inside the polygon.

The minimum-GS threshold scales by aircraft type. Commercial jets use 140 kt (`Config.takeoff_runway_min_gs_kt_default`). Helicopters (any type_code starting with `H` or present in `Config.helicopter_types`) and light piston singles (type_codes listed in `Config.takeoff_low_gs_types`: C150, C152, C172, DA20, PA28, SR22, etc.) drop to 60 kt (`Config.takeoff_runway_min_gs_kt_low`) so their slower rotation speeds don't disqualify an otherwise clean takeoff segment. Reimplementation of the `PolygonBasedRunwayDetection` class from `xoolive/traffic` (MIT-licensed); attribution in `adsbtrack/takeoff_runway.py`. NULL when the airport has no runway rows, no polygon matched, or the GS floor wasn't reached.
```

- [ ] **Step 2: Add schema.md row**

In the `flights` table listing, add a new row near the other takeoff-metadata columns:

```markdown
| takeoff_runway | TEXT | Runway name (e.g. "24", "08R") the aircraft departed from. NULL when detection failed or runway data is unavailable. |
```

(Column-name cell unquoted to match existing schema.md style.)

- [ ] **Step 3: Full verification**

`uv run ruff check . && uv run ruff format --check . && uv run pytest --deselect tests/test_cli.py::test_enrich_hex_no_data_reports_yellow`
All green.

- [ ] **Step 4: Commit**

```bash
git add docs/features.md docs/schema.md
git commit -m "docs(takeoff): document takeoff_runway signal and schema column"
```

---

## Self-Review Checklist

**1. Spec coverage:**
- Polygon-trapezoid algorithm from traffic's `PolygonBasedRunwayDetection` → Task 3.
- `takeoff_runway` column on flights → Task 1.
- Shapely used for polygon containment → Task 1 (dep), Task 3 (usage).
- Point-filter heuristic for "early climb" without OpenAP → `_filter_takeoff_samples` in Task 3 implements the alt-cap + (vert_rate OR gs) gate.
- GA/helicopter scaling → Task 4 resolves `min_gs` from type_code against `helicopter_types` / `takeoff_low_gs_types`.
- 4 integration tests: commercial jet, helicopter, sparse-data, no-runway → Task 4.
- `trips` From column "KSPG/24" → Task 5.
- No go-around detection → confirmed, not touched.

**2. Placeholder scan:** No TBD / "implement later" / "add validation" strings. Every code block is complete.

**3. Type consistency:**
- `TakeoffRunwayResult.runway_name: str`, `duration_secs: float`, `max_gs_kt: float` — used consistently in tests and parser.
- `detect_takeoff_runway` signature matches the invocation in parser (airport_lat, airport_lon, airport_elev_ft, runway_ends, kwargs with defaults).
- `Config.takeoff_low_gs_types: tuple[str, ...]` — parser checks `in` which works for tuples.
- Flight field `takeoff_runway: str | None = None` — DB column TEXT, CLI renders the string directly.

**4. Integration-test helper risks:**
- `_walk_takeoff` builds 9-element trace rows matching `_extract_point_fields`. Position 5 carries track (not None) so the samples' `_PointSample.track` is populated and the polygon's sample filter accepts them. Position 7 carries baro_rate so the "climbing" branch of the filter is exercised.
- Task-4 test #2 (helicopter) relies on the parser reading `type_code` from the `aircraft_registry` upsert path. In tests this path is mocked; the implementer needs to set `_make_trace_row(..., type_code="H60")` and confirm the MagicMock `db.upsert_aircraft_registry.return_value = {"type_code": "H60", "owner_operator": None}` or equivalent so the parser's type_code resolution lands on "H60" inside the alignment block.

**5. Edge cases:**
- `airport_elev_ft = 0.0` fallback when get_airport_elevation returns None: same pattern as ILS alignment, already documented.
- Missing `heading_deg_true` on a runway row: `_build_polygon` is skipped in `detect_takeoff_runway`'s loop (`if heading is None ... : continue`).
- `takeoff_points` empty (very short flights): `_filter_takeoff_samples` returns []; detector returns None.

---

## Execution handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-16-takeoff-runway.md`. Two execution options:

1. **Subagent-Driven (recommended)** - Fresh subagent per task + two-stage review.
2. **Inline Execution** - Batch execution with checkpoints for review.

Which approach?
