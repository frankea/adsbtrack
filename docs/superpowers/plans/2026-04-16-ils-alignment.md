# ILS Alignment Landing Signal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a runway-aware geometric signal that records whether (and for how long) an arriving aircraft was established on final for a specific runway end, feed it into landing confidence, and let it upgrade `signal_lost` classifications to `dropped_on_approach` when the alignment is strong.

**Architecture:** A pure detection module (`adsbtrack/ils_alignment.py`) consumes `FlightMetrics.recent_points` plus the runway-end rows for a candidate airport and returns the longest geometric alignment segment across all runway ends. Parser glue invokes it after landing-airport matching, stores three columns on `flights`, bumps landing confidence in tiers, and optionally upgrades `signal_lost` → `dropped_on_approach` when the alignment crosses 60 seconds. No code is copied from `xoolive/traffic`; the algorithm description is reimplemented in our style with attribution in the module docstring.

**Tech Stack:** Python 3.12, spherical Haversine (already in `classifier._haversine_m`), circular-mean bearing (already in `features._bearing_deg`), SQLite (existing `runways` table from the prior milestone), Click + Rich (existing CLI stack).

---

## Scope notes

Per the user's spec, min segment duration is 60 s for segments to count, but the confidence-bonus schedule rewards both `>= 30 s` (+0.15) and `>= 60 s` (+0.25). Resolution: **run the detector with a 30 s minimum** (so the `>= 30 s` tier is ever reachable) and keep the 60 s tier as the preferred "+0.25" threshold. Rename the config knob `ils_alignment_min_duration_secs = 30.0` to reflect the implementation.

Helicopters rarely fly ILS approaches. Alignment runs on every flight with a candidate airport regardless of type, but rotorcraft will overwhelmingly score NULL because their approach geometry doesn't meet the offset + towards-threshold checks. No type gating is required.

---

## File structure

**New files**
- `adsbtrack/ils_alignment.py` - pure detection: `IlsAlignmentResult` dataclass + `detect_ils_alignment(metrics, *, airport_elev_ft, runway_ends, ...)` function.
- `tests/test_ils_alignment.py` - unit tests for bearing/offset math, segment splitting, min-duration gating, multi-runway selection, airport-without-runways fallthrough.
- `tests/fixtures/runways_alignment.csv` - 3-runway fixture (ILS test airport + a no-runway heliport).

**Modified files**
- `adsbtrack/classifier.py` - add `track: float | None = None` to `_PointSample`, pass it from `record_point`. No other behavior change.
- `adsbtrack/db.py` - 3 new ALTER columns in `_migrate_add_flight_columns`, matching `flights` CREATE TABLE, `insert_flight` extension, new `get_runways_for_airport(ident)` helper.
- `adsbtrack/models.py` - 3 new optional fields on `Flight`: `aligned_runway`, `aligned_seconds`, `aligned_min_offset_m`.
- `adsbtrack/config.py` - 5 new fields: `ils_alignment_max_offset_m`, `ils_alignment_min_duration_secs`, `ils_alignment_split_gap_secs`, `ils_alignment_max_ft_above_airport`, and the two bonus thresholds (`ils_alignment_bonus_short_secs=30.0`, `ils_alignment_bonus_long_secs=60.0`) + amounts (`ils_alignment_bonus_short=0.15`, `ils_alignment_bonus_long=0.25`).
- `adsbtrack/parser.py` - after landing-airport match: query runways, call `detect_ils_alignment`, set flight columns, apply confidence bonus, apply classification upgrade rule.
- `adsbtrack/cli.py` - `trips --alignment/--no-alignment` flag adds a rendered column.
- `tests/test_parser.py` - integration tests (confirmed landing aligned, overflight not aligned, DROP aligned, no-runway-data candidate).
- `tests/test_cli.py` - trips `--alignment` column smoke test.
- `docs/features.md` - new "ILS alignment" subsection; update "Landing types" and "Landing confidence" prose.
- `docs/schema.md` - add the three alignment columns to the `flights` table listing.

---

## Task 1: Config knobs, Flight model fields, DB schema + migration

**Files:**
- Modify: `adsbtrack/config.py` (append to `@dataclass Config` around line 280 next to `landing_anchor_window_minutes`)
- Modify: `adsbtrack/models.py` (append to `@dataclass Flight`)
- Modify: `adsbtrack/db.py`:
  - `SCHEMA` CREATE TABLE flights (add 3 columns near the other landing-confidence columns, around line 60)
  - `_migrate_add_flight_columns` (append 3 entries)
  - `insert_flight` (extend column list, `?` placeholder count, and value tuple)
  - Add `get_runways_for_airport` method
- Test: `tests/test_db.py` (new file may already exist; extend or create)

- [ ] **Step 1: Write the failing test for new DB columns + runway query**

Add to `tests/test_db.py` (create if missing):

```python
from __future__ import annotations

import sqlite3
from pathlib import Path

from adsbtrack.db import Database


def test_flights_table_has_alignment_columns(tmp_path: Path) -> None:
    db_path = tmp_path / "a.db"
    with Database(db_path) as db:
        cols = {row[1] for row in db.conn.execute("PRAGMA table_info(flights)").fetchall()}
    assert {"aligned_runway", "aligned_seconds", "aligned_min_offset_m"}.issubset(cols)


def test_get_runways_for_airport_returns_ordered_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "a.db"
    with Database(db_path) as db:
        db.insert_runway_ends(
            [
                ("KFAKE", "08R", 33.64, -84.43, 1026, 82.7, 9000, 150, "ASP", 0, 0),
                ("KFAKE", "26L", 33.64, -84.44, 1026, 262.7, 9000, 150, "ASP", 0, 0),
                ("KOTHR", "18", 33.00, -84.00, 800, 180.0, 5000, 100, "ASP", 0, 0),
            ]
        )
        rows = db.get_runways_for_airport("KFAKE")
    names = [r["runway_name"] for r in rows]
    assert names == ["08R", "26L"]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_db.py -v`
Expected: FAIL on `test_flights_table_has_alignment_columns` (AssertionError: columns not a subset) and on `test_get_runways_for_airport_returns_ordered_rows` (AttributeError: no method).

- [ ] **Step 3: Add config fields**

Open `adsbtrack/config.py`. Immediately AFTER the block ending with `landing_anchor_window_minutes: float = 10.0` (near line 280), insert:

```python
    # --- ILS alignment detector (see adsbtrack/ils_alignment.py) ---
    # A trajectory point is "aligned" with a runway end when it is within
    # `max_offset_m` of the extended centerline, moving toward the threshold,
    # and below `max_ft_above_airport` AGL. Aligned points are split into
    # segments on gaps larger than `split_gap_secs`; any segment at least
    # `min_duration_secs` long becomes the flight's aligned_runway. The two
    # `bonus_*` pairs drive the landing-confidence bump: no bonus under
    # `bonus_short_secs`, `bonus_short` between short and long thresholds,
    # `bonus_long` at or above the long threshold.
    ils_alignment_max_offset_m: float = 100.0
    ils_alignment_min_duration_secs: float = 30.0
    ils_alignment_split_gap_secs: float = 20.0
    ils_alignment_max_ft_above_airport: float = 5000.0
    ils_alignment_bonus_short_secs: float = 30.0
    ils_alignment_bonus_long_secs: float = 60.0
    ils_alignment_bonus_short: float = 0.15
    ils_alignment_bonus_long: float = 0.25
```

- [ ] **Step 4: Add Flight fields**

Open `adsbtrack/models.py`. Append inside the `Flight` dataclass, AFTER `landing_anchor_method: str | None = None`:

```python

    # --- ILS alignment signal (adsbtrack/ils_alignment.py) ---
    # Populated when the flight's landing airport has runway rows and the
    # trajectory geometrically aligned with one of them for at least the
    # configured minimum duration. NULL when no alignment was found, the
    # airport has no runway data, or the flight never had a landing match.
    aligned_runway: str | None = None
    aligned_seconds: float | None = None
    aligned_min_offset_m: float | None = None
```

- [ ] **Step 5: Add columns to SCHEMA CREATE TABLE flights**

Open `adsbtrack/db.py`. Find the `CREATE TABLE IF NOT EXISTS flights (` block. Just BEFORE the closing `)` and AFTER the `landing_anchor_method TEXT,` line, insert:

```sql
    aligned_runway TEXT,
    aligned_seconds REAL,
    aligned_min_offset_m REAL,
```

- [ ] **Step 6: Extend `_migrate_add_flight_columns`**

At the end of the `new_columns` list in `_migrate_add_flight_columns`, just after `("landing_anchor_method", "TEXT"),`, append:

```python
        # ILS alignment signal (adsbtrack/ils_alignment.py)
        ("aligned_runway", "TEXT"),
        ("aligned_seconds", "REAL"),
        ("aligned_min_offset_m", "REAL"),
```

- [ ] **Step 7: Extend insert_flight INSERT**

In `Database.insert_flight`, update three places:

1. Extend the column list in the SQL text. Replace:

```python
                acars_out, acars_off, acars_on, acars_in, landing_anchor_method)
```

with:

```python
                acars_out, acars_off, acars_on, acars_in, landing_anchor_method,
                aligned_runway, aligned_seconds, aligned_min_offset_m)
```

2. Extend the VALUES placeholder line. Find the last placeholder group `?, ?, ?, ?, ?)` and replace with `?, ?, ?, ?, ?, ?, ?, ?)`.

3. Extend the value tuple. Replace `flight.landing_anchor_method,` with:

```python
                flight.landing_anchor_method,
                flight.aligned_runway,
                flight.aligned_seconds,
                flight.aligned_min_offset_m,
```

- [ ] **Step 8: Add `get_runways_for_airport`**

In `adsbtrack/db.py`, immediately AFTER `runway_count` (around line 997), add:

```python
    def get_runways_for_airport(self, airport_ident: str) -> list[sqlite3.Row]:
        """Return all runway-end rows for the given airport ordered by name."""
        return self.conn.execute(
            "SELECT * FROM runways WHERE airport_ident = ? ORDER BY runway_name",
            (airport_ident,),
        ).fetchall()
```

- [ ] **Step 9: Run the DB tests to verify they pass**

Run: `uv run pytest tests/test_db.py -v`
Expected: PASS.

- [ ] **Step 10: Run the full test suite (sanity)**

Run: `uv run pytest -x --deselect tests/test_cli.py::test_enrich_hex_no_data_reports_yellow`
Expected: PASS (the deselected test hits a live API that returns real data on `N512WB` and is unrelated pre-existing flake).

- [ ] **Step 11: Commit**

```bash
git add adsbtrack/config.py adsbtrack/models.py adsbtrack/db.py tests/test_db.py
git commit -m "feat(ils): add config, model, and schema for alignment columns"
```

---

## Task 2: Add `track` to `_PointSample`

Bearing-vs-track tests need each sample's ground track. The existing `_PointSample` carries position but not track. Add a trailing optional field (mirrors how `lat`/`lon` were added in the landing-anchor milestone) and populate it from `record_point`.

**Files:**
- Modify: `adsbtrack/classifier.py` (`_PointSample` dataclass near line 64, `record_point` near line 281)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_classifier.py` (or create if absent):

```python
from adsbtrack.classifier import FlightMetrics, PointData


def test_record_point_captures_track_on_recent_points() -> None:
    metrics = FlightMetrics()
    p = PointData(
        ts=100.0, lat=33.64, lon=-84.43, baro_alt=3000, gs=150.0,
        track=87.0, geom_alt=3050, baro_rate=-800.0, geom_rate=None,
        squawk=None, category=None, nav_altitude_mcp=None, nav_qnh=None,
        emergency_field=None, true_heading=None, callsign=None,
    )
    metrics.record_point(p, ground_state="airborne", ground_reason="ok")
    assert len(metrics.recent_points) == 1
    sample = metrics.recent_points[-1]
    assert sample.track == 87.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_classifier.py::test_record_point_captures_track_on_recent_points -v`
Expected: FAIL (`AttributeError: '_PointSample' object has no attribute 'track'`).

- [ ] **Step 3: Add `track` to `_PointSample`**

In `adsbtrack/classifier.py`, update `_PointSample` by appending after `lon: float | None = None`:

```python
    # Ground track in degrees true. Used by adsbtrack.ils_alignment. None
    # when the point's trace didn't carry track (older readsb builds, or
    # ground samples). Placed last so every existing construction call site
    # continues to compile unchanged.
    track: float | None = None
```

- [ ] **Step 4: Populate `track` in `record_point`**

Inside `record_point`, find the `_PointSample(...)` construction (around line 282). Replace it with:

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
                track=float(point.track) if point.track is not None else None,
            )
        )
```

- [ ] **Step 5: Run the classifier test**

Run: `uv run pytest tests/test_classifier.py::test_record_point_captures_track_on_recent_points -v`
Expected: PASS.

- [ ] **Step 6: Run the full suite**

Run: `uv run pytest -x --deselect tests/test_cli.py::test_enrich_hex_no_data_reports_yellow`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add adsbtrack/classifier.py tests/test_classifier.py
git commit -m "feat(ils): extend _PointSample with ground track"
```

---

## Task 3: Pure `ils_alignment` detection module

Build the alignment detector as a self-contained module. No DB, no I/O. Public surface: one dataclass and one function.

**Files:**
- Create: `adsbtrack/ils_alignment.py`
- Create: `tests/test_ils_alignment.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_ils_alignment.py`:

```python
"""Tests for adsbtrack.ils_alignment."""

from __future__ import annotations

from collections import deque

from adsbtrack.classifier import _PointSample
from adsbtrack.ils_alignment import detect_ils_alignment


def _sample(ts: float, lat: float, lon: float, alt: int, track: float) -> _PointSample:
    return _PointSample(
        ts=ts, baro_alt=alt, geom_alt=None, gs=150.0, baro_rate=-800.0,
        lat=lat, lon=lon, track=track,
    )


class _Metrics:
    """Stand-in that exposes the attribute the detector reads."""

    def __init__(self, samples: list[_PointSample]):
        self.recent_points = deque(samples)


def _walk_toward(runway_lat: float, runway_lon: float, runway_heading: float, start_ts: float, n: int, spacing_secs: float):
    """Generate `n` samples approaching a runway threshold along its extended centerline."""
    import math

    approach_bearing = (runway_heading + 180.0) % 360.0
    samples: list[_PointSample] = []
    for i in range(n):
        km_out = (n - i) * 0.3  # 300m per step toward threshold
        br_rad = math.radians(approach_bearing)
        dlat = (km_out / 111.0) * math.cos(br_rad)
        dlon = (km_out / (111.0 * math.cos(math.radians(runway_lat)))) * math.sin(br_rad)
        lat = runway_lat + dlat
        lon = runway_lon + dlon
        alt = 1500 - i * 30  # gentle descent
        samples.append(_sample(start_ts + i * spacing_secs, lat, lon, alt, runway_heading))
    return samples


def test_no_runways_returns_none() -> None:
    metrics = _Metrics([_sample(0, 33.6, -84.4, 1000, 90)])
    assert detect_ils_alignment(metrics, airport_elev_ft=1000, runway_ends=[]) is None


def test_clean_ils_captures_segment() -> None:
    # Runway 09 (heading 090) at (33.64, -84.43), elevation 1026 ft.
    runway = {"runway_name": "09", "latitude_deg": 33.64, "longitude_deg": -84.43, "heading_deg_true": 90.0}
    # Walk 30 samples at 3s intervals = 87s duration toward the threshold on centerline
    samples = _walk_toward(33.64, -84.43, 90.0, start_ts=0.0, n=30, spacing_secs=3.0)
    metrics = _Metrics(samples)
    result = detect_ils_alignment(metrics, airport_elev_ft=1026, runway_ends=[runway])
    assert result is not None
    assert result.runway_name == "09"
    assert result.duration_secs >= 60.0
    assert result.min_offset_m < 100.0


def test_overflight_not_on_centerline_returns_none() -> None:
    # 90s of level overflight at 3000 ft AGL, passing abeam the runway
    # (translate the walk 5 km north so perpendicular offset blows past 100m).
    runway = {"runway_name": "09", "latitude_deg": 33.64, "longitude_deg": -84.43, "heading_deg_true": 90.0}
    samples = _walk_toward(33.64 + 0.045, -84.43, 90.0, start_ts=0.0, n=30, spacing_secs=3.0)
    metrics = _Metrics(samples)
    result = detect_ils_alignment(metrics, airport_elev_ft=1026, runway_ends=[runway])
    assert result is None


def test_too_high_returns_none() -> None:
    # Clean centerline but at 10,000 ft AGL; exceeds max_ft_above_airport (5,000)
    runway = {"runway_name": "09", "latitude_deg": 33.64, "longitude_deg": -84.43, "heading_deg_true": 90.0}
    samples = _walk_toward(33.64, -84.43, 90.0, start_ts=0.0, n=30, spacing_secs=3.0)
    # Override altitudes to 10,000 ft
    high = [
        _PointSample(
            ts=s.ts, baro_alt=10_000, geom_alt=None, gs=s.gs, baro_rate=s.baro_rate,
            lat=s.lat, lon=s.lon, track=s.track,
        )
        for s in samples
    ]
    metrics = _Metrics(high)
    result = detect_ils_alignment(metrics, airport_elev_ft=1026, runway_ends=[runway])
    assert result is None


def test_split_on_gap_picks_longest_segment() -> None:
    runway = {"runway_name": "09", "latitude_deg": 33.64, "longitude_deg": -84.43, "heading_deg_true": 90.0}
    # 15 samples (45s), then a 40s gap, then 25 samples (75s).
    s1 = _walk_toward(33.64, -84.43, 90.0, start_ts=0.0, n=15, spacing_secs=3.0)
    s2 = _walk_toward(33.64, -84.43, 90.0, start_ts=120.0, n=25, spacing_secs=3.0)
    metrics = _Metrics(s1 + s2)
    result = detect_ils_alignment(metrics, airport_elev_ft=1026, runway_ends=[runway])
    assert result is not None
    assert result.duration_secs >= 70.0  # the longer segment wins


def test_short_segment_below_min_duration_returns_none() -> None:
    runway = {"runway_name": "09", "latitude_deg": 33.64, "longitude_deg": -84.43, "heading_deg_true": 90.0}
    samples = _walk_toward(33.64, -84.43, 90.0, start_ts=0.0, n=8, spacing_secs=3.0)  # 21s
    metrics = _Metrics(samples)
    result = detect_ils_alignment(metrics, airport_elev_ft=1026, runway_ends=[runway])
    assert result is None


def test_missing_heading_skips_runway() -> None:
    runway = {"runway_name": "09", "latitude_deg": 33.64, "longitude_deg": -84.43, "heading_deg_true": None}
    samples = _walk_toward(33.64, -84.43, 90.0, start_ts=0.0, n=30, spacing_secs=3.0)
    metrics = _Metrics(samples)
    assert detect_ils_alignment(metrics, airport_elev_ft=1026, runway_ends=[runway]) is None


def test_samples_moving_away_from_threshold_are_excluded() -> None:
    # Samples walk AWAY from the threshold (track = runway heading opposite)
    runway = {"runway_name": "09", "latitude_deg": 33.64, "longitude_deg": -84.43, "heading_deg_true": 90.0}
    import math
    samples = []
    for i in range(30):
        km_out = 0.5 + i * 0.3
        br_rad = math.radians(90.0)  # east, moving past and away from threshold
        dlat = 0.0
        dlon = km_out / (111.0 * math.cos(math.radians(33.64)))
        samples.append(_sample(i * 3.0, 33.64, -84.43 + dlon, 1500, 90.0))
    metrics = _Metrics(samples)
    # Aircraft is east of threshold, heading east, so bearing from point to
    # threshold is west (270) but track is 90: cos(bearing - track) = cos(180) = -1
    assert detect_ils_alignment(metrics, airport_elev_ft=1026, runway_ends=[runway]) is None
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_ils_alignment.py -v`
Expected: FAIL (ModuleNotFoundError: no `adsbtrack.ils_alignment`).

- [ ] **Step 3: Create the module**

Create `adsbtrack/ils_alignment.py`:

```python
"""Geometric ILS-alignment detector.

For each runway end at a candidate landing airport, compute per-trace-point
the bearing to the runway threshold and the perpendicular offset from the
extended centerline, then collect contiguous segments of points that are

  * within ``max_offset_m`` of the centerline
  * moving toward the threshold (cos(bearing - track) > 0)
  * below ``airport_elev_ft + max_ft_above_airport``

Segments are split on trace gaps longer than ``split_gap_secs``. Any segment
at least ``min_duration_secs`` long is a candidate. The longest candidate
across all runway ends wins and is returned.

Attribution: the geometric approach (perpendicular offset =
distance * |bearing - runway_heading| in radians, split-on-gap, min-duration
filter, AGL cap) matches xoolive/traffic's ``LandingAlignedOnILS``
(MIT-licensed). This module reimplements the algorithm in our style using
the trace-point buffer and per-flight FlightMetrics already present in this
codebase; no code is copied from traffic.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

from .classifier import FlightMetrics, _PointSample, _haversine_m


@dataclass(frozen=True)
class IlsAlignmentResult:
    """Winning alignment segment for a flight."""

    runway_name: str
    duration_secs: float
    min_offset_m: float


def _bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initial bearing from (lat1, lon1) to (lat2, lon2) in degrees [0, 360)."""
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    x = math.sin(dlon) * math.cos(rlat2)
    y = math.cos(rlat1) * math.sin(rlat2) - math.sin(rlat1) * math.cos(rlat2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360.0) % 360.0


def _smallest_angle(a_deg: float, b_deg: float) -> float:
    """Smallest unsigned angle (degrees) between two bearings, in [0, 180]."""
    d = (a_deg - b_deg) % 360.0
    return d if d <= 180.0 else 360.0 - d


def _sample_alt(s: _PointSample) -> int | None:
    if s.baro_alt is not None:
        return s.baro_alt
    return s.geom_alt


def _alignment_for_runway(
    samples: Sequence[_PointSample],
    runway: Mapping[str, object],
    *,
    airport_elev_ft: float,
    max_offset_m: float,
    max_ft_above_airport: float,
    split_gap_secs: float,
    min_duration_secs: float,
) -> IlsAlignmentResult | None:
    heading = runway.get("heading_deg_true")
    r_lat = runway.get("latitude_deg")
    r_lon = runway.get("longitude_deg")
    if heading is None or r_lat is None or r_lon is None:
        return None

    r_heading_f = float(heading)
    r_lat_f = float(r_lat)
    r_lon_f = float(r_lon)
    alt_cap = airport_elev_ft + max_ft_above_airport

    # Collect (ts, offset_m) for every qualifying sample.
    kept: list[tuple[float, float]] = []
    for s in samples:
        if s.lat is None or s.lon is None or s.track is None:
            continue
        alt = _sample_alt(s)
        if alt is None or alt > alt_cap:
            continue

        distance_m = _haversine_m(s.lat, s.lon, r_lat_f, r_lon_f)
        bearing_to_threshold = _bearing_deg(s.lat, s.lon, r_lat_f, r_lon_f)
        # Moving-toward-threshold gate: cos(bearing - track) > 0.
        delta_track = (bearing_to_threshold - float(s.track) + 540.0) % 360.0 - 180.0
        if math.cos(math.radians(delta_track)) <= 0.0:
            continue

        # Perpendicular offset from extended centerline, small-angle
        # approximation: distance * |smallest_angle(bearing, runway_heading)|
        # in radians. The threshold (100m) is tiny compared to typical
        # distances (km), so the approximation holds well under the
        # alignment cone and blows past the threshold well outside it.
        angle_to_centerline = _smallest_angle(bearing_to_threshold, r_heading_f)
        offset_m = distance_m * math.radians(angle_to_centerline)
        if offset_m >= max_offset_m:
            continue

        kept.append((s.ts, offset_m))

    if not kept:
        return None

    # Split on gaps larger than split_gap_secs.
    segments: list[list[tuple[float, float]]] = [[kept[0]]]
    for prev, cur in zip(kept, kept[1:]):
        if cur[0] - prev[0] > split_gap_secs:
            segments.append([cur])
        else:
            segments[-1].append(cur)

    # Pick the longest segment that meets the duration floor.
    best: IlsAlignmentResult | None = None
    best_dur = 0.0
    for seg in segments:
        dur = seg[-1][0] - seg[0][0]
        if dur < min_duration_secs:
            continue
        if dur > best_dur:
            best = IlsAlignmentResult(
                runway_name=str(runway.get("runway_name", "")),
                duration_secs=round(dur, 1),
                min_offset_m=round(min(o for _, o in seg), 1),
            )
            best_dur = dur
    return best


def detect_ils_alignment(
    metrics: FlightMetrics,
    *,
    airport_elev_ft: float,
    runway_ends: Iterable[Mapping[str, object]],
    max_offset_m: float = 100.0,
    max_ft_above_airport: float = 5000.0,
    split_gap_secs: float = 20.0,
    min_duration_secs: float = 30.0,
) -> IlsAlignmentResult | None:
    """Run the alignment check across every provided runway end.

    Returns the longest qualifying segment, or None if no runway end
    produces a segment meeting `min_duration_secs`.
    """
    samples = list(metrics.recent_points)
    if not samples:
        return None

    best: IlsAlignmentResult | None = None
    for runway in runway_ends:
        cand = _alignment_for_runway(
            samples,
            runway,
            airport_elev_ft=airport_elev_ft,
            max_offset_m=max_offset_m,
            max_ft_above_airport=max_ft_above_airport,
            split_gap_secs=split_gap_secs,
            min_duration_secs=min_duration_secs,
        )
        if cand is None:
            continue
        if best is None or cand.duration_secs > best.duration_secs:
            best = cand
    return best
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_ils_alignment.py -v`
Expected: PASS on all 8 tests.

- [ ] **Step 5: Commit**

```bash
git add adsbtrack/ils_alignment.py tests/test_ils_alignment.py
git commit -m "feat(ils): add geometric alignment detector"
```

---

## Task 4: Parser integration (detection, column population, confidence bonus, classification upgrade)

This task wires the detector into the hot extraction path and applies the confidence + classification rules.

**Files:**
- Modify: `adsbtrack/parser.py` (around lines 800-981, the main per-flight loop)
- Modify: `tests/test_parser.py` (integration tests)

**Rules to implement (documented in docs/features.md in Task 6):**

1. Alignment is computed only when the flight has a candidate landing airport (either `destination_icao` from the on-field match, or `nearest_destination_icao`, or `probable_destination_icao`). We resolve which identifier to use in this priority order and query `get_runways_for_airport`.
2. If the airport has no runways, all three alignment columns stay NULL, no confidence bonus, no classification change.
3. If a segment meets `ils_alignment_min_duration_secs`, set `aligned_runway`, `aligned_seconds`, `aligned_min_offset_m`.
4. Confidence bonus (additive, applied AFTER `score_confidence`):
   - `aligned_seconds >= ils_alignment_bonus_long_secs` → `landing_confidence = min(1.0, landing_confidence + ils_alignment_bonus_long)`
   - else `aligned_seconds >= ils_alignment_bonus_short_secs` → `+ ils_alignment_bonus_short`
   - else no bonus (not possible by construction since `min_duration_secs <= bonus_short_secs`, but guards against future tuning).
5. Classification upgrade: if `flight.landing_type == "signal_lost"` and `aligned_seconds >= ils_alignment_bonus_long_secs` and `metrics.last_airborne_alt is not None and metrics.last_airborne_alt < airport_elev_ft + ils_alignment_max_ft_above_airport`, upgrade to `"dropped_on_approach"`. This is the only classification change. `confirmed`, `dropped_on_approach`, `uncertain`, and `altitude_error` are never downgraded or reshuffled by alignment.

- [ ] **Step 1: Write the failing integration tests**

Append to `tests/test_parser.py`:

```python
def test_parser_aligned_confirmed_landing_bumps_confidence(tmp_path: Path) -> None:
    """A confirmed landing with a 90s ILS alignment gets the +0.25 bonus
    on landing_confidence and stores aligned_runway/seconds/offset."""
    # Build a trivial trace: takeoff, cruise, approach along runway 09
    # centerline, touchdown. Seed runway data for the landing airport.
    from tests.fixtures.alignment_helpers import build_aligned_confirmed_trace

    db_path = tmp_path / "a.db"
    trace_rows, runway_rows, airport_rows = build_aligned_confirmed_trace()
    with Database(db_path) as db:
        for r in airport_rows:
            db.conn.execute(
                "INSERT OR REPLACE INTO airports (ident, type, name, latitude_deg, longitude_deg, elevation_ft) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                r,
            )
        db.insert_runway_ends(runway_rows)
        for tr in trace_rows:
            db.insert_trace_day(**tr)
        flights = extract_flights(db, "abc123", config=Config())

    assert len(flights) == 1
    f = flights[0]
    assert f.landing_type == "confirmed"
    assert f.aligned_runway == "09"
    assert f.aligned_seconds is not None and f.aligned_seconds >= 60.0
    assert f.aligned_min_offset_m is not None and f.aligned_min_offset_m < 100.0
    # Confidence must be at least the bonus floor after clamping
    assert f.landing_confidence is not None and f.landing_confidence >= 0.25


def test_parser_overflight_no_alignment_no_bonus(tmp_path: Path) -> None:
    from tests.fixtures.alignment_helpers import build_overflight_trace

    db_path = tmp_path / "a.db"
    trace_rows, runway_rows, airport_rows = build_overflight_trace()
    with Database(db_path) as db:
        for r in airport_rows:
            db.conn.execute(
                "INSERT OR REPLACE INTO airports (ident, type, name, latitude_deg, longitude_deg, elevation_ft) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                r,
            )
        db.insert_runway_ends(runway_rows)
        for tr in trace_rows:
            db.insert_trace_day(**tr)
        flights = extract_flights(db, "abc124", config=Config())

    assert len(flights) >= 1
    f = flights[0]
    assert f.aligned_runway is None
    assert f.aligned_seconds is None
    assert f.aligned_min_offset_m is None


def test_parser_drop_with_alignment_upgrades_to_drop(tmp_path: Path) -> None:
    """A signal_lost flight with aligned_seconds >= 60 at low altitude
    upgrades to dropped_on_approach."""
    from tests.fixtures.alignment_helpers import build_signal_lost_aligned_trace

    db_path = tmp_path / "a.db"
    trace_rows, runway_rows, airport_rows = build_signal_lost_aligned_trace()
    with Database(db_path) as db:
        for r in airport_rows:
            db.conn.execute(
                "INSERT OR REPLACE INTO airports (ident, type, name, latitude_deg, longitude_deg, elevation_ft) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                r,
            )
        db.insert_runway_ends(runway_rows)
        for tr in trace_rows:
            db.insert_trace_day(**tr)
        flights = extract_flights(db, "abc125", config=Config())

    assert len(flights) == 1
    f = flights[0]
    assert f.aligned_seconds is not None and f.aligned_seconds >= 60.0
    assert f.landing_type == "dropped_on_approach"


def test_parser_candidate_airport_without_runways_leaves_alignment_null(tmp_path: Path) -> None:
    """Matched airport has no rows in the runways table; alignment columns
    remain NULL and the flight is not rejected."""
    from tests.fixtures.alignment_helpers import build_aligned_confirmed_trace

    db_path = tmp_path / "a.db"
    trace_rows, _runway_rows, airport_rows = build_aligned_confirmed_trace()
    with Database(db_path) as db:
        for r in airport_rows:
            db.conn.execute(
                "INSERT OR REPLACE INTO airports (ident, type, name, latitude_deg, longitude_deg, elevation_ft) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                r,
            )
        # NOTE: runways intentionally not inserted
        for tr in trace_rows:
            db.insert_trace_day(**tr)
        flights = extract_flights(db, "abc126", config=Config())

    assert len(flights) == 1
    f = flights[0]
    assert f.landing_type == "confirmed"
    assert f.aligned_runway is None
    assert f.aligned_seconds is None
    assert f.aligned_min_offset_m is None
```

- [ ] **Step 2: Create test helpers module**

Create `tests/fixtures/alignment_helpers.py` with a small set of deterministic trace builders. The exact trace shape should follow existing patterns in `tests/test_parser.py` -- examine how other integration tests in that file build `insert_trace_day` payloads, and mirror that structure:

```python
"""Fixture builders for ILS-alignment integration tests.

Each builder returns (trace_rows, runway_rows, airport_rows) ready to
insert into a fresh test database. Builders use a fictional airport
"KFAKE" at (33.64, -84.43), elevation 1026 ft, with runway 09/27 aligned
east-west through the reference point.
"""

from __future__ import annotations

import json
import math
from typing import Any

FAKE_LAT = 33.64
FAKE_LON = -84.43
FAKE_ELEV = 1026
RUNWAY_09_HEADING = 90.0

_AIRPORTS = [
    ("KFAKE", "medium_airport", "Fake Intl", FAKE_LAT, FAKE_LON, FAKE_ELEV),
]

_RUNWAYS = [
    # (airport_ident, runway_name, lat, lon, elevation_ft, heading_deg_true,
    #  length_ft, width_ft, surface, closed, displaced_threshold_ft)
    ("KFAKE", "09", FAKE_LAT, FAKE_LON, FAKE_ELEV, RUNWAY_09_HEADING, 9000, 150, "ASP", 0, 0),
    ("KFAKE", "27", FAKE_LAT, FAKE_LON + 0.08, FAKE_ELEV, 270.0, 9000, 150, "ASP", 0, 0),
]


def _centerline_point(km_out: float, runway_heading: float, ref_lat: float, ref_lon: float) -> tuple[float, float]:
    """Point `km_out` kilometres outbound along the runway's extended centerline."""
    approach_bearing = (runway_heading + 180.0) % 360.0
    br_rad = math.radians(approach_bearing)
    dlat = (km_out / 111.0) * math.cos(br_rad)
    dlon = (km_out / (111.0 * math.cos(math.radians(ref_lat)))) * math.sin(br_rad)
    return ref_lat + dlat, ref_lon + dlon


def _trace_point(ts: float, lat: float, lon: float, alt: int | str, gs: float, track: float, vr: float) -> list[Any]:
    """Build a 14-element readsb trace row matching parser._extract_point_fields."""
    return [
        ts, lat, lon, alt, gs, track, "none", vr, None, "adsb_icao",
        None, 2000, None,
        {"track": track, "flight": "N12345 ", "type": "adsb_icao", "gs": gs},
    ]


def _day_payload(base_ts: float, points: list[list[Any]]) -> dict[str, Any]:
    """Wrap a list of trace points into the trace_days JSON payload shape."""
    return {"icao": "abc123", "trace": [[p[0] - base_ts] + p[1:] for p in points]}


def build_aligned_confirmed_trace() -> tuple[list[dict], list[tuple], list[tuple]]:
    """90s of centerline approach to runway 09 at KFAKE, touchdown on the
    threshold. Origin is a separate airport 200km west."""
    base_ts = 1_700_000_000.0
    origin_lat, origin_lon = FAKE_LAT, FAKE_LON - 2.0  # ~200km west
    points: list[list[Any]] = []
    # Takeoff roll + climb from origin (simple)
    for i in range(5):
        points.append(_trace_point(base_ts + i * 2.0, origin_lat, origin_lon, "ground", 0.0, 90.0, 0.0))
    for i in range(30):
        points.append(_trace_point(base_ts + 10 + i * 3.0, origin_lat, origin_lon, 500 + i * 500, 200.0, 90.0, 1500.0))
    # Cruise eastbound
    for i in range(30):
        frac = (i + 1) / 35.0
        lat = origin_lat + (FAKE_LAT - origin_lat) * frac * 0.5
        lon = origin_lon + (FAKE_LON - origin_lon) * frac * 0.5
        points.append(_trace_point(base_ts + 100 + i * 60.0, lat, lon, 15_000, 400.0, 90.0, 0.0))
    # Approach: 30 samples at 3s intervals on runway 09 centerline
    approach_base = base_ts + 2200.0
    for i in range(30):
        km_out = (30 - i) * 0.3
        lat, lon = _centerline_point(km_out, RUNWAY_09_HEADING, FAKE_LAT, FAKE_LON)
        alt = 3000 - i * 100
        points.append(_trace_point(approach_base + i * 3.0, lat, lon, alt, 150.0, 90.0, -700.0))
    # Touchdown + rollout
    rollout_base = approach_base + 30 * 3.0
    for i in range(6):
        points.append(_trace_point(rollout_base + i * 2.0, FAKE_LAT, FAKE_LON + i * 0.0002, "ground", 80.0 - i * 15.0, 90.0, 0.0))

    day = {
        "icao": "abc123",
        "date": "2023-11-14",
        "trace_json": json.dumps({"icao": "abc123", "trace": [[p[0] - base_ts] + list(p[1:]) for p in points]}),
        "timestamp": base_ts,
        "source": "adsbx",
    }
    return [day], _RUNWAYS, _AIRPORTS


def build_overflight_trace() -> tuple[list[dict], list[tuple], list[tuple]]:
    """Level pass at 3000 ft AGL, offset 2 km north of the runway. Long enough
    to be a flight but not aligned with either end."""
    base_ts = 1_700_100_000.0
    origin_lat, origin_lon = FAKE_LAT + 0.05, FAKE_LON - 1.0
    points: list[list[Any]] = []
    for i in range(5):
        points.append(_trace_point(base_ts + i * 2.0, origin_lat, origin_lon, "ground", 0.0, 90.0, 0.0))
    for i in range(40):
        frac = i / 40.0
        lat = FAKE_LAT + 0.018  # ~2 km north
        lon = origin_lon + (FAKE_LON + 1.0 - origin_lon) * frac
        points.append(_trace_point(base_ts + 10 + i * 10.0, lat, lon, 4000, 200.0, 90.0, 0.0))
    # Landing back at origin
    rollout_base = base_ts + 10 + 40 * 10.0 + 20
    for i in range(6):
        points.append(_trace_point(rollout_base + i * 2.0, origin_lat, origin_lon, "ground", 50.0 - i * 10, 90.0, 0.0))

    day = {
        "icao": "abc124",
        "date": "2023-11-14",
        "trace_json": json.dumps({"icao": "abc124", "trace": [[p[0] - base_ts] + list(p[1:]) for p in points]}),
        "timestamp": base_ts,
        "source": "adsbx",
    }
    return [day], _RUNWAYS, _AIRPORTS


def build_signal_lost_aligned_trace() -> tuple[list[dict], list[tuple], list[tuple]]:
    """Clean centerline approach to runway 09 cut off 200 ft AGL - no ground
    transition. Initial classification is signal_lost; alignment should
    upgrade to dropped_on_approach."""
    base_ts = 1_700_200_000.0
    origin_lat, origin_lon = FAKE_LAT, FAKE_LON - 2.0
    points: list[list[Any]] = []
    for i in range(5):
        points.append(_trace_point(base_ts + i * 2.0, origin_lat, origin_lon, "ground", 0.0, 90.0, 0.0))
    for i in range(10):
        points.append(_trace_point(base_ts + 10 + i * 3.0, origin_lat, origin_lon, 500 + i * 500, 200.0, 90.0, 1500.0))
    for i in range(20):
        frac = (i + 1) / 25.0
        lat = origin_lat + (FAKE_LAT - origin_lat) * frac
        lon = origin_lon + (FAKE_LON - origin_lon) * frac
        points.append(_trace_point(base_ts + 100 + i * 60.0, lat, lon, 15_000, 400.0, 90.0, 0.0))
    approach_base = base_ts + 1400.0
    # 30 samples: descending from 3000 ft down to 200 ft AGL (1226 MSL), then cut off.
    for i in range(30):
        km_out = (30 - i) * 0.3
        lat, lon = _centerline_point(km_out, RUNWAY_09_HEADING, FAKE_LAT, FAKE_LON)
        alt = 3000 - i * 90  # ends at ~300 ft
        points.append(_trace_point(approach_base + i * 3.0, lat, lon, alt, 150.0, 90.0, -700.0))
    # NO ground points - trace ends here

    day = {
        "icao": "abc125",
        "date": "2023-11-14",
        "trace_json": json.dumps({"icao": "abc125", "trace": [[p[0] - base_ts] + list(p[1:]) for p in points]}),
        "timestamp": base_ts,
        "source": "adsbx",
    }
    return [day], _RUNWAYS, _AIRPORTS
```

Important: the exact `insert_trace_day` keyword signature and `trace_json` shape must match the current codebase. Before dispatching the implementer, verify by reading `adsbtrack/db.py::insert_trace_day` and `adsbtrack/parser.py::_extract_point_fields`. Adjust the helper accordingly.

- [ ] **Step 3: Run the parser integration tests to verify they fail**

Run: `uv run pytest tests/test_parser.py -v -k "aligned or overflight or drop_with_alignment or candidate_airport_without_runways"`
Expected: FAIL (module import error on `alignment_helpers` OR assertion failure because alignment columns not populated).

- [ ] **Step 4: Wire parser integration**

Open `adsbtrack/parser.py`. Add import near the top with the other `from .landing_anchor` import:

```python
from .ils_alignment import IlsAlignmentResult, detect_ils_alignment
```

In the main per-flight loop (around lines 870-890, immediately AFTER the `score_confidence` block that assigns `flight.takeoff_confidence` / `flight.landing_confidence`), insert the alignment block. Find this existing code:

```python
        takeoff_conf, landing_conf = score_confidence(
            metrics,
            has_landing,
            flight.landing_type,
            origin_distance_km=flight.origin_distance_km,
            dest_distance_km=flight.destination_distance_km,
            duration_minutes=flight.duration_minutes,
        )
        flight.takeoff_confidence = takeoff_conf
        flight.landing_confidence = landing_conf
```

Immediately AFTER it, add:

```python
        # --- ILS alignment (reads runways from DB, updates flight columns,
        # bumps landing_confidence, may upgrade signal_lost -> dropped_on_approach) ---
        alignment_icao = (
            flight.destination_icao
            or flight.nearest_destination_icao
            or flight.probable_destination_icao
        )
        alignment: IlsAlignmentResult | None = None
        airport_elev_ft = 0.0
        if alignment_icao:
            airport_row = db.conn.execute(
                "SELECT elevation_ft FROM airports WHERE ident = ?",
                (alignment_icao,),
            ).fetchone()
            if airport_row is not None and airport_row["elevation_ft"] is not None:
                airport_elev_ft = float(airport_row["elevation_ft"])
            runway_rows = db.get_runways_for_airport(alignment_icao)
            if runway_rows:
                alignment = detect_ils_alignment(
                    metrics,
                    airport_elev_ft=airport_elev_ft,
                    runway_ends=[dict(r) for r in runway_rows],
                    max_offset_m=config.ils_alignment_max_offset_m,
                    max_ft_above_airport=config.ils_alignment_max_ft_above_airport,
                    split_gap_secs=config.ils_alignment_split_gap_secs,
                    min_duration_secs=config.ils_alignment_min_duration_secs,
                )

        if alignment is not None:
            flight.aligned_runway = alignment.runway_name
            flight.aligned_seconds = alignment.duration_secs
            flight.aligned_min_offset_m = alignment.min_offset_m

            # Confidence bonus (additive, clamped to 1.0). Applied only when
            # score_confidence produced a non-None value -- don't revive a
            # NULL signal_lost score.
            if flight.landing_confidence is not None:
                if alignment.duration_secs >= config.ils_alignment_bonus_long_secs:
                    bonus = config.ils_alignment_bonus_long
                elif alignment.duration_secs >= config.ils_alignment_bonus_short_secs:
                    bonus = config.ils_alignment_bonus_short
                else:
                    bonus = 0.0
                if bonus > 0.0:
                    flight.landing_confidence = round(min(1.0, flight.landing_confidence + bonus), 2)

            # Classification upgrade: a signal_lost flight that was on a
            # 60s+ geometric ILS segment at low altitude is indistinguishable
            # from a dropped_on_approach. Promote the type so downstream
            # consumers see the stronger classification. last_airborne_alt is
            # the AGL proxy: we use "last alt < airport_elev + max_ft_above_airport"
            # as the altitude gate so the upgrade rule matches the detector's
            # altitude cap.
            if (
                flight.landing_type == "signal_lost"
                and alignment.duration_secs >= config.ils_alignment_bonus_long_secs
                and metrics.last_airborne_alt is not None
                and metrics.last_airborne_alt < airport_elev_ft + config.ils_alignment_max_ft_above_airport
            ):
                flight.landing_type = "dropped_on_approach"
```

- [ ] **Step 5: Run the parser integration tests to verify they pass**

Run: `uv run pytest tests/test_parser.py -v -k "aligned or overflight or drop_with_alignment or candidate_airport_without_runways"`
Expected: PASS all four.

- [ ] **Step 6: Run the full suite**

Run: `uv run pytest -x --deselect tests/test_cli.py::test_enrich_hex_no_data_reports_yellow`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add adsbtrack/parser.py tests/test_parser.py tests/fixtures/alignment_helpers.py
git commit -m "feat(ils): wire alignment into extraction, confidence, classification"
```

---

## Task 5: CLI `trips --alignment` flag

Render a new column "Aligned" (format `"RWY 09 / 85s"`) when the user passes `--alignment`, or when any row in the result set has `aligned_runway` set (same treatment as the ACARS column - show only when relevant).

**Files:**
- Modify: `adsbtrack/cli.py` (the `trips` command)
- Modify: `tests/test_cli.py`

- [ ] **Step 1: Write the failing CLI test**

Append to `tests/test_cli.py`:

```python
def test_trips_renders_alignment_column_when_flag_set(tmp_path, runner) -> None:
    """`trips --alignment` must add the RWY column and render a row when
    alignment data exists."""
    from adsbtrack.models import Flight
    from datetime import datetime

    db_path = tmp_path / "a.db"
    with Database(db_path) as db:
        f = Flight(
            icao="abc123",
            takeoff_time=datetime(2023, 11, 14, 10, 0),
            takeoff_lat=33.0,
            takeoff_lon=-84.0,
            takeoff_date="2023-11-14",
            landing_time=datetime(2023, 11, 14, 11, 0),
            landing_lat=33.64,
            landing_lon=-84.43,
            landing_date="2023-11-14",
            destination_icao="KFAKE",
            destination_name="Fake Intl",
            destination_distance_km=0.5,
            duration_minutes=60.0,
            landing_type="confirmed",
            landing_confidence=0.85,
            aligned_runway="09",
            aligned_seconds=85.0,
            aligned_min_offset_m=42.3,
        )
        db.insert_flight(f)

    result = runner.invoke(
        cli,
        ["trips", "--hex", "abc123", "--db", str(db_path), "--alignment"],
    )
    assert result.exit_code == 0, result.output
    assert "Aligned" in result.output
    assert "RWY 09" in result.output and "85s" in result.output
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_cli.py -v -k "trips_renders_alignment"`
Expected: FAIL (no `--alignment` option, column not added).

- [ ] **Step 3: Add the flag + column**

In `adsbtrack/cli.py`, modify the `trips` command. Add a click option:

```python
@click.option("--alignment/--no-alignment", "show_alignment", default=False,
              help="Show ILS alignment runway and duration")
```

Update the function signature and body. Keep the `show_alignment` default False; show the column when the flag is set OR when any row has alignment data (mirroring ACARS). Inside `trips`, after the `has_acars` computation:

```python
        has_alignment_data = any(
            (row["aligned_runway"] is not None if "aligned_runway" in row.keys() else False)
            for row in flights
        )
        show_alignment_col = show_alignment or has_alignment_data
```

Add the column header block alongside the ACARS one:

```python
        if show_alignment_col:
            table.add_column("Aligned", justify="right", style="cyan")
```

Inside the row loop, just before `table.add_row(*row_cells)`, append the alignment cell:

```python
            if show_alignment_col:
                runway = _col(f, "aligned_runway")
                seconds = _col(f, "aligned_seconds")
                if runway and seconds is not None:
                    alignment_cell = f"[green]RWY {runway} / {int(round(seconds))}s[/]"
                else:
                    alignment_cell = "[dim]--[/]"
                row_cells.append(alignment_cell)
```

- [ ] **Step 4: Run the CLI test to verify it passes**

Run: `uv run pytest tests/test_cli.py -v -k "trips_renders_alignment"`
Expected: PASS.

- [ ] **Step 5: Run the full suite**

Run: `uv run pytest -x --deselect tests/test_cli.py::test_enrich_hex_no_data_reports_yellow`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add adsbtrack/cli.py tests/test_cli.py
git commit -m "feat(ils): trips --alignment flag and RWY column"
```

---

## Task 6: Docs + final verification

**Files:**
- Modify: `docs/features.md`
- Modify: `docs/schema.md`

- [ ] **Step 1: Update `docs/features.md`**

Add a new subsection immediately BEFORE the "Landing airport anchor." paragraph, titled "**ILS alignment.**":

```markdown
**ILS alignment.** `aligned_runway`, `aligned_seconds`, `aligned_min_offset_m`. A geometric signal that says "the aircraft was established on final for runway X for N seconds." For each runway end at the candidate landing airport (from `destination_icao`, else `nearest_destination_icao`, else `probable_destination_icao`), the detector keeps trace points where (a) perpendicular offset from the extended centerline is under 100 m (`Config.ils_alignment_max_offset_m`), (b) the bearing to the threshold has a positive track-component (aircraft moving toward it), and (c) the altitude is under `airport_elevation + 5,000 ft` (`Config.ils_alignment_max_ft_above_airport`). Kept points are split on gaps longer than 20 s; any segment at least 30 s long (`Config.ils_alignment_min_duration_secs`) becomes a candidate. The longest candidate across all runway ends wins. Reimplementation of the algorithm in `xoolive/traffic`'s `LandingAlignedOnILS` (MIT-licensed); attribution in `adsbtrack/ils_alignment.py`. NULL when the airport has no runway rows or no segment qualified.

The alignment result feeds two downstream signals:

1. **Landing confidence bump.** `landing_confidence` gets an additive bonus (clamped to 1.0): `+0.15` when `aligned_seconds >= 30` (`Config.ils_alignment_bonus_short_secs`), `+0.25` when `aligned_seconds >= 60` (`Config.ils_alignment_bonus_long_secs`). This is independent of the geometric-mean factors inside `score_confidence` so a missing or noisy factor cannot cancel the alignment evidence.

2. **Classification upgrade.** A `signal_lost` flight with `aligned_seconds >= 60` at an altitude below `airport_elevation + 5,000 ft` is promoted to `dropped_on_approach`. The alignment proves the aircraft was geometrically committed to a specific runway at low altitude even though we never observed touchdown, which is precisely what `dropped_on_approach` is meant to capture. Other types (`confirmed`, `dropped_on_approach`, `uncertain`, `altitude_error`) are never re-classified by alignment; they record the alignment columns as metadata only.
```

- [ ] **Step 2: Update `docs/schema.md`**

Find the `flights` table listing. Add rows for the three new columns in the same style as the existing entries near `landing_anchor_method`:

```markdown
| `aligned_runway` | TEXT | Runway end the aircraft was geometrically aligned with on short final, e.g. `"09"` / `"26L"`. NULL when no segment qualified. |
| `aligned_seconds` | REAL | Duration in seconds of the longest qualifying alignment segment. NULL when no segment qualified. |
| `aligned_min_offset_m` | REAL | Minimum perpendicular offset in meters from the extended centerline over the winning segment. NULL when no segment qualified. |
```

- [ ] **Step 3: Run the full suite once more**

Run: `uv run ruff check . && uv run ruff format --check . && uv run pytest --deselect tests/test_cli.py::test_enrich_hex_no_data_reports_yellow`
Expected: all green.

- [ ] **Step 4: Commit**

```bash
git add docs/features.md docs/schema.md
git commit -m "docs(ils): document alignment signal and schema columns"
```

---

## Self-Review Checklist (ran before handoff)

**1. Spec coverage:**
- ILS alignment detection using the traffic-style algorithm → Task 3.
- Spherical Haversine for distance/bearing (no `pitot.geodesy`) → `adsbtrack.ils_alignment` imports `_haversine_m`; `_bearing_deg` lives in the same module.
- Compute during extraction when the candidate airport has runway data → Task 4 parser integration.
- Store `aligned_runway`, `aligned_seconds`, `aligned_min_offset_m` → Task 1 (schema/model), Task 4 (write).
- Bonus weights `+0.15` @ 30s, `+0.25` @ 60s documented → Task 4 parser code + Task 6 docs.
- Classification rule documented and implemented → Task 4 (rule), Task 6 (docs).
- Four test fixtures: confirmed-aligned, overflight-not-aligned, DROP-aligned, no-runway-data → Task 4 integration tests.
- Trips CLI flag showing alignment info → Task 5.
- No takeoff detection, no go-around in this task → confirmed, not touched.

**2. Placeholder scan:** No TBD / "implement later" / "add validation" strings. Every code block is complete.

**3. Type consistency:**
- `IlsAlignmentResult` fields: `runway_name: str`, `duration_secs: float`, `min_offset_m: float` - matches usage in parser and tests.
- `detect_ils_alignment` signature accepts `runway_ends: Iterable[Mapping[str, object]]` - parser passes `[dict(r) for r in runway_rows]` which satisfies this.
- Flight dataclass fields `aligned_runway: str | None`, `aligned_seconds: float | None`, `aligned_min_offset_m: float | None` line up with DB columns (TEXT / REAL / REAL) and the test assertions.
- `_PointSample.track` is `float | None` - parser's `record_point` casts `point.track` via `float(...)` when not None, matching.

**4. Integration-test helper risks:**
- The trace_json construction in `tests/fixtures/alignment_helpers.py` assumes the current `insert_trace_day` signature and the shape of JSON the parser accepts. The implementer must validate this against the live `adsbtrack/db.py::insert_trace_day` and `adsbtrack/parser.py::_extract_point_fields` before committing Task 4. If the shape differs, adjust the helper - do NOT skip the integration tests.

---

## Execution handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-16-ils-alignment.md`. Two execution options:

1. **Subagent-Driven (recommended)** - Fresh subagent per task with two-stage review between tasks (spec compliance, then code quality).
2. **Inline Execution** - Execute tasks in this session using executing-plans, with batch checkpoints for review.

Which approach?
