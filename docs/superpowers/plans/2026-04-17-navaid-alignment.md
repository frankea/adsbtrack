# Navaid Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fingerprint each flight by the chain of VOR/NDB/fix navaids its bearing tracks directly toward for long enough to be meaningful, then expose that chain via a new `route` CLI.

**Architecture:** Mirrors the ils_alignment pattern: one pure-algorithm module (`navaid_alignment.py`) reused from the parser extract loop, plus one I/O module (`navaids.py`) that handles the `navaids` table schema, OurAirports CSV refresh, and a bounding-box query. Each flight gains a single JSON column `navaid_track`. Bounding-box pre-filtering (flight bbox + 50 nm buffer) plus a per-extract-run bbox-keyed cache keep the algorithm's cost bounded. Attribution for the algorithm: xoolive/traffic's `BeaconTrackBearingAlignment` (MIT-licensed). No code is copied; algorithm is reimplemented against our `FlightMetrics.recent_points` layer.

**Tech Stack:** Python 3.12+, sqlite3 (stdlib), httpx (already a dep), click, rich, pytest. OurAirports navaids.csv at https://davidmegginson.github.io/ourairports-data/navaids.csv.

---

## File Structure

**Create:**
- `adsbtrack/navaid_alignment.py` — pure algorithm: `NavaidAlignmentSegment` dataclass, `detect_navaid_alignments(metrics, *, navaids, ...)`. Mirrors `ils_alignment.py`. No DB or I/O dependencies.
- `adsbtrack/navaids.py` — I/O module: `download_navaids()`, `refresh_navaids()`, `query_navaids_in_bbox()`. Mirrors `airports.py`/`runways.py`.
- `tests/test_navaid_alignment.py` — pure-algorithm tests (fixture trajectories, segment splitting, filters).
- `tests/test_navaids.py` — refresh + bbox query tests (uses CSV fixture).

**Modify:**
- `adsbtrack/config.py` — new alignment thresholds + OurAirports navaids URL.
- `adsbtrack/db.py` — `navaids` table DDL + index; `flights.navaid_track TEXT` column + migration; `insert_flight` column-count grows 101 → 102.
- `adsbtrack/models.py` — `Flight.navaid_track: str | None = None`.
- `adsbtrack/parser.py` — extract-loop integration: bbox → cache → `detect_navaid_alignments` → JSON → `flight.navaid_track`.
- `adsbtrack/cli.py` — new `navaids` click group with `refresh` subcommand; new top-level `route` command.
- `tests/test_db.py` — schema/migration/round-trip test for `navaid_track` and `navaids` table.
- `tests/test_parser.py` — end-to-end integration test using MagicMock parser scaffold.
- `tests/test_cli.py` — `route` + `navaids refresh` CLI tests using CliRunner.
- `docs/features.md` — "Navaid alignment" subsection with algorithm + limitations.
- `docs/schema.md` — `navaid_track` row; new `navaids` table section.

**No changes:** `adsbtrack/features.py` (parser-level integration, not derive_all, following the `ils_alignment` precedent in `parser.py:1128-1140`).

---

## Task 1: Navaids table schema, download, and refresh CLI

**Files:**
- Modify: `adsbtrack/config.py` — add `navaids_csv_url` field to `Config`.
- Modify: `adsbtrack/db.py` — add `CREATE TABLE navaids`, indexes; migration no-op (new table).
- Create: `adsbtrack/navaids.py` — `download_navaids()`, `refresh_navaids()`.
- Modify: `adsbtrack/cli.py` — `navaids` click group + `refresh` subcommand.
- Create: `tests/test_navaids.py` — initial scaffolding + refresh tests.
- Modify: `tests/test_db.py` — schema test for navaids table.

### Schema

The `navaids` table stores one row per navaid:

```sql
CREATE TABLE IF NOT EXISTS navaids (
    ident TEXT NOT NULL,
    name TEXT,
    type TEXT,
    latitude_deg REAL NOT NULL,
    longitude_deg REAL NOT NULL,
    elevation_ft INTEGER,
    frequency_khz INTEGER,
    iso_country TEXT,
    PRIMARY KEY (ident, latitude_deg, longitude_deg)
);
CREATE INDEX IF NOT EXISTS idx_navaids_latlon ON navaids(latitude_deg, longitude_deg);
CREATE INDEX IF NOT EXISTS idx_navaids_ident ON navaids(ident);
```

Primary key is (ident, lat, lon) because OurAirports uses duplicate `ident` strings for different navaids in different regions (e.g. short "COL" idents recycled globally).

### CSV columns we consume

OurAirports navaids.csv columns: `id, filename, ident, name, type, frequency_khz, latitude_deg, longitude_deg, elevation_ft, iso_country, dme_frequency_khz, dme_channel, dme_latitude_deg, dme_longitude_deg, dme_elevation_ft, slaved_variation_deg, magnetic_variation_deg, usageType, power, associated_airport`. We keep: `ident`, `name`, `type`, `latitude_deg`, `longitude_deg`, `elevation_ft`, `frequency_khz`, `iso_country`.

- [ ] **Step 1.1: Add config field**

In `adsbtrack/config.py`, inside the `Config` dataclass near `runways_csv_url`:

```python
navaids_csv_url: str = "https://davidmegginson.github.io/ourairports-data/navaids.csv"
```

- [ ] **Step 1.2: Add table DDL to db.py**

In `adsbtrack/db.py` insert the `CREATE TABLE navaids` block from "Schema" above after the existing `CREATE TABLE runways` block (around line 215). Add both indexes in the `CREATE INDEX` block (around line 343).

- [ ] **Step 1.3: Write failing test for navaids table existence**

Add to `tests/test_db.py`:

```python
def test_navaids_table_schema(tmp_path):
    from adsbtrack.db import Database
    db_path = tmp_path / "t.db"
    with Database(db_path) as db:
        cols = {r[1] for r in db.conn.execute("PRAGMA table_info(navaids)").fetchall()}
    assert {"ident", "name", "type", "latitude_deg", "longitude_deg",
            "elevation_ft", "frequency_khz", "iso_country"} <= cols
```

Run: `uv run pytest tests/test_db.py::test_navaids_table_schema -v`
Expected: PASS once DDL from Step 1.2 is in place.

- [ ] **Step 1.4: Implement download_navaids in navaids.py**

Create `adsbtrack/navaids.py`:

```python
"""OurAirports navaids.csv ingestion + bounding-box query helper.

Attribution: the per-flight alignment algorithm that consumes this table
(see adsbtrack/navaid_alignment.py) is inspired by xoolive/traffic's
BeaconTrackBearingAlignment (MIT-licensed). No code is copied from
traffic; this module only handles I/O and table storage.
"""

from __future__ import annotations

import contextlib
import csv
import io
from pathlib import Path

import httpx
from rich.progress import Progress

from .config import Config
from .db import Database


def _parse_row(row: dict) -> tuple | None:
    try:
        lat = float(row["latitude_deg"])
        lon = float(row["longitude_deg"])
    except (ValueError, KeyError):
        return None
    ident = (row.get("ident") or "").strip()
    if not ident:
        return None
    elev: int | None = None
    freq: int | None = None
    if row.get("elevation_ft"):
        with contextlib.suppress(ValueError):
            elev = int(float(row["elevation_ft"]))
    if row.get("frequency_khz"):
        with contextlib.suppress(ValueError):
            freq = int(float(row["frequency_khz"]))
    return (
        ident,
        (row.get("name") or "").strip() or None,
        (row.get("type") or "").strip() or None,
        lat,
        lon,
        elev,
        freq,
        (row.get("iso_country") or "").strip() or None,
    )


def _read_csv(text: str) -> list[tuple]:
    reader = csv.DictReader(io.StringIO(text))
    rows: list[tuple] = []
    for raw in reader:
        parsed = _parse_row(raw)
        if parsed is not None:
            rows.append(parsed)
    return rows


def refresh_navaids(
    db: Database,
    config: Config,
    *,
    local_csv: Path | None = None,
) -> int:
    """Download (or load local) OurAirports navaids.csv and upsert into navaids.

    Returns the number of rows written. Idempotent: re-running replaces
    rows with identical primary key (ident, latitude_deg, longitude_deg).
    """
    if local_csv is not None:
        text = Path(local_csv).read_text()
    else:
        with Progress() as progress:
            task = progress.add_task("Downloading navaids...", total=None)
            resp = httpx.get(config.navaids_csv_url, follow_redirects=True, timeout=60)
            resp.raise_for_status()
            progress.update(task, completed=100)
            text = resp.text

    rows = _read_csv(text)
    db.conn.executemany(
        "INSERT OR REPLACE INTO navaids"
        " (ident, name, type, latitude_deg, longitude_deg,"
        "  elevation_ft, frequency_khz, iso_country)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    db.conn.commit()
    return len(rows)
```

- [ ] **Step 1.5: Write failing tests for refresh_navaids**

Add fixture file `tests/fixtures/navaids_sample.csv` with 3 rows (one VOR, one NDB, one with missing elevation). Example content (wrapped for readability in the plan — commit as single CSV):

```
id,filename,ident,name,type,frequency_khz,latitude_deg,longitude_deg,elevation_ft,iso_country,dme_frequency_khz,dme_channel,dme_latitude_deg,dme_longitude_deg,dme_elevation_ft,slaved_variation_deg,magnetic_variation_deg,usageType,power,associated_airport
1,test1,CLT,CHARLOTTE,VOR-DME,115800,35.1888,-80.9508,750,US,,,,,,,,,,
2,test2,SHAWZ,SHAWZ,FIX,,34.567,-81.234,,US,,,,,,,,,,
3,test3,KEEMO,KEEMO,NDB,356,36.7777,-83.1234,1200,US,,,,,,,,,,
```

Add tests to `tests/test_navaids.py` (create new file):

```python
from pathlib import Path

import pytest

from adsbtrack.config import Config
from adsbtrack.db import Database
from adsbtrack.navaids import refresh_navaids


FIXTURE = Path(__file__).parent / "fixtures" / "navaids_sample.csv"


def test_refresh_loads_fixture(tmp_path):
    cfg = Config(db_path=tmp_path / "nav.db")
    with Database(cfg.db_path) as db:
        n = refresh_navaids(db, cfg, local_csv=FIXTURE)
        assert n == 3
        rows = db.conn.execute(
            "SELECT ident, name, type, elevation_ft, frequency_khz FROM navaids ORDER BY ident"
        ).fetchall()
        assert rows[0]["ident"] == "CLT"
        assert rows[0]["type"] == "VOR-DME"
        assert rows[0]["elevation_ft"] == 750
        assert rows[0]["frequency_khz"] == 115800
        # Missing elevation tolerated.
        fix_row = [r for r in rows if r["ident"] == "SHAWZ"][0]
        assert fix_row["elevation_ft"] is None


def test_refresh_is_idempotent(tmp_path):
    cfg = Config(db_path=tmp_path / "nav.db")
    with Database(cfg.db_path) as db:
        refresh_navaids(db, cfg, local_csv=FIXTURE)
        refresh_navaids(db, cfg, local_csv=FIXTURE)
        count = db.conn.execute("SELECT COUNT(*) FROM navaids").fetchone()[0]
        assert count == 3
```

Run: `uv run pytest tests/test_navaids.py -v`
Expected: FAIL with ImportError (no navaids module yet) or fixture not found. Then PASS after Steps 1.4 + 1.5 both land.

- [ ] **Step 1.6: Wire refresh CLI command**

In `adsbtrack/cli.py` near the `runways` group definition (around line 906), add (imports at top of file):

```python
from .navaids import refresh_navaids as _refresh_navaids


@cli.group()
def navaids():
    """OurAirports navaid reference data (VOR / NDB / fixes)."""


@navaids.command("refresh")
@click.option(
    "--csv",
    "csv_path",
    type=click.Path(exists=True, path_type=Path, dir_okay=False),
    default=None,
    help="Use a local navaids.csv instead of downloading from OurAirports.",
)
@click.option("--db", "db_path", default="adsbtrack.db", help="Database path")
def navaids_refresh(csv_path, db_path):
    """Download OurAirports navaids.csv and upsert global navaid reference data.

    Idempotent - re-running replaces existing rows.
    """
    import httpx

    cfg = Config(db_path=Path(db_path))
    try:
        with Database(cfg.db_path) as db:
            count = _refresh_navaids(db, cfg, local_csv=csv_path)
    except httpx.HTTPError as e:
        raise click.ClickException(f"failed to download navaids.csv: {e}") from e
    except FileNotFoundError as e:
        raise click.ClickException(str(e)) from e
    except OSError as e:
        raise click.ClickException(f"filesystem error: {e}") from e
    console.print(f"[green]Navaid reference data loaded:[/] {count} navaids")
```

- [ ] **Step 1.7: Write failing CLI test**

Add to `tests/test_cli.py`:

```python
def test_navaids_refresh_local_csv(tmp_path, monkeypatch):
    from click.testing import CliRunner

    from adsbtrack.cli import cli

    db_path = tmp_path / "nav.db"
    fixture = Path(__file__).parent / "fixtures" / "navaids_sample.csv"

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["navaids", "refresh", "--csv", str(fixture), "--db", str(db_path)],
    )
    assert result.exit_code == 0, result.output
    assert "3 navaids" in result.output
```

Run: `uv run pytest tests/test_cli.py::test_navaids_refresh_local_csv -v`
Expected: PASS.

- [ ] **Step 1.8: Commit**

```bash
git add adsbtrack/config.py adsbtrack/db.py adsbtrack/navaids.py \
  adsbtrack/cli.py tests/test_navaids.py tests/test_db.py tests/test_cli.py \
  tests/fixtures/navaids_sample.csv
git commit -m "Add navaids table + OurAirports refresh CLI"
```

---

## Task 2: Pure alignment algorithm

**Files:**
- Create: `adsbtrack/navaid_alignment.py` — algorithm only, no DB or I/O.
- Modify: `adsbtrack/config.py` — alignment thresholds.
- Create: `tests/test_navaid_alignment.py`.

### Algorithm (per spec)

For each candidate navaid (pre-filtered by bbox in Task 3) the algorithm walks `metrics.recent_points` in chronological order and, at each point:

1. Require `lat`, `lon`, `track` all present.
2. Distance (km) from point to navaid < 500 nm (926 km).
3. Bearing from point to navaid computed; `delta = smallest_angle(bearing, track)`.
4. Keep the point when `|delta| < 1°`.
5. Keep its `ts` for later segment building.

Then:

6. Segment: split the kept-point sequence on any gap > `split_gap_secs` (default 120 s = 2 min).
7. Filter: drop segments with duration < `min_duration_secs` (default 30 s).
8. Filter: drop segments whose min point-to-navaid distance >= `near_pass_max_nm` (default 80 nm, 148.16 km). Ensures the aircraft actually passed close to the navaid rather than briefly aligning from a sector 300 nm away.

Each surviving segment emits a `NavaidAlignmentSegment(navaid_ident, start_ts, end_ts, min_distance_km)`. Cross-navaid results are returned as a single list ordered by `start_ts`.

### Config thresholds

- [ ] **Step 2.1: Add config fields**

In `adsbtrack/config.py` after the takeoff runway block:

```python
# --- Navaid alignment (see adsbtrack/navaid_alignment.py) ---
# For each navaid within navaid_max_distance_nm of any flight point, the
# algorithm keeps points whose bearing-to-navaid delta from track is under
# navaid_alignment_tolerance_deg. Consecutive kept points separated by at
# most navaid_split_gap_secs form a segment. A segment qualifies when it
# runs at least navaid_min_duration_secs AND the flight's closest approach
# to that navaid during the segment is under navaid_near_pass_max_nm.
navaid_alignment_tolerance_deg: float = 1.0
navaid_max_distance_nm: float = 500.0
navaid_split_gap_secs: float = 120.0
navaid_min_duration_secs: float = 30.0
navaid_near_pass_max_nm: float = 80.0
# Bounding-box buffer (nm) applied when prefiltering navaids per flight.
navaid_bbox_buffer_nm: float = 50.0
```

- [ ] **Step 2.2: Write failing test for detect_navaid_alignments - simple two-navaid track**

Create `tests/test_navaid_alignment.py` (imports and first test):

```python
"""Tests for the navaid alignment algorithm.

These tests build synthetic FlightMetrics with recent_points directly so we
can pin down the algorithm's behavior without the parser pipeline.
"""

from __future__ import annotations

from collections import deque

from adsbtrack.classifier import FlightMetrics, _PointSample
from adsbtrack.navaid_alignment import (
    NavaidAlignmentSegment,
    detect_navaid_alignments,
)


def _sample(ts: float, lat: float, lon: float, track: float) -> _PointSample:
    """Build a minimal _PointSample for navaid alignment tests."""
    return _PointSample(
        ts=ts,
        lat=lat,
        lon=lon,
        alt=5000,
        geom_alt=5000,
        baro_alt=5000,
        gs=250.0,
        track=track,
        baro_rate=0.0,
        geom_rate=0.0,
        squawk=None,
        mlat=0,
        tisb=0,
    )


def _metrics_from_points(points: list[_PointSample]) -> FlightMetrics:
    m = FlightMetrics(icao="test", first_point_ts=points[0].ts)
    for p in points:
        m.recent_points.append(p)
    return m


def test_detect_aligned_segment_near_single_navaid():
    """An aircraft flying due north (track=0) straight toward a navaid
    1 degree north of its position generates a sustained alignment."""
    navaids = [{"ident": "TEST", "latitude_deg": 35.5, "longitude_deg": -80.0, "type": "VOR"}]
    # 40 samples, 2s apart, at lat 34.5 moving toward lat 35.5 (distance shrinks
    # from ~60 nm to ~0 as ts advances). Track stays 0 (due north).
    points = []
    for i in range(40):
        lat = 34.5 + (i * 0.02)  # 0.02 deg ~ 1.2 nm step; 40 steps ~ 48 nm
        points.append(_sample(ts=1000.0 + 2.0 * i, lat=lat, lon=-80.0, track=0.0))
    metrics = _metrics_from_points(points)
    segs = detect_navaid_alignments(
        metrics,
        navaids=navaids,
        tolerance_deg=1.0,
        max_distance_nm=500.0,
        split_gap_secs=120.0,
        min_duration_secs=30.0,
        near_pass_max_nm=80.0,
    )
    assert len(segs) == 1
    assert segs[0].navaid_ident == "TEST"
    assert segs[0].end_ts - segs[0].start_ts >= 30.0
    assert segs[0].min_distance_km < 80.0 * 1.852
```

Run: `uv run pytest tests/test_navaid_alignment.py::test_detect_aligned_segment_near_single_navaid -v`
Expected: FAIL with ImportError (module doesn't exist yet).

- [ ] **Step 2.3: Implement detect_navaid_alignments**

Create `adsbtrack/navaid_alignment.py`:

```python
"""Geometric navaid-alignment detector.

For each candidate navaid (pre-filtered by bbox to keep cost bounded) the
algorithm walks ``metrics.recent_points`` and keeps every point whose
bearing-to-navaid lies within a degree or so of the ground track, subject to
a maximum range. Kept points are split into segments on long gaps, then
filtered by minimum duration and minimum closest-approach distance. The
surviving list is this flight's navaid track fingerprint.

Attribution: the geometric idea (|bearing-to-beacon - track| under a
threshold, split-on-gap, duration + close-pass filter) mirrors xoolive/
traffic's ``BeaconTrackBearingAlignment`` (MIT-licensed). No code is copied
from traffic; this module reimplements the algorithm in our style against
the FlightMetrics / recent_points layer already present in this codebase.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from .classifier import FlightMetrics, _PointSample

_KM_PER_NM = 1.852
_EARTH_R_KM = 6371.0


@dataclass(frozen=True)
class NavaidAlignmentSegment:
    """One qualifying alignment segment between a flight and one navaid."""

    navaid_ident: str
    start_ts: float
    end_ts: float
    min_distance_km: float


def _bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    x = math.sin(dlon) * math.cos(rlat2)
    y = math.cos(rlat1) * math.sin(rlat2) - math.sin(rlat1) * math.cos(rlat2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360.0) % 360.0


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = rlat2 - rlat1
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return _EARTH_R_KM * 2 * math.asin(math.sqrt(a))


def _smallest_angle(a_deg: float, b_deg: float) -> float:
    d = (a_deg - b_deg) % 360.0
    return d if d <= 180.0 else 360.0 - d


def _alignments_for_navaid(
    samples: Sequence[_PointSample],
    navaid: Mapping[str, object],
    *,
    tolerance_deg: float,
    max_distance_km: float,
    split_gap_secs: float,
    min_duration_secs: float,
    near_pass_max_km: float,
) -> list[NavaidAlignmentSegment]:
    ident = str(navaid.get("ident") or "")
    if not ident:
        return []
    n_lat = navaid.get("latitude_deg")
    n_lon = navaid.get("longitude_deg")
    if n_lat is None or n_lon is None:
        return []
    n_lat_f = float(n_lat)  # type: ignore[arg-type]
    n_lon_f = float(n_lon)  # type: ignore[arg-type]

    kept: list[tuple[float, float]] = []  # (ts, distance_km)
    for s in samples:
        if s.lat is None or s.lon is None or s.track is None:
            continue
        dist_km = _haversine_km(s.lat, s.lon, n_lat_f, n_lon_f)
        if dist_km > max_distance_km:
            continue
        bearing = _bearing_deg(s.lat, s.lon, n_lat_f, n_lon_f)
        if _smallest_angle(bearing, float(s.track)) >= tolerance_deg:
            continue
        kept.append((s.ts, dist_km))

    if not kept:
        return []

    segments: list[list[tuple[float, float]]] = [[kept[0]]]
    for prev, cur in zip(kept, kept[1:], strict=False):
        if cur[0] - prev[0] > split_gap_secs:
            segments.append([cur])
        else:
            segments[-1].append(cur)

    results: list[NavaidAlignmentSegment] = []
    for seg in segments:
        duration = seg[-1][0] - seg[0][0]
        if duration < min_duration_secs:
            continue
        min_d = min(d for _, d in seg)
        if min_d >= near_pass_max_km:
            continue
        results.append(
            NavaidAlignmentSegment(
                navaid_ident=ident,
                start_ts=seg[0][0],
                end_ts=seg[-1][0],
                min_distance_km=round(min_d, 3),
            )
        )
    return results


def detect_navaid_alignments(
    metrics: FlightMetrics,
    *,
    navaids: Iterable[Mapping[str, object]],
    tolerance_deg: float = 1.0,
    max_distance_nm: float = 500.0,
    split_gap_secs: float = 120.0,
    min_duration_secs: float = 30.0,
    near_pass_max_nm: float = 80.0,
) -> list[NavaidAlignmentSegment]:
    """Return every qualifying alignment segment across all provided navaids,
    chronologically ordered by start_ts. Empty list if no segments qualify."""
    samples = list(metrics.recent_points)
    if not samples:
        return []
    max_distance_km = max_distance_nm * _KM_PER_NM
    near_pass_max_km = near_pass_max_nm * _KM_PER_NM

    out: list[NavaidAlignmentSegment] = []
    for nav in navaids:
        out.extend(
            _alignments_for_navaid(
                samples,
                nav,
                tolerance_deg=tolerance_deg,
                max_distance_km=max_distance_km,
                split_gap_secs=split_gap_secs,
                min_duration_secs=min_duration_secs,
                near_pass_max_km=near_pass_max_km,
            )
        )
    out.sort(key=lambda s: s.start_ts)
    return out
```

- [ ] **Step 2.4: Run the first test**

Run: `uv run pytest tests/test_navaid_alignment.py::test_detect_aligned_segment_near_single_navaid -v`
Expected: PASS.

- [ ] **Step 2.5: Add negative and splitting tests**

Append to `tests/test_navaid_alignment.py`:

```python
def test_no_navaids_returns_empty():
    # Single realistic sample, but no navaids to check against.
    points = [_sample(ts=1000.0, lat=34.5, lon=-80.0, track=0.0)]
    metrics = _metrics_from_points(points)
    assert detect_navaid_alignments(metrics, navaids=[]) == []


def test_track_misaligned_rejects_all_points():
    """Aircraft heading east (track=90) while a navaid sits due north sees
    bearing=0 but track=90: delta=90 >> 1-degree tolerance, nothing kept."""
    navaids = [{"ident": "NORTH", "latitude_deg": 35.5, "longitude_deg": -80.0}]
    points = [_sample(1000.0 + 2.0 * i, 34.5, -80.0 + 0.005 * i, 90.0) for i in range(20)]
    metrics = _metrics_from_points(points)
    assert detect_navaid_alignments(metrics, navaids=navaids) == []


def test_gap_splits_segment():
    """Two chunks of track-aligned samples separated by a >2-min gap split
    into two segments. Both must pass the duration + close-pass filters."""
    navaids = [{"ident": "TEST", "latitude_deg": 35.5, "longitude_deg": -80.0}]
    first = [_sample(1000.0 + 2.0 * i, 34.5 + 0.02 * i, -80.0, 0.0) for i in range(20)]
    # 3-minute gap.
    second = [_sample(2000.0 + 2.0 * i, 34.7 + 0.02 * i, -80.0, 0.0) for i in range(20)]
    metrics = _metrics_from_points(first + second)
    segs = detect_navaid_alignments(metrics, navaids=navaids)
    assert len(segs) == 2
    assert segs[0].end_ts < segs[1].start_ts


def test_far_pass_rejected_by_near_pass_filter():
    """Points can briefly align toward a very distant navaid by coincidence.
    The closest-approach filter (80 nm) rejects those as fingerprints."""
    navaids = [{"ident": "FAR", "latitude_deg": 40.0, "longitude_deg": -80.0}]
    # Aircraft sits at 34.5N (~330 nm south). Track 0 points it roughly
    # toward the navaid so bearing-track delta can be tiny. Kept points exist
    # but min distance is ~330 nm -> far exceeds 80 nm, segment dropped.
    points = [_sample(1000.0 + 2.0 * i, 34.5, -80.0, 0.0) for i in range(40)]
    metrics = _metrics_from_points(points)
    assert detect_navaid_alignments(metrics, navaids=navaids, near_pass_max_nm=80.0) == []


def test_short_segment_filtered_by_min_duration():
    """Only 10 s of aligned flight isn't enough to count."""
    navaids = [{"ident": "TEST", "latitude_deg": 35.5, "longitude_deg": -80.0}]
    # 5 samples, 2s apart = 8s wall-clock -> below 30s floor.
    points = [_sample(1000.0 + 2.0 * i, 34.5 + 0.02 * i, -80.0, 0.0) for i in range(5)]
    metrics = _metrics_from_points(points)
    assert detect_navaid_alignments(metrics, navaids=navaids) == []
```

Run: `uv run pytest tests/test_navaid_alignment.py -v`
Expected: all 5 tests PASS.

- [ ] **Step 2.6: Commit**

```bash
git add adsbtrack/config.py adsbtrack/navaid_alignment.py tests/test_navaid_alignment.py
git commit -m "Add navaid alignment algorithm"
```

---

## Task 3: Bounding-box pre-filter + helper

**Files:**
- Modify: `adsbtrack/navaids.py` — add `query_navaids_in_bbox`, `flight_bbox_from_metrics`.
- Modify: `tests/test_navaids.py` — bbox query test.

### Design

The parser runs navaid alignment once per flight. We do not want each flight to scan the ~13 k rows in the navaids table. Two-level optimization:

1. **Per-flight bbox pre-filter (SQL).** Compute (min_lat, max_lat, min_lon, max_lon) of the flight's `recent_points`. Expand by `navaid_bbox_buffer_nm` (50 nm). Query `WHERE latitude_deg BETWEEN ? AND ? AND longitude_deg BETWEEN ? AND ?`. The indexed lat/lon query returns ~20-200 navaids for a typical flight.
2. **Per-extract-run bbox cache.** Different flights often cover nearly the same bbox (same home base, same route). Quantize the bbox corners to 0.5° and cache the navaid list per quantized key. Cache lives as a `dict` on the parser extract loop alongside `airport_elev_cache` and `runway_cache`.

We deliberately skip antimeridian handling: routes crossing the 180° meridian are rare enough in the hex-scoped extraction workload that a fallback (skip navaid alignment when `max_lon - min_lon > 180.0`) is fine. Document this caveat in Task 7.

- [ ] **Step 3.1: Write failing bbox query test**

Append to `tests/test_navaids.py`:

```python
def test_query_navaids_in_bbox(tmp_path):
    from adsbtrack.navaids import query_navaids_in_bbox

    cfg = Config(db_path=tmp_path / "nav.db")
    with Database(cfg.db_path) as db:
        refresh_navaids(db, cfg, local_csv=FIXTURE)
        # All three fixture rows land between lat 34-37, lon -83 to -80.
        rows = query_navaids_in_bbox(db.conn, 34.0, 37.0, -84.0, -80.0)
        idents = {r["ident"] for r in rows}
        assert idents == {"CLT", "SHAWZ", "KEEMO"}

        # Narrow box around SHAWZ only.
        rows = query_navaids_in_bbox(db.conn, 34.5, 34.6, -81.3, -81.2)
        assert {r["ident"] for r in rows} == {"SHAWZ"}
```

Run: `uv run pytest tests/test_navaids.py::test_query_navaids_in_bbox -v`
Expected: FAIL (function not yet defined).

- [ ] **Step 3.2: Implement bbox query + bbox helper**

Append to `adsbtrack/navaids.py`:

```python
import sqlite3

from .classifier import FlightMetrics

_KM_PER_NM = 1.852


def query_navaids_in_bbox(
    conn: sqlite3.Connection,
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
) -> list[sqlite3.Row]:
    """Return navaid rows inside the [min_lat, max_lat] x [min_lon, max_lon] box.

    Uses the (latitude_deg, longitude_deg) indexes. The caller is responsible
    for buffering the box (see navaid_bbox_buffer_nm in Config).
    """
    return list(
        conn.execute(
            "SELECT ident, name, type, latitude_deg, longitude_deg,"
            "       elevation_ft, frequency_khz, iso_country"
            "  FROM navaids"
            " WHERE latitude_deg BETWEEN ? AND ?"
            "   AND longitude_deg BETWEEN ? AND ?",
            (min_lat, max_lat, min_lon, max_lon),
        ).fetchall()
    )


def flight_bbox_from_metrics(
    metrics: FlightMetrics,
    *,
    buffer_nm: float,
) -> tuple[float, float, float, float] | None:
    """Compute (min_lat, max_lat, min_lon, max_lon) of a flight, expanded by
    buffer_nm in every direction. Returns None if the flight has no samples
    with lat/lon or crosses the antimeridian (|lon_span| > 180 deg)."""
    lats: list[float] = []
    lons: list[float] = []
    for s in metrics.recent_points:
        if s.lat is not None and s.lon is not None:
            lats.append(s.lat)
            lons.append(s.lon)
    if not lats:
        return None
    min_lat, max_lat = min(lats), max(lats)
    min_lon, max_lon = min(lons), max(lons)
    if max_lon - min_lon > 180.0:
        return None
    # 1 deg lat ~ 60 nm. Longitude scales with cos(lat); use the higher-abs
    # latitude to be safe so the box is always wide enough.
    buffer_lat = buffer_nm / 60.0
    worst_lat_rad = math.radians(max(abs(min_lat), abs(max_lat)))
    cos_lat = max(0.01, math.cos(worst_lat_rad))  # clamp near the poles
    buffer_lon = buffer_lat / cos_lat
    return (
        min_lat - buffer_lat,
        max_lat + buffer_lat,
        min_lon - buffer_lon,
        max_lon + buffer_lon,
    )
```

Add `import math` at the top of `navaids.py` if not already present.

- [ ] **Step 3.3: Add bbox helper test**

Append to `tests/test_navaid_alignment.py`:

```python
def test_flight_bbox_from_metrics_basic():
    from adsbtrack.navaids import flight_bbox_from_metrics

    points = [_sample(1000.0 + 2.0 * i, 34.5 + 0.02 * i, -80.0, 0.0) for i in range(10)]
    metrics = _metrics_from_points(points)
    bbox = flight_bbox_from_metrics(metrics, buffer_nm=50.0)
    assert bbox is not None
    min_lat, max_lat, min_lon, max_lon = bbox
    # Raw lat span was 34.5..34.68, buffer 50 nm ~ 0.83 deg.
    assert min_lat < 34.5 - 0.8
    assert max_lat > 34.68 + 0.8
    # Lon was constant at -80 so buffered box still symmetric.
    assert min_lon < -80.0
    assert max_lon > -80.0


def test_flight_bbox_returns_none_with_no_points():
    from adsbtrack.navaids import flight_bbox_from_metrics

    metrics = FlightMetrics(icao="test", first_point_ts=0.0)
    assert flight_bbox_from_metrics(metrics, buffer_nm=50.0) is None
```

Run: `uv run pytest tests/test_navaids.py tests/test_navaid_alignment.py -v`
Expected: all tests PASS.

- [ ] **Step 3.4: Commit**

```bash
git add adsbtrack/navaids.py tests/test_navaids.py tests/test_navaid_alignment.py
git commit -m "Add navaid bbox query and per-flight bbox helper"
```

---

## Task 4: Flight.navaid_track field + DB column + insert_flight invariant

**Files:**
- Modify: `adsbtrack/models.py` — add `navaid_track`.
- Modify: `adsbtrack/db.py` — column in `CREATE TABLE flights`, migration entry, `insert_flight` row tuple + placeholders + columns list (101 → 102).
- Modify: `tests/test_db.py` — schema test + round-trip test.

- [ ] **Step 4.1: Add Flight.navaid_track field**

In `adsbtrack/models.py`, find the `Flight` dataclass (sorted near the bottom by convention for this project). Add after `primary_squawk`:

```python
    navaid_track: str | None = None
```

- [ ] **Step 4.2: Add DDL column**

In `adsbtrack/db.py`, extend `CREATE TABLE IF NOT EXISTS flights` (ends at line 137 before `UNIQUE`). Add after `primary_squawk TEXT,`:

```python
    navaid_track TEXT,
```

- [ ] **Step 4.3: Add migration entry**

Around line 582 (migration tuple list after `("primary_squawk", "TEXT"),`) append:

```python
        ("navaid_track", "TEXT"),
```

- [ ] **Step 4.4: Update insert_flight invariant**

In `adsbtrack/db.py:insert_flight` (around line 768), locate the `INSERT INTO flights (...)` statement. Append `, navaid_track` to the column list. Append `?` to the placeholder list (bringing the count to 102). Append `flight.navaid_track` at the matching position in the value tuple (around line 890 after `flight.primary_squawk`).

Verify the invariant by running:

```bash
uv run python - <<'PY'
import re, ast
src = open("adsbtrack/db.py").read()
# Find the exact insert_flight statement and count tokens.
m = re.search(r"INSERT INTO flights \(([^)]+)\)\s*VALUES\s*\(([^)]+)\)", src, re.S)
cols = [c.strip() for c in m.group(1).split(",")]
placeholders = [p.strip() for p in m.group(2).split(",")]
print("columns:", len(cols), "placeholders:", len(placeholders))
PY
```

Expected output: `columns: 102 placeholders: 102`.

- [ ] **Step 4.5: Write failing schema test**

Add to `tests/test_db.py`:

```python
def test_flights_navaid_track_column(tmp_path):
    from adsbtrack.db import Database

    with Database(tmp_path / "nt.db") as db:
        cols = {r[1] for r in db.conn.execute("PRAGMA table_info(flights)").fetchall()}
    assert "navaid_track" in cols
```

Run: `uv run pytest tests/test_db.py::test_flights_navaid_track_column -v`
Expected: PASS.

- [ ] **Step 4.6: Write round-trip test**

Add to `tests/test_db.py`:

```python
def test_flight_insert_round_trip_navaid_track(tmp_path):
    from datetime import datetime

    from adsbtrack.db import Database
    from adsbtrack.models import Flight

    db_path = tmp_path / "rt.db"
    with Database(db_path) as db:
        f = Flight(
            icao="abc123",
            takeoff_time=datetime(2026, 3, 27, 12, 0, 0),
            takeoff_lat=35.0,
            takeoff_lon=-80.0,
            takeoff_date="2026-03-27",
            navaid_track='[{"navaid_ident": "CLT", "start_ts": 100.0, "end_ts": 280.0, "min_distance_nm": 2.5}]',
        )
        db.insert_flight(f)
        row = db.conn.execute(
            "SELECT navaid_track FROM flights WHERE icao=?", ("abc123",)
        ).fetchone()
        assert row["navaid_track"].startswith("[")
        assert "CLT" in row["navaid_track"]
```

Run: `uv run pytest tests/test_db.py::test_flight_insert_round_trip_navaid_track -v`
Expected: PASS.

- [ ] **Step 4.7: Sanity-run the full test suite**

Run: `uv run pytest -q`
Expected: no regressions. Earlier tests that build `Flight` without `navaid_track` still pass (default is None).

- [ ] **Step 4.8: Commit**

```bash
git add adsbtrack/models.py adsbtrack/db.py tests/test_db.py
git commit -m "Add navaid_track column and Flight field"
```

---

## Task 5: Parser integration with bbox cache

**Files:**
- Modify: `adsbtrack/parser.py` — thread `navaid_cache` through extract loop; call `detect_navaid_alignments`; serialize to JSON; set `flight.navaid_track`.
- Modify: `tests/test_parser.py` — end-to-end integration test using MagicMock parser scaffold.

### Integration point

In `adsbtrack/parser.py`, the extract loop already builds `airport_elev_cache` and `runway_cache` (around line 820). Add a third cache:

```python
navaid_cache: dict[tuple[int, int, int, int], list] = {}
```

After the go-around + pattern override block (after `parser.py:1156`), insert the navaid alignment block. Prefer extracting it to a small helper inside `parser.py` to keep the loop readable:

```python
def _compute_navaid_track_json(
    metrics: FlightMetrics,
    *,
    db: Database,
    config: Config,
    navaid_cache: dict[tuple[int, int, int, int], list],
) -> str | None:
    """Emit the navaid_track JSON column value for one flight. Returns None
    when the flight has no qualifying alignment, so legacy flights without a
    navaid_track stay uniform with freshly-computed empty results."""
    from .navaids import flight_bbox_from_metrics, query_navaids_in_bbox
    from .navaid_alignment import detect_navaid_alignments

    bbox = flight_bbox_from_metrics(metrics, buffer_nm=config.navaid_bbox_buffer_nm)
    if bbox is None:
        return None

    # Quantize to 0.5 deg so near-duplicate routes share cached navaid rows.
    key = tuple(int(math.floor(v * 2)) for v in bbox)  # type: ignore[assignment]
    if key not in navaid_cache:
        navaid_cache[key] = query_navaids_in_bbox(db.conn, *bbox)
    navaids = navaid_cache[key]
    if not navaids:
        return None

    segments = detect_navaid_alignments(
        metrics,
        navaids=[dict(r) for r in navaids],
        tolerance_deg=config.navaid_alignment_tolerance_deg,
        max_distance_nm=config.navaid_max_distance_nm,
        split_gap_secs=config.navaid_split_gap_secs,
        min_duration_secs=config.navaid_min_duration_secs,
        near_pass_max_nm=config.navaid_near_pass_max_nm,
    )
    if not segments:
        return None
    payload = [
        {
            "navaid_ident": s.navaid_ident,
            "start_ts": s.start_ts,
            "end_ts": s.end_ts,
            "min_distance_nm": round(s.min_distance_km / 1.852, 2),
        }
        for s in segments
    ]
    return json.dumps(payload, ensure_ascii=True)
```

Add `import math` and `import json` at the top of `parser.py` if not already present.

- [ ] **Step 5.1: Add helper and cache**

Apply the changes above in `parser.py`: define `_compute_navaid_track_json` at module scope (right after `_any_climb_between` or similar module-level helper). Initialize `navaid_cache: dict[tuple[int, int, int, int], list] = {}` in the extract function near `airport_elev_cache`.

Invoke it inside the extract loop, right after the pattern-cycles/mission override block (before `flight.turnaround_minutes` logic):

```python
        # --- Navaid alignment (adsbtrack/navaid_alignment.py) ---
        # Emits a JSON route fingerprint of navaids the aircraft's ground
        # track pointed directly toward for long enough to be meaningful.
        flight.navaid_track = _compute_navaid_track_json(
            metrics,
            db=db,
            config=config,
            navaid_cache=navaid_cache,
        )
```

- [ ] **Step 5.2: Write failing parser test**

Add to `tests/test_parser.py`:

```python
def test_parser_sets_navaid_track_when_aligned(tmp_path, monkeypatch):
    """End-to-end: a flight whose track is aligned with one navaid for 60+ s
    gets a JSON navaid_track column populated via the extract loop."""
    from unittest.mock import MagicMock
    import json

    from adsbtrack.classifier import FlightMetrics, _PointSample
    from adsbtrack.config import Config
    from adsbtrack.models import Flight
    from adsbtrack.parser import _compute_navaid_track_json

    cfg = Config(db_path=tmp_path / "p.db")

    # Build metrics with 40 aligned points, track=0, lat marching toward 35.5.
    m = FlightMetrics(icao="abc", first_point_ts=1000.0)
    for i in range(40):
        m.recent_points.append(
            _PointSample(
                ts=1000.0 + 2.0 * i,
                lat=34.5 + 0.02 * i,
                lon=-80.0,
                alt=5000,
                geom_alt=5000,
                baro_alt=5000,
                gs=250.0,
                track=0.0,
                baro_rate=0.0,
                geom_rate=0.0,
                squawk=None,
                mlat=0,
                tisb=0,
            )
        )

    # MagicMock db with one navaid matching the trajectory endpoint.
    mock_db = MagicMock()
    mock_db.conn.execute.return_value.fetchall.return_value = [
        {
            "ident": "TEST",
            "name": "TESTNV",
            "type": "VOR",
            "latitude_deg": 35.5,
            "longitude_deg": -80.0,
            "elevation_ft": 0,
            "frequency_khz": None,
            "iso_country": "US",
        }
    ]

    result = _compute_navaid_track_json(m, db=mock_db, config=cfg, navaid_cache={})
    assert result is not None
    payload = json.loads(result)
    assert payload[0]["navaid_ident"] == "TEST"
    assert payload[0]["end_ts"] - payload[0]["start_ts"] >= 30.0


def test_parser_navaid_track_cache_reused_across_flights(tmp_path):
    """A second call with an overlapping bbox reuses the cache key and does
    not re-query the DB."""
    from unittest.mock import MagicMock

    from adsbtrack.classifier import FlightMetrics, _PointSample
    from adsbtrack.config import Config
    from adsbtrack.parser import _compute_navaid_track_json

    cfg = Config(db_path=tmp_path / "p.db")
    mock_db = MagicMock()
    mock_db.conn.execute.return_value.fetchall.return_value = []

    cache: dict = {}

    def one_metric(offset: float) -> FlightMetrics:
        m = FlightMetrics(icao="abc", first_point_ts=1000.0)
        for i in range(10):
            m.recent_points.append(
                _PointSample(
                    ts=1000.0 + 2.0 * i,
                    lat=34.5 + 0.001 * i + offset,
                    lon=-80.0 + 0.001 * i,
                    alt=5000,
                    geom_alt=5000,
                    baro_alt=5000,
                    gs=250.0,
                    track=0.0,
                    baro_rate=0.0,
                    geom_rate=0.0,
                    squawk=None,
                    mlat=0,
                    tisb=0,
                )
            )
        return m

    _compute_navaid_track_json(one_metric(0.0), db=mock_db, config=cfg, navaid_cache=cache)
    _compute_navaid_track_json(one_metric(0.01), db=mock_db, config=cfg, navaid_cache=cache)

    # The two tight bboxes quantize to the same 0.5-deg key.
    assert len(cache) == 1
    # And the DB was only queried once (cache hit on second call).
    assert mock_db.conn.execute.call_count == 1
```

Run: `uv run pytest tests/test_parser.py::test_parser_sets_navaid_track_when_aligned tests/test_parser.py::test_parser_navaid_track_cache_reused_across_flights -v`
Expected: FAIL (helper not in place yet).

- [ ] **Step 5.3: Run tests; fix imports/typing as needed**

After Step 5.1 is landed, run: `uv run pytest tests/test_parser.py -v`
Expected: PASS. If `math` or `json` imports are missing at the top of `parser.py`, add them now.

- [ ] **Step 5.4: Full suite**

Run: `uv run ruff check . && uv run ruff format --check . && uv run pytest`
Expected: all green. If ruff format flags the helper, run `uv run ruff format .` and stage the result.

- [ ] **Step 5.5: Commit**

```bash
git add adsbtrack/parser.py tests/test_parser.py
git commit -m "Integrate navaid alignment into extract pipeline with bbox cache"
```

---

## Task 6: `route` CLI command

**Files:**
- Modify: `adsbtrack/cli.py` — add `route` command at top level.
- Modify: `tests/test_cli.py` — CLI invocation tests.

### Output format (per spec)

One line per flight with a non-empty navaid_track, in ascending takeoff time:

```
2026-03-27 KSPG -> KHKY  SHAWZ (15m) -> KEEMO (8m) -> CLT VOR (3m)
```

- Date column: `takeoff_date` (ISO).
- Origin/destination: `origin_icao` or `-`; `destination_icao` or `-` (when NULL). Falls back to `nearest_origin_icao` / `nearest_destination_icao` in parens when the primary fields are NULL. Keep the rendering compact; no extra whitespace beyond the double-space separator before the navaid chain.
- Navaid chain: each segment as `IDENT (Nm)` where N = `round((end_ts - start_ts) / 60)`. When duration < 60 s, render as `(<1m)` rather than `(0m)` so short-but-qualifying alignments stay visible. Chain separator is ` -> ` (space hyphen greater-than space).
- A flight with NULL or empty-list `navaid_track` is skipped.

- [ ] **Step 6.1: Implement route command**

In `adsbtrack/cli.py`, near the `trips` command (around line 301), add:

```python
@cli.command()
@click.option("--hex", "hex_code", help="ICAO hex code (6 chars)")
@click.option("--tail", "tail_number", help="Tail number; resolved to hex")
@click.option("--db", "db_path", default="adsbtrack.db", help="Database path")
def route(hex_code, tail_number, db_path):
    """Print the navaid track fingerprint for each flight of an aircraft."""
    import json as _json

    resolved = _resolve_hex(hex_code, tail_number)
    cfg = Config(db_path=Path(db_path))
    with Database(cfg.db_path) as db:
        rows = db.conn.execute(
            "SELECT takeoff_date, origin_icao, destination_icao,"
            "       nearest_origin_icao, nearest_destination_icao, navaid_track"
            "  FROM flights"
            " WHERE icao = ?"
            "   AND navaid_track IS NOT NULL"
            " ORDER BY takeoff_time",
            (resolved,),
        ).fetchall()

    if not rows:
        console.print(f"No navaid track data for [cyan]{resolved}[/]")
        return

    for row in rows:
        try:
            payload = _json.loads(row["navaid_track"])
        except (ValueError, TypeError):
            continue
        if not payload:
            continue
        origin = row["origin_icao"] or (
            f"({row['nearest_origin_icao']})" if row["nearest_origin_icao"] else "-"
        )
        destination = row["destination_icao"] or (
            f"({row['nearest_destination_icao']})" if row["nearest_destination_icao"] else "-"
        )
        chain_parts = []
        for seg in payload:
            dur_secs = float(seg.get("end_ts", 0.0)) - float(seg.get("start_ts", 0.0))
            if dur_secs < 60.0:
                label = "<1m"
            else:
                label = f"{int(round(dur_secs / 60.0))}m"
            chain_parts.append(f"{seg['navaid_ident']} ({label})")
        chain = " -> ".join(chain_parts)
        console.print(f"{row['takeoff_date']} {origin} -> {destination}  {chain}")
```

- [ ] **Step 6.2: Write failing test**

Add to `tests/test_cli.py`:

```python
def test_route_cli_prints_chain(tmp_path, monkeypatch):
    import json
    from datetime import datetime

    from click.testing import CliRunner

    from adsbtrack.cli import cli
    from adsbtrack.db import Database
    from adsbtrack.models import Flight

    monkeypatch.setenv("COLUMNS", "200")
    db_path = tmp_path / "r.db"
    track = json.dumps(
        [
            {"navaid_ident": "SHAWZ", "start_ts": 0.0, "end_ts": 900.0, "min_distance_nm": 30.0},
            {"navaid_ident": "KEEMO", "start_ts": 900.0, "end_ts": 1380.0, "min_distance_nm": 20.0},
            {"navaid_ident": "CLT", "start_ts": 1400.0, "end_ts": 1580.0, "min_distance_nm": 1.5},
        ]
    )
    with Database(db_path) as db:
        db.insert_flight(
            Flight(
                icao="abc123",
                takeoff_time=datetime(2026, 3, 27, 14, 0, 0),
                takeoff_lat=35.0,
                takeoff_lon=-80.0,
                takeoff_date="2026-03-27",
                origin_icao="KSPG",
                destination_icao="KHKY",
                navaid_track=track,
            )
        )

    result = CliRunner().invoke(
        cli, ["route", "--hex", "abc123", "--db", str(db_path)]
    )
    assert result.exit_code == 0, result.output
    assert "2026-03-27 KSPG -> KHKY" in result.output
    assert "SHAWZ (15m) -> KEEMO (8m) -> CLT (3m)" in result.output


def test_route_cli_no_data(tmp_path):
    from click.testing import CliRunner

    from adsbtrack.cli import cli
    from adsbtrack.db import Database

    db_path = tmp_path / "r.db"
    with Database(db_path):
        pass  # empty DB, schema only

    result = CliRunner().invoke(
        cli, ["route", "--hex", "abc123", "--db", str(db_path)]
    )
    assert result.exit_code == 0
    assert "No navaid track" in result.output


def test_route_cli_short_segment_under_a_minute(tmp_path, monkeypatch):
    """A segment that lasts 40 s is rendered as '<1m' (stays visible but
    not misreported as 0m)."""
    import json
    from datetime import datetime

    from click.testing import CliRunner

    from adsbtrack.cli import cli
    from adsbtrack.db import Database
    from adsbtrack.models import Flight

    monkeypatch.setenv("COLUMNS", "200")
    db_path = tmp_path / "r.db"
    track = json.dumps(
        [{"navaid_ident": "NDB1", "start_ts": 0.0, "end_ts": 40.0, "min_distance_nm": 5.0}]
    )
    with Database(db_path) as db:
        db.insert_flight(
            Flight(
                icao="abc123",
                takeoff_time=datetime(2026, 3, 27, 14, 0, 0),
                takeoff_lat=35.0,
                takeoff_lon=-80.0,
                takeoff_date="2026-03-27",
                navaid_track=track,
            )
        )
    result = CliRunner().invoke(
        cli, ["route", "--hex", "abc123", "--db", str(db_path)]
    )
    assert result.exit_code == 0, result.output
    assert "NDB1 (<1m)" in result.output
```

Run: `uv run pytest tests/test_cli.py::test_route_cli_prints_chain tests/test_cli.py::test_route_cli_no_data tests/test_cli.py::test_route_cli_short_segment_under_a_minute -v`
Expected: PASS.

- [ ] **Step 6.3: Commit**

```bash
git add adsbtrack/cli.py tests/test_cli.py
git commit -m "Add route CLI: print navaid fingerprint per flight"
```

---

## Task 7: Performance profiling + docs

**Files:**
- Modify: `docs/features.md` — add "Navaid alignment" subsection.
- Modify: `docs/schema.md` — `navaid_track` row + `navaids` table rows.
- Modify: `docs/internals.md` (if it references the extract pipeline or cache structures) — mention the new `navaid_cache`.

### Performance baseline

The spec requires documenting whether adding navaid alignment inflates `extract --reprocess` runtime by less than 50% on a representative aircraft with ~500 flights. We will:

1. Pick a hex with ~500 flights already in the local DB (any well-tracked airframe).
2. Run `uv run python -m adsbtrack.cli extract --hex <icao> --reprocess --db <copy1>.db` twice: once on the merged branch, once on main. Capture wall-clock from `/usr/bin/time -p`.
3. Report baseline vs. delta in the features doc: absolute seconds + percentage.

If the overhead exceeds 50%, investigate (usually the bbox cache hit rate). Common wins: raise the quantization from 0.5° to 1°, or skip the per-point bearing computation for navaids whose rough distance from the flight's centroid exceeds `max_distance_nm + 50 nm` (we already do this with the bbox pre-filter, but adding a second centroid-distance short-circuit inside the Python loop can halve work).

- [ ] **Step 7.1: Run the benchmark**

On a machine with an adsbtrack.db containing ~500 flights for at least one aircraft, run on main (no changes):

```bash
/usr/bin/time -p uv run python -m adsbtrack.cli extract --hex <icao> --reprocess \
  --db bench-main.db 2>&1 | tee bench-main.txt
```

Then run the same command on the feature branch (copy of the db):

```bash
/usr/bin/time -p uv run python -m adsbtrack.cli extract --hex <icao> --reprocess \
  --db bench-nav.db 2>&1 | tee bench-nav.txt
```

Record the "real" seconds in both files. If the implementer environment lacks a suitable DB, use a synthetic workload: script 500 synthetic extracts with a reduced navaid set (~500 rows) and report both absolute + ratio.

- [ ] **Step 7.2: Write features doc section**

Append to `docs/features.md` (following the existing heading convention):

```markdown
## Navaid alignment

**Column:** `navaid_track` (JSON string or NULL).

**What it captures.** For each flight, the ordered list of VORs / NDBs / fixes
whose bearing the ground track pointed directly toward for at least
`navaid_min_duration_secs` seconds, excluding segments whose closest approach
to the navaid exceeded `navaid_near_pass_max_nm` nm. This is a compact
fingerprint of the enroute routing: a helicopter that always flies
`SHAWZ -> KEEMO -> direct destination` will show that chain on most
flights, while a point-to-point shuttle will typically show zero or one
navaid.

**Algorithm (adsbtrack/navaid_alignment.py).** For each flight and each
pre-filtered navaid within the flight's bounding box (plus `navaid_bbox_buffer_nm`
buffer), the per-point bearing-to-navaid is compared with the ground track.
Points with delta under `navaid_alignment_tolerance_deg` and distance under
`navaid_max_distance_nm` are kept. Kept points are split into segments on
any gap longer than `navaid_split_gap_secs`, then segments are filtered by
`navaid_min_duration_secs` duration and `navaid_near_pass_max_nm` closest-
approach distance. Defaults: 1 degree tolerance, 500 nm cutoff, 120 s gap
split, 30 s duration floor, 80 nm closest-approach cap.

**Output shape.** Each qualifying segment serializes as
`{"navaid_ident": "<IDENT>", "start_ts": <unix>, "end_ts": <unix>, "min_distance_nm": <number>}`.
Flights with no qualifying segments emit NULL rather than `[]` so the
column is informative.

**CLI.** `adsbtrack.cli route --hex <icao>` prints one line per flight with
a non-empty navaid_track:

    2026-03-27 KSPG -> KHKY  SHAWZ (15m) -> KEEMO (8m) -> CLT (3m)

**Limitations.**

- A 1-degree tolerance and 500 nm range mean that on any long straight leg,
  a passing sector can coincidentally align with a distant navaid. The
  80 nm closest-approach filter rejects most such spurious matches but not
  all: navaids the aircraft actually flew past by 30-80 nm will register
  even when the pilot had no intent to track them. Alignment is not
  intent.
- The algorithm treats each navaid independently. An aircraft that
  alternates between two parallel airways a few nm apart will produce
  both navaids in its fingerprint.
- Antimeridian-crossing flights are skipped (`flight_bbox_from_metrics`
  returns None when the longitude span exceeds 180 deg). This is
  negligible for US/Europe/single-operator workloads; log if it
  becomes relevant for a future region.
- The `navaids` table must be refreshed via `adsbtrack.cli navaids refresh`
  for this column to ever populate. With an empty navaids table,
  `navaid_track` is always NULL.

**Performance.** On a representative aircraft with ~500 flights, extract
reprocess with navaid alignment enabled adds approximately X seconds to
the baseline runtime (Y% increase). The per-extract-run bbox cache
(quantized to 0.5 degrees) is the primary cost control; with typical
fleet extraction workloads the cache hit rate is N%.
```

Replace `X`, `Y`, `N` with real numbers after Step 7.1.

- [ ] **Step 7.3: Write schema doc entries**

Append to `docs/schema.md` in the appropriate flights-table column section:

```markdown
| `navaid_track` | TEXT | NULL | JSON array of navaid alignment segments for this flight; see adsbtrack/navaid_alignment.py. NULL when no segment qualified. Each entry: `{navaid_ident, start_ts, end_ts, min_distance_nm}`. |
```

Add a new top-level `navaids` table section:

```markdown
### `navaids` table

Cached OurAirports navaid reference data, populated by
`adsbtrack.cli navaids refresh`. Primary key is (ident, latitude_deg,
longitude_deg) because OurAirports reuses short idents across regions.

| Column | Type | Description |
| --- | --- | --- |
| `ident` | TEXT | Navaid identifier (3-5 letter code). |
| `name` | TEXT | Long name. |
| `type` | TEXT | `VOR`, `VOR-DME`, `VORTAC`, `NDB`, `FIX`, `TACAN`, etc. |
| `latitude_deg` | REAL | WGS-84. |
| `longitude_deg` | REAL | WGS-84. |
| `elevation_ft` | INTEGER | Navaid elevation (NULL when OurAirports lacks it). |
| `frequency_khz` | INTEGER | Station frequency in kHz (NULL for fixes). |
| `iso_country` | TEXT | Two-letter ISO country code. |
```

- [ ] **Step 7.4: Commit**

```bash
git add docs/features.md docs/schema.md
git commit -m "Document navaid alignment feature and navaids table"
```

---

## Final self-review

Before handing off for review, re-run the full quality gate on the tip:

```bash
uv run ruff check . && uv run ruff format --check . && uv run pytest
```

All three must pass. Mypy (`uv run mypy adsbtrack`) is informational per CLAUDE.md.

Verify the column invariant stayed consistent:

```bash
uv run python - <<'PY'
import re
src = open("adsbtrack/db.py").read()
m = re.search(r"INSERT INTO flights \(([^)]+)\)\s*VALUES\s*\(([^)]+)\)", src, re.S)
cols = [c.strip() for c in m.group(1).split(",")]
placeholders = [p.strip() for p in m.group(2).split(",")]
print("columns:", len(cols), "placeholders:", len(placeholders))
PY
```

Expected: `columns: 102 placeholders: 102`.
