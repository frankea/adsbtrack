# Runway Geometry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest OurAirports `runways.csv` into a new `runways` table (one row per runway end) with a `runways refresh` CLI command so later features can consume per-runway geometry for landing detection.

**Architecture:** Mirror the existing `airports.py` pattern - streaming CSV download via `httpx`, pure-function row parser, bulk upsert through `Database`. A new `adsbtrack/runways.py` module owns the download + parse + upsert pipeline. A new `runways` Click subgroup in `cli.py` exposes `runways refresh`. Schema gets a new `CREATE TABLE runways` statement plus supporting indexes. One row per runway end (not per pair) keeps downstream distance queries simple.

**Tech Stack:** Python 3.12+, httpx, click, sqlite3 (via `Database`), pytest. No new runtime dependencies.

**CSV column reference** (pinned 2026-04-16 from `https://davidmegginson.github.io/ourairports-data/runways.csv`):

```
id, airport_ref, airport_ident, length_ft, width_ft, surface, lighted, closed,
le_ident, le_latitude_deg, le_longitude_deg, le_elevation_ft, le_heading_degT, le_displaced_threshold_ft,
he_ident, he_latitude_deg, he_longitude_deg, he_elevation_ft, he_heading_degT, he_displaced_threshold_ft
```

Notes gleaned from the file that shape this plan:
- Heliports appear with `le_ident="H1"` and every lat/lon/heading/elevation NULL. These must produce zero runway rows.
- Airports can appear with `le_ident` / `he_ident` populated but coordinates blank. Per the spec, skip endpoints with missing lat/lon.
- `length_ft`, `width_ft`, and the per-end `displaced_threshold_ft` may be blank; store NULL.
- `surface` is a short code (`"ASPH"`, `"GRVL"`, `"CONC-G"`, etc.) - preserve as-is.
- `closed` and `lighted` are `0` / `1` ints.
- `airport_ident` is the OurAirports ident (can be ICAO like `"KATL"` or FAA local like `"67FL"`). Preserve exactly; do not normalize.

---

## File Structure

**New files:**
- `adsbtrack/runways.py` - download, parse, upsert pipeline (mirror of `airports.py`).
- `tests/test_runways.py` - unit tests + CLI integration test driven by an in-repo fixture CSV.
- `tests/fixtures/runways_sample.csv` - hand-crafted fixture covering KATL (major, 5 runway pairs), KSPG (small GA, 1 pair), an airport with a single runway where only one endpoint has coordinates, and two heliports (one with `H1`, one that's a named heliport).

**Modified files:**
- `adsbtrack/db.py` - add `CREATE TABLE IF NOT EXISTS runways`, the supporting indexes, and two new methods: `insert_runway_ends(...)` and `clear_runways_for_airport(ident)`.
- `adsbtrack/cli.py` - add `@cli.group() runways` and `@runways.command("refresh")`.
- `docs/schema.md` - document the `runways` table.

---

## Task 1: Add `runways` table to schema

**Goal:** Persist per-runway-end rows. One row per end, keyed by `(airport_ident, runway_name)`. Additive only - nothing else reads the table yet.

**Files:**
- Modify: `adsbtrack/db.py` (SCHEMA string and indexes section)
- Test: `tests/test_db.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_db.py` (after the other schema-presence tests):

```python
def test_runways_table_created(db_path):
    """The runways table should exist on a fresh DB with the expected columns."""
    database = Database(db_path)
    cols = {row[1] for row in database.conn.execute("PRAGMA table_info(runways)").fetchall()}
    expected = {
        "airport_ident",
        "runway_name",
        "latitude_deg",
        "longitude_deg",
        "elevation_ft",
        "heading_deg_true",
        "length_ft",
        "width_ft",
        "surface",
        "closed",
        "displaced_threshold_ft",
    }
    assert expected.issubset(cols), f"missing columns: {expected - cols}"
    # Primary key enforces one row per (ident, end).
    pk_rows = database.conn.execute("PRAGMA index_list(runways)").fetchall()
    assert any(r["unique"] for r in pk_rows), "expected a unique index / PK on runways"
    database.close()


def test_runways_index_on_airport_ident(db_path):
    """Lookups by airport_ident must be indexed."""
    database = Database(db_path)
    indexes = database.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='runways'"
    ).fetchall()
    names = {r["name"] for r in indexes}
    assert "idx_runways_airport_ident" in names
    database.close()
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `uv run pytest tests/test_db.py::test_runways_table_created tests/test_db.py::test_runways_index_on_airport_ident -v`
Expected: FAIL with "no such table: runways" or equivalent.

- [ ] **Step 3: Add the table and indexes to the SCHEMA string**

In `adsbtrack/db.py`, inside the `SCHEMA = """..."""` multi-line string, add after the `CREATE TABLE IF NOT EXISTS helipads (...)` block and before the faa tables:

```sql
CREATE TABLE IF NOT EXISTS runways (
    airport_ident TEXT NOT NULL,
    runway_name TEXT NOT NULL,
    latitude_deg REAL NOT NULL,
    longitude_deg REAL NOT NULL,
    elevation_ft INTEGER,
    heading_deg_true REAL,
    length_ft INTEGER,
    width_ft INTEGER,
    surface TEXT,
    closed INTEGER DEFAULT 0,
    displaced_threshold_ft INTEGER,
    PRIMARY KEY (airport_ident, runway_name)
);
```

Then add these indexes to the `CREATE INDEX` block lower in SCHEMA (right after `idx_airports_lon`):

```sql
CREATE INDEX IF NOT EXISTS idx_runways_airport_ident ON runways(airport_ident);
CREATE INDEX IF NOT EXISTS idx_runways_latlon ON runways(latitude_deg, longitude_deg);
```

- [ ] **Step 4: Run the tests and verify they pass**

Run: `uv run pytest tests/test_db.py::test_runways_table_created tests/test_db.py::test_runways_index_on_airport_ident -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add adsbtrack/db.py tests/test_db.py
git commit -m "Add runways table and indexes (schema only)"
```

---

## Task 2: Add `Database.insert_runway_ends` and `clear_runways_for_airport` helpers

**Goal:** Boundary-layer methods so the runway loader doesn't embed SQL. `insert_runway_ends` uses `INSERT OR REPLACE` for idempotency. `clear_runways_for_airport` is used by the loader to avoid stale endpoints when an airport's runway set shrinks between downloads.

**Files:**
- Modify: `adsbtrack/db.py` (append a `# -- runways --` section after the airports block, around line 935)
- Test: `tests/test_db.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_db.py`:

```python
def test_insert_runway_ends_roundtrip(db):
    """Insert two ends for one airport, assert we can read both back."""
    rows = [
        # (airport_ident, runway_name, lat, lon, elev, heading, length_ft,
        #  width_ft, surface, closed, displaced_threshold_ft)
        ("KSPG", "18", 27.7656, -82.6271, 7, 180.0, 2864, 75, "ASPH", 0, 0),
        ("KSPG", "36", 27.7735, -82.6271, 7, 360.0, 2864, 75, "ASPH", 0, 0),
    ]
    db.insert_runway_ends(rows)
    fetched = db.conn.execute(
        "SELECT runway_name FROM runways WHERE airport_ident = ? ORDER BY runway_name",
        ("KSPG",),
    ).fetchall()
    assert [r["runway_name"] for r in fetched] == ["18", "36"]


def test_insert_runway_ends_is_idempotent(db):
    """Re-inserting the same (airport_ident, runway_name) should overwrite, not duplicate."""
    row = ("KSPG", "18", 27.7656, -82.6271, 7, 180.0, 2864, 75, "ASPH", 0, 0)
    db.insert_runway_ends([row])
    # Re-run with a changed surface - row count stays at 1, surface updates.
    updated = ("KSPG", "18", 27.7656, -82.6271, 7, 180.0, 2864, 75, "CONC", 0, 0)
    db.insert_runway_ends([updated])
    rows = db.conn.execute("SELECT surface FROM runways WHERE airport_ident = ?", ("KSPG",)).fetchall()
    assert len(rows) == 1
    assert rows[0]["surface"] == "CONC"


def test_clear_runways_for_airport(db):
    """clear_runways_for_airport removes only that airport's rows."""
    db.insert_runway_ends(
        [
            ("KSPG", "18", 27.77, -82.63, 7, 180.0, 2864, 75, "ASPH", 0, 0),
            ("KSPG", "36", 27.77, -82.63, 7, 360.0, 2864, 75, "ASPH", 0, 0),
            ("KATL", "09L", 33.63, -84.44, 1026, 94.0, 9000, 150, "CONC", 0, 0),
        ]
    )
    db.clear_runways_for_airport("KSPG")
    remaining = db.conn.execute("SELECT airport_ident FROM runways").fetchall()
    assert {r["airport_ident"] for r in remaining} == {"KATL"}
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `uv run pytest tests/test_db.py -k "runway_ends or clear_runways" -v`
Expected: FAIL with `AttributeError: 'Database' object has no attribute 'insert_runway_ends'`.

- [ ] **Step 3: Implement the two methods**

In `adsbtrack/db.py`, add immediately after `find_nearby_airports` (around line 951):

```python
    # -- runways --

    def insert_runway_ends(self, rows: list[tuple]) -> None:
        """Bulk upsert runway ends. Each tuple must match the column order of
        the runways table (see SCHEMA). Uses INSERT OR REPLACE keyed on
        (airport_ident, runway_name) so repeated refreshes are idempotent."""
        self.conn.executemany(
            """INSERT OR REPLACE INTO runways
               (airport_ident, runway_name, latitude_deg, longitude_deg,
                elevation_ft, heading_deg_true, length_ft, width_ft,
                surface, closed, displaced_threshold_ft)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        self.conn.commit()

    def clear_runways_for_airport(self, airport_ident: str) -> None:
        """Delete all runway rows for one airport. Used by the refresh pipeline
        to drop ends that disappeared from the upstream CSV."""
        self.conn.execute("DELETE FROM runways WHERE airport_ident = ?", (airport_ident,))
        self.conn.commit()

    def runway_count(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) AS cnt FROM runways").fetchone()
        return int(row["cnt"])
```

- [ ] **Step 4: Run the tests and verify they pass**

Run: `uv run pytest tests/test_db.py -k "runway_ends or clear_runways" -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add adsbtrack/db.py tests/test_db.py
git commit -m "Add Database helpers for runway end upserts"
```

---

## Task 3: Build the runways fixture CSV

**Goal:** A small, deterministic CSV we can drive the parser + refresh flow against. Mirrors the real file shape exactly (same header, same quoting style).

**Files:**
- Create: `tests/fixtures/runways_sample.csv`

- [ ] **Step 1: Create the fixture**

Write the file below exactly as shown. Note the blank fields inside pairs of commas and the header row must match the live CSV column order.

File `tests/fixtures/runways_sample.csv`:

```csv
"id","airport_ref","airport_ident","length_ft","width_ft","surface","lighted","closed","le_ident","le_latitude_deg","le_longitude_deg","le_elevation_ft","le_heading_degT","le_displaced_threshold_ft","he_ident","he_latitude_deg","he_longitude_deg","he_elevation_ft","he_heading_degT","he_displaced_threshold_ft"
230010,3682,"KATL",9000,150,"CON",1,0,"08L",33.6361,-84.4575,1003,90.29,1000,"26R",33.6361,-84.4288,1026,270.29,0
230011,3682,"KATL",9000,150,"CON",1,0,"09R",33.6356,-84.4575,1003,90.29,0,"27L",33.6356,-84.4288,1026,270.29,0
230012,3682,"KATL",10000,150,"CON",1,0,"08R",33.6292,-84.4617,1009,90.29,0,"26L",33.6292,-84.4289,1022,270.29,0
230013,3682,"KATL",12390,150,"CON",1,0,"09L",33.6284,-84.4617,1009,90.29,0,"27R",33.6284,-84.4220,1022,270.29,0
230014,3682,"KATL",9000,150,"CON",1,0,"10",33.6225,-84.4575,1005,90.29,0,"28",33.6225,-84.4288,1030,270.29,0
255099,6789,"KSPG",2864,75,"ASPH",1,0,"18",27.77327,-82.69509,7,180.0,0,"36",27.76539,-82.69509,7,360.0,0
255100,6790,"SINGLE",3500,40,"GRVL",0,0,"09",40.0000,-100.0000,1500,90.0,0,"27",,,,,
255101,6791,"BOTHBAD",2000,30,"GRS",0,0,"N",,,,,,"S",,,,,
900001,9001,"00A",80,80,"ASPH-G",1,0,"H1",,,,,,,,,,,
900002,9002,"HELIPORT2",,,"CONC",0,0,"H1",,,,,,,,,,,
```

Rows summary:
- 5 KATL runway pairs -> 10 valid runway ends
- 1 KSPG runway pair -> 2 valid runway ends
- SINGLE: one end has coordinates, one is blank -> 1 valid end (`"09"`)
- BOTHBAD: no coordinates on either end -> 0 valid ends
- 00A: heliport-shape row (`le_ident="H1"`, everything blank) -> 0 rows
- HELIPORT2: another heliport shape -> 0 rows

Expected total: 13 rows inserted.

- [ ] **Step 2: Verify the fixture parses as CSV**

Run: `uv run python -c "import csv; list(csv.DictReader(open('tests/fixtures/runways_sample.csv')))"`
Expected: no traceback.

- [ ] **Step 3: Commit**

```bash
git add tests/fixtures/runways_sample.csv
git commit -m "Add runway CSV fixture covering KATL, KSPG, heliports, and partial rows"
```

---

## Task 4: Write the `parse_runway_row` pure function

**Goal:** Turn one CSV dict row into 0, 1, or 2 runway-end tuples. No I/O, no side effects. This is where the "skip endpoints with missing lat/lon" rule lives.

**Files:**
- Create: `adsbtrack/runways.py`
- Test: `tests/test_runways.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_runways.py`:

```python
"""Tests for adsbtrack.runways -- OurAirports runway ingestion."""

from pathlib import Path

import pytest

from adsbtrack.runways import parse_runway_row

FIXTURE = Path(__file__).parent / "fixtures" / "runways_sample.csv"


def _row(**overrides) -> dict[str, str]:
    """Build a DictReader-shaped row with sensible defaults.

    Defaults model a well-formed small GA runway pair so individual tests
    only override the fields they care about."""
    row = {
        "id": "1",
        "airport_ref": "1",
        "airport_ident": "KSPG",
        "length_ft": "2864",
        "width_ft": "75",
        "surface": "ASPH",
        "lighted": "1",
        "closed": "0",
        "le_ident": "18",
        "le_latitude_deg": "27.77327",
        "le_longitude_deg": "-82.69509",
        "le_elevation_ft": "7",
        "le_heading_degT": "180.0",
        "le_displaced_threshold_ft": "0",
        "he_ident": "36",
        "he_latitude_deg": "27.76539",
        "he_longitude_deg": "-82.69509",
        "he_elevation_ft": "7",
        "he_heading_degT": "360.0",
        "he_displaced_threshold_ft": "0",
    }
    row.update(overrides)
    return row


def test_parse_runway_row_emits_both_ends():
    """A fully-populated row yields two tuples, one per end."""
    ends = parse_runway_row(_row())
    assert len(ends) == 2
    low, high = ends
    # (airport_ident, runway_name, lat, lon, elev, heading, length_ft,
    #  width_ft, surface, closed, displaced_threshold_ft)
    assert low[0] == "KSPG"
    assert low[1] == "18"
    assert low[2] == pytest.approx(27.77327)
    assert low[3] == pytest.approx(-82.69509)
    assert low[5] == pytest.approx(180.0)
    assert low[6] == 2864
    assert low[8] == "ASPH"
    assert low[9] == 0
    assert high[1] == "36"


def test_parse_runway_row_skips_end_with_missing_latlon():
    """If he_latitude_deg is blank, only the le end is emitted."""
    row = _row(he_latitude_deg="", he_longitude_deg="")
    ends = parse_runway_row(row)
    assert len(ends) == 1
    assert ends[0][1] == "18"


def test_parse_runway_row_skips_both_ends_when_both_blank():
    row = _row(
        le_latitude_deg="",
        le_longitude_deg="",
        he_latitude_deg="",
        he_longitude_deg="",
    )
    assert parse_runway_row(row) == []


def test_parse_runway_row_skips_heliport_H1_pattern():
    """OurAirports represents heliports with le_ident='H1' and blank everything."""
    row = _row(
        le_ident="H1",
        le_latitude_deg="",
        le_longitude_deg="",
        le_elevation_ft="",
        le_heading_degT="",
        le_displaced_threshold_ft="",
        he_ident="",
        he_latitude_deg="",
        he_longitude_deg="",
        he_elevation_ft="",
        he_heading_degT="",
        he_displaced_threshold_ft="",
    )
    assert parse_runway_row(row) == []


def test_parse_runway_row_skips_endpoint_with_blank_ident():
    """An end with a blank ident can't be uniquely keyed - skip it."""
    row = _row(he_ident="")
    ends = parse_runway_row(row)
    assert len(ends) == 1
    assert ends[0][1] == "18"


def test_parse_runway_row_handles_blank_numeric_fields():
    """length_ft / width_ft / displaced_threshold_ft blank -> NULL."""
    row = _row(length_ft="", width_ft="", le_displaced_threshold_ft="")
    ends = parse_runway_row(row)
    low = ends[0]
    assert low[6] is None  # length_ft
    assert low[7] is None  # width_ft
    assert low[10] is None  # displaced_threshold_ft


def test_parse_runway_row_preserves_airport_ident_casing():
    """FAA local codes like '67FL' must survive unchanged."""
    row = _row(airport_ident="67FL")
    ends = parse_runway_row(row)
    assert ends[0][0] == "67FL"


def test_parse_runway_row_closed_flag():
    row = _row(closed="1")
    ends = parse_runway_row(row)
    assert ends[0][9] == 1
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `uv run pytest tests/test_runways.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'adsbtrack.runways'`.

- [ ] **Step 3: Create `adsbtrack/runways.py` with the parser**

```python
"""OurAirports runway ingestion.

Downloads `runways.csv` from OurAirports, parses rows into one tuple per
runway end (so "09" and "27" become two rows), and upserts into the local
`runways` table. Idempotent on re-run - repeated refreshes of the same
airport overwrite existing rows without duplicating.

OurAirports uses two representations we filter out at parse time:
  * Heliport-shape rows where `le_ident="H1"` and every coordinate is blank.
  * Rows where an endpoint's `*_latitude_deg` / `*_longitude_deg` is blank -
    we emit only the endpoints that have coordinates (or zero rows if both
    are missing).

`airport_ident` is preserved exactly as given (can be ICAO like "KATL" or
a FAA local code like "67FL"); our airport-matching code already tolerates
both.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path

import httpx
from rich.progress import Progress

from .config import Config
from .db import Database

# Pinned in the plan on 2026-04-16; lives in Config so tests can override.
OURAIRPORTS_RUNWAYS_URL = "https://davidmegginson.github.io/ourairports-data/runways.csv"

# Order must match db.Database.insert_runway_ends INSERT column order.
RunwayEnd = tuple[
    str,          # airport_ident
    str,          # runway_name
    float,        # latitude_deg
    float,        # longitude_deg
    int | None,   # elevation_ft
    float | None, # heading_deg_true
    int | None,   # length_ft
    int | None,   # width_ft
    str | None,   # surface
    int,          # closed
    int | None,   # displaced_threshold_ft
]


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    try:
        return int(float(stripped))
    except ValueError:
        return None


def _parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    try:
        return float(stripped)
    except ValueError:
        return None


def _parse_str(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _build_end(
    *,
    airport_ident: str,
    runway_name: str,
    lat: float,
    lon: float,
    elev: int | None,
    heading: float | None,
    length_ft: int | None,
    width_ft: int | None,
    surface: str | None,
    closed: int,
    displaced_threshold: int | None,
) -> RunwayEnd:
    return (
        airport_ident,
        runway_name,
        lat,
        lon,
        elev,
        heading,
        length_ft,
        width_ft,
        surface,
        closed,
        displaced_threshold,
    )


def parse_runway_row(row: dict[str, str]) -> list[RunwayEnd]:
    """Turn one OurAirports runways.csv DictReader row into 0-2 tuples.

    Returns an empty list when:
      * airport_ident is blank (malformed row)
      * both endpoints are missing lat/lon or ident
    """
    airport_ident = _parse_str(row.get("airport_ident"))
    if airport_ident is None:
        return []

    length_ft = _parse_int(row.get("length_ft"))
    width_ft = _parse_int(row.get("width_ft"))
    surface = _parse_str(row.get("surface"))
    closed_raw = _parse_int(row.get("closed"))
    closed = 1 if closed_raw == 1 else 0

    ends: list[RunwayEnd] = []
    for prefix in ("le_", "he_"):
        runway_name = _parse_str(row.get(f"{prefix}ident"))
        lat = _parse_float(row.get(f"{prefix}latitude_deg"))
        lon = _parse_float(row.get(f"{prefix}longitude_deg"))
        # Skip endpoints we can't uniquely key or locate. The spec's
        # "airport centroid fallback" is intentionally deferred - we skip
        # when uncertain rather than guess a direction.
        if runway_name is None or lat is None or lon is None:
            continue
        ends.append(
            _build_end(
                airport_ident=airport_ident,
                runway_name=runway_name,
                lat=lat,
                lon=lon,
                elev=_parse_int(row.get(f"{prefix}elevation_ft")),
                heading=_parse_float(row.get(f"{prefix}heading_degT")),
                length_ft=length_ft,
                width_ft=width_ft,
                surface=surface,
                closed=closed,
                displaced_threshold=_parse_int(row.get(f"{prefix}displaced_threshold_ft")),
            )
        )
    return ends
```

- [ ] **Step 4: Run the tests and verify they pass**

Run: `uv run pytest tests/test_runways.py -v`
Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
git add adsbtrack/runways.py tests/test_runways.py
git commit -m "Add parse_runway_row for OurAirports runway rows"
```

---

## Task 5: Add `import_runways_from_path` - parse a local CSV into the DB

**Goal:** Read a file off disk, run `parse_runway_row` on every row, bulk-upsert. Factored out from `refresh_runways` so tests don't have to mock HTTP.

**Files:**
- Modify: `adsbtrack/runways.py`
- Test: `tests/test_runways.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_runways.py`:

```python
from adsbtrack.config import Config
from adsbtrack.db import Database
from adsbtrack.runways import import_runways_from_path


def test_import_runways_from_path_counts_ends(tmp_path):
    """The fixture should produce 13 valid runway ends total:
      * KATL:     5 runway pairs * 2 ends    = 10
      * KSPG:     1 runway pair  * 2 ends    =  2
      * SINGLE:   1 valid end (other blank)  =  1
      * BOTHBAD:  0 (no coordinates)         =  0
      * 00A, HELIPORT2: 0 each (heliports)   =  0
    """
    db_path = tmp_path / "t.db"
    with Database(db_path) as db:
        inserted = import_runways_from_path(db, FIXTURE)
        assert inserted == 13
        assert db.runway_count() == 13


def test_import_runways_from_path_katl_has_ten_ends(tmp_path):
    db_path = tmp_path / "t.db"
    with Database(db_path) as db:
        import_runways_from_path(db, FIXTURE)
        rows = db.conn.execute(
            "SELECT runway_name FROM runways WHERE airport_ident = ? ORDER BY runway_name",
            ("KATL",),
        ).fetchall()
        names = [r["runway_name"] for r in rows]
        assert names == sorted(["08L", "26R", "09R", "27L", "08R", "26L", "09L", "27R", "10", "28"])


def test_import_runways_from_path_kspg_has_two_ends(tmp_path):
    db_path = tmp_path / "t.db"
    with Database(db_path) as db:
        import_runways_from_path(db, FIXTURE)
        rows = db.conn.execute(
            "SELECT runway_name, heading_deg_true FROM runways WHERE airport_ident = ? ORDER BY runway_name",
            ("KSPG",),
        ).fetchall()
        assert [r["runway_name"] for r in rows] == ["18", "36"]
        assert rows[0]["heading_deg_true"] == pytest.approx(180.0)


def test_import_runways_from_path_skips_heliport(tmp_path):
    db_path = tmp_path / "t.db"
    with Database(db_path) as db:
        import_runways_from_path(db, FIXTURE)
        count_00a = db.conn.execute(
            "SELECT COUNT(*) AS cnt FROM runways WHERE airport_ident = ?",
            ("00A",),
        ).fetchone()["cnt"]
        count_h2 = db.conn.execute(
            "SELECT COUNT(*) AS cnt FROM runways WHERE airport_ident = ?",
            ("HELIPORT2",),
        ).fetchone()["cnt"]
        assert count_00a == 0
        assert count_h2 == 0


def test_import_runways_from_path_single_end(tmp_path):
    """SINGLE has one well-formed end and one blank end -> 1 row."""
    db_path = tmp_path / "t.db"
    with Database(db_path) as db:
        import_runways_from_path(db, FIXTURE)
        rows = db.conn.execute(
            "SELECT runway_name FROM runways WHERE airport_ident = ?",
            ("SINGLE",),
        ).fetchall()
        assert [r["runway_name"] for r in rows] == ["09"]


def test_import_runways_from_path_is_idempotent(tmp_path):
    """Running twice should leave row count unchanged."""
    db_path = tmp_path / "t.db"
    with Database(db_path) as db:
        import_runways_from_path(db, FIXTURE)
        first = db.runway_count()
        import_runways_from_path(db, FIXTURE)
        assert db.runway_count() == first
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `uv run pytest tests/test_runways.py -k import_runways_from_path -v`
Expected: FAIL with `ImportError: cannot import name 'import_runways_from_path'`.

- [ ] **Step 3: Implement `import_runways_from_path`**

Append to `adsbtrack/runways.py`:

```python
def import_runways_from_path(db: Database, path: Path) -> int:
    """Parse a local runways.csv at `path`, upsert every valid end, return
    the count of ends inserted.

    Idempotent: repeated calls upsert via (airport_ident, runway_name) so
    the total row count is bounded by the unique-key space, not the call
    count.
    """
    ends: list[RunwayEnd] = []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            ends.extend(parse_runway_row(row))
    db.insert_runway_ends(ends)
    return len(ends)
```

- [ ] **Step 4: Run the tests and verify they pass**

Run: `uv run pytest tests/test_runways.py -v`
Expected: PASS (14 tests - 8 parser + 6 importer).

- [ ] **Step 5: Commit**

```bash
git add adsbtrack/runways.py tests/test_runways.py
git commit -m "Add import_runways_from_path for local CSV ingestion"
```

---

## Task 6: Add `refresh_runways` - download + parse + upsert

**Goal:** The orchestrator the CLI will call. Downloads the upstream CSV (with a reasonable timeout and HTTP error surfacing), writes it to a temp file, then delegates to `import_runways_from_path`. Accepts an optional `local_csv` override so the tests and future operators can skip the network.

**Files:**
- Modify: `adsbtrack/runways.py`
- Modify: `adsbtrack/config.py` (add `runways_csv_url`)
- Test: `tests/test_runways.py`

- [ ] **Step 1: Add the Config field**

In `adsbtrack/config.py`, add after the existing `airports_csv_url` line (currently line 228):

```python
    runways_csv_url: str = "https://davidmegginson.github.io/ourairports-data/runways.csv"
```

- [ ] **Step 2: Write the failing test**

Append to `tests/test_runways.py`:

```python
from adsbtrack.runways import refresh_runways


def test_refresh_runways_accepts_local_csv(tmp_path):
    """refresh_runways should accept a local path and skip the HTTP fetch."""
    cfg = Config(db_path=tmp_path / "t.db")
    with Database(cfg.db_path) as db:
        inserted = refresh_runways(db, cfg, local_csv=FIXTURE)
        assert inserted == 13
        assert db.runway_count() == 13


def test_refresh_runways_is_idempotent(tmp_path):
    cfg = Config(db_path=tmp_path / "t.db")
    with Database(cfg.db_path) as db:
        first = refresh_runways(db, cfg, local_csv=FIXTURE)
        second = refresh_runways(db, cfg, local_csv=FIXTURE)
        assert first == second == 13
        assert db.runway_count() == 13


def test_config_has_runways_csv_url_default():
    cfg = Config()
    assert cfg.runways_csv_url == "https://davidmegginson.github.io/ourairports-data/runways.csv"
```

- [ ] **Step 3: Run the tests and verify they fail**

Run: `uv run pytest tests/test_runways.py -k refresh_runways -v`
Expected: FAIL with `ImportError: cannot import name 'refresh_runways'`.

- [ ] **Step 4: Implement `refresh_runways`**

Append to `adsbtrack/runways.py`:

```python
def refresh_runways(
    db: Database,
    cfg: Config,
    *,
    local_csv: Path | None = None,
    timeout: float = 120.0,
) -> int:
    """Refresh the runways table from the upstream OurAirports CSV.

    When `local_csv` is provided, the HTTP fetch is skipped and the file is
    parsed directly. Otherwise, the CSV is streamed into memory via httpx
    (file is ~5MB), written nowhere - we parse the text straight from the
    response body.

    Raises httpx.HTTPError variants unchanged; the CLI wrapper surfaces
    them as click.ClickException. Returns the number of runway ends
    inserted.
    """
    if local_csv is not None:
        return import_runways_from_path(db, local_csv)

    with Progress() as progress:
        task = progress.add_task("Downloading OurAirports runways.csv...", total=None)
        resp = httpx.get(cfg.runways_csv_url, follow_redirects=True, timeout=timeout)
        resp.raise_for_status()
        progress.update(task, completed=50)

        reader = csv.DictReader(io.StringIO(resp.text))
        ends: list[RunwayEnd] = []
        for row in reader:
            ends.extend(parse_runway_row(row))
        db.insert_runway_ends(ends)
        progress.update(task, completed=100)
    return len(ends)
```

- [ ] **Step 5: Run the tests and verify they pass**

Run: `uv run pytest tests/test_runways.py -v`
Expected: PASS (17 tests).

- [ ] **Step 6: Commit**

```bash
git add adsbtrack/config.py adsbtrack/runways.py tests/test_runways.py
git commit -m "Add refresh_runways orchestrator with local-csv override"
```

---

## Task 7: Add `adsbtrack runways refresh` CLI command

**Goal:** User-facing entry point. Mirrors the `registry update` pattern: a `@cli.group()` plus a single subcommand. Surfaces HTTP and filesystem errors as `click.ClickException` so the user gets a one-line message, not a traceback.

**Files:**
- Modify: `adsbtrack/cli.py` (add near the `registry` group, around line 640)
- Test: `tests/test_runways.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_runways.py`:

```python
from click.testing import CliRunner


def test_cli_runways_refresh_with_local_csv(tmp_path):
    """`adsbtrack runways refresh --csv FIXTURE` should load and print a summary."""
    from adsbtrack.cli import cli

    db_path = tmp_path / "t.db"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["runways", "refresh", "--csv", str(FIXTURE), "--db", str(db_path)],
    )
    assert result.exit_code == 0, result.output
    assert "13" in result.output  # count surfaces in the summary

    # Verify rows landed in the DB
    with Database(db_path) as db:
        assert db.runway_count() == 13


def test_cli_runways_refresh_surfaces_missing_file(tmp_path):
    from adsbtrack.cli import cli

    db_path = tmp_path / "t.db"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["runways", "refresh", "--csv", str(tmp_path / "does-not-exist.csv"), "--db", str(db_path)],
    )
    assert result.exit_code != 0
    assert "does-not-exist.csv" in result.output or "not exist" in result.output.lower()
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `uv run pytest tests/test_runways.py -k cli_runways -v`
Expected: FAIL with `Error: No such command 'runways'`.

- [ ] **Step 3: Add the group + command to `adsbtrack/cli.py`**

Near the top of `adsbtrack/cli.py`, add to the imports block (next to the `airports` import):

```python
from .runways import refresh_runways
```

Then at the end of the file (after the last existing `@cli.command` - grep for "def mil" or the last command and append below it; a good spot is right after the `registry` group definition, around line 812):

```python
@cli.group()
def runways():
    """OurAirports runway geometry ingestion."""


@runways.command("refresh")
@click.option(
    "--csv",
    "csv_path",
    type=click.Path(exists=True, path_type=Path, dir_okay=False),
    default=None,
    help="Use a local runways.csv instead of downloading from OurAirports.",
)
@click.option("--db", "db_path", default="adsbtrack.db", help="Database path")
def runways_refresh(csv_path, db_path):
    """Download OurAirports runways.csv and upsert runway geometry.

    Idempotent - re-running overwrites existing rows keyed by
    (airport_ident, runway_name).
    """
    import httpx

    cfg = Config(db_path=Path(db_path))
    try:
        with Database(cfg.db_path) as db:
            count = refresh_runways(db, cfg, local_csv=csv_path)
    except httpx.HTTPError as e:
        raise click.ClickException(f"failed to download runways.csv: {e}") from e
    except FileNotFoundError as e:
        raise click.ClickException(str(e)) from e
    except OSError as e:
        raise click.ClickException(f"filesystem error: {e}") from e
    console.print(f"[green]Runway geometry loaded:[/] {count} runway ends")
```

- [ ] **Step 4: Run the tests and verify they pass**

Run: `uv run pytest tests/test_runways.py -v`
Expected: PASS (19 tests total).

- [ ] **Step 5: Commit**

```bash
git add adsbtrack/cli.py tests/test_runways.py
git commit -m "Add runways refresh CLI command"
```

---

## Task 8: Update `docs/schema.md`

**Goal:** Document the new table. Placement: right after `airports`, before `helipads` (airports -> runways -> helipads reads naturally).

**Files:**
- Modify: `docs/schema.md`

- [ ] **Step 1: Insert the `runways` section**

In `docs/schema.md`, find the `## helipads` heading (line 176). Immediately before it, insert:

```markdown
## runways

Per-runway-end geometry from OurAirports `runways.csv`. One row per runway END (so runway 09/27 is two rows, not one). Loaded by `runways refresh`.

Rows with missing endpoint lat/lon are skipped (heliports, some unsurveyed fields). The `airport_ident` is preserved exactly as published by OurAirports - may be an ICAO code ("KSPG") or an FAA local code ("67FL"); airport matching code already tolerates both.

| Column | Type | Description |
|--------|------|-------------|
| airport_ident | TEXT | OurAirports ident (ICAO or FAA local code); primary key with runway_name |
| runway_name | TEXT | Runway end designator (e.g. "09", "27L", "18") |
| latitude_deg / longitude_deg | REAL | Endpoint coordinates (not null; rows without coordinates are skipped at load) |
| elevation_ft | INTEGER | Threshold elevation (MSL) |
| heading_deg_true | REAL | Runway heading in degrees true (from `*_heading_degT`) |
| length_ft | INTEGER | Overall runway length |
| width_ft | INTEGER | Runway width |
| surface | TEXT | Surface code ("ASPH", "CONC", "GRVL", ...) |
| closed | INTEGER | 1 if the runway is marked closed in OurAirports |
| displaced_threshold_ft | INTEGER | Displaced threshold distance at this end |

```

- [ ] **Step 2: Verify the doc renders as markdown**

Run: `uv run python -c "import pathlib; print(pathlib.Path('docs/schema.md').read_text().count('## runways'))"`
Expected output: `1`.

- [ ] **Step 3: Commit**

```bash
git add docs/schema.md
git commit -m "Document runways table in schema.md"
```

---

## Task 9: Final verification

**Goal:** Prove the full stack runs green.

- [ ] **Step 1: Run the full test suite**

Run: `uv run pytest`
Expected: all tests pass (prior 285 tests + the 19 new runway tests = 304).

- [ ] **Step 2: Run ruff + format check**

Run: `uv run ruff check . && uv run ruff format --check .`
Expected: no issues.

- [ ] **Step 3: Run mypy (informational)**

Run: `uv run mypy adsbtrack`
Expected: no new errors introduced in `runways.py` or the edits to `db.py`, `cli.py`, `config.py`. (Pre-existing warnings elsewhere are acceptable.)

- [ ] **Step 4: Smoke-test the CLI against the fixture**

Run:
```bash
uv run python -m adsbtrack.cli runways refresh \
  --csv tests/fixtures/runways_sample.csv \
  --db /tmp/runways-smoke.db
```
Expected output: `Runway geometry loaded: 13 runway ends`.

Then inspect:
```bash
uv run python -c "
import sqlite3
c = sqlite3.connect('/tmp/runways-smoke.db')
c.row_factory = sqlite3.Row
for r in c.execute('SELECT airport_ident, runway_name, heading_deg_true FROM runways ORDER BY airport_ident, runway_name'):
    print(dict(r))
"
```
Expected: 13 rows with KATL (10), KSPG (2), SINGLE (1) and no rows for 00A/HELIPORT2/BOTHBAD.

- [ ] **Step 5: Clean up the smoke-test DB**

Run: `rm -f /tmp/runways-smoke.db`

---

## Self-Review Checklist

The plan has been reviewed for:

**Spec coverage:**
- [x] Req 1 "Plan before coding" - this document.
- [x] Req 2 "Add runways table, one row per end" - Task 1 schema, PRIMARY KEY (airport_ident, runway_name).
- [x] Req 2 columns (airport_ident, runway_name, latitude, longitude, elevation_ft, heading_deg_true, length_ft, width_ft, surface, closed, displaced_threshold_ft) - all present in Task 1 SQL.
- [x] Req 2 "airport centroid fallback only if we can confidently pick a direction, else skip" - plan defers the centroid fallback; parser skips endpoints with missing lat/lon. Documented in parse_runway_row docstring.
- [x] Req 3 "adsbtrack runways refresh CLI" - Task 7.
- [x] Req 3 "idempotent on re-run" - Task 2 INSERT OR REPLACE, Task 5 idempotency test, Task 6 idempotency test.
- [x] Req 4 "preserve airport_ident exactly, don't normalize" - Task 4 test `test_parse_runway_row_preserves_airport_ident_casing`.
- [x] Req 5 "don't block existing functionality; additive only" - no callers, no writes from existing pipelines.
- [x] Req 6 "tests with fixture covering major ICAO w/ parallel runways (KATL), small GA (KSPG), heliport (0 rows), single-runway airport" - Task 3 fixture + Task 5 tests.
- [x] Req 7 "update docs/schema.md" - Task 8.
- [x] Req "no landing detection code" - this plan stops at data plumbing.

**Placeholder scan:** No `TBD`, `TODO`, `implement later`, or "similar to Task N" references. Every step shows the actual code.

**Type consistency:**
- `RunwayEnd` tuple order defined in Task 4 matches the INSERT column order in Task 2 (`airport_ident, runway_name, latitude_deg, longitude_deg, elevation_ft, heading_deg_true, length_ft, width_ft, surface, closed, displaced_threshold_ft`).
- Test assertions on tuple indices (Task 4) match that order.
- Task 7 imports `refresh_runways` from `.runways`; Task 6 defines it there.
- Task 7 CLI option is named `--csv`; Task 7 tests invoke it as `--csv`.
