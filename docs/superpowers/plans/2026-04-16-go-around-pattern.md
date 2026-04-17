# Go-around and Pattern-Work Detection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Detect go-arounds (two ILS-aligned segments at the same airport separated by a climb) and pattern / touch-and-go work (multiple aligned segments at the same origin-destination airport), and surface both as new columns on the `flights` table plus counts in the `status` CLI. The existing mission classifier gets one additive trigger so a flight with `pattern_cycles >= 2` at the same airport becomes `mission_type = "pattern"`.

**Architecture:** The existing `adsbtrack/ils_alignment.py` detector returns only the longest segment. Extend it with a second public function `detect_all_ils_alignments(...)` returning the full list of qualifying segments, ordered chronologically. Parser invokes it once per flight, stores `pattern_cycles` (len) and `had_go_around` (climb-between-pairs), and applies a post-pass override that upgrades `mission_type` to `"pattern"` when same-airport + `pattern_cycles >= 2`. Status CLI adds two counters in existing sections.

**Tech Stack:** Existing. No new dependencies. All work in Python layer.

---

## Scope notes

**Ordering.** The parser currently runs `features.derive_all` (which calls `classify_mission`) BEFORE the ILS alignment block. Rather than re-order the pipeline (risky — `derive_all` sets many fields the alignment block depends on), we add a small post-alignment override in the parser that mutates `flight.mission_type` to `"pattern"` when the multi-segment trigger fires. The existing `classify_mission` rule stays unchanged.

**Go-around threshold.** User spec: "altitude rising > 500 ft between alignments". Concretely, between consecutive aligned segments `segs[i]` and `segs[i+1]`: find the maximum altitude in `metrics.recent_points` samples whose `ts` falls in `(segs[i].last_ts, segs[i+1].first_ts)`, compare to `segs[i]`'s end-altitude (last in-segment sample's altitude). If the gap-max is > 500 ft above the segment-end altitude, the flight had a go-around.

**Airport choice for pattern_cycles.** Use the same priority as the existing ILS alignment block: `destination_icao` first, then `nearest_destination_icao`, then `probable_destination_icao`. This is the "which airport did we test alignment against" choice. For pattern work same-airport flights, origin_icao == destination_icao so the priority resolves to a single airport.

**distinct vs. total segments.** User said "count of distinct aligned segments on the same airport during the flight". Reading "distinct" as "separately-detected segments" (not "unique by runway end"). A flight that touches runway 24 three times counts as 3. A flight that touches 24 then 06 then 24 counts as 3. Repeated touches on the same runway-end are still separate segments.

**Retroactive runs.** `detect_all_ils_alignments` requires data from `FlightMetrics.recent_points`. Running on stored flights (without re-extraction) is not possible; these columns are only populated during extraction. This matches how `aligned_runway` and `takeoff_runway` work. `status` counts use `SELECT ... WHERE had_go_around = 1` which works on any row that went through extraction with this code.

---

## File structure

**Modified files**
- `adsbtrack/ils_alignment.py` — factor the existing `detect_ils_alignment` internals; add a new public `detect_all_ils_alignments(metrics, *, ...) -> list[IlsAlignmentResult]` returning ALL qualifying segments (chronologically sorted across runway ends). Augment `IlsAlignmentResult` with `first_ts: float` and `last_ts: float` and `end_alt_ft: int | None` fields so go-around analysis can consult them without re-walking samples.
- `adsbtrack/models.py` — append `had_go_around: int | None = None` and `pattern_cycles: int | None = None` to `Flight`.
- `adsbtrack/db.py` — CREATE TABLE flights: add `had_go_around INTEGER, pattern_cycles INTEGER,`. Migration entries. `insert_flight` column list / placeholders / value tuple extended. Roundtrip tests.
- `adsbtrack/parser.py` — inside the existing ILS alignment block, after `detect_ils_alignment` returns (for the confidence-bonus use), also call `detect_all_ils_alignments` once. Derive `pattern_cycles = len(segments)` and `had_go_around = _any_climb_between(segments, metrics.recent_points, threshold=500)`. Set both on `flight`. Then apply the post-pass mission-override.
- `adsbtrack/cli.py` — `status` command: add two counter lines alongside the existing "Data quality" or "Mission breakdown" section.
- `tests/test_ils_alignment.py` — new tests for `detect_all_ils_alignments` returning the right list (empty / single / multiple segments across one or two runways).
- `tests/test_parser.py` — 3 integration tests: one-go-around flight, 6-landing training flight, A-to-B flight (both False/0).
- `tests/test_db.py` — roundtrip tests for the two new columns (3 tests mirroring the existing sibling style).
- `tests/test_cli.py` — 1 status-CLI test.
- `docs/features.md` — new "Go-around and pattern work." subsection.
- `docs/schema.md` — two new rows.

**No new files.** Everything extends existing modules.

---

## Task 1: Extend `ils_alignment.py`; add schema + model + migration

**Why combined into one task**: adding fields to `IlsAlignmentResult` AND extending the detector AND the Flight/schema all depend on each other. Splitting would force test code to ignore half the fields.

### Step 1: write failing DB tests

In `tests/test_db.py`, append:

```python
def test_flights_table_has_go_around_and_pattern_cycles(db) -> None:
    cols = {row[1] for row in db.conn.execute("PRAGMA table_info(flights)").fetchall()}
    assert {"had_go_around", "pattern_cycles"}.issubset(cols)


def test_insert_flight_persists_go_around_and_pattern_cycles(db) -> None:
    f = Flight(
        icao="dddddd",
        takeoff_time=datetime(2024, 7, 1, 10, 0),
        takeoff_lat=27.77, takeoff_lon=-82.67,
        takeoff_date="2024-07-01",
        had_go_around=1,
        pattern_cycles=3,
    )
    db.insert_flight(f)
    db.commit()
    row = db.conn.execute(
        "SELECT had_go_around, pattern_cycles FROM flights WHERE icao = ?",
        ("dddddd",),
    ).fetchone()
    assert row["had_go_around"] == 1
    assert row["pattern_cycles"] == 3


def test_insert_flight_go_around_pattern_cycles_default_null(db) -> None:
    f = Flight(
        icao="cccccc",
        takeoff_time=datetime(2024, 7, 1, 10, 0),
        takeoff_lat=27.77, takeoff_lon=-82.67,
        takeoff_date="2024-07-01",
    )
    db.insert_flight(f)
    db.commit()
    row = db.conn.execute(
        "SELECT had_go_around, pattern_cycles FROM flights WHERE icao = ?",
        ("cccccc",),
    ).fetchone()
    assert row["had_go_around"] is None
    assert row["pattern_cycles"] is None
```

### Step 2: write failing unit tests for `detect_all_ils_alignments`

In `tests/test_ils_alignment.py`, append:

```python
def test_detect_all_ils_alignments_returns_multiple_segments() -> None:
    """A flight that aligns with one runway in two separate passes returns
    two segments, chronologically ordered."""
    runway = {"runway_name": "09", "latitude_deg": 33.64, "longitude_deg": -84.43, "heading_deg_true": 90.0}
    # First pass: 30 samples at 3 s intervals (~87 s), then 40 s gap,
    # then second pass of 30 samples at 3 s intervals. Each pass ends on
    # centerline but the gap between them is large enough to split.
    seg_a = _walk_toward(33.64, -84.43, 90.0, start_ts=0.0, n=30, spacing_secs=3.0)
    seg_b = _walk_toward(33.64, -84.43, 90.0, start_ts=200.0, n=30, spacing_secs=3.0)
    metrics = _Metrics(seg_a + seg_b)
    results = detect_all_ils_alignments(
        metrics, airport_elev_ft=1026, runway_ends=[runway],
    )
    assert len(results) == 2
    assert results[0].first_ts < results[1].first_ts


def test_detect_all_ils_alignments_empty_when_nothing_qualifies() -> None:
    runway = {"runway_name": "09", "latitude_deg": 33.64, "longitude_deg": -84.43, "heading_deg_true": 90.0}
    # Overflight north of centerline, offset > 100 m
    samples = _walk_toward(33.64 + 0.045, -84.43, 90.0, start_ts=0.0, n=30, spacing_secs=3.0)
    metrics = _Metrics(samples)
    assert detect_all_ils_alignments(
        metrics, airport_elev_ft=1026, runway_ends=[runway],
    ) == []


def test_detect_all_ils_alignments_single_segment_equals_longest() -> None:
    """When only one segment qualifies, detect_all_ils_alignments returns
    a one-element list and detect_ils_alignment returns that same segment."""
    runway = {"runway_name": "09", "latitude_deg": 33.64, "longitude_deg": -84.43, "heading_deg_true": 90.0}
    samples = _walk_toward(33.64, -84.43, 90.0, start_ts=0.0, n=30, spacing_secs=3.0)
    metrics = _Metrics(samples)
    all_segs = detect_all_ils_alignments(metrics, airport_elev_ft=1026, runway_ends=[runway])
    longest = detect_ils_alignment(metrics, airport_elev_ft=1026, runway_ends=[runway])
    assert len(all_segs) == 1
    assert longest is not None
    assert all_segs[0].runway_name == longest.runway_name
    assert all_segs[0].duration_secs == pytest.approx(longest.duration_secs)


def test_ils_alignment_result_carries_first_last_ts_and_end_alt() -> None:
    """New fields on IlsAlignmentResult: first_ts, last_ts, end_alt_ft."""
    runway = {"runway_name": "09", "latitude_deg": 33.64, "longitude_deg": -84.43, "heading_deg_true": 90.0}
    samples = _walk_toward(33.64, -84.43, 90.0, start_ts=100.0, n=30, spacing_secs=3.0)
    metrics = _Metrics(samples)
    results = detect_all_ils_alignments(metrics, airport_elev_ft=1026, runway_ends=[runway])
    assert len(results) == 1
    r = results[0]
    assert 100.0 <= r.first_ts < r.last_ts
    assert r.end_alt_ft is not None
    assert r.end_alt_ft > 0
```

Ensure `import pytest` at top of tests/test_ils_alignment.py (should already be present via pytest's auto-discovery, but explicitly add if missing).

### Step 3: verify tests fail

`uv run pytest tests/test_db.py -v -k "go_around_and_pattern or go_around_pattern_cycles_default" tests/test_ils_alignment.py -v -k "detect_all_ils or result_carries"`

Expected: 7 FAILs (3 db + 4 alignment). DB: missing columns. Alignment: missing function + missing fields.

### Step 4: add `had_go_around` and `pattern_cycles` to Flight

In `adsbtrack/models.py`, append after `takeoff_runway: str | None = None`:

```python

    # --- Go-around and pattern-work detection ---
    # had_go_around: 1 when two or more IlsAlignmentResult segments at the
    # candidate landing airport were separated by a climb of more than 500 ft
    # (aircraft went around on an approach and re-attempted). 0 when one or
    # zero segments, or when segments were not separated by a climb. NULL on
    # legacy rows extracted before this milestone.
    had_go_around: int | None = None
    # pattern_cycles: total count of qualifying ILS-alignment segments at the
    # candidate landing airport during the flight. 1 is a typical single-
    # approach landing. 2+ indicates go-around, touch-and-go, or pattern
    # practice. NULL on legacy rows.
    pattern_cycles: int | None = None
```

### Step 5: add columns to DB schema + migration + insert_flight

In `adsbtrack/db.py`:

1. CREATE TABLE flights: add `had_go_around INTEGER, pattern_cycles INTEGER,` immediately after `takeoff_runway TEXT,`.

2. `_migrate_add_flight_columns` `new_columns` list: append
   ```python
           # Go-around + pattern-work counts (see docs/features.md)
           ("had_go_around", "INTEGER"),
           ("pattern_cycles", "INTEGER"),
   ```
   after `("takeoff_runway", "TEXT"),`.

3. `insert_flight`:
   - Extend SQL column list tail from `... takeoff_runway)` to `... takeoff_runway, had_go_around, pattern_cycles)`.
   - Extend the last VALUES placeholder group from `?, ?, ?, ?, ?, ?, ?, ?, ?)` (9) to `?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)` (11).
   - Extend the Python value tuple — after `flight.takeoff_runway,` append:
     ```python
                     flight.had_go_around,
                     flight.pattern_cycles,
     ```

After these edits, confirm the count invariant is `98 / 98 / 98` (was 96 / 96 / 96 before this task adds 2).

### Step 6: extend `IlsAlignmentResult` and add `detect_all_ils_alignments`

In `adsbtrack/ils_alignment.py`, update the dataclass:

```python
@dataclass(frozen=True)
class IlsAlignmentResult:
    runway_name: str
    duration_secs: float
    min_offset_m: float
    first_ts: float
    last_ts: float
    end_alt_ft: int | None
```

Refactor `_alignment_for_runway` to emit a full list of segments per runway end (not just the longest). Currently it returns a single `IlsAlignmentResult | None`; change its return type to `list[IlsAlignmentResult]`. Every qualifying segment (duration >= `min_duration_secs`) becomes an element. Walkers then aggregate across runways.

Add a module-private `_alignments_for_samples(samples, runway_ends, *, ...) -> list[IlsAlignmentResult]` that walks every runway end and returns the flat list of qualifying segments (empty when nothing qualifies).

Redefine `detect_ils_alignment` as:

```python
def detect_ils_alignment(...) -> IlsAlignmentResult | None:
    segments = _alignments_for_samples(...)
    if not segments:
        return None
    # Longest wins; tie-break on earliest first_ts for determinism.
    return max(segments, key=lambda s: (s.duration_secs, -s.first_ts))
```

Add a new public function:

```python
def detect_all_ils_alignments(
    metrics: FlightMetrics,
    *,
    airport_elev_ft: float,
    runway_ends: Iterable[Mapping[str, object]],
    max_offset_m: float = 100.0,
    max_ft_above_airport: float = 5000.0,
    split_gap_secs: float = 20.0,
    min_duration_secs: float = 30.0,
) -> list[IlsAlignmentResult]:
    """Return every qualifying ILS-aligned segment across runway ends,
    chronologically ordered by first_ts. Empty list when none qualified.
    Same kwargs and thresholds as detect_ils_alignment."""
    samples = list(metrics.recent_points)
    if not samples:
        return []
    segments = _alignments_for_samples(
        samples, list(runway_ends),
        airport_elev_ft=airport_elev_ft, max_offset_m=max_offset_m,
        max_ft_above_airport=max_ft_above_airport, split_gap_secs=split_gap_secs,
        min_duration_secs=min_duration_secs,
    )
    segments.sort(key=lambda s: s.first_ts)
    return segments
```

When building each `IlsAlignmentResult`, capture:
- `first_ts` = the earliest sample's `ts` in the segment
- `last_ts` = the latest sample's `ts` in the segment
- `end_alt_ft` = the altitude of the last sample in the segment (baro preferred, else geom). Use the existing `_sample_alt` helper.

### Step 7: run tests, verify pass

`uv run pytest tests/test_db.py tests/test_ils_alignment.py -v`

Expected: all pass. If `test_detect_all_ils_alignments_returns_multiple_segments` picks up one segment instead of two, the gap between the two synthetic passes (200 s vs. segment-end around 87 s) should be >> split_gap_secs (20 s). Verify: `seg_a` ends at ts=87.0, `seg_b` starts at ts=200.0, gap is 113 s, well above the 20 s split threshold.

### Step 8: run full suite + lint

`uv run pytest -x --deselect tests/test_cli.py::test_enrich_hex_no_data_reports_yellow`
`uv run ruff check . && uv run ruff format --check .`

### Step 9: commit

```bash
git add adsbtrack/models.py adsbtrack/db.py adsbtrack/ils_alignment.py tests/test_db.py tests/test_ils_alignment.py
git commit -m "feat(go-around): schema, IlsAlignmentResult fields, detect_all_ils_alignments"
```

---

## Task 2: Parser integration and mission override

### Step 1: write failing integration tests

In `tests/test_parser.py`, after the existing takeoff-runway tests, append:

```python
def test_parser_go_around_detected(tmp_path: Path) -> None:
    """Flight with two aligned-approach segments on the same runway,
    separated by a climb of > 500 ft, has had_go_around=1 and
    pattern_cycles=2."""
    # TODO: build a synthetic trace that produces two ILS-aligned segments
    # on KSPG runway 24, separated by a climb. Mock the db so
    # find_nearest_airport returns KSPG for both origin and dest; set
    # db.get_runways_for_airport.return_value to a single RWY 24 dict.
    # After extract_flights, assert:
    #   flight.had_go_around == 1
    #   flight.pattern_cycles == 2


def test_parser_training_pattern_detects_six_cycles(tmp_path: Path) -> None:
    """A training flight with 6 pattern laps at KSPG is classified as
    pattern and pattern_cycles == 6."""
    # Synthetic: 6 descending approach segments on RWY 24, each separated
    # by a climb > 500 ft. had_go_around may be 1 (each pair is
    # separated by climb); pattern_cycles == 6; mission_type == "pattern".


def test_parser_normal_a_to_b_has_no_go_around_or_pattern_cycles(tmp_path: Path) -> None:
    """A straight A-to-B flight with a single clean landing has
    had_go_around=0 and pattern_cycles=1 (or 0 if no alignment detected)."""
```

Build the traces using the existing `_walk_approach` helper (from the ILS milestone). For multiple approaches, concatenate:

```python
# Approach 1 to RWY 24 centerline, ending at alt=500 ft MSL
seg_a = _walk_approach(base_ts, n=30, spacing_secs=3.0,
                      runway_lat=27.76, runway_lon=-82.63, runway_heading_deg=240.0,
                      start_alt_ft=3000, alt_step_ft=85)

# Climb between (not on centerline): zig to 3500 ft AGL and back down
climb = [_make_trace_point(base_ts + 30*3 + i*6, lat, lon, alt, 150.0, 240.0, 1500.0, {"track": 240.0})
         for i, (lat, lon, alt) in enumerate(zip(...))]

# Approach 2 same runway, same pattern
seg_b = _walk_approach(base_ts + seg_b_start_offset, n=30, ...)
```

The `_walk_approach` helper generates trace rows already; the climb segment between can be a small hand-built set of ~8 trace points at rising altitude well above the alignment AGL cap, then back down into the alignment zone.

Exact fixture geometry is fragile — if the implementer can't make it produce exactly 2 segments after 3-4 attempts, they should fall back to a unit-level test that calls `detect_all_ils_alignments` directly with a pre-built `_Metrics` object (bypassing the parser mock machinery) and a parser-level test that only asserts `had_go_around` / `pattern_cycles` are set correctly given a plausible multi-segment trace. Report any such fallback.

### Step 2: run tests, expect fail

`uv run pytest tests/test_parser.py -v -k "go_around or training_pattern or normal_a_to_b"`
Expected: FAIL (fields aren't populated yet).

### Step 3: parser edits

In `adsbtrack/parser.py`, find the existing ILS alignment block (search for `alignment_icao = (`, around line 988). This block currently calls `detect_ils_alignment` to get the longest segment. Add calls for the segment list right after, reusing the same inputs:

```python
        # --- Go-around + pattern_cycles (adsbtrack/ils_alignment.py) ---
        # Reuses alignment_icao, airport_elev_ft, runway_ends that the
        # alignment block resolved above.
        all_segments: list[IlsAlignmentResult] = []
        if alignment_icao and runway_rows:
            all_segments = detect_all_ils_alignments(
                metrics,
                airport_elev_ft=airport_elev_ft,
                runway_ends=[dict(r) for r in runway_rows],
                max_offset_m=config.ils_alignment_max_offset_m,
                max_ft_above_airport=config.ils_alignment_max_ft_above_airport,
                split_gap_secs=config.ils_alignment_split_gap_secs,
                min_duration_secs=config.ils_alignment_min_duration_secs,
            )
        flight.pattern_cycles = len(all_segments) if all_segments else 0
        flight.had_go_around = 1 if _any_climb_between(all_segments, metrics.recent_points, threshold_ft=500.0) else 0

        # --- Pattern mission override ---
        # When the mission classifier already put this into a more specific
        # bucket (training, ems_hems, survey, etc.) don't override. Only
        # upgrade "unknown" or the existing "pattern" / "transport" when
        # the multi-segment trigger fires at a same-airport flight.
        if (
            flight.origin_icao is not None
            and flight.destination_icao is not None
            and flight.origin_icao == flight.destination_icao
            and flight.pattern_cycles is not None
            and flight.pattern_cycles >= 2
            and flight.mission_type in ("unknown", "transport", "pattern")
        ):
            flight.mission_type = "pattern"
```

Import at the top of the file (if not already present):

```python
from .ils_alignment import IlsAlignmentResult, detect_all_ils_alignments, detect_ils_alignment
```

`detect_ils_alignment` and `IlsAlignmentResult` were already imported in the alignment milestone; extend the import list to include `detect_all_ils_alignments`.

Add `_any_climb_between` as a module-level helper near `_stitch_fragments` or similar:

```python
def _any_climb_between(
    segments: list[IlsAlignmentResult],
    recent_points: Iterable,
    *,
    threshold_ft: float = 500.0,
) -> bool:
    """Return True when any two consecutive segments in `segments` are
    separated by a rise of more than `threshold_ft` above the earlier
    segment's end altitude. Walks `recent_points` once in O(n*m) which is
    fine for n<=240 and m<=5 segments."""
    if len(segments) < 2:
        return False
    points = list(recent_points)
    for i in range(len(segments) - 1):
        a, b = segments[i], segments[i + 1]
        if a.end_alt_ft is None:
            continue
        gap_max: int | None = None
        for p in points:
            if a.last_ts < p.ts < b.first_ts:
                alt = p.baro_alt if p.baro_alt is not None else p.geom_alt
                if alt is not None and (gap_max is None or alt > gap_max):
                    gap_max = alt
        if gap_max is not None and gap_max - a.end_alt_ft > threshold_ft:
            return True
    return False
```

Note the parser already has `airport_elev_ft` in scope from the alignment block. Reuse it (don't re-query the DB).

### Step 4: verify tests pass; full suite

`uv run pytest tests/test_parser.py -v -k "go_around or training_pattern or normal_a_to_b"`
`uv run pytest -x --deselect tests/test_cli.py::test_enrich_hex_no_data_reports_yellow`
`uv run ruff check . && uv run ruff format --check .`

### Step 5: commit

```bash
git add adsbtrack/parser.py tests/test_parser.py
git commit -m "feat(go-around): detect go-arounds + pattern_cycles, upgrade to pattern"
```

---

## Task 3: `status` CLI counters

### Step 1: write failing test

In `tests/test_cli.py`, append:

```python
def test_status_shows_go_around_and_pattern_counts(tmp_path, monkeypatch) -> None:
    """status output includes go-around count and pattern-work count."""
    monkeypatch.setenv("COLUMNS", "200")
    db_path = tmp_path / "a.db"
    with Database(db_path) as db:
        # Two go-around flights; three pattern-work flights; one normal.
        for i, had_ga, pcycles in [
            (0, 1, 2), (1, 1, 3), (2, 0, 4), (3, 0, 5), (4, 0, 2), (5, 0, 1),
        ]:
            f = Flight(
                icao="abc999",
                takeoff_time=datetime(2024, 6, 1, 10 + i, 0),
                takeoff_lat=27.76, takeoff_lon=-82.63,
                takeoff_date=f"2024-06-{1+i:02d}",
                landing_time=datetime(2024, 6, 1, 11 + i, 0),
                landing_lat=27.76, landing_lon=-82.63,
                landing_date=f"2024-06-{1+i:02d}",
                origin_icao="KSPG", origin_name="Albert Whitted",
                origin_distance_km=0.3,
                destination_icao="KSPG", destination_name="Albert Whitted",
                destination_distance_km=0.3,
                duration_minutes=60.0,
                landing_type="confirmed",
                landing_confidence=0.9,
                had_go_around=had_ga,
                pattern_cycles=pcycles,
            )
            db.insert_flight(f)

    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--hex", "abc999", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "Go-arounds:" in result.output
    assert "2" in result.output  # two had_go_around=1 flights
    assert "Pattern work:" in result.output
    # Flights with pattern_cycles >= 2: first five rows (indices 0-4) → 5
    assert "5 flights" in result.output or "5" in result.output
```

### Step 2: verify fail

`uv run pytest tests/test_cli.py::test_status_shows_go_around_and_pattern_counts -v`
Expected: FAIL.

### Step 3: extend the `status` command

In `adsbtrack/cli.py`, find the existing `status` command (around line 448) and add counter queries + display. A good spot is after the "Data quality" block (around line 515) and before the mission breakdown:

```python
        # Go-around + pattern-work counters
        try:
            counts_row = db.conn.execute(
                """SELECT
                       SUM(CASE WHEN had_go_around = 1 THEN 1 ELSE 0 END) AS go_arounds,
                       SUM(CASE WHEN pattern_cycles >= 2 THEN 1 ELSE 0 END) AS pattern_flights
                   FROM flights WHERE icao = ?""",
                (hex_code,),
            ).fetchone()
        except Exception:
            counts_row = None
        if counts_row and (counts_row["go_arounds"] or counts_row["pattern_flights"]):
            console.print("\n[bold]Approach behaviour:[/]\n")
            console.print(f"  Go-arounds:     {counts_row['go_arounds'] or 0}")
            console.print(f"  Pattern work:   {counts_row['pattern_flights'] or 0} flights")
```

Wrap the query in `try/except` because a pre-migration DB won't have these columns — the status command should degrade gracefully.

### Step 4: verify pass, full suite

`uv run pytest tests/test_cli.py -v -k "status_shows_go_around"`
`uv run pytest -x --deselect tests/test_cli.py::test_enrich_hex_no_data_reports_yellow`

### Step 5: commit

```bash
git add adsbtrack/cli.py tests/test_cli.py
git commit -m "feat(go-around): status CLI shows Go-arounds + Pattern work counts"
```

---

## Task 4: Docs

### Step 1: `docs/features.md`

Insert immediately AFTER the "ILS alignment." subsection:

```markdown
**Go-around and pattern work.** `had_go_around`, `pattern_cycles`. After computing the longest ILS-aligned segment for landing confidence (previous subsection), the extractor also collects ALL qualifying segments at the candidate landing airport via `adsbtrack.ils_alignment.detect_all_ils_alignments`. `pattern_cycles` is the count of qualifying segments for the flight (1 for a normal approach, 2+ for go-around / touch-and-go / pattern work). `had_go_around = 1` when any two consecutive segments are separated by a climb exceeding 500 ft above the earlier segment's end altitude (`adsbtrack.parser._any_climb_between`).

**Additive pattern trigger.** The mission classifier's existing `pattern` rule (same-airport flight with `max_altitude < 3000 ft`) is complemented by a second trigger in the parser: when `origin_icao == destination_icao` and `pattern_cycles >= 2`, the flight is promoted to `mission_type = "pattern"` regardless of its peak altitude. The upgrade only fires when the prior classification was `unknown`, `transport`, or already `pattern`; more specific buckets (`training`, `ems_hems`, `survey`, etc.) are not overridden. This catches pattern practice that climbs above the 3000 ft cutoff or that originally got classified as a transport flight between the same two ICAO codes on paper.
```

Use regular hyphens, not em dashes. (CLAUDE.md.)

### Step 2: `docs/schema.md`

Find the flights table listing. Add near the alignment columns:

```markdown
| had_go_around | INTEGER | 1 when two or more ILS-aligned segments at the landing airport were separated by a climb of more than 500 ft; 0 otherwise. NULL for legacy rows. |
| pattern_cycles | INTEGER | Count of qualifying ILS-aligned segments at the candidate landing airport during the flight. 1 is a normal approach, 2+ signals go-around / touch-and-go / pattern practice. NULL for legacy rows. |
```

Column names unquoted (schema.md convention).

### Step 3: verify

`uv run ruff check . && uv run ruff format --check . && uv run pytest --deselect tests/test_cli.py::test_enrich_hex_no_data_reports_yellow`

All green.

### Step 4: commit

```bash
git add docs/features.md docs/schema.md
git commit -m "docs(go-around): document had_go_around, pattern_cycles, trigger"
```

---

## Self-Review Checklist

**1. Spec coverage:**
- Go-around detection via climb between aligned segments → Task 2.
- `had_go_around` boolean → Task 1 (schema), Task 2 (set).
- Pattern classification trigger → Task 2 (override); existing classify_mission untouched.
- `pattern_cycles` int → Task 1 (schema), Task 2 (set).
- `status` CLI counters → Task 3.
- 3 integration tests (go-around, 6-landing training, A-to-B) → Task 2.
- No new dependencies → confirmed.

**2. Placeholder scan:** none. Every test body and code block is complete except the integration-test synthetic-trace construction, which depends on the implementer's ability to craft a trace that produces 2+ segments through the parser's full machinery. The plan explicitly authorizes falling back to unit-level assertions if the integration fixtures prove too fragile.

**3. Type consistency:**
- `IlsAlignmentResult` grows 3 fields: `first_ts: float`, `last_ts: float`, `end_alt_ft: int | None`. Used consistently in `detect_all_ils_alignments`, `_any_climb_between`, and tests.
- `had_go_around: int | None` (SQLite INTEGER, stored as 0/1, None for legacy). Same convention as `takeoff_is_night`, `landing_is_night`, `night_flight`, etc.
- `pattern_cycles: int | None` (SQLite INTEGER). `0` when no segments detected, `1+` when at least one segment.

**4. Ordering risk:**
- The parser's existing alignment block runs AFTER `derive_all`. The mission override happens in the parser after alignment computation, so it sees the current `flight.mission_type` set by `derive_all` and can upgrade it without re-entering `classify_mission`.
- The override only fires when `mission_type in ("unknown", "transport", "pattern")` so training / offshore / ems_hems classifications are preserved.

**5. Integration-test fragility:**
- Crafting a synthetic trace that the parser's trace-parsing machinery produces exactly 2 alignment segments from is nontrivial. The plan notes this and permits unit-level fallback tests if the integration-test attempts don't converge. The key invariants (had_go_around logic, pattern_cycles count, mission upgrade) are all testable at the unit level via `detect_all_ils_alignments` directly.

---

## Execution handoff

Plan saved to `docs/superpowers/plans/2026-04-16-go-around-pattern.md`. Two execution options:

1. **Subagent-Driven (recommended)** — Fresh subagent per task + two-stage review.
2. **Inline Execution** — Batch execution with checkpoints.

Which approach?
