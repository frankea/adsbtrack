# Squawk Signal Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract three additional squawk-derived columns from ADS-B trace data — `squawks_observed`, `had_emergency`, `primary_squawk` — surface them via `trips --show-squawk` and via a per-emergency-code breakdown in the `status` command's Data Quality section. Existing squawk columns (`squawk_first`, `squawk_last`, `squawk_changes`, `emergency_squawk`, `vfr_flight`) are already tracked and stay unchanged.

**Architecture:** Extend `FlightMetrics` with a time-attribution accumulator (`squawk_durations: dict[str, float]`) that credits each point's inter-point interval to the then-held squawk. Extend `compute_squawk_summary` to emit the three new outputs. Wire them through `derive_all` to `Flight` and persist them. CLI changes are purely cosmetic.

**Tech Stack:** Existing. No new dependencies.

---

## Scope notes

**What already exists** (do not re-implement):
- `FlightMetrics.squawk_first`, `squawk_last`, `squawk_changes`, `squawk_1200_count`, `squawk_total_count`, `emergency_squawks_seen`
- `Flight.squawk_first`, `squawk_last`, `squawk_changes`, `emergency_squawk`, `vfr_flight`
- `features.compute_squawk_summary` emitting the above five
- Parser per-point squawk tracking in `FlightMetrics.record_point` (lines ~441-454)

**What's genuinely new**:
- Per-squawk cumulative duration (time accumulation, not just point count)
- `squawks_observed` (JSON sorted unique list, parallels existing `callsigns` field)
- `had_emergency` (explicit 0/1, parallels `vfr_flight` convention)
- `primary_squawk` (TEXT — the squawk with the greatest cumulative duration)

**User test expectation check.** The user said "flight with three squawk handoffs (squawk_changes=2)". Under the existing `squawk_changes` counter (raw transitions between `squawk_last` and current), a linear sequence 1200 → 5201 → 5203 produces exactly 2 transitions — so the existing counter already produces `squawk_changes=2` for the canonical test case. No semantic change to `squawk_changes` is required.

**Backfill.** New columns default to NULL in the DB schema. Legacy rows that were extracted before this milestone carry NULL until `extract --reprocess` is run.

**Military squawks (4000-4777).** Per user requirement #5, we do NOT flag or visually call out military squawks. They're persisted in `squawks_observed` like any other squawk; no special rendering anywhere.

---

## File structure

**Modified files**
- `adsbtrack/classifier.py` — `FlightMetrics` gains `squawk_durations: dict[str, float]`, `_current_squawk: str | None`, `_current_squawk_started_ts: float | None`, and a `flush_open_squawk(self)` method. `record_point` updates the duration accumulator on each point carrying a squawk.
- `adsbtrack/features.py` — `compute_squawk_summary` emits three new keys: `squawks_observed`, `had_emergency`, `primary_squawk`. `derive_all` copies them onto `Flight`.
- `adsbtrack/models.py` — `Flight` gains `squawks_observed: str | None`, `had_emergency: int | None`, `primary_squawk: str | None`.
- `adsbtrack/db.py` — CREATE TABLE + migration + insert_flight extended. 98/98/98 → 101/101/101.
- `adsbtrack/cli.py` — `status` Data Quality section gains per-emergency-code breakdown + avg squawk changes line. `trips` gains `--show-squawk/--no-show-squawk` flag that renders `primary_squawk` as a right-justified "Squawk" column.
- `tests/test_classifier.py` — new test for `squawk_durations` accumulator behavior.
- `tests/test_features.py` — new tests for `compute_squawk_summary`'s three outputs (emergency, three-squawks, no-data).
- `tests/test_db.py` — 3 roundtrip tests for the new columns.
- `tests/test_cli.py` — 1 status test (emergency breakdown) + 1 trips test (--show-squawk).
- `docs/features.md` — append "Squawk signals." subsection.
- `docs/schema.md` — add three rows.

**No new files.**

---

## Task 1: FlightMetrics accumulator + features emission + Flight fields + schema

### Step 1: write failing DB tests

In `tests/test_db.py`, append near the existing sibling column tests:

```python
def test_flights_table_has_squawk_signal_columns(db) -> None:
    cols = {row[1] for row in db.conn.execute("PRAGMA table_info(flights)").fetchall()}
    assert {"squawks_observed", "had_emergency", "primary_squawk"}.issubset(cols)


def test_insert_flight_persists_squawk_signals(db) -> None:
    f = Flight(
        icao="777777",
        takeoff_time=datetime(2024, 8, 1, 10, 0),
        takeoff_lat=27.77, takeoff_lon=-82.67,
        takeoff_date="2024-08-01",
        squawks_observed='["1200","5201","7700"]',
        had_emergency=1,
        primary_squawk="1200",
    )
    db.insert_flight(f)
    db.commit()
    row = db.conn.execute(
        "SELECT squawks_observed, had_emergency, primary_squawk FROM flights WHERE icao = ?",
        ("777777",),
    ).fetchone()
    assert row["squawks_observed"] == '["1200","5201","7700"]'
    assert row["had_emergency"] == 1
    assert row["primary_squawk"] == "1200"


def test_insert_flight_squawk_signals_default_to_null(db) -> None:
    f = Flight(
        icao="888888",
        takeoff_time=datetime(2024, 8, 1, 10, 0),
        takeoff_lat=27.77, takeoff_lon=-82.67,
        takeoff_date="2024-08-01",
    )
    db.insert_flight(f)
    db.commit()
    row = db.conn.execute(
        "SELECT squawks_observed, had_emergency, primary_squawk FROM flights WHERE icao = ?",
        ("888888",),
    ).fetchone()
    assert row["squawks_observed"] is None
    assert row["had_emergency"] is None
    assert row["primary_squawk"] is None
```

### Step 2: write failing classifier test (the time-attribution logic)

In `tests/test_classifier.py`, append:

```python
def test_squawk_durations_accumulate_per_code() -> None:
    """Time spent on each squawk is credited to that squawk's cumulative duration."""
    metrics = FlightMetrics()

    def _pt(ts: float, squawk: str | None) -> PointData:
        return PointData(
            ts=ts, lat=27.77, lon=-82.67, baro_alt=1000, gs=150.0,
            track=90.0, geom_alt=1000, baro_rate=0.0, geom_rate=None,
            squawk=squawk, category=None, nav_altitude_mcp=None, nav_qnh=None,
            emergency_field=None, true_heading=None, callsign=None,
        )

    # t=0  squawk=1200
    # t=60 squawk=1200  → 60 s credited to 1200
    # t=90 squawk=5201  → next 30 s credited to 1200 (1200 held until transition)
    #                     wait: we attribute [t=90, ...) to 5201
    # Let's be explicit: 1200 from 0..90, 5201 from 90..150, 1200 from 150..210.
    for ts, sq in [(0, "1200"), (60, "1200"), (90, "5201"), (120, "5201"), (150, "1200"), (210, "1200")]:
        metrics.record_point(_pt(float(ts), sq), ground_state="airborne", ground_reason="ok")
    metrics.flush_open_squawk()

    # 1200 held 0..90 (90 s) + 150..210 (60 s) = 150 s total
    # 5201 held 90..150 = 60 s
    assert metrics.squawk_durations.get("1200", 0.0) == pytest.approx(150.0)
    assert metrics.squawk_durations.get("5201", 0.0) == pytest.approx(60.0)


def test_squawk_durations_no_squawk_points_stays_empty() -> None:
    metrics = FlightMetrics()
    p = PointData(
        ts=10.0, lat=27.77, lon=-82.67, baro_alt=1000, gs=150.0,
        track=90.0, geom_alt=1000, baro_rate=0.0, geom_rate=None,
        squawk=None, category=None, nav_altitude_mcp=None, nav_qnh=None,
        emergency_field=None, true_heading=None, callsign=None,
    )
    metrics.record_point(p, ground_state="airborne", ground_reason="ok")
    metrics.flush_open_squawk()
    assert metrics.squawk_durations == {}
```

Ensure `import pytest` is at top of `tests/test_classifier.py` (it may already be).

### Step 3: write failing features tests

In `tests/test_features.py`, append (or create the file if absent):

```python
"""Tests for adsbtrack.features.compute_squawk_summary new outputs."""

from __future__ import annotations

import json

from adsbtrack.classifier import FlightMetrics
from adsbtrack.config import Config
from adsbtrack.features import compute_squawk_summary


def _metrics_with_squawk_history(squawks_with_ts: list[tuple[float, str | None]]) -> FlightMetrics:
    """Build a FlightMetrics by feeding record_point with (ts, squawk) tuples."""
    from adsbtrack.classifier import PointData

    m = FlightMetrics()
    for ts, sq in squawks_with_ts:
        p = PointData(
            ts=ts, lat=0.0, lon=0.0, baro_alt=1000, gs=150.0,
            track=0.0, geom_alt=1000, baro_rate=0.0, geom_rate=None,
            squawk=sq, category=None, nav_altitude_mcp=None, nav_qnh=None,
            emergency_field=None, true_heading=None, callsign=None,
        )
        m.record_point(p, ground_state="airborne", ground_reason="ok")
    m.flush_open_squawk()
    return m


def test_compute_squawk_summary_has_emergency() -> None:
    """Flight with 7700 at any point has had_emergency=1 and emergency_squawk='7700'."""
    m = _metrics_with_squawk_history([
        (0.0, "1200"), (60.0, "1200"), (120.0, "7700"), (180.0, "7700"),
        (240.0, "1200"), (300.0, "1200"),
    ])
    out = compute_squawk_summary(m, config=Config())
    assert out["had_emergency"] == 1
    assert out["emergency_squawk"] == "7700"
    assert out["primary_squawk"] == "1200"  # longer cumulative duration


def test_compute_squawk_summary_three_handoffs() -> None:
    """Three distinct squawks yields squawk_changes=2 (raw transitions) and
    squawks_observed listed sorted."""
    m = _metrics_with_squawk_history([
        (0.0, "1200"), (60.0, "1200"),
        (90.0, "5201"), (150.0, "5201"),
        (180.0, "5203"), (240.0, "5203"),
    ])
    out = compute_squawk_summary(m, config=Config())
    assert out["squawk_changes"] == 2
    assert out["had_emergency"] == 0
    assert out["squawks_observed"] == json.dumps(["1200", "5201", "5203"])


def test_compute_squawk_summary_no_squawk_data() -> None:
    """Flight with no observed squawks: all squawk-derived fields NULL-ish."""
    m = _metrics_with_squawk_history([(0.0, None), (60.0, None)])
    out = compute_squawk_summary(m, config=Config())
    assert out["squawk_first"] is None
    assert out["squawk_last"] is None
    assert out["squawk_changes"] is None
    assert out["emergency_squawk"] is None
    assert out["squawks_observed"] is None
    assert out["had_emergency"] == 0
    assert out["primary_squawk"] is None
```

### Step 4: verify tests fail

```bash
uv run pytest tests/test_db.py tests/test_classifier.py tests/test_features.py -v -k "squawk or squawks or emergency"
```

Expected: 8 FAILs (3 DB + 2 classifier + 3 features). DB: missing columns. Classifier: missing attrs + missing flush_open_squawk. Features: missing keys in output dict.

### Step 5: extend FlightMetrics

In `adsbtrack/classifier.py`, add to the `FlightMetrics` dataclass alongside the existing squawk fields (look for `emergency_squawks_seen`):

```python
    # --- Squawk duration attribution (for primary_squawk + squawks_observed) ---
    # Per-squawk cumulative seconds. record_point credits each inter-point
    # interval to the then-held squawk. flush_open_squawk() closes the final
    # run using the last observed ts (called from compute_squawk_summary).
    squawk_durations: dict[str, float] = field(default_factory=dict)
    _current_squawk: str | None = None
    _current_squawk_started_ts: float | None = None
```

Add a method below `record_point`:

```python
    def flush_open_squawk(self) -> None:
        """Close the currently-open squawk run by crediting the remaining
        time to its duration total. Called at end of flight before reading
        squawk_durations for primary_squawk / squawks_observed."""
        if (
            self._current_squawk is not None
            and self._current_squawk_started_ts is not None
            and self.last_point_ts is not None
        ):
            dt = self.last_point_ts - self._current_squawk_started_ts
            if dt > 0:
                self.squawk_durations[self._current_squawk] = (
                    self.squawk_durations.get(self._current_squawk, 0.0) + dt
                )
        # Reset so double-calls don't double-credit.
        self._current_squawk = None
        self._current_squawk_started_ts = None
```

In `record_point`, find the existing squawk block (around line 441-454, starts with `sq = point.squawk`). Immediately BEFORE or AFTER the existing logic (it doesn't interact with the existing counters), add the duration-attribution block:

```python
        # Squawk-duration attribution. On a point with a squawk:
        #   - If a run is open with the same squawk, keep it open (no action).
        #   - If a run is open with a different squawk, close it (credit the
        #     elapsed time) and start a new run with the current squawk.
        #   - If no run is open, start one.
        # Done in addition to the existing squawk_changes counter so per-code
        # durations can be aggregated at end of flight.
        if sq:
            if self._current_squawk is None:
                self._current_squawk = sq
                self._current_squawk_started_ts = ts
            elif self._current_squawk != sq:
                if self._current_squawk_started_ts is not None:
                    dt = ts - self._current_squawk_started_ts
                    if dt > 0:
                        self.squawk_durations[self._current_squawk] = (
                            self.squawk_durations.get(self._current_squawk, 0.0) + dt
                        )
                self._current_squawk = sq
                self._current_squawk_started_ts = ts
            # else: same squawk, still open run — no transition yet
```

IMPORTANT: `sq` in the existing block is assigned via `sq = point.squawk`; this new block can reuse that variable (place the new block inside the same `if sq:` branch, or structure carefully so `sq` is stripped. Read the current code first.)

Actually simpler placement: put the new block as the FIRST thing after `sq = point.squawk`, so it doesn't depend on the stripping the existing code does. That way it runs on every point with a truthy squawk.

### Step 6: extend compute_squawk_summary

In `adsbtrack/features.py`, modify `compute_squawk_summary` (around line 594) to emit the three new keys:

```python
def compute_squawk_summary(metrics: FlightMetrics, *, config: Config) -> dict:
    # Flush the final open squawk run before reading durations. Safe to call
    # multiple times (second call is a no-op).
    metrics.flush_open_squawk()

    emergency = None
    if metrics.emergency_squawks_seen:
        prio = config.emergency_squawk_priority
        emergency = max(metrics.emergency_squawks_seen, key=lambda s: prio.get(s, 0))

    vfr = None
    if metrics.squawk_total_count > 0:
        vfr = 1 if metrics.squawk_1200_count / metrics.squawk_total_count >= 0.8 else 0

    # New outputs
    had_emergency = 1 if metrics.emergency_squawks_seen else 0
    observed = sorted(metrics.squawk_durations.keys())
    squawks_observed = json.dumps(observed, ensure_ascii=True) if observed else None
    primary_squawk: str | None = None
    if metrics.squawk_durations:
        primary_squawk = max(metrics.squawk_durations.items(), key=lambda kv: kv[1])[0]

    return {
        "squawk_first": metrics.squawk_first,
        "squawk_last": metrics.squawk_last,
        "squawk_changes": metrics.squawk_changes if metrics.squawk_total_count > 0 else None,
        "emergency_squawk": emergency,
        "vfr_flight": vfr,
        "squawks_observed": squawks_observed,
        "had_emergency": had_emergency,
        "primary_squawk": primary_squawk,
    }
```

`json` is already imported at the top of features.py. If not, add `import json`.

### Step 7: wire the new outputs into derive_all

Still in `adsbtrack/features.py`, find the `derive_all` function where `compute_squawk_summary` is called (the result is unpacked into `flight.squawk_first`, `flight.squawk_last`, etc., around line 958). Extend the assignment block:

```python
    sq = compute_squawk_summary(metrics, config=config)
    flight.squawk_first = sq["squawk_first"]
    flight.squawk_last = sq["squawk_last"]
    flight.squawk_changes = sq["squawk_changes"]
    flight.emergency_squawk = sq["emergency_squawk"]
    flight.vfr_flight = sq["vfr_flight"]
    flight.squawks_observed = sq["squawks_observed"]
    flight.had_emergency = sq["had_emergency"]
    flight.primary_squawk = sq["primary_squawk"]
```

### Step 8: add Flight fields

In `adsbtrack/models.py`, append after `pattern_cycles: int | None = None`:

```python

    # --- Squawk signals (see adsbtrack/features.py:compute_squawk_summary) ---
    # squawks_observed: JSON sorted list of unique squawks seen during the
    # flight (e.g., '["1200","5201","7700"]'). NULL when no squawks
    # observed or for legacy rows.
    squawks_observed: str | None = None
    # had_emergency: 1 when any of 7500 / 7600 / 7700 appeared at any point,
    # 0 otherwise. NULL for legacy rows (re-extract to populate).
    had_emergency: int | None = None
    # primary_squawk: the squawk code held for the longest cumulative
    # duration during the flight. NULL when no squawks were observed or
    # for legacy rows.
    primary_squawk: str | None = None
```

### Step 9: add schema/migration/insert

In `adsbtrack/db.py`:

1. CREATE TABLE flights: add `squawks_observed TEXT, had_emergency INTEGER, primary_squawk TEXT,` immediately after `pattern_cycles INTEGER,`.

2. `_migrate_add_flight_columns` new_columns list: append after `("pattern_cycles", "INTEGER"),`:
   ```python
           # Squawk signals (docs/features.md)
           ("squawks_observed", "TEXT"),
           ("had_emergency", "INTEGER"),
           ("primary_squawk", "TEXT"),
   ```

3. `insert_flight`:
   - Extend SQL column list tail from `... pattern_cycles)` to `... pattern_cycles, squawks_observed, had_emergency, primary_squawk)`.
   - Extend the last VALUES placeholder group from 11 placeholders to 14 (add three `?`).
   - Extend Python value tuple by appending after `flight.pattern_cycles,`:
     ```python
                     flight.squawks_observed,
                     flight.had_emergency,
                     flight.primary_squawk,
     ```

After these edits, verify the count invariant moves from 98/98/98 to 101/101/101:

```bash
uv run python -c "import re; t=open('adsbtrack/db.py').read(); m=re.search(r'INSERT OR REPLACE INTO flights\s*\((.*?)\)\s*VALUES\s*\((.*?)\)', t, re.S); cols=[c.strip() for c in m.group(1).split(',')]; q=m.group(2).count('?'); print(len(cols), q)"
```

Expected: `101 101`. Count the Python value tuple too.

### Step 10: verify tests pass

`uv run pytest tests/test_db.py tests/test_classifier.py tests/test_features.py -v -k "squawk or squawks or emergency"`

Expected: 8 PASSed (3 DB + 2 classifier + 3 features).

### Step 11: full suite + lint

`uv run pytest -x --deselect tests/test_cli.py::test_enrich_hex_no_data_reports_yellow`
`uv run ruff check . && uv run ruff format --check .`
`uv run mypy adsbtrack/classifier.py adsbtrack/features.py` — clean

### Step 12: commit

```bash
git add adsbtrack/classifier.py adsbtrack/features.py adsbtrack/models.py adsbtrack/db.py tests/test_db.py tests/test_classifier.py tests/test_features.py
git commit -m "feat(squawk): add squawks_observed, had_emergency, primary_squawk"
```

---

## Task 2: `status` CLI — emergencies breakdown + avg squawk changes

### Step 1: failing test

In `tests/test_cli.py`, append:

```python
def test_status_shows_emergency_breakdown_and_avg_squawk_changes(tmp_path, monkeypatch) -> None:
    """status output includes a per-code emergency breakdown and the
    average squawk_changes across flights."""
    monkeypatch.setenv("COLUMNS", "200")
    db_path = tmp_path / "a.db"
    with Database(db_path) as db:
        # Two 7700 flights, one 7600 flight, two normal with changes
        seed = [
            ("7700", 3),
            ("7700", 1),
            ("7600", 0),
            (None, 5),
            (None, 2),
        ]
        for i, (em, changes) in enumerate(seed):
            f = Flight(
                icao="aaaeme",
                takeoff_time=datetime(2024, 6, 1, 10 + i, 0),
                takeoff_lat=27.76, takeoff_lon=-82.63,
                takeoff_date=f"2024-06-{1+i:02d}",
                landing_time=datetime(2024, 6, 1, 11 + i, 0),
                landing_lat=27.76, landing_lon=-82.63,
                landing_date=f"2024-06-{1+i:02d}",
                origin_icao="KSPG",
                destination_icao="KSPG",
                duration_minutes=60.0,
                landing_type="confirmed",
                landing_confidence=0.9,
                emergency_squawk=em,
                had_emergency=1 if em else 0,
                squawk_changes=changes,
            )
            db.insert_flight(f)

    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--hex", "aaaeme", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    # Emergency breakdown line should list both codes with counts
    assert "Emergencies:" in result.output
    # Use regex: "2 (7700)" for the two 7700 flights
    assert re.search(r"2\s*\(7700\)", result.output) is not None
    assert re.search(r"1\s*\(7600\)", result.output) is not None
    # Avg squawk changes should be (3+1+0+5+2)/5 = 2.2
    assert re.search(r"Squawk changes.*2\.2", result.output) is not None
```

`import re` should already be imported at the top of test_cli.py from Task 3 of the prior go-around milestone. Verify.

### Step 2: verify fail

`uv run pytest tests/test_cli.py -v -k "status_shows_emergency_breakdown"`

Expected: FAIL.

### Step 3: extend status

In `adsbtrack/cli.py`, find the `status` command's Data Quality block (around line 499). AFTER the existing landing-type loop and BEFORE the newer "Approach behaviour" section (added in the go-around milestone), add:

```python
        # Emergency-squawk breakdown: per-code counts. Only rendered when
        # at least one flight in the scope has an emergency.
        try:
            emergency_rows = db.conn.execute(
                """SELECT emergency_squawk, COUNT(*) AS cnt FROM flights
                   WHERE icao = ? AND emergency_squawk IS NOT NULL
                   GROUP BY emergency_squawk ORDER BY emergency_squawk""",
                (hex_code,),
            ).fetchall()
        except Exception:
            emergency_rows = []
        if emergency_rows:
            parts = ", ".join(f"{row['cnt']} ({row['emergency_squawk']})" for row in emergency_rows)
            console.print(f"  [red]Emergencies:{' ' * (22 - len('Emergencies:'))}{parts}[/]")

        # Average squawk changes per flight. Skip when no flight has a
        # non-null squawk_changes column (typically all flights that
        # observed at least one squawk carry the field).
        try:
            avg_row = db.conn.execute(
                "SELECT AVG(squawk_changes) AS avg_changes, COUNT(squawk_changes) AS n FROM flights WHERE icao = ?",
                (hex_code,),
            ).fetchone()
        except Exception:
            avg_row = None
        if avg_row and avg_row["n"] and avg_row["avg_changes"] is not None:
            console.print(f"  Squawk changes per flight (avg): {avg_row['avg_changes']:.1f}")
```

Exact padding/format can vary — the test uses regex `r"2\s*\(7700\)"` and `r"Squawk changes.*2\.2"` so it's tolerant of whitespace and decoration.

### Step 4: verify pass + full suite

```bash
uv run pytest tests/test_cli.py -v -k "status_shows_emergency"
uv run pytest -x --deselect tests/test_cli.py::test_enrich_hex_no_data_reports_yellow
uv run ruff check . && uv run ruff format --check .
```

### Step 5: commit

```bash
git add adsbtrack/cli.py tests/test_cli.py
git commit -m "feat(squawk): status shows emergency breakdown + avg squawk changes"
```

---

## Task 3: `trips --show-squawk` flag

### Step 1: failing test

In `tests/test_cli.py`, append:

```python
def test_trips_show_squawk_renders_primary_column(tmp_path, monkeypatch) -> None:
    """trips --show-squawk adds a Squawk column and renders primary_squawk."""
    monkeypatch.setenv("COLUMNS", "200")
    db_path = tmp_path / "a.db"
    with Database(db_path) as db:
        f = Flight(
            icao="sqwk01",
            takeoff_time=datetime(2024, 6, 1, 10, 0),
            takeoff_lat=27.76, takeoff_lon=-82.63,
            takeoff_date="2024-06-01",
            landing_time=datetime(2024, 6, 1, 11, 0),
            landing_lat=28.0, landing_lon=-82.5,
            landing_date="2024-06-01",
            origin_icao="KSPG", destination_icao="KPIE",
            duration_minutes=60.0,
            landing_type="confirmed",
            landing_confidence=0.9,
            primary_squawk="1200",
        )
        db.insert_flight(f)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["trips", "--hex", "sqwk01", "--db", str(db_path), "--show-squawk"],
    )
    assert result.exit_code == 0, result.output
    assert "Squawk" in result.output
    assert "1200" in result.output


def test_trips_no_squawk_column_by_default(tmp_path, monkeypatch) -> None:
    """Without --show-squawk the Squawk column is hidden."""
    monkeypatch.setenv("COLUMNS", "200")
    db_path = tmp_path / "a.db"
    with Database(db_path) as db:
        f = Flight(
            icao="sqwk02",
            takeoff_time=datetime(2024, 6, 1, 10, 0),
            takeoff_lat=27.76, takeoff_lon=-82.63,
            takeoff_date="2024-06-01",
            landing_time=datetime(2024, 6, 1, 11, 0),
            landing_lat=28.0, landing_lon=-82.5,
            landing_date="2024-06-01",
            origin_icao="KSPG", destination_icao="KPIE",
            duration_minutes=60.0,
            landing_type="confirmed",
            landing_confidence=0.9,
            primary_squawk="1200",
        )
        db.insert_flight(f)

    runner = CliRunner()
    result = runner.invoke(cli, ["trips", "--hex", "sqwk02", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "Squawk" not in result.output  # column hidden by default
```

### Step 2: verify fail

`uv run pytest tests/test_cli.py -v -k "trips_show_squawk or trips_no_squawk_column"`

Expected: FAIL (no flag yet).

### Step 3: implementation

In `adsbtrack/cli.py`, modify the `trips` command. Add a click option alongside the existing `--alignment` option:

```python
@click.option(
    "--show-squawk/--no-show-squawk",
    "show_squawk",
    default=False,
    help="Show the primary squawk code held by each flight.",
)
```

Add `show_squawk` as a parameter in the `def trips(...)` signature.

After the existing `show_alignment_col` computation block (which was added in the ILS alignment milestone), add:

```python
        show_squawk_col = show_squawk
```

(No auto-show — unlike ACARS and alignment columns, squawk is opt-in only per user spec.)

In the column header block (alongside the alignment column addition):

```python
        if show_squawk_col:
            table.add_column("Squawk", justify="right", style="cyan")
```

In the row-building loop, append the squawk cell alongside the alignment cell:

```python
            if show_squawk_col:
                squawk_cell = _col(f, "primary_squawk") or "[dim]--[/]"
                row_cells.append(squawk_cell)
```

### Step 4: verify pass + full suite

```bash
uv run pytest tests/test_cli.py -v -k "trips_show_squawk or trips_no_squawk_column"
uv run pytest -x --deselect tests/test_cli.py::test_enrich_hex_no_data_reports_yellow
uv run ruff check . && uv run ruff format --check .
```

### Step 5: commit

```bash
git add adsbtrack/cli.py tests/test_cli.py
git commit -m "feat(squawk): trips --show-squawk flag renders primary_squawk"
```

---

## Task 4: Docs

### Step 1: `docs/features.md`

Append a new subsection near the Go-around and pattern work subsection (or find a spot near the other mission/classification prose — the Callsigns section is a natural neighbor since callsigns and squawks are both per-point signals accumulated across the flight):

```markdown
**Squawk signals.** `squawks_observed`, `had_emergency`, `primary_squawk`, plus the pre-existing `squawk_first`, `squawk_last`, `squawk_changes`, `emergency_squawk`, `vfr_flight`. Every trace point carries a transponder squawk code; the extractor credits each point's inter-point interval to the then-held code and emits three new aggregate columns at end of flight.

- `squawks_observed` is a JSON-encoded sorted list of every unique squawk code seen, e.g. `'["1200","5201","7700"]'`. NULL when the flight carried no squawk data.
- `had_emergency = 1` when any of the three emergency codes (7500 hijack, 7600 radio failure, 7700 emergency) appeared at any point. Independent of `emergency_squawk`, which records the single most-severe code observed.
- `primary_squawk` is the squawk held for the greatest cumulative duration. For steady-state VFR flights this is typically "1200" (US) or "7000" (EU); for flights with ATC handoffs it is the code held for the longest single segment.

These columns are diagnostic only and do not feed into mission classification or confidence scoring. Military-allocation squawks (US 4000-4777 block, MODE 3/A) are persisted in `squawks_observed` like any other code; the extractor deliberately does not tag or visually highlight them.
```

Use regular hyphens. (CLAUDE.md.)

### Step 2: `docs/schema.md`

Add three rows to the flights table listing, near the other squawk columns:

```markdown
| squawks_observed | TEXT | JSON-encoded sorted list of unique squawk codes seen during the flight. NULL when no squawk data was observed or for legacy rows. |
| had_emergency | INTEGER | 1 when any of 7500 / 7600 / 7700 appeared at any point, 0 otherwise. NULL for legacy rows. |
| primary_squawk | TEXT | Squawk code held for the longest cumulative duration during the flight. NULL when no squawk data was observed or for legacy rows. |
```

### Step 3: verify

```bash
uv run ruff check . && uv run ruff format --check . && uv run pytest --deselect tests/test_cli.py::test_enrich_hex_no_data_reports_yellow
```

All green.

### Step 4: commit

```bash
git add docs/features.md docs/schema.md
git commit -m "docs(squawk): document squawks_observed, had_emergency, primary_squawk"
```

---

## Self-Review Checklist

**1. Spec coverage:**
- `squawks_observed` (JSON array per spec "comma-separated or JSON") → Task 1.
- `squawk_changes` — already exists; no change needed (confirmed in scope notes).
- `had_emergency` — Task 1.
- `primary_squawk` (longest cumulative duration) — Task 1 via `squawk_durations` accumulator.
- Status CLI emergency breakdown + avg squawk changes → Task 2.
- `trips --show-squawk` flag → Task 3.
- No visual flagging of military squawks → confirmed (persisted but not highlighted).
- `extract --reprocess` recomputes — this works automatically because all logic runs in `extract_flights` → `derive_all` → `compute_squawk_summary`.

**2. Placeholder scan:** No TBD / FIXME / "implement later". All test bodies and code blocks are complete.

**3. Type consistency:**
- `squawks_observed: str | None` (JSON-encoded) — matches existing `callsigns: str | None` convention.
- `had_emergency: int | None` — 0/1 int like `vfr_flight`, `takeoff_is_night`, etc.
- `primary_squawk: str | None` — 4-char TEXT.
- `FlightMetrics.squawk_durations: dict[str, float]` — uses `field(default_factory=dict)` to avoid mutable default.
- `FlightMetrics._current_squawk: str | None`, `_current_squawk_started_ts: float | None` — private accumulator state.

**4. Test quality:**
- User spec asked for 3 tests (emergency, 3 handoffs, no-data). All three are present at the features-level (unit) tests; the DB tests exercise the persistence layer; the classifier tests exercise the accumulator. No fixture fragility — all tests construct FlightMetrics directly.

**5. Backward compat:**
- Legacy rows get NULL until re-extracted — matches every prior schema expansion in this project.
- `squawk_changes` semantics NOT changed — the test expectation matches the existing raw-transition counter.

---

## Execution handoff

Plan saved to `docs/superpowers/plans/2026-04-16-squawk-signals.md`. Two execution options:

1. **Subagent-Driven (recommended)** — Fresh subagent per task + two-stage review.
2. **Inline Execution** — Batch execution with checkpoints.

Which approach?
