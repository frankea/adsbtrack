# Fetch Robustness + Extraction Correctness + Inspect Command Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the four defects found in 2026-08-19 live casework (GitHub issues #19, #20, #21, #22) and productize the ad-hoc day-forensics workflow as an `inspect` command (#23).

**Architecture:** All changes ride the existing layers: CLI planning logic in `adsbtrack/cli.py`, thresholds in `adsbtrack/config.Config`, accumulators in `adsbtrack/classifier.FlightMetrics` (with per-field merge metadata), gating/stitching in `adsbtrack/parser.py`, one new leaf module `adsbtrack/forensics.py`. No schema changes: source health derives from the existing `fetch_log.fetched_at` column.

**Tech Stack:** Python 3.12+, Click, Rich, sqlite3, pytest.

**Spec:** GitHub issues frankea/adsbtrack #19-#23 (each task names its issue; the issue text is the spec). Controller rulings that refine the issues are quoted inline in each task.

## Global Constraints

- Line length 120; regular hyphens, never em dashes, in all output and docs.
- Every new threshold goes in `adsbtrack/config.Config` with a comment; no inline constants.
- Tests use tmp_path databases only. NEVER open, migrate, or read the user's live `adsbtrack.db`.
- No schema changes in this plan. If you believe you need one, STOP and report BLOCKED - do not add columns or bump SCHEMA_VERSION.
- `FlightMetrics.max_altitude` / `max_gs_kt` stay `@property` accessors; never re-add them as fields.
- `parser.pool_spoof_scores` must keep working for `events.py` (day-level events keep using it); only the extract-gate stops consuming it (Task 4).
- Before each commit: `uv run ruff check . && uv run ruff format --check . && uv run pytest` must pass (use `uv run --no-sync` if plain `uv run` hangs).
- Commit messages: concise, why-focused, reference the GitHub issue (e.g. "fixes #21"), and end with the trailer line `Claude-Session: https://claude.ai/code/session_018hjWwnYNy7YC73ayhRDThJ`.

---

### Task 1: Per-source fetch resume with lookback clamp (#19)

**Files:**
- Modify: `adsbtrack/cli.py` (`_resume_start_for_all_sources` at :86 becomes per-source; `fetch` command planning block :275-345 and the `_fetch_one` thread body :378-391)
- Modify: `adsbtrack/config.py` (new `resume_max_lookback_days`)
- Test: `tests/test_cli.py`

**Interfaces:**
- Consumes: `db.get_fetched_dates(hex, source)` (success-filtered, unchanged), `SOURCE_URLS`, `_default_fetch_start()`.
- Produces: `_resume_starts_per_source(db, hex_code) -> dict[str, str]` (source -> last success-filtered fetch date, sources without history absent) and a `per_source_start: dict[str, date]` used by the `--source all` fan-out. Task 2 inserts its health filter into the same planning block, so keep the block readable: compute `sources_to_fetch` first, then per-source starts.

**Controller ruling:** the lookback clamp applies ONLY under `--source all` without an explicit `--start`. A single named source and an explicit `--start` are user intent and never clamped.

- [ ] **Step 1: Write failing tests** in `tests/test_cli.py`. Follow the existing pattern in this file for invoking `fetch` with `fetch_traces` monkeypatched (there are ~12 existing resume tests to copy setup from). Capture per-call `(source, start, end)`:

```python
def _capture_fetch_calls(monkeypatch):
    calls = []

    def fake_fetch_traces(db, config, hex_code, start, end, source="adsbx", progress=None):
        calls.append((source, start, end))
        return {"fetched": 0, "with_data": 0, "skipped": 0, "errors": 0, "failed_days": []}

    monkeypatch.setattr("adsbtrack.cli.fetch_traces", fake_fetch_traces)
    return calls


def test_source_all_resumes_each_source_from_its_own_history(tmp_path, monkeypatch):
    # Seed: adsbx last success 10 days ago, airplaneslive last success 40 days ago.
    # Expect adsbx to start 9 days ago and airplaneslive 39 days ago - NOT both from 39 days ago.
    ...


def test_source_all_clamps_dead_source_to_lookback(tmp_path, monkeypatch):
    # Seed: adsblol last success 300 days ago, adsbx last success 5 days ago.
    # With Config.resume_max_lookback_days = 90, adsblol's start is clamped to
    # (today - 90 days) and the output warns that older days were skipped.
    ...


def test_single_source_resume_never_clamped(tmp_path, monkeypatch):
    # fetch --source adsblol with last success 300 days ago resumes from 300 days ago.
    ...


def test_explicit_start_overrides_per_source_resume(tmp_path, monkeypatch):
    # fetch --source all --start <date> uses <date> for every source, unclamped.
    ...


def test_source_with_no_history_uses_earliest_peer_start(tmp_path, monkeypatch):
    # adsbx has history (last success 10 days ago), theairtraffic has none.
    # theairtraffic starts from the same date as the earliest peer start.
    ...
```

Seed fetch_log via `Database.insert_fetch_log(hex, date_str, 200, source=src)` on a tmp DB. Assert on the captured `calls` list and on `result.output`.

- [ ] **Step 2: Run the new tests, verify they fail** (`uv run pytest tests/test_cli.py -k resume -v`).

- [ ] **Step 3: Implement.**

In `adsbtrack/config.py`, next to the fetch settings:

```python
    # fetch --source all resume: cap how far back a per-source resume may
    # reach. A source that has been dead for months (adsblol, 2025-10 to
    # present) otherwise drags its own catch-up window back across its whole
    # outage on every run. Clamped sources print a warning naming the days
    # skipped; pass --start explicitly to backfill deeper. Only applies to
    # --source all resumes - a single named source is explicit user intent.
    resume_max_lookback_days: int = 90
```

In `adsbtrack/cli.py`, replace `_resume_start_for_all_sources` with:

```python
def _resume_starts_per_source(db: Database, hex_code: str) -> dict[str, str]:
    """Last success-filtered fetch date per readsb source. Sources with no
    history are absent from the dict (a missing source resumes from the
    earliest peer's start so it can catch up; see the fetch command)."""
    starts: dict[str, str] = {}
    for src in SOURCE_URLS:
        fetched_dates = db.get_fetched_dates(hex_code, source=src)
        if fetched_dates:
            starts[src] = max(fetched_dates)
    return starts
```

In `fetch()`: for `source == "all"` build `per_source_start: dict[str, date]`:

- explicit `--start`: every source uses it (current behavior, no clamp).
- otherwise: `last = _resume_starts_per_source(db, hex_code)`; if empty and `--since-last`, keep the existing UsageError; if empty otherwise, every source starts at `_default_fetch_start()`. If non-empty: `fallback = min(last.values()) + 1 day`; each source with history starts at `its last + 1 day`, each source without history starts at `fallback`. Then clamp each start: `if (end - start).days > config.resume_max_lookback_days: start = end - timedelta(days=config.resume_max_lookback_days)` and `console.print` a yellow warning naming the source, the original start, the clamped start, and "pass --start to backfill".
- `opensky` (when credentials present) uses the min of the readsb per-source starts, clamped the same way.
- Single-source path: unchanged (resume from that source's own history, no clamp).
- `_fetch_one(src)` uses `per_source_start[src]`; the banner prints one line per source when starts differ, or the existing single line when uniform.

- [ ] **Step 4: Run the new tests + the full suite** (`uv run pytest`). All pass.
- [ ] **Step 5: Commit** (`git add -A && git commit`), message referencing #19, with the session trailer.

---

### Task 2: Source health skip + retention annotations (#20)

**Files:**
- Modify: `adsbtrack/db.py` (new `recent_source_outcomes` next to `get_fetched_dates` at :1125)
- Modify: `adsbtrack/cli.py` (health filter + retention note in the `--source all` planning block from Task 1; new `--include-unhealthy` flag on `fetch`)
- Modify: `adsbtrack/config.py` (three new settings)
- Test: `tests/test_db.py`, `tests/test_cli.py`

**Interfaces:**
- Consumes: Task 1's `sources_to_fetch` / `per_source_start` planning block; `config.is_retryable_fetch_status`.
- Produces: `Database.recent_source_outcomes(source, limit) -> list[int]`; `_source_is_unhealthy(db, source, config) -> tuple[bool, int]` in cli.py.

**Controller ruling (deviation from issue #20 text):** NO new `source_health` table. `fetch_log` already carries `fetched_at`, so health is derived at plan time with one bounded query per source. This avoids a schema migration entirely. If the derived query proves too slow someday, an index or table can follow; do not add either now.

- [ ] **Step 1: Write failing tests.**

`tests/test_db.py`:

```python
def test_recent_source_outcomes_newest_first(tmp_path):
    db = Database(tmp_path / "t.db")
    for i, status in enumerate([200, 404, 502, 502]):
        db.insert_fetch_log("aaaaaa", f"2026-01-{i + 1:02d}", status, source="adsblol")
    assert db.recent_source_outcomes("adsblol", limit=3) == [502, 502, 404]
    assert db.recent_source_outcomes("adsbx", limit=3) == []
```

`tests/test_cli.py`:

```python
def test_source_all_skips_unhealthy_source(tmp_path, monkeypatch):
    # Seed 20 consecutive 502 rows for adsblol (any icao), healthy history for adsbx.
    # fetch --source all: adsblol is NOT in the captured fetch calls; output contains
    # "Skipping adsblol" and mentions --include-unhealthy.

def test_include_unhealthy_forces_sick_source(tmp_path, monkeypatch):
    # Same seed + --include-unhealthy: adsblol IS fetched.

def test_named_single_source_never_health_skipped(tmp_path, monkeypatch):
    # fetch --source adsblol with the same sick history still fetches.

def test_all_sources_unhealthy_falls_back_to_unfiltered(tmp_path, monkeypatch):
    # Every readsb source seeded sick: fetch proceeds with all of them plus a warning
    # (health filtering must never brick the fetch entirely).

def test_retention_note_printed_for_old_window(tmp_path, monkeypatch):
    # With source_retention_days = {"theairtraffic": 90} and a start 200 days back,
    # output contains a note that theairtraffic days older than ~90 days may be expired.
```

- [ ] **Step 2: Run them, verify they fail.**

- [ ] **Step 3: Implement.**

`adsbtrack/db.py` (next to `get_fetched_dates`):

```python
    def recent_source_outcomes(self, source: str, limit: int = 30) -> list[int]:
        """Most recent day-request statuses for a source across all aircraft,
        newest first. The fetch planner's health check reads the leading run
        of retryable failures from this; bounded by ``limit`` so the scan
        stays cheap however large fetch_log grows."""
        rows = self.conn.execute(
            "SELECT status FROM fetch_log WHERE source = ? ORDER BY fetched_at DESC, rowid DESC LIMIT ?",
            (source, limit),
        ).fetchall()
        return [row["status"] for row in rows]
```

`adsbtrack/config.py`:

```python
    # Source health (fetch --source all planning). A source whose most recent
    # source_health_skip_threshold day-requests were ALL retryable failures
    # (403/429/5xx - see is_retryable_fetch_status) is skipped for the run
    # with a warning; --include-unhealthy forces it back in. Derived from
    # fetch_log.fetched_at at plan time - no separate health table.
    source_health_window: int = 30
    source_health_skip_threshold: int = 20
    # Observed archive retention per source (days), None = unknown/unlimited.
    # theairtraffic: returned no data for dates ~90 days old that two other
    # networks had dense coverage for (2026-08 observation). Used only to
    # annotate fetch output - a 404 beyond retention reads "probably
    # expired", not "aircraft not seen".
    source_retention_days: dict[str, int | None] = field(default_factory=lambda: {"theairtraffic": 90})
```

(If `Config.load()` only maps scalar TOML keys, leave `source_retention_days` code-configured; do not extend the loader in this task.)

`adsbtrack/cli.py`:

```python
def _source_is_unhealthy(db: Database, source: str, config: Config) -> tuple[bool, int]:
    """(unhealthy, leading_failures): unhealthy when the source's most recent
    outcomes are an unbroken run of at least source_health_skip_threshold
    retryable failures."""
    outcomes = db.recent_source_outcomes(source, limit=config.source_health_window)
    leading = 0
    for status in outcomes:
        if is_retryable_fetch_status(status):
            leading += 1
        else:
            break
    return leading >= config.source_health_skip_threshold, leading
```

Wire into the `--source all` branch only (after `sources_to_fetch` is built, before per-source starts): filter unhealthy sources out with `[yellow]Skipping {src}: last {leading} attempts all failed (403/429/5xx); pass --include-unhealthy to force[/]`. If the filter would empty the readsb set, keep the original list and print a warning instead. Add `--include-unhealthy` as a Click flag on `fetch`. After per-source starts are known, for each source with a non-None `config.source_retention_days` entry whose start is older than `end - retention`, print the dim retention note.

- [ ] **Step 4: Run new tests + full suite.**
- [ ] **Step 5: Commit**, message referencing #20 and noting the no-new-table ruling, with the session trailer.

---

### Task 3: Stitcher fresh-departure veto (#21)

**Files:**
- Modify: `adsbtrack/classifier.py` (new `first_airborne_alt` accumulator; find where `last_airborne_alt` is maintained and set `first_airborne_alt` once, on the first airborne sample)
- Modify: `adsbtrack/parser.py` (`_stitch_fragments` acceptance block at :344)
- Modify: `adsbtrack/config.py` (three new stitch settings)
- Test: `tests/test_parser.py`, `tests/test_classifier.py`

**Interfaces:**
- Consumes: `FlightMetrics` merge-metadata conventions (`field(..., metadata={"merge": _MERGE_KEEP_FIRST})` declared next to each field; see classifier.py :118-153).
- Produces: `FlightMetrics.first_airborne_alt: float | None` (merge strategy `_MERGE_KEEP_FIRST`) - Task 4's implementer will see this field when adding its own accumulators; keep the declaration style identical to neighbors.

**The bug being fixed (spec evidence from #21):** RAF Hawk ZK019 (43c556) on 2026-08-17: sortie 1 (12:05-13:08Z, dropped_on_approach at EGOV) and sortie 2 (14:38-15:40Z, fresh departure) with ~90 min of ground time merged into one flight row carrying both squawk sets, because the 90-min gap fit `stitch_max_gap_minutes` and all other criteria passed.

- [ ] **Step 1: Write failing tests** in `tests/test_parser.py` using the existing `_make_trace_point` / `_make_trace_row` / `_make_db_mock` helpers:

```python
def test_stitch_vetoes_fresh_departure_after_ground_gap():
    # Fragment A: airborne, descends toward a field, signal lost at 1,050 ft (no landing).
    # Gap: 90 minutes.
    # Fragment B: first airborne point 1,200 ft, climbs to 17,000 ft.
    # Expect: TWO flights extracted, not one.

def test_stitch_still_merges_go_around_reappearance():
    # Fragment A ends at 900 ft on approach; fragment B reappears 3 minutes later
    # at 1,500 ft climbing to 3,000 ft then landing.
    # Gap 180 s < stitch_min_ground_gap_secs: expect ONE flight.

def test_stitch_still_merges_cruise_coverage_hole():
    # Fragment A signal lost at 39,000 ft; fragment B reappears 60 min later at
    # 39,000 ft (first airborne alt >= stitch_fresh_departure_alt_ft).
    # Expect ONE flight.

def test_stitch_still_merges_low_descending_reappearance():
    # Fragment A ends at 3,000 ft descending; fragment B reappears 20 min later at
    # 2,500 ft and only descends to landing (no climb above first alt + 2,000 ft).
    # Expect ONE flight (the climb condition is what distinguishes a new sortie).
```

And in `tests/test_classifier.py`: a unit test that `first_airborne_alt` records the FIRST airborne sample's altitude, ignores ground samples, and merges keep-first.

- [ ] **Step 2: Run them, verify they fail.**

- [ ] **Step 3: Implement.**

`adsbtrack/config.py`, next to the existing stitch settings (:468-477):

```python
    # Fresh-departure veto: a stitch candidate whose gap exceeds
    # stitch_min_ground_gap_secs AND whose next fragment starts airborne
    # below stitch_fresh_departure_alt_ft AND then climbs more than
    # stitch_fresh_departure_climb_ft above that first altitude is a new
    # sortie taking off, not a coverage hole - refuse the stitch. Keeps
    # go-arounds (short gap) and cruise coverage holes (high reappearance)
    # stitching exactly as before. See issue #21 (ZK019 double-sortie merge).
    stitch_min_ground_gap_secs: float = 900.0
    stitch_fresh_departure_alt_ft: float = 8000.0
    stitch_fresh_departure_climb_ft: float = 2000.0
```

`adsbtrack/classifier.py`: add `first_airborne_alt: float | None = None` with `metadata={"merge": _MERGE_KEEP_FIRST}` next to the other takeoff-side fields; set it in `record_point` at the same place `last_airborne_alt` is updated, only when it is still None.

`adsbtrack/parser.py` `_stitch_fragments`: compute the veto and fold it into the acceptance condition:

```python
                    # Fresh-departure veto (#21): a long gap followed by a
                    # reappearance that starts low and then climbs away is a
                    # new sortie, not a coverage hole in the same flight.
                    next_peak = next_metrics.raw_peak_altitude_ft  # raw observed peak; see note below
                    fresh_departure = (
                        gap_secs > config.stitch_min_ground_gap_secs
                        and next_metrics.first_airborne_alt is not None
                        and next_metrics.first_airborne_alt < config.stitch_fresh_departure_alt_ft
                        and next_peak is not None
                        and next_peak - next_metrics.first_airborne_alt > config.stitch_fresh_departure_climb_ft
                    )

                    if dist_km <= plausible and alt_ok and not fresh_departure:
```

Note on `next_peak`: use the fragment's RAW observed altitude peak, not the AP-validated `max_altitude` property (which can be None when no AP data corroborates - that must not disable the veto). Find the raw-peak accessor on FlightMetrics (the `_raw_*` dual-track state described in CLAUDE.md); if no public accessor exists, add a read-only property `raw_peak_altitude_ft` beside `max_altitude` rather than reaching into privates from parser.py. Do NOT convert `max_altitude` itself to a field.

- [ ] **Step 4: Run new tests + full suite.**
- [ ] **Step 5: Commit**, message referencing #21, with the session trailer.

---

### Task 4: Flight-scoped spoof gate with teleport corroboration (#22)

**Files:**
- Modify: `adsbtrack/classifier.py` (PointData: `adsb_version`/`sil`/`nic`; FlightMetrics: `v2_samples`/`v2_sil0`/`v2_nic0`/`max_implied_speed_kt`)
- Modify: `adsbtrack/parser.py` (`_extract_point_fields` at :56; `_flight_is_spoofed` at :470; the extract path that consults it; `pool_spoof_scores` docstring)
- Modify: `adsbtrack/config.py` (two new spoof settings)
- Modify: `docs/features.md` (spoof section)
- Test: `tests/test_classifier.py`, `tests/test_parser.py`

**Interfaces:**
- Consumes: Task 3's `first_airborne_alt` declaration style; `integrity.count_v2_integrity`'s per-point predicate (`ac["version"] == 2`, `sil == 0`, `nic == 0`) - the flight-scoped counters MUST use the identical predicate so flight stats and day stats can never disagree about the same point.
- Produces: `_flight_is_spoofed(flight, metrics, config)` (new signature - metrics replaces the day-scores dict). `events.py` is NOT modified; `pool_spoof_scores` and `_pool_spoof_scores_from_stats` keep powering day-level events.

**The bug being fixed (spec evidence from #22):** A6-EUY (896483) 2026-05-19: day-level stats (817 v2 samples, 21.5% SIL=0) quarantined all five extracted candidates, including two real flights. At flight scope the day separates: real DXB-DUS leg ~18-25% sil0 (jammed corridor, plausible positions), ghost fragments 87-100% sil0 with implied speeds over 1,000 kt.

**Controller ruling (two-tier gate):** reject a flight when EITHER
1. flight sil0 share >= `spoof_flight_sil0_hard_pct` (60.0), OR
2. flight sil0 share >= `spoof_v2_sil0_pct` (existing, 10.0) AND `max_implied_speed_kt` > `spoof_teleport_speed_kt` (900.0).

Tier 2 keeps catching Hormuz-style 25-50% sil0 spoofs (they teleport); tier 1 catches saturated garbage; a real jammed flight (18-25% sil0, physical speeds) passes. The crude EK-callsign gate is unchanged. Day-level pooling remains ONLY for `events` detection.

- [ ] **Step 1: Write failing tests.**

`tests/test_classifier.py`:

```python
def test_v2_integrity_counters_accumulate_and_merge():
    # record_point with PointData(adsb_version=2, sil=0, nic=0) increments all three;
    # version None / version 1 points do not; merge() sums the counters.

def test_max_implied_speed_records_teleports_and_ignores_jitter():
    # Two consecutive points 100 km apart with dt=60 s -> ~3,240 kt recorded.
    # Two points 1 km apart with dt=2 s (dt < 10 s) -> ignored, stays None/prior value.
```

`tests/test_parser.py` (via `_make_db_mock` end-to-end extraction; craft point detail dicts with `{"version": 2, "sil": 0, "nic": 0}` on the appropriate share of points):

```python
def test_mixed_day_extracts_clean_flight_and_quarantines_ghost():
    # Same trace day, two airborne windows:
    #  Flight A: 60 v2 points, 0 with sil=0, physical speeds -> lands in flights.
    #  Flight B: 60 v2 points, 55 with sil=0 (92%) -> lands in spoofed_broadcasts,
    #  reason "bimodal_integrity", reason_detail scope == "flight".

def test_jammed_but_real_flight_passes_flight_gate():
    # 25% sil0, max implied speed ~480 kt -> extracted into flights.

def test_moderate_sil0_with_teleport_is_quarantined():
    # 30% sil0 AND a 1,500 kt inter-fix jump -> quarantined, trigger "sil0_plus_teleport".

def test_crude_ek_callsign_gate_still_fires():
    # Existing behavior regression: low-alt EK-numbered no-endpoint flight rejected.
```

- [ ] **Step 2: Run them, verify they fail.**

- [ ] **Step 3: Implement.**

`adsbtrack/config.py`, in the spoof block (:319-343):

```python
    # Flight-scoped gate (issue #22). Day-scoped rejection quarantined real
    # flights that merely transited GPS-jamming corridors on days that also
    # carried ghost broadcasts (A6-EUY 2026-05-19: real DXB-DUS leg ~18-25%
    # sil0 vs ghost fragments 87-100% sil0 at 1,000+ kt implied speed).
    # Reject a flight when its own sil0 share >= spoof_flight_sil0_hard_pct,
    # OR when it is >= spoof_v2_sil0_pct AND the flight contains an inter-fix
    # jump faster than spoof_teleport_speed_kt (no aircraft in this DB's
    # scope sustains 900 kt over ground; Hormuz-style 25-50% sil0 spoofs
    # teleport, jammed-but-real flights do not).
    spoof_flight_sil0_hard_pct: float = 60.0
    spoof_teleport_speed_kt: float = 900.0
```

`adsbtrack/classifier.py`:
- `PointData`: add `adsb_version: int | None = None`, `sil: int | None = None`, `nic: int | None = None` (defaults keep every existing constructor call working).
- `FlightMetrics`: add, with metadata declared beside each field like the neighbors:
  - `v2_samples: int = 0` (`_MERGE_SUM`)
  - `v2_sil0: int = 0` (`_MERGE_SUM`)
  - `v2_nic0: int = 0` (`_MERGE_SUM`)
  - `max_implied_speed_kt: float | None = None` (`_MERGE_MAX`)
- `record_point`: when `point.adsb_version == 2`, increment `v2_samples`, and `v2_sil0` / `v2_nic0` when `sil == 0` / `nic == 0` (identical predicate to `integrity.count_v2_integrity`). For implied speed: at the site where consecutive-point distance is already computed for `path_length_km`, when `dt >= 10.0` seconds compute `speed_kt = dist_km / (dt / 3600.0) / 1.852` and fold into `max_implied_speed_kt`.

`adsbtrack/parser.py`:
- `_extract_point_fields`: inside the `if detail:` block, extract `version` / `sil` / `nic` (ints only, `isinstance(v, int)` guard) into the new PointData fields.
- Replace `_flight_is_spoofed` with the metrics-driven version:

```python
def _flight_is_spoofed(flight: Flight, metrics: FlightMetrics, config: Config) -> tuple[str, dict] | None:
    """Return ``(reason, detail)`` when a flight should be rejected.

    Flight-scoped bimodal-integrity gate (two tiers, see Config comment and
    issue #22), then the unchanged crude EK-callsign heuristic. Day-level
    pooling no longer rejects flights - it survives only as the events-layer
    detector (events._detect_spoof_events).
    """
    v2 = metrics.v2_samples
    if v2 >= config.spoof_min_v2_samples:
        sil_pct = 100.0 * metrics.v2_sil0 / v2
        teleport = metrics.max_implied_speed_kt
        hard = sil_pct >= config.spoof_flight_sil0_hard_pct
        corroborated = (
            sil_pct >= config.spoof_v2_sil0_pct
            and teleport is not None
            and teleport > config.spoof_teleport_speed_kt
        )
        if hard or corroborated:
            return "bimodal_integrity", {
                "scope": "flight",
                "date": flight.takeoff_date,
                "v2_samples": v2,
                "v2_sil0_pct": round(sil_pct, 2),
                "v2_nic0_pct": round(100.0 * metrics.v2_nic0 / v2, 2),
                "max_implied_speed_kt": round(teleport, 1) if teleport is not None else None,
                "trigger": "hard_sil0" if hard else "sil0_plus_teleport",
            }
    cs = (flight.callsign or "").strip()
    if (
        flight.max_altitude is not None
        and flight.max_altitude < config.spoof_crude_max_altitude_ft
        and flight.origin_icao is None
        and flight.destination_icao is None
        and _EK_FLIGHTNUM_RE.fullmatch(cs)
    ):
        return "crude_heuristic", {
            "max_altitude": flight.max_altitude,
            "callsign": cs,
            "pattern": r"^EK\d+$",
        }
    return None
```

- In the extract path, remove the `pool_spoof_scores` call and the day-scores plumbing; the gate site must receive each flight's `FlightMetrics` (the flight/metrics pairs exist through stitching - thread them to wherever the gate is consulted; if the current gate site only has `Flight` objects, move the gate to where pairs are still available).
- `pool_spoof_scores` docstring: update the "shared by" sentence to name only the events layer. The function body is unchanged.
- `docs/features.md`: rewrite the spoofed-broadcast section: flight-scoped two-tier gate (thresholds by name), crude gate unchanged, day-level pooling now events-only, `reason_detail.scope == "flight"` marks new-style rows (old day-scoped rows remain in existing DBs and are still valid history).

- [ ] **Step 4: Run new tests + full suite.**
- [ ] **Step 5: Commit**, message referencing #22, with the session trailer.

Post-merge calibration (controller runs this, not the implementer, per the CLAUDE.md three-aircraft rule): re-extract 896483 (mixed spoof day - expect real legs in flights, ghosts quarantined), 43c556 (all-real - expect zero quarantined), ad3f65 (817-flight corpus - expect no new quarantines) on a scratch copy of those hexes' trace rows. Never against the live DB in-place.

---

### Task 5: forensics module + `inspect` command (#23)

**Files:**
- Create: `adsbtrack/forensics.py`
- Modify: `adsbtrack/cli.py` (new `inspect` command)
- Modify: `README.md` (command list + short section), `CLAUDE.md` (commands block + architecture line)
- Test: `tests/test_forensics.py` (new), `tests/test_cli.py` (CLI smoke)

**Interfaces:**
- Consumes: `db.decode_trace_json`, `integrity.count_v2_integrity`'s predicate, `geo.haversine_km`, the `airports` table (`SELECT latitude_deg, longitude_deg FROM airports WHERE ident = ?`).
- Produces: pure functions over decoded rows (testable without a DB): `split_fragments`, `summarize_fragments`, `squawk_timeline`, `callsign_timeline`, `closest_approach`.

**Controller ruling:** the module is named `forensics.py`, NOT `inspect.py`, to avoid shadowing the stdlib `inspect` module in tooling.

- [ ] **Step 1: Write failing tests** in `tests/test_forensics.py` (build inputs with the same shapes as `tests/test_parser.py`'s `_make_trace_point`/`_make_trace_row`; import or replicate those helpers):

```python
def test_split_fragments_breaks_on_gap():
    # Points at offsets 0, 60, 120, then 1000 (gap 880 s > 300 s): two fragments.

def test_summarize_fragments_counts_integrity_and_identity():
    # A fragment whose points carry {"version": 2, "sil": 0, "nic": 0, "flight": "TEST1",
    # "squawk": "7700"} details reports v2/sil0/nic0 counts, callsign and squawk sets,
    # and alt/gs min-max.

def test_squawk_timeline_reports_change_points_only():
    # squawks 1200, 1200, 7700, 7700, 1200 -> [(t0, "1200"), (t2, "7700"), (t4, "1200")].

def test_closest_approach_finds_minimum_distance_point():
    # Track passing a known point: closest_approach returns the nearest fix's
    # distance (verify against haversine_km), timestamp, and altitude.

def test_inspect_cli_renders_and_json_round_trips(tmp_path):
    # Seed a tmp DB with one trace day (insert_trace_day), run
    # `inspect --hex aaaaaa --date ... --json`: exit 0, json.loads succeeds, one source
    # entry with >= 1 fragment. Non-json run: output contains the fragment table header.
```

- [ ] **Step 2: Run them, verify they fail.**

- [ ] **Step 3: Implement `adsbtrack/forensics.py`:**

```python
"""Day-level trace forensics behind the `inspect` CLI command.

Pure functions over decoded trace rows (list-of-points readsb format), so
tests drive them with synthetic data and the CLI stays a thin renderer.
The "what happened here" loop for one aircraft-day: fragment table,
integrity stats, squawk/callsign timeline, closest approach to a fix.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field

from .geo import haversine_km

DEFAULT_FRAGMENT_GAP_SECS = 300.0


@dataclass
class FragmentSummary:
    source: str
    start_ts: float
    end_ts: float
    n_points: int
    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float
    alt_min: float | None
    alt_max: float | None
    gs_min: float | None
    gs_max: float | None
    v2_samples: int
    v2_sil0: int
    v2_nic0: int
    callsigns: list[str]
    squawks: list[str]
    position_sources: dict[str, int] = field(default_factory=dict)
```

`split_fragments(trace, base_ts, gap_secs)` walks points (skipping non-list rows), starting a new fragment when the inter-point gap exceeds `gap_secs`, returning lists of `(abs_ts, point)` pairs. `summarize_fragments(source, base_ts, trace, gap_secs)` builds a `FragmentSummary` per fragment: altitude min/max from numeric `point[3]` values only (the string `"ground"` counts as 0 for min and is excluded from max), gs from `point[4]`, integrity via the same predicate as `integrity.count_v2_integrity` (version == 2 on `point[8]` dicts, sil/nic == 0), callsigns/squawks from the detail dict, position source from `point[9]` falling back to `detail["type"]`. `squawk_timeline(base_ts, trace)` and `callsign_timeline(base_ts, trace)` return change points only. `closest_approach(base_ts, trace, lat, lon)` returns `(dist_km, ts, alt)` of the minimum-distance fix, or None for an empty trace.

CLI command in `adsbtrack/cli.py`:

```python
@cli.command("inspect")
@click.option("--hex", "hex_code", callback=_validate_hex, help="ICAO hex code")
@click.option("--tail", "tail_number", help=TAIL_HELP)
@click.option("--date", "date_str", required=True, help="Day to inspect (YYYY-MM-DD)")
@click.option("--source", default=None, help="Limit to one source (default: every source with data)")
@click.option("--gap-secs", default=None, type=float, help="Fragment split gap in seconds (default 300)")
@click.option("--airport", default=None, help="Airport ident for closest-approach (e.g. EGOV, KTYS)")
@click.option("--json", "as_json", is_flag=True, help="Machine-readable output")
@_db_option
def inspect_cmd(hex_code, tail_number, date_str, source, gap_secs, airport, as_json, db_path):
    """Forensic view of one aircraft-day: fragments, integrity, squawks."""
```

Renders one Rich table per source (columns: FRAG, START Z, END Z, PTS, FROM, TO, ALT, GS, V2/SIL0/NIC0, CS, SQ, SRC-MIX), then the squawk/callsign timelines, then the closest-approach line when `--airport` is given (resolve the ident from the airports table; `click.UsageError` for an unknown ident; call `ensure_airports(db, config)` first like `fetch` does). `--json` emits `{"hex":..., "date":..., "sources": {src: [fragment dicts...]}, "squawk_timeline": [...], "callsign_timeline": [...], "closest_approach": {...} | null}` via `json.dumps(dataclasses.asdict(...))`. When no trace rows exist for the hex/date, print a clear message and exit 0 (json: empty sources object).

Docs:
- `README.md`: add `inspect` to the command list with a one-line description and a short usage example under a "Day forensics" heading.
- `CLAUDE.md`: add the command line to the Commands block and `adsbtrack/forensics.py -- day-level fragment/integrity/squawk forensics (inspect command)` to the architecture section.

- [ ] **Step 4: Run new tests + full suite.**
- [ ] **Step 5: Commit**, message referencing #23, with the session trailer.
