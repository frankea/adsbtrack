# App Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement all 40 findings from the 2026-08-18 full-app review: data-integrity fixes, pipeline correctness, DB/TUI/GUI performance, CLI UX, and two large parser refactors.

**Architecture:** Fixes land as small, independently testable tasks grouped by file to avoid cross-task conflicts. Schema-touching tasks (11-13) run adjacently and end with a migration-reviewer audit. The two large refactors (extract_flights decomposition, incremental extraction) come last, on top of the corrected small pieces.

**Tech Stack:** Python 3.12+, sqlite3, click, rich, httpx, shapely, textual (tui extra), pytest, ruff, mypy.

**Spec:** `notes/adsbtrack_app_review_2026-08-18.md` - the review report. Finding IDs (A1, P8, U5, F-items...) used below refer to that file. Read the referenced finding before implementing its task.

## Global Constraints

- Line length 120; regular hyphens, never em dashes, in all text output.
- New thresholds go in `adsbtrack/config.Config`, never inline constants.
- Schema changes touch seven files: CREATE TABLE, migration list, `_migrate_drop_*` (for DROPs), INSERT column list + VALUES placeholders, `Flight` dataclass, `docs/schema.md`, legacy-fixture round-trip test.
- `FlightMetrics.max_altitude` / `max_gs_kt` stay `@property` accessors; never re-add as fields.
- GUI renderer never writes untrusted strings through `innerHTML`; DOM via `createElement` + `textContent` only (guarded by `tests/test_gui_export.py::test_export_app_js_uses_safe_dom_construction`).
- Before every commit: `uv run ruff check . && uv run ruff format --check . && uv run pytest`.
- Tests always use tmp-path databases. NEVER open, migrate, or optimize the user's live `adsbtrack.db`.
- TDD: write the failing test first for every behavior change. Behavior-preserving refactors instead prove the full suite green before and after.
- Commit after each task with a concise "why"-focused message.

---

### Task 1: Fetch stops losing error-exhausted days; failed days reported (A1, U6)

**Files:**
- Modify: `adsbtrack/db.py:774-781` (`get_fetched_dates`)
- Modify: `adsbtrack/fetcher.py` (surface failed days in stats; see fetcher.py:316-329, 388-393)
- Modify: `adsbtrack/cli.py:254-259` (fetch summary)
- Test: `tests/test_db.py`, `tests/test_fetcher_async.py`, `tests/test_cli.py`

**Interfaces:**
- Produces: `get_fetched_dates(icao, source)` excludes dates whose only fetch_log rows carry retryable statuses (403, 429, >=500). `fetch_traces` result dict gains `"failed_days": list[tuple[str, int]]` (date, terminal status).

**Requirements:**
1. Read `insert_fetch_log` and the writer paths at `fetcher.py:316-329` first to learn exactly which statuses get logged for exhausted days. A date counts as fetched only if it has at least one fetch_log row with a success status (200, or the no-data status the code uses for empty days - determine it from the code). Dates whose rows are all 403/429/>=500 must be returned by future fetches.
2. `fetch_traces` collects (date, status) for every day that exhausts retries and returns them in the stats dict as `failed_days`.
3. The CLI fetch summary prints failed dates with their statuses after the counters, e.g. `Failed days (will retry on next run): 2026-05-02 (403), 2026-05-03 (503)`. Print nothing extra when empty.

- [ ] **Step 1:** In `tests/test_db.py`, add a failing test: insert fetch_log rows for one icao/source - date A with status 200, date B with status 403 only, date C with status 200 for a different source. Assert `get_fetched_dates(icao, source)` returns {A} only. Add a second test: date D with a 403 row AND a later 200 row is included (a retried-and-succeeded day counts).
- [ ] **Step 2:** Run the new tests, verify they fail.
- [ ] **Step 3:** Implement the status filter in `get_fetched_dates` (SQL, not Python filtering - e.g. `GROUP BY date HAVING SUM(CASE WHEN status ... THEN 1 ELSE 0 END) > 0`).
- [ ] **Step 4:** In `tests/test_fetcher_async.py`, extend the exhaustion-path test (find the existing 403/429 exhaustion tests) to assert `stats["failed_days"]` contains the exhausted date and status. Implement collection in the fetch loop.
- [ ] **Step 5:** In `tests/test_cli.py`, add a test that a fetch whose mocked stats include failed_days prints the dates and statuses; and a fetch with empty failed_days does not print the header. Implement in cli.py.
- [ ] **Step 6:** Full check suite, then commit.

---

### Task 2: Hex validation, --tail on every command, credentials test isolation (A9, U1, A3)

**Files:**
- Modify: `adsbtrack/cli.py` (`_resolve_hex` at :45-60, per-command options at :274, :364, :587, :1292, :1376)
- Test: `tests/test_cli.py` (including the failing `test_acars_cli_errors_without_api_key` at :482)

**Interfaces:**
- Produces: `_validate_hex(value: str) -> str` in cli.py - lowercases, strips, validates `^[0-9a-f]{6}$`, raises `click.BadParameter` otherwise. All `--hex` options funnel through it.

**Requirements:**
1. Every command taking `--hex` validates via `_validate_hex` (use a click callback so it applies uniformly).
2. `extract`, `trips`, `status`, `gaps`, `events` gain `--tail` and resolve through `_resolve_hex_db` exactly like `fetch` does; `links` and `route` switch from `_resolve_hex` to `_resolve_hex_db`. Exactly one of `--hex`/`--tail` must be given where `--hex` was previously required.
3. Fix `test_acars_cli_errors_without_api_key`: it currently fails on machines with a real `credentials.json` in the CWD because `_load_airframes_api_key` (cli.py:295-313) falls back to `Config.credentials_path` relative to CWD. Isolate with `runner.isolated_filesystem()` (or monkeypatch a tmp `Config.credentials_path`). Add the inverse test: with an isolated cwd containing `credentials.json` holding `{"airframesApiKey": "k"}`, the loader returns "k" with the env var unset.

- [ ] **Step 1:** Failing tests: `fetch --hex zzz999` and `trips --hex abc12` exit nonzero with a message naming the expected format; `trips --tail N512WB` resolves (seed the tmp DB registry the same way existing `fetch --tail` tests do); `status --tail ...` likewise.
- [ ] **Step 2:** Verify failures, implement `_validate_hex` + option plumbing.
- [ ] **Step 3:** Fix the credentials test isolation both directions as above; run `uv run pytest tests/test_cli.py -v` until green.
- [ ] **Step 4:** Full check suite, commit.

---

### Task 3: fetch resume story and runtime default start; ADSBTRACK_DB env var (U5, U7-part)

**Files:**
- Modify: `adsbtrack/cli.py:157` (start default), the shared `--db` option on every command
- Test: `tests/test_cli.py`

**Interfaces:**
- Produces: `fetch --since-last` flag; `--db` honors `envvar="ADSBTRACK_DB"` everywhere.

**Requirements:**
1. Add `--since-last` to `fetch`: start from `MAX(date) + 1 day` in fetch_log for this hex+source (across the success-filtered dates from Task 1). Error cleanly if no prior fetches exist.
2. When neither `--start` nor `--since-last` is given AND fetch_log has rows for this hex+source, behave as `--since-last` and print one line saying so (`Resuming from 2026-05-04 (last fetched day; pass --start to override)`).
3. Replace the frozen `2025-01-01` default: when there is no prior fetch and no `--start`, default to January 1 of the previous calendar year, computed at runtime. Update the option help text.
4. Add `envvar="ADSBTRACK_DB"` to the `--db` click option. It is defined per-command (~20 sites) - if a shared decorator/constant exists use it; otherwise create one `_db_option` decorator and apply it everywhere so the envvar lives in one place.

- [ ] **Step 1:** Failing tests: (a) fetch with prior fetch_log rows and no --start calls the fetcher with start = last+1 (mock the fetcher, inspect call args, assert the resume line printed); (b) `--since-last` with empty fetch_log exits nonzero; (c) `ADSBTRACK_DB` env var is honored by `status` (CliRunner env= parameter); (d) no-history default start is Jan 1 of last year (freeze via monkeypatched date if needed).
- [ ] **Step 2:** Verify failures, implement, iterate to green.
- [ ] **Step 3:** Full check suite, commit.

---

### Task 4: JSON output for read commands (U2)

**Files:**
- Modify: `adsbtrack/cli.py` (`trips` :384-538, `status` :589-828, `gaps` :1308-1365, `events` :1391-1445)
- Test: `tests/test_cli.py`

**Interfaces:**
- Produces: `--json` flag on trips/status/gaps/events printing a single JSON document to stdout (no Rich markup), datetimes as ISO-8601 strings, `None` as null.

**Requirements:**
1. Build the row dicts once, then branch: `--json` serializes with `json.dumps(..., indent=2)`; default path renders the existing tables unchanged. Do not duplicate query logic between branches.
2. `trips --json`: list of flight objects (the fields already displayed, plus icao and flight id). `status --json`: one object mirroring the printed sections (registration, type, date range, quality counts, utilization, top airports, acars summary when present). `gaps --json` and `events --json`: lists of the row dicts already built.
3. Output must be loadable by `json.loads` in tests - no ANSI codes (route around Rich console or use a plain print).

- [ ] **Step 1:** Failing tests: for each command, run with `--json` on a seeded tmp DB (reuse the seeding helpers already in test_cli.py), `json.loads` the output, assert one known field per command; assert the default (non-json) output still contains the table title it does today.
- [ ] **Step 2:** Verify failures, implement, iterate to green.
- [ ] **Step 3:** Full check suite, commit.

---

### Task 5: Progress reporting: acars, enrich all, multi-source fetch (U3, U4)

**Files:**
- Modify: `adsbtrack/cli.py:221-244` (multi-source fetch), :352-353 (acars), :1215-1221 (enrich all)
- Modify: `adsbtrack/fetcher.py:415-421` (`fetch_traces` Progress ownership)
- Test: `tests/test_cli.py`, `tests/test_fetcher_async.py`

**Interfaces:**
- Consumes: `fetch_acars(..., progress_callback=...)` (acars.py:285) and `enrich_all(..., progress_callback=...)` (hex_crossref.py:402) - already exist; do not change their signatures.
- Produces: `fetch_traces(..., progress=None)` - optional externally owned `rich.progress.Progress`; when passed, fetch_traces adds its own task to it instead of creating one.

**Requirements:**
1. `acars` CLI wires a Rich progress bar via the existing callback, showing flights processed and, in the bar description, the client's `minute_remaining`/`daily_remaining` (airframes.py:72-73) when available.
2. `enrich all` wires a progress bar via its existing callback (hexes processed / total).
3. `fetch --source all`: create ONE `Progress` in the CLI and pass it to every per-source `fetch_traces` call so each source renders as its own task line (Rich Progress is thread-safe for task updates). Print a per-source stats line at the end (`adsbx: 120 fetched, 3 errors ...`) instead of only the summed totals.
4. When `fetch_traces` is called without `progress` (single-source path, TUI, tests), behavior is unchanged: it creates its own Progress exactly as today.

- [ ] **Step 1:** Failing tests: (a) fetcher: `fetch_traces` with an injected fake Progress object registers a task on it and does not construct its own (assert via monkeypatched `rich.progress.Progress`); (b) CLI: `--source all` run with mocked per-source fetch returns per-source lines in output; (c) acars/enrich: callback wiring smoke test - invoke command with mocked pipeline that calls the callback, assert no crash and bar-completed output.
- [ ] **Step 2:** Verify failures, implement, iterate to green.
- [ ] **Step 3:** Full check suite, commit.

---

### Task 6: Config file loader (U7)

**Files:**
- Modify: `adsbtrack/config.py`
- Modify: `adsbtrack/cli.py` (`get_db_and_config`)
- Test: `tests/test_cli.py` or new `tests/test_config.py`
- Modify: `README.md` (short section documenting the file)

**Interfaces:**
- Produces: `Config.load(path: Path | None = None) -> Config` classmethod. Resolution order: explicit `path` arg > `$ADSBTRACK_CONFIG` > `~/.config/adsbtrack/config.toml` > pure defaults.

**Requirements:**
1. TOML keys map 1:1 onto `Config` dataclass field names; values type-checked against the field annotation (int/float/str/bool/Path); unknown keys raise `ValueError` naming the key; partial files override only the named fields.
2. Use stdlib `tomllib`. `Path`-typed fields accept strings and expanduser.
3. `get_db_and_config` in cli.py switches from `Config()` to `Config.load()`. Everything else keeps constructing `Config()` directly (tests, library use) - the loader is opt-in at the CLI boundary.
4. README gains a short "Configuration file" section listing the path resolution order and a 3-line example overriding `airport_match_threshold_km` and `fetch_rate_limit_secs` (use two real field names verified from config.py).

- [ ] **Step 1:** Failing tests: load from a tmp TOML overriding one float and one str field, assert others keep defaults; unknown key raises with the key name in the message; `ADSBTRACK_CONFIG` env var is honored; absent file returns pure defaults.
- [ ] **Step 2:** Verify failures, implement, iterate to green.
- [ ] **Step 3:** Full check suite, commit.

---

### Task 7: Day/night uses per-sample coordinates; circuit detection sees the whole flight (A5, A6)

**Files:**
- Modify: `adsbtrack/features.py:691-720` (`compute_day_night`)
- Modify: `adsbtrack/parser.py:1263,1312-1313` (ILS/pattern call sites), `adsbtrack/ils_alignment.py:215`
- Test: `tests/test_features.py`, `tests/test_ils_alignment.py`

**Requirements:**
1. A5: `compute_day_night`'s per-sample loop evaluates `is_night_at(sample.lat, sample.lon, ...)` per sample (fields exist on `_PointSample`, classifier.py:83-84), not `metrics.last_seen_lat/lon`. Keep the existing `night_flight` definition at features.py:713-720 intact; the loop's counts become meaningful again for long flights.
2. A6: `detect_all_ils_alignments` receives the full point stream. Change ils_alignment.py:215 to accept the samples list as a parameter (signature change: pass `metrics.all_points` from the parser call site at parser.py:1263 and :1312-1313) instead of reading `metrics.recent_points` (a 240-sample deque, ~20 min). Verify `metrics.all_points` is still populated at those call sites (it is cleared later, parser.py:1339 - do not reorder that clear).
3. `pattern_cycles` and `had_go_around` derived from the alignment output now count circuits over the whole flight.

- [ ] **Step 1:** Failing test A5: build metrics whose samples sit at longitude X in daylight but whose last point is at longitude Y in darkness (pick coordinates ~90 degrees apart with a fixed timestamp; solar.py has the primitives); assert the day/night counts reflect per-sample positions.
- [ ] **Step 2:** Failing test A6: construct a synthetic flight with 3 clear ILS-aligned circuit passes spread over more than 240 samples (build on the existing fixture generators in test_ils_alignment.py); assert all 3 detected, where the old code found only the tail ones.
- [ ] **Step 3:** Verify failures, implement both, iterate to green.
- [ ] **Step 4:** Full check suite, commit.

---

### Task 8: One spoof-pooling implementation; traces parsed once per extract (A7, P5)

**Files:**
- Modify: `adsbtrack/events.py:39-43,78-128`, `adsbtrack/parser.py:419-473,430,168` (call sites :651-653, :662-665), `adsbtrack/gaps.py:244-247`
- Test: `tests/test_events.py`, `tests/test_parser.py`, `tests/test_gaps.py`

**Interfaces:**
- Produces: `pool_spoof_scores(rows, config) -> dict[str, SpoofScore]` (module: parser.py or a small shared home - implementer picks the import direction that avoids cycles; events.py and parser.py both call it). A shared `iter_parsed_trace_days(rows)` helper yielding `(row, parsed_trace)` used by parser, gaps, and events so each caller parses each row exactly once.

**Requirements:**
1. Delete `_SPOOF_V2_SIL0_PCT` / `_SPOOF_MIN_V2_SAMPLES` from events.py; both callers read `Config.spoof_v2_sil0_pct` / `spoof_min_v2_samples` (config.py:279-280). The near-duplicate pooling scans (events.py:84-128 vs parser.py:419-473) collapse into one function. Behavior must match the parser's current version where the two differ (the parser one is Config-driven and newer).
2. P5: inside `extract_flights`, each trace_json is `json.loads`-ed exactly once even with `reject_spoofed_flights` on: parse up front, feed the parsed structures to both `_compute_spoof_scores_by_date` (via pool_spoof_scores) and `_merge_trace_rows`.
3. `gaps.detect_gaps` and `events._detect_spoof_events` route through the same parse helper (they run in separate calls so they still parse for themselves - the win there is one shared, tested decoder, which Task 11 extends for compression).

- [ ] **Step 1:** Failing test A7: set a Config override for `spoof_v2_sil0_pct` and assert the events detector's threshold moves with it (today it cannot).
- [ ] **Step 2:** Failing test P5: monkeypatch-count `json.loads` calls during `extract_flights` on a 3-day fixture with spoof checks on; assert exactly 3 (one per row).
- [ ] **Step 3:** Verify failures, implement, iterate to green. Existing spoof tests (test_events.py:357 area, parser spoof-gate tests) must stay green - they pin the shared function's behavior.
- [ ] **Step 4:** Full check suite, commit.

---

### Task 9: FlightMetrics.merge with declared per-field semantics (A2)

**Files:**
- Modify: `adsbtrack/classifier.py` (FlightMetrics), `adsbtrack/parser.py:325-377` (`_stitch_fragments`)
- Test: `tests/test_classifier.py`, `tests/test_parser.py`

**Interfaces:**
- Produces: `FlightMetrics.merge(other: FlightMetrics) -> None` - folds a LATER fragment (`other`) into an EARLIER one (`self`), used by `_stitch_fragments` in place of its ~25 hand-written field merges.

**Requirements:**
1. Every mergeable accumulator on FlightMetrics gets an explicit strategy, declared next to the field definitions (dict of field name -> strategy, or per-field metadata): sum (counters like `data_points`, `other_points`, `adsc_points`), max/min (peaks, floors), union/extend (sets, lists, `squawk_durations`), keep-first (takeoff-side fields: `takeoff_points`, `takeoff_tracks`, and anything derived from the departure), keep-last (landing-side fields). Read every field currently touched in parser.py:325-377 and every accumulator field on FlightMetrics; any field in neither the strategy table nor an explicit exclusion list makes `merge` raise at import or first call - that is the guardrail against the next silently-unmerged field.
2. The bug being fixed: `other_points`, `adsc_points`, `squawk_durations` are never merged today, so stitched flights report `other_pct`/`adsc_pct` (parser.py:1148-1152) with tail-only numerators over whole-flight denominators; and takeoff fields wrongly come from the later fragment. New behavior: those sum/union/keep-first correctly.
3. The dual-track `_raw_*`/`_persisted_*` altitude state and the `@property` accessors are preserved exactly (Global Constraints); merge the underlying raw/persisted fields, never the properties.
4. `_stitch_fragments` shrinks to: decide-to-stitch (unchanged) + `earlier.merge(later)` + the flight-row field fixups that live outside metrics.

- [ ] **Step 1:** Failing test: build two FlightMetrics fragments with known `other_points`, `adsc_points`, `squawk_durations`, `takeoff_points`, `data_points`; merge; assert sums, unions, and keep-first respectively. Second failing test at parser level: a stitched two-fragment flight fixture asserts `other_pct` computed over BOTH fragments (this is the end-to-end pin on the A2 bug).
- [ ] **Step 2:** Guardrail test: monkeypatch-add a fake accumulator field to a FlightMetrics instance-copy and assert merge raises naming it.
- [ ] **Step 3:** Verify failures, implement, iterate. All existing stitching tests in test_parser.py must stay green.
- [ ] **Step 4:** Full check suite, commit.

---

### Task 10: DB open cost, streaming trace reads, generated INSERT (P8, P7, D4)

**Files:**
- Modify: `adsbtrack/db.py:699-721` (init/migrations), :380-389 (indexes), :761-763 (`get_trace_days`), :853-905 (`insert_flight`)
- Modify: `adsbtrack/parser.py:597` (get_trace_days consumer)
- Test: `tests/test_db.py`

**Requirements:**
1. P8: gate the migration battery behind `PRAGMA user_version`. Define `SCHEMA_VERSION` (int) in db.py; on open, if `user_version == SCHEMA_VERSION` skip `_migrate_add_flight_columns` (both calls, :705 and :711), `_migrate_add_source`, `_migrate_drop_callsign_count`, `_migrate_add_v4_columns`, and mil-hex re-seeding; otherwise run them all then stamp. CREATE TABLE IF NOT EXISTS statements still always run (cheap, and new DBs need them before stamping). Every future schema change bumps `SCHEMA_VERSION` - add that to the schema checklist comment in db.py and to `docs/schema.md`.
2. P8: drop `idx_flights_icao_time`, `idx_trace_days_icao_date`, `idx_spoofed_broadcasts_icao_time` (db.py:380-389) - each is an exact prefix of its table's UNIQUE autoindex. Remove the CREATE INDEX statements AND add `DROP INDEX IF EXISTS` migration entries so existing DBs shed them.
3. P7: `get_trace_days` becomes a generator (yield rows from the cursor, ordered by date as today). Update the parser consumer at :597 (and any other caller found by grep) - watch for code that took `len()` or indexed the list.
4. D4: `insert_flight` builds `INSERT INTO flights (...) VALUES (:field, ...)` from `dataclasses.fields(Flight)` with a named-parameter dict; datetime fields serialized to ISO strings in one shim spot. Column order comes from the dataclass; no hand-maintained column list or placeholder block remains. The public signature of `insert_flight` is unchanged.

- [ ] **Step 1:** Failing tests: (a) open a tmp DB twice; monkeypatch-count `_migrate_add_flight_columns` invocations - second open runs zero; (b) after open, `PRAGMA user_version` equals `SCHEMA_VERSION`; (c) a pre-versioned DB (user_version 0 with old schema fixture) still migrates then stamps; (d) dropped indexes absent from `sqlite_master` after open of a fixture DB that had them; (e) `get_trace_days` returns an iterator whose contents equal the old list; (f) insert_flight round-trips a fully populated Flight identically (the existing legacy-fixture round-trip test must stay green - it is the D4 safety net).
- [ ] **Step 2:** Verify failures, implement, iterate to green.
- [ ] **Step 3:** Full check suite, commit.

---

### Task 11: Compressed trace storage (P1)

**Files:**
- Modify: `adsbtrack/db.py:755` (insert path), the shared decoder from Task 8
- Modify: `adsbtrack/tui/queries.py:381-389`, `adsbtrack/events.py` (readers route through the decoder)
- Modify: `docs/schema.md` (trace_json note)
- Test: `tests/test_db.py`

**Interfaces:**
- Produces: `decode_trace_json(blob: bytes | str) -> Any` in db.py - sniffs zlib (first byte 0x78) vs raw JSON (first byte `[` or `{`), returns the parsed structure. `insert_trace_day` stores `zlib.compress(json.dumps(trace).encode(), 6)` as a BLOB.

**Requirements:**
1. New writes compressed; old raw-JSON rows keep working forever via the sniff. Every reader of `trace_json` in the codebase (grep for it: db.py, tui/queries.py, events.py, gaps.py via Task 8's helper, gui_export.py if it reads traces) goes through `decode_trace_json`.
2. NEVER migrate existing rows in this task and never touch a live DB; migration of old rows is Task 12's `db optimize`.
3. sqlite3 must store BLOBs (bytes) not TEXT - verify the column affinity accepts it (it will; document in schema.md that trace_json is raw JSON text in legacy rows and zlib BLOB in new rows).

- [ ] **Step 1:** Failing tests: (a) insert a trace day, read the raw column with plain SQL, assert `bytes` starting with `0x78`; (b) `get_trace_days` returns identical parsed content for a compressed row and a hand-inserted legacy raw-JSON row in the same table; (c) TUI `load_trace_points` works on both row types (extend test_tui_queries.py).
- [ ] **Step 2:** Verify failures, implement, iterate to green.
- [ ] **Step 3:** Full check suite, commit.

---

### Task 12: Materialized per-day integrity stats + `db optimize` (P2)

**Files:**
- Modify: `adsbtrack/db.py` (trace_days columns, insert_trace_day, migration entry, SCHEMA_VERSION bump)
- Modify: `adsbtrack/events.py:78-128`, `adsbtrack/tui/queries.py:239-244` (consume the columns)
- Modify: `adsbtrack/cli.py` (new `db` group, `optimize` command)
- Modify: `docs/schema.md`
- Test: `tests/test_db.py`, `tests/test_events.py`, `tests/test_cli.py`

**Interfaces:**
- Produces: `trace_days` gains `v2_samples INTEGER`, `v2_sil0 INTEGER`, `v2_nic0 INTEGER`, `v2_callsigns INTEGER` (NULL on legacy rows). `adsbtrack db optimize [--vacuum] [--db ...]`: batch-rewrites legacy rows - compresses trace_json (Task 11 codec) and fills the four stat columns - with a Rich progress bar; `--vacuum` runs VACUUM at the end and warns it needs free disk roughly the size of the final DB.

**Requirements:**
1. `insert_trace_day` computes the four counters from the trace it is already holding (same definitions the spoof pooling uses - reuse `pool_spoof_scores`'s counting core so the numbers cannot drift from the detector).
2. The spoof/events path uses the columns via SQL when every row for the aircraft has them non-NULL; any NULL row falls back to the parse-based path for that aircraft (correctness first, speed after optimize). The all-aircraft events view (queries.py:239-244) becomes a single grouped query over the stat columns for fully-optimized aircraft.
3. `db optimize` processes in batches of ~200 rows per transaction, prints progress, is safe to interrupt and re-run (idempotent: skips rows already compressed AND stat-filled).
4. Migration entry + SCHEMA_VERSION bump + docs/schema.md rows for the four columns.

- [ ] **Step 1:** Failing tests: (a) insert_trace_day fills the counters for a fixture trace with known v2/sil0/nic0 composition; (b) events detector on a fully-stat-filled aircraft produces identical results to the parse path (same fixture, both routes); (c) NULL-stat aircraft still gets correct events (fallback); (d) `db optimize` on a tmp DB with legacy rows compresses them, fills stats, and a second run reports zero rows to do.
- [ ] **Step 2:** Verify failures, implement, iterate to green.
- [ ] **Step 3:** Full check suite, commit.

---

### Task 13: extractor_version on every flight row (D1) + schema audit

**Files:**
- Modify: `adsbtrack/parser.py` (EXTRACTOR_VERSION constant, set on every Flight), `adsbtrack/models.py` (Flight field), `adsbtrack/db.py` (CREATE TABLE, migration entry, SCHEMA_VERSION bump; INSERT is generated post-D4 so no column-list edit), `docs/schema.md`
- Test: `tests/test_parser.py`, `tests/test_db.py` (legacy-fixture round-trip)

**Requirements:**
1. `EXTRACTOR_VERSION = 1` module constant in parser.py with a comment: bump on any behavior change to extraction/derivation. Every flight row written by `extract_flights` carries it; legacy rows stay NULL.
2. Follow the seven-touchpoint checklist (Global Constraints). The legacy-fixture round-trip test gains the new column.
3. This is the last schema task: after it is reviewed, the controller dispatches the `migration-reviewer` agent over Tasks 10-13's db.py changes as an extra gate.

- [ ] **Step 1:** Failing tests: extracted flights carry `extractor_version == EXTRACTOR_VERSION`; legacy fixture round-trips with NULL.
- [ ] **Step 2:** Verify failures, implement, iterate to green.
- [ ] **Step 3:** Full check suite, commit.

---

### Task 14: TUI filters stop re-querying per keystroke (P4)

**Files:**
- Modify: `adsbtrack/tui/views/events.py:73-75,108-110`, `adsbtrack/tui/views/flights.py:120-160`, `adsbtrack/tui/views/aircraft.py:126-128`
- Test: `tests/test_tui_queries.py` (query layer) + new `tests/test_tui_views_filtering.py` (pure filter functions)

**Requirements:**
1. In each of the three views, split data fetch from filtering: `refresh_data` queries and caches `self._rows`; `on_input_changed` re-filters the cached rows only (extract the needle-match into a module-level pure function per view so it is unit-testable without Textual).
2. Explicit refresh (set_icao, refresh keybinding) is the only path that re-queries.
3. The pure filter functions are tested directly - no Textual dependency in this task's tests (Pilot coverage arrives in Task 16).

- [ ] **Step 1:** Failing tests for the extracted filter functions (match on the fields the current needle logic matches - read it first).
- [ ] **Step 2:** Verify failures, implement the split in all three views, iterate to green.
- [ ] **Step 3:** Full check suite, commit.

---

### Task 15: TUI queries move off the event loop (P3)

**Files:**
- Modify: `adsbtrack/tui/app.py:71-74`, the `refresh_data` call paths in `adsbtrack/tui/views/` (events.py:74, map.py:467, flights, aircraft, status)
- Test: covered structurally by Task 16's Pilot tests; this task keeps the full suite green

**Requirements:**
1. Wrap view data loads in Textual workers: `@work(thread=True, exclusive=True)` per view (exclusive per group so a stale slow query never overwrites a newer one - use `group=` per view).
2. sqlite3 connections are thread-bound (`check_same_thread=True` default): workers must not use the app's main `Database`. Give the app a `db_factory` (callable returning a fresh `Database` for the same path) and have each worker open/close its own, or maintain a small per-worker connection. WAL mode (db.py:695) already allows concurrent readers.
3. UI updates (table population) happen back on the event loop via `call_from_thread` or by returning results from the worker and handling them in `on_worker_state_changed` - follow Textual's documented worker pattern.
4. The views must show a lightweight loading state while the worker runs (Textual `loading = True` on the widget is sufficient).

- [ ] **Step 1:** Read the current app/view wiring fully. Implement the worker pattern on the events view first (the slowest), run the TUI manually against a seeded tmp DB (`uv run python -m adsbtrack.cli tui --db ...`) to verify no crash and data renders.
- [ ] **Step 2:** Roll the same pattern to flights, aircraft, map, status views.
- [ ] **Step 3:** Full check suite (Textual-less tests must stay green - guard imports as the codebase already does), commit.

---

### Task 16: Textual Pilot smoke tests + CI runs the extras (T1)

**Files:**
- Create: `tests/test_tui_app.py`
- Modify: `.github/workflows/ci.yml`
- Test: the new file is the deliverable

**Requirements:**
1. `pytest.importorskip("textual")` at module top so environments without the tui extra skip cleanly.
2. Async Pilot tests (`app.run_test()`): boot the app against a seeded tmp DB (reuse seeding helpers from test_tui_queries.py); switch to each of the six views via their keybindings; assert no exception and that each view's main widget mounted. One test drives the filter input on the events view and asserts the row count changes (exercises Tasks 14-15 wiring).
3. CI: change the install step to `uv sync --extra dev --extra tui --extra mcp` so these tests and test_mcp.py actually execute in CI. Verify pytest-asyncio (or Textual's pytest plugin) is present for async tests - add to dev extras if needed.

- [ ] **Step 1:** Write the smoke tests; run `uv run pytest tests/test_tui_app.py -v` until green (they are new coverage, not TDD red/green on a behavior change - failures here are real bugs to fix or report).
- [ ] **Step 2:** Update ci.yml. Full check suite, commit.

---

### Task 17: Map view fixes from the branch review (F1-F8)

**Files:**
- Modify: `adsbtrack/tui/views/map.py`, `adsbtrack/config.py` (gap threshold), `adsbtrack/tui/widgets.py` if BrailleCanvas lives there
- Test: `tests/test_tui_queries.py` area or new `tests/test_tui_map.py` (pure helpers)

**Requirements (all from spec section F; read each):**
1. F1: legend computes degrees-per-cell from the actual bbox and canvas size; no hardcoded `0.01°/cell`.
2. F2: empty `alts` renders `alt ground` (or `-`), never `alt 0 .. 0 ft`.
3. F3: LAYERS panel adds an `other` row aggregating sources not in `_SOURCE_LEGEND` so percentages sum sensibly.
4. F4: endpoint label placement never overwrites its own marker cell (clamp-then-truncate instead of writing across it); delete or implement the pass-4 panel-avoidance comment - do not leave a comment describing behavior that does not exist.
5. F5: `_airport_or_coords` catches `sqlite3.Error` only.
6. F6: `_GAP_SECS` moves to `Config` (e.g. `map_trace_gap_secs: float = 60.0`), map.py reads it from its Config instance.
7. F7 cleanups: `BrailleCanvas.line` gains an optional dash pattern (delete `_dashed_line`'s copied Bresenham); a public cell iterator on BrailleCanvas replaces `_compose`'s use of `_bits`/`_colours` privates (delete dead `render()` or make both paths share it); footer legend built from `_SOURCE_LEGEND`; coalesce same-colour runs into one `rich.text.Text` instead of per-character markup; hoist per-render stat/bbox computation into the context object built once per refresh; derive the panel-gate width from the panel constants instead of magic `56`.
8. F8: `_rasterise_trace` early-returns when projection yields no dots for nonempty points.

- [ ] **Step 1:** Failing tests for the pure pieces: scale computation from a known bbox/cell grid; alt-string formatting for empty alts; LAYERS percentage aggregation including unknown sources; label placement geometry (marker cell never overwritten, using the review's arithmetic: 30-col pane, marker col 14, 18-char label).
- [ ] **Step 2:** Verify failures, implement all items, iterate to green. Manually run the TUI map view once against a seeded DB.
- [ ] **Step 3:** Full check suite, commit.

---

### Task 18: GUI bundle works from file:// and is honest about scope (A4, U8, P10)

**Files:**
- Modify: `adsbtrack/gui_export.py` (snapshot builder :103-145, boot :541, map render :816-831, index.html script tags)
- Test: `tests/test_gui_export.py`

**Requirements:**
1. A4: emit `data.js` containing `window.ADSB_DATA = <json>;` loaded via `<script src="data.js">`; boot reads `window.ADSB_DATA` (no fetch). Drop `indent=2`. Keep emitting valid JSON inside the assignment (json.dumps output is a valid JS expression given `</script>` is escaped - escape `</` as `<\/` in the dumped string to be safe).
2. U8: `_build_data_snapshot` exports per-aircraft keyed maps (`flights_by_icao`, `events_by_icao`, `status_by_icao`, spoofs likewise) for every aircraft in the DB, using the same per-aircraft limits `list_flights`/`list_events` already apply; `selectAircraft` renders the selected aircraft's own data. Traces stay focus-aircraft-only (size); the trace tab for other aircraft shows the existing "rerun adsbtrack gui" note.
3. P10: Leaflet map uses `preferCanvas: true`; trace drawn as per-segment-coloured `L.polyline` segments (same source-colour mapping); tooltip markers decimated to at most ~500 sampled points.
4. Safe-DOM rule holds (guard test must stay green). Vendoring Leaflet is OUT of scope - keep the CDN tags.

- [ ] **Step 1:** Failing tests: (a) export writes `data.js` not `data.json`, and index.html references it via script tag before app.js; (b) app.js contains no `fetch(`; (c) snapshot JSON (parse the `window.ADSB_DATA = ` payload) has `flights_by_icao` with entries for two seeded aircraft; (d) app.js uses `preferCavas`... (typo guard: assert the literal `preferCanvas: true` present and `circleMarker` absent from the per-point path or bounded by the decimation constant).
- [ ] **Step 2:** Verify failures, implement, iterate to green. Open the exported bundle from file:// in a browser manually once and confirm it renders (report the result honestly).
- [ ] **Step 3:** Full check suite, commit.

---

### Task 19: Registry refresh is transactional; zip download cached (A8)

**Files:**
- Modify: `adsbtrack/registry.py:322-334,344-359,434-440`
- Test: `tests/test_registry.py`

**Requirements:**
1. `refresh_faa_registry` validates all three files' headers (`_require_headers`) BEFORE truncating anything, then performs truncate + import inside one transaction so a mid-import failure leaves the previous registry intact (sqlite3: just don't commit until the end; ensure no interim commits inside the import helpers - restructure if they commit internally).
2. `download_faa_zip` honors `faa_registry_cache_path` with a max-age (new Config field, e.g. `faa_registry_cache_max_age_hours: float = 24.0` - Config, not inline) matching the OurAirports cache pattern at config.py:231-232; `--force` style bypass not needed (passing `--zip` already covers manual control).

- [ ] **Step 1:** Failing tests: (a) a zip fixture with a broken MASTER header leaves previously imported rows in place and raises; (b) a fresh cache file younger than max-age short-circuits the download (monkeypatch httpx and assert no request); (c) an old cache re-downloads.
- [ ] **Step 2:** Verify failures, implement, iterate to green.
- [ ] **Step 3:** Full check suite, commit.

---

### Task 20: Navaid neighbourhood walk clamped to the loaded set (P9)

**Files:**
- Modify: `adsbtrack/navaid_alignment.py:110-117,139-143`
- Test: `tests/test_navaid_alignment.py`

**Requirements:**
1. When the loaded navaid list is small (pick a threshold via Config, e.g. `navaid_grid_min_count: int = 64`), skip the grid and scan the list directly with the existing per-axis degree gate. Otherwise clamp the walk radius to the bounding box of the loaded navaids rather than the theoretical 500 nm radius.
2. Results must be identical to the current implementation - this is pure perf. Pin with a test comparing old-path/new-path outputs on a fixture flight + navaid set straddling the threshold.

- [ ] **Step 1:** Write the equivalence test against current behavior first (capture expected output), then implement and keep it green.
- [ ] **Step 2:** Full check suite, commit.

---

### Task 21: Shared segment-split core; geo imports come from geo (D3)

**Files:**
- Modify: `adsbtrack/geo.py` (new `split_on_gaps`), `adsbtrack/ils_alignment.py:109-133`, `adsbtrack/navaid_alignment.py:165-185`, `adsbtrack/takeoff_runway.py:97-145`
- Modify: import sites `adsbtrack/navaid_alignment.py:28`, `adsbtrack/features.py:23-24`
- Test: `tests/test_ils_alignment.py`, `tests/test_navaid_alignment.py`, `tests/test_takeoff_runway.py` (existing suites are the behavior pin), plus direct unit tests for `split_on_gaps`

**Requirements:**
1. `split_on_gaps(points, split_gap_secs, min_duration_secs, extra_predicate=None)` (exact signature implementer's choice, but it must express all three current variants) lives in geo.py or a new small `segments.py`; the three detectors keep only their per-point qualification predicates.
2. Behavior-preserving: all existing detector tests green, unchanged.
3. Import hygiene: `_bearing_deg`, `_smallest_angle`, `_haversine_m` are imported from `adsbtrack.geo` everywhere; the re-export chains through ils_alignment/classifier are removed (leave aliases only if external tests import them - check first).

- [ ] **Step 1:** Unit tests for `split_on_gaps` (gap splitting, min-duration filter, extra predicate).
- [ ] **Step 2:** Refactor one detector at a time, running its test file after each.
- [ ] **Step 3:** Full check suite, commit.

---

### Task 22: README, repo hygiene, coverage gaps (T2, T3, T4)

**Files:**
- Modify: `README.md`, `.gitignore`
- Delete: `docs/superpowers/plans/2026-04-16-faa-registry.md` (shipped long ago)
- Test: `tests/test_tui_queries.py`

**Requirements:**
1. T2: README documents the eight missing commands with one short usage block each: `route`, `gaps`, `events`, `mcp-serve`, `tui`, `gui`, `runways refresh`, `navaids refresh`. Mirror the existing README style (short intro sentence, fenced command, one-line options note). Read each command's click help text for accuracy - do not invent options.
2. T3: add `deliverables/` to .gitignore; delete the stale FAA plan file.
3. T4: add tests for the all-aircraft `list_events` path (`icao=None` route through queries.py:239-244 - post-Task-12 both the stats path and fallback) and `list_aircraft`'s row limit (seed >limit aircraft cheaply with direct SQL inserts, assert the cap).

- [ ] **Step 1:** Write the T4 tests (red only if they expose real bugs; otherwise they are pin tests - run and keep green).
- [ ] **Step 2:** README + .gitignore + deletion. Full check suite, commit.

---

### Task 23: Decompose extract_flights (D2)

**Files:**
- Modify: `adsbtrack/parser.py:593-1464`
- Test: existing full suite is the pin, especially `tests/test_parser.py` and `tests/test_analytical_snapshots.py`

**Requirements:**
1. Behavior-preserving refactor of the ~870-line `extract_flights` into module-private stages, each testable without a DB where feasible:
   - `_load_and_merge(db, icao, config) -> merged_days` (registry upsert stays at the top level; day loading, spoof scoring via Task 8's helpers, merge)
   - `_run_state_machine(merged_days, config) -> list[(flight, metrics)]` (state machine + filtering + stitching)
   - `_enrich_flight(flight, metrics, ctx) -> None` (the per-flight enrichment loop body; `ctx` carries the caches at parser.py:997-999 and the db/config handles)
   - `_persist(db, flights, ...) -> None` (DB writes + the post-passes)
2. The ordering constraints currently documented only in comments ("must run AFTER classify_landing", "after derive_all so MIL_FW override is set", "spoof gate after all derivations") become structural: sequence them inside `_enrich_flight`/`_persist` in one obvious place, and keep the comments where a constraint is still non-obvious.
3. No behavior change: `tests/test_analytical_snapshots.py` and the whole parser suite must pass unchanged. If any test output changes, the refactor has a bug - fix the refactor, never the snapshot.
4. Public API (`extract_flights` signature and semantics) unchanged.

- [ ] **Step 1:** Run the full suite green as the baseline. Refactor stage by stage, running `uv run pytest tests/test_parser.py tests/test_analytical_snapshots.py` after each extraction.
- [ ] **Step 2:** Full check suite, commit. (Multiple commits, one per extracted stage, are encouraged.)

---

### Task 24: Incremental extraction (P6)

**Files:**
- Modify: `adsbtrack/parser.py` (`extract_flights` gains `since_date`), `adsbtrack/db.py` (`clear_flights_since`), `adsbtrack/cli.py` (fetch->extract glue, `extract --since`)
- Test: `tests/test_parser.py`, `tests/test_cli.py`, `tests/test_db.py`

**Interfaces:**
- Produces: `extract_flights(db, icao, config, since_date: date | None = None)`; `db.clear_flights_since(icao, boundary_date)` deleting from BOTH `flights` and `spoofed_broadcasts` where takeoff/first-seen date >= boundary.

**Requirements:**
1. Boundary rule: given the earliest newly-relevant date N, walk trace-day coverage backwards from N-1 until a coverage gap of >= `Config.max_day_gap_days` (config.py:253, the state-machine reset boundary) or the start of data; the boundary is the first day after that gap. Process only days >= boundary; delete only flights/spoof rows >= boundary. This guarantees no flight, stitch, or state-machine carryover spans the boundary.
2. Before implementing, VERIFY that assumption in code: confirm `_stitch_fragments` and the turnaround/chaining logic never reach across a >= max_day_gap_days coverage gap. If any linkage can cross it, widen the boundary rule accordingly and document why in the code.
3. `fetch` passes the earliest date it actually inserted this run into the auto-extract call. `extract --since DATE` exposes it manually; `extract --reprocess` still does the full clear+rebuild; plain `extract` with no flags keeps current full behavior.
4. extractor_version guard (Task 13): if any existing flight row for the icao has `extractor_version != EXTRACTOR_VERSION` (or NULL), incremental mode refuses and falls back to full reprocess with a printed one-line reason - never mix algorithm generations within one aircraft.
5. Equivalence is the acceptance bar: for a multi-day fixture, full reprocess vs (extract all, then delete tail flights, then incremental re-extract of the tail) produce byte-identical flight rows (ignoring autoincrement ids).

- [ ] **Step 1:** Failing tests: (a) the equivalence test above on a fixture with a coverage gap in the middle; (b) boundary walker unit test (gap detection across missing days); (c) clear_flights_since clears both tables and only >= boundary; (d) version-mismatch fallback path prints and does full rebuild; (e) CLI glue: fetch with one new day calls extract with that date (mock extract, inspect args).
- [ ] **Step 2:** Verify failures, implement, iterate to green.
- [ ] **Step 3:** Full check suite, commit.
