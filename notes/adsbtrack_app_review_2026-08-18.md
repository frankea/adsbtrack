# adsbtrack app review - 2026-08-18

Full-codebase improvement review: four parallel passes (ingest/CLI, DB/TUI/GUI, extraction pipeline, plus a diff review of the unpushed TUI map work) with every finding verified against source, and the performance items measured against the real 2.2 GB `adsbtrack.db` (64 aircraft, 20,695 trace days, 25,950 flights). Findings are grouped by theme and ordered by priority within each group. Effort tags: S/M/L.

Environment note (not a code finding): this machine has 871 MB free of 460 GB. One command already failed with ENOSPC during this review. Free disk space before doing heavy work; item P1 below would also reclaim ~1.8 GB from the DB itself.

---

## A. Data integrity and correctness

### A1. Error-exhausted fetch days are permanently skipped (S) - highest priority
`adsbtrack/db.py:774-781`, `adsbtrack/fetcher.py:316-329`

`get_fetched_dates` treats any `fetch_log` row as "done" with no status filter, but the writer also logs rows for 403-exhausted and 429/5xx-exhausted days. After one bot-protection episode those days silently vanish forever: the next run counts them as "Skipped (already fetched)" and only manual SQL recovers them. Fix: exclude retryable statuses (403/429/5xx) from the skip set, or add `fetch --retry-errors`.

### A2. Fragment stitching silently skips merging several accumulators (M)
`adsbtrack/parser.py:325-377`

`_stitch_fragments` hand-merges ~25 fields far from their definitions in `classifier.FlightMetrics`, and `other_points`, `adsc_points`, and `squawk_durations` are never merged. Stitched flights therefore compute `other_pct`/`adsc_pct` against a combined `data_points` total while the numerators only cover the tail fragment; `takeoff_points`/`takeoff_tracks` also stay from the wrong fragment. Fix structurally: a `FlightMetrics.merge(other)` next to the field declarations (or per-field merge metadata: sum/max/union/keep-first) so every future accumulator must declare a strategy.

### A3. Test suite is red on this machine: credentials fallback leaks into tests (S)
`adsbtrack/cli.py:295-313`, `adsbtrack/config.py:227`, `tests/test_cli.py:482`

`test_acars_cli_errors_without_api_key` fails locally (`1 failed, 535 passed`): the test unsets `AIRFRAMES_API_KEY` but `_load_airframes_api_key` falls back to `credentials.json` resolved relative to the CWD, and the repo root has a real one. CI passes only because it has no credentials file. Fix the test with `runner.isolated_filesystem()` or by monkeypatching `Config.credentials_path`; consider also anchoring the default credentials path somewhere stable (XDG config dir) instead of the CWD.

### A4. GUI bundle does not actually open from file:// (S)
`adsbtrack/gui_export.py:541` (boot), 8-10 (docstring promise), 66, 233-234, 313-314

`boot()` does `await fetch('data.json')`, which Chrome/Safari/Firefox all block on `file://` pages, so the documented "open index.html directly" flow fails unless the user runs a web server. Emit `data.js` (`window.ADSB_DATA = {...}`) loaded via `<script src>` instead. While there: drop `indent=2` on the snapshot (halves the file), and consider vendoring Leaflet (currently pulled from unpkg, so the map needs network regardless).

### A5. `compute_day_night` evaluates every sample at the landing coordinates (S)
`adsbtrack/features.py:691-720`

The per-sample loop calls `is_night_at` for up to 240 samples but passes `metrics.last_seen_lat/lon` for every one instead of `sample.lat/lon` (which `_PointSample` carries). Since the `night_flight` redefinition the loop's counts only serve as a `total > 0` gate anyway. Either delete the loop or use the per-sample coordinates so the number means something.

### A6. `pattern_cycles` and ILS alignment only see the last ~20 minutes of a flight (S)
`adsbtrack/ils_alignment.py:215`, `adsbtrack/classifier.py:148`, `adsbtrack/parser.py:1263,1312-1313`

`detect_all_ils_alignments` reads `metrics.recent_points` (deque maxlen=240, ~20 min at 5 s cadence), so `pattern_cycles` and `had_go_around` undercount on longer training flights. `metrics.all_points` holds the full stream and is still alive at that call site (cleared later at parser.py:1339); pass it instead.

### A7. Spoof thresholds duplicated and hardcoded in events.py, drifting from Config (S)
`adsbtrack/events.py:39-43,84-128` vs `adsbtrack/parser.py:419-473`, `adsbtrack/config.py:279-280`

The per-day v2/sil0/nic0 pooling scan is duplicated nearly line-for-line, and events.py hardcodes `_SPOOF_V2_SIL0_PCT = 10.0` / `_SPOOF_MIN_V2_SAMPLES = 25` while the parser reads `Config`. Tuning Config silently desynchronizes the events detector from the extraction gate - exactly the drift the CLAUDE.md "thresholds go in Config" rule exists to prevent. Extract one `pool_spoof_scores(rows, config)` used by both.

### A8. `registry update` truncates FAA tables before validating the new download (M)
`adsbtrack/registry.py:434-440,322-334,344-359`

`refresh_faa_registry` truncates and commits before `import_master_from_path` runs `_require_headers`, so FAA schema drift leaves an empty registry until a successful re-run. Validate headers first, or stage into temp tables / one transaction. Also honor `faa_registry_cache_path` with a max-age instead of unconditionally re-downloading the zip (the OurAirports side already has this pattern).

### A9. No ICAO hex validation on any entry point (S)
`adsbtrack/cli.py:45-60,279,349,591`

`--hex` is only lowercased; a typo launches hours of guaranteed-404 fetching at rate-limit pace and pollutes `fetch_log`/`trace_days` under the bogus key. Add a shared `_validate_hex` (`[0-9a-f]{6}`) raising `click.BadParameter` everywhere.

---

## B. Performance and scalability (measured on the real 2.2 GB DB)

### P1. Compress `trace_json` - it is 90% of the database (M)
`adsbtrack/db.py:755` (insert), readers at `db.py:761-763`, `tui/queries.py:381-389`, `events.py:85-87`

Measured: trace_json totals 1,987 MB of the 2.2 GB file (avg 98 KB/row, max 9.7 MB); zlib level 6 compresses a 200-row sample 6.4x. Store zlib BLOBs and sniff on read (raw JSON starts with `[`, zlib with 0x78) so old rows keep working; route all readers through one `_decode_trace()` helper. Shrinks the DB to ~400 MB and cuts read I/O for every consumer. Especially relevant while the machine is nearly out of disk.

### P2. Materialize per-day integrity stats so spoof detection stops re-parsing all JSON (M)
`adsbtrack/events.py:78-128`, `adsbtrack/tui/queries.py:239-244`

The events feed recomputes v2/sil0/nic0 counts by `json.loads`-ing every trace row; the all-aircraft path parses the full ~2 GB (~11 s of json.loads alone, before the per-sample Python loop). Add `v2_samples`, `v2_sil0`, `v2_nic0`, `v2_callsigns` columns to `trace_days`, computed once in `insert_trace_day`, and the detector becomes a cheap GROUP BY.

### P3. TUI runs heavy queries synchronously on the UI thread (M)
`adsbtrack/tui/app.py:71-74`, `adsbtrack/tui/views/events.py:74`, `views/map.py:467`

Every `refresh_data` is a blocking call on the Textual event loop; the all-aircraft events view freezes the app for tens of seconds on this DB. Move query calls into `@work(thread=True)` workers with a per-worker `Database` (WAL already supports concurrent readers; sqlite3 default is check_same_thread=True).

### P4. TUI filter bars re-run the full query on every keystroke (S)
`adsbtrack/tui/views/events.py:73-75,108-110`; same shape in `views/flights.py:120-160`, `views/aircraft.py:126-128`

`on_input_changed` calls `refresh_data`, re-running the full fetch (including P2's trace scan) per keypress even though `_rows` is already cached and the needle match happens in Python afterwards. Split fetch from filter: re-query only on `set_icao`/explicit refresh, filter cached rows on input.

### P5. Every extract JSON-parses each trace day twice; other passes re-parse again (S)
`adsbtrack/parser.py:430` and `:168` (via `:651-653` and `:662-665`); also `gaps.py:244-247`, `events.py:85-87`

With spoof rejection on (default), `_compute_spoof_scores_by_date` parses every `trace_json`, then `_merge_trace_rows` parses the same rows again. Parse once into `(row_meta, parsed_trace)` pairs and feed both consumers; share the helper with gaps/events.

### P6. Incremental extraction instead of full-history reprocess (L)
`adsbtrack/parser.py:593-599`, `adsbtrack/db.py:761-763,785-789`

After a fetch adds one day, `extract_flights` reloads and re-runs the state machine over the entire multi-year history and `clear_flights` rebuilds every row. Per-day fetch state already exists; extraction could restart from earliest-new-date minus `max_day_gap_days` (the state-machine reset boundary) and rebuild only flights at or after it. Stitching and turnaround chaining only look one flight back, so the boundary math is tractable.

### P7. Stream `get_trace_days` instead of `fetchall()` (S)
`adsbtrack/db.py:761-763`, consumer `adsbtrack/parser.py:597`

The biggest aircraft has 2,310 days / 233 MB of raw JSON loaded into memory at once before parsing overhead. Rows are already date-ordered; yield per-day groups from a cursor for bounded memory.

### P8. Gate startup migrations behind `PRAGMA user_version`; drop three redundant indexes (S)
`adsbtrack/db.py:699-721,380-389`

Every `Database()` open runs the ~88-entry ALTER battery twice (lines 705 and 711, each ALTER raising+suppressing), re-executes all CREATEs, and re-seeds mil hex ranges - and the list grows forever. Stamp `user_version` and skip when current. Also `idx_flights_icao_time`, `idx_trace_days_icao_date`, and `idx_spoofed_broadcasts_icao_time` are exact prefixes of their tables' UNIQUE autoindexes (verified both exist in the live DB); drop them.

### P9. Navaid grid walk scans ~437 mostly-empty cells per point (S)
`adsbtrack/navaid_alignment.py:110-117,139-143`, `adsbtrack/parser.py:552`

Default `max_distance_nm=500` makes the neighborhood walk ~19x23 dict lookups per point (a million-plus per long flight), yet candidates were already bbox-prefiltered to the flight envelope +50 nm. Clamp the walk radius to the loaded navaid extent, or below a small `len(nav_list)` skip the grid and loop directly.

### P10. GUI map creates one SVG DOM node per trace point (S)
`adsbtrack/gui_export.py:816-831`

Each point becomes an `L.circleMarker` with tooltip; a 9.7 MB day (~100K points) will lock the browser. Use `preferCanvas: true`, one per-segment-colored `L.polyline`, and decimate tooltip markers.

---

## C. CLI / product UX

### U1. `--tail` accepted inconsistently across commands (S)
`adsbtrack/cli.py:274,364,587,1292,1376`

`fetch` gets the DB-backed resolver, `links`/`route` get the algorithmic one, and `extract`, `trips`, `status`, `gaps`, `events` require `--hex` outright - the most-used read commands are the ones missing it. Route everything through `_resolve_hex_db`.

### U2. No machine-readable output on any read command (M)
`adsbtrack/cli.py:384-538,589-828,1308-1365,1391-1445`

Everything is Rich tables; the only scripting affordance is `links --urls-only`. Add `--json` to `trips`, `status`, `gaps`, `events` (row dicts already exist; mostly a serialization branch).

### U3. `acars` and `enrich all` run for minutes with zero progress output (S)
`adsbtrack/cli.py:352-353,1215-1221`; `adsbtrack/acars.py:285,370-371`; `adsbtrack/hex_crossref.py:402,447-448`

Both pipelines already accept a `progress_callback` (the TUI uses it) but the CLI passes nothing, and the airframes client's tracked `daily_remaining`/`minute_remaining` (airframes.py:72-73) is never displayed. Wire a Rich progress bar into both.

### U4. `fetch --source all` garbles output with concurrent Progress displays (M)
`adsbtrack/cli.py:221-244`, `adsbtrack/fetcher.py:415-421`

Each source thread creates its own Rich `Progress`; Rich supports one live display per terminal. Stats are also summed across sources so failures per source are invisible. Share one `Progress` with a task per source and print a per-source breakdown.

### U5. `fetch --start` hardcodes 2025-01-01; no resume story (S)
`adsbtrack/cli.py:157`

A frozen calendar default ages badly and fires ~600 days of requests for a new aircraft today. Add `--since-last` (or default to `MAX(date)+1` per source when fetch_log has rows) so routine top-ups are argument-free.

### U6. Failed fetch days are not reported (S)
`adsbtrack/cli.py:254-259`, `adsbtrack/fetcher.py:388-393`

The summary is four counters ("Errors: 3") with no which/why; combined with A1 there is no path from the counter to a fixed dataset. Print failed dates + terminal statuses after the summary; show live `current_delay` in the progress bar so backoff-crawling is visibly rate-limited.

### U7. Config is code-only (M)
`adsbtrack/config.py:224-246`, `adsbtrack/cli.py:159-167`

No env vars, no config file; tuning any threshold means editing source. Cheapest first step: `envvar="ADSBTRACK_DB"` on the `--db` option (repeated on ~20 commands). Fuller fix: a `~/.config/adsbtrack.toml` loader mapped onto the dataclass.

### U8. GUI shell pretends to be multi-aircraft but exports one aircraft's data (M)
`adsbtrack/gui_export.py:103-145,635-681`

The left rail lists all 64 aircraft; clicking any renders the focus aircraft's flights under the other aircraft's name. Export per-aircraft keyed maps (single-digit MB at current scale, cheap once P2 lands) or lock the rail to the focus aircraft.

---

## D. Architecture / maintainability

### D1. Stamp `extractor_version` on every flight row (M)
`adsbtrack/db.py:853-875`, `adsbtrack/models.py:232,246,259`

Comments narrate v3-v15 algorithm revisions but rows carry no record of which produced them, so DBs silently mix generations. A module-level `EXTRACTOR_VERSION` written per row enables `extract --stale-only` and version-aware analytics. Follows the seven-file schema checklist (migration-reviewer audits it).

### D2. Decompose the 870-line `extract_flights` (L)
`adsbtrack/parser.py:593-1464`

One function mixes registry upsert, spoof scoring, day merge, the state machine, filtering, stitching, a ~400-line enrichment loop, DB writes, and four post-passes, with ordering constraints living only in comments. Split into `_run_state_machine`, `_enrich_flight`, `_persist` for unit-testability and one obvious seam for new heuristics.

### D3. Share a segment-split core across the three geometric detectors (M)
`adsbtrack/ils_alignment.py:109-133`, `adsbtrack/navaid_alignment.py:165-185`, `adsbtrack/takeoff_runway.py:97-145`

The keep/split-on-gap/min-duration logic is implemented three times. Extract `split_on_gaps(kept, split_gap_secs)` + duration filter, leaving each detector as its per-point predicate. Related hygiene: import geo helpers from `geo.py` directly - `navaid_alignment.py:28` and `features.py:23-24` import `_bearing_deg`/`_haversine_m` through `ils_alignment`/`classifier` re-export chains.

### D4. Build `insert_flight` from `dataclasses.fields(Flight)` with named parameters (M)
`adsbtrack/db.py:853-905`, `adsbtrack/models.py:32`

The 100-column INSERT maintains a hand-aligned column list and visually-counted `?` block; field names already match column names. Named parameters generated from the dataclass eliminate two of the seven schema-change touchpoints and make drift impossible rather than merely audited.

---

## E. Tests, CI, docs, repo hygiene

### T1. The Textual app itself has zero test coverage (M)
`tests/test_tui_queries.py`, `tests/test_mcp.py` only exercise query helpers; nothing imports textual. Six of the last eight commits are TUI work and several are "X not visible / crash" fixes - classic untested-UI churn. Add smoke tests per screen with Textual's `App.run_test()`/Pilot, and a CI leg with `--extra tui --extra mcp` so the app at least imports under CI.

### T2. README omits eight commands (S)
`route`, `gaps`, `events`, `mcp-serve`, `tui`, `gui`, `runways refresh`, `navaids refresh` are all undocumented in README.md - including the TUI/GUI/MCP surfaces that are the project's recent headline work.

### T3. Repo clutter (S)
- `docs/superpowers/plans/2026-04-16-faa-registry.md` (untracked) describes work that shipped long ago; delete or archive.
- `deliverables/` is untracked and not in `.gitignore`; add it (or move it out).

### T4. Coverage gaps flagged by the reviewers (S)
No test for the all-aircraft `list_events` path (the most expensive query in the app) or `list_aircraft`'s 5,000-row limit; A1/U1/U4/U5 behavior changes all need new tests rather than being covered incidentally.

---

## F. Unpushed TUI map work (diff review, `adsbtrack/tui/views/map.py`)

Worth fixing before pushing the branch:

1. **Legend hardcodes `grid 0.01°/cell approx`** (map.py:428) but the projection is bbox-fit, so scale varies ~600x between a transcontinental trace and a helicopter hop. Compute from the actual bbox/cell count or drop the claim.
2. **Ground-only traces print `alt 0 .. 0 ft`** (map.py:291,301): `alt_ft` is None for ground points, so `alts == []` renders as sea level. Show "ground"/"-" like the old strip did.
3. **LAYERS percentages hide unknown/mode_s/other sources** (map.py:268-270): sources missing from `_SOURCE_LEGEND` still count in the denominator, so a trace dominated by them shows near-0% on every visible row. Add an "other" row or exclude unlisted sources.
4. **Endpoint label can overwrite its own marker** (map.py:333-338) when left-placement clamps at col 0; and the pass-4 comment about pushing labels off panels is not implemented (labels write through panel interiors).
5. **Bare `except Exception` in `_airport_or_coords`** (map.py:489): narrow to `sqlite3.Error` so contract breaks surface instead of silently falling back to coords.
6. **`_GAP_SECS = 60.0` inline** (map.py:71) violates the CLAUDE.md thresholds-in-Config rule; Config already houses the sibling gap thresholds.
7. Cleanups: `_dashed_line` duplicates `BrailleCanvas.line`'s Bresenham (add a dash pattern param instead); `_compose` reads `canvas._bits`/`_colours` privates leaving `render()` dead; the footer hand-duplicates `_SOURCE_LEGEND` (third copy of the mapping); per-character Rich markup parses ~10k tags per repaint (coalesce same-colour runs into a `Text`); per-render stat recomputation and triple bbox computation (hoist into `_MapCtx`); `56 == 1+22+32+1` panel-gate magic numbers are interlocked but underived.
8. Latent guard: `_rasterise_trace` IndexErrors if `_project_to_dots` returns `[]` for nonempty points (`dot_w <= 1`); only the caller's `w <= 2` guard prevents it - add an early return.

Judgement call flagged, not counted as a bug: start/end labels use the nearest airport to the first/last received point, so a coverage-truncated cruise trace gets labeled with an overflown airport. Defensible, but decide consciously (altitude is available on `TracePoint` to gate it).

---

## Suggested sequencing

1. **Now (small, high value):** A1 (fetch data loss), A3 (red test), A7 (spoof threshold drift), A5/A6 (wrong-coords day/night, 240-sample window), P4 (keystroke re-query), P8 (user_version + index drop), U5 (start default), U6 (failed-day report), A9 (hex validation), F items before pushing the map branch.
2. **Next (medium):** P1 (trace compression - also solves the disk crunch), P2+P3 (materialized integrity stats + TUI workers - makes the events view usable), A2 (FlightMetrics.merge), A4+U8 (make the GUI bundle actually work), U1-U3, U7, D1 (extractor_version), T1 (TUI tests), T2 (README).
3. **Later (large):** P6 (incremental extraction), D2 (extract_flights decomposition), D3/D4.
