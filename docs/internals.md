# How it works

ADS-B Exchange and sibling trackers store daily trace files for every aircraft they've seen. Each trace is a series of timestamped position reports with lat/lon, barometric altitude, ground speed, vertical rate, and geometric altitude. The fetcher downloads these day by day and stores them in SQLite.

## Async fetcher

`adsbtrack/fetcher.py::fetch_traces` is an async implementation wrapped in a sync-compatible public signature. Bounded concurrency via `asyncio.Semaphore(Config.fetch_concurrency)` (default 4, overridable with `--concurrency` on `fetch`). `concurrency=1` is byte-identical to the old serial path.

**Rate limiting on request starts, not completions.** A single `asyncio.Lock` serializes slot reservations so consecutive HTTP starts are at least `current_delay` seconds apart regardless of how many workers are in flight. Per-worker delays would let N workers each burn through a bucket-worth of requests in parallel and trip the CDN's rate limiter; the shared lock enforces the guarantee the user asked for with `--rate`.

**429 backoff.** On HTTP 429 the worker takes the lock, doubles `current_delay` (capped at `Config.rate_limit_max = 30.0 s`), resets the `successes_since_backoff` counter, and sleeps any `Retry-After` hint inside the lock so queued workers pick up the new delay on their next slot acquisition. After `Config.rate_limit_recovery = 10` consecutive successes the delay halves back down toward `Config.rate_limit`.

**403 circuit breaker.** A per-day outcome map plus a reverse-scan of `sorted_days` trips a `RuntimeError` when the three most-recently-completed days (skipping in-flight ones) all exhausted their retry budget on 403. Any non-403 terminal outcome on an intervening day resets the streak. Without this, a CDN that flips from 429 to 403 (bot-detection escalation) would silently churn through the entire range.

**DB writes serialized via a single `_db_writer` task** draining an `asyncio.Queue`. Each day's `insert_trace_day` + `insert_fetch_log` + `commit` is atomic; SIGINT mid-run either commits both rows or neither. Rich `Progress.advance` runs inside the writer task after commit so the progress bar reflects durable state, never "optimistic" counters from workers that later failed.

### Rate-limit floor is mathematical

Wall time has a hard lower bound from the rate-limit lock alone:

```
min_wall_time = (N_days - 1) * rate_limit + last_request_latency
```

For 50 days at `rate_limit = 0.5`, that's `~25.0 s` at any concurrency. This is not a pathology to debug - it's the guarantee the lock provides. Parallelism can only help when per-request latency exceeds `rate_limit` and workers can overlap in-flight *between* the enforced start gaps. On a CDN with sub-100 ms responses and `rate_limit = 0.5`, `concurrency=1` and `concurrency=4` will produce indistinguishable wall times: rate limit is the binding constraint, not concurrency.

### Benchmark protocol

Use `python -m adsbtrack.bench` (not the CLI's `fetch` command) to time fetching in isolation. The bench module skips `ensure_airports` and the post-fetch auto-extract, prints a self-documenting `[bench]` header with the active config, and reports per-run status histograms plus orphan-200 counts (rows where `fetch_log` recorded a 200 but `trace_days` never saw the insert - the canonical check for DB-consistency bugs under concurrency).

Two-phase pattern so airport download doesn't dominate:

```bash
# 1. Build an airport-populated template DB once.
rm -f bench-airports.db
uv run python -c "
from adsbtrack.db import Database
from adsbtrack.config import Config
from adsbtrack.airports import download_airports
from pathlib import Path
p = Path('bench-airports.db')
with Database(p) as db:
    cfg = Config(db_path=p)
    n = download_airports(db, cfg)
    db.commit()
    print(f'airports loaded: {n}')
"

# 2. Copy the template and bench fetch only, per concurrency.
export HEX=a66ad3 START=2024-06-01 END=2024-07-20 SRC=adsbx  # 50-day window
for C in 1 2 4; do
  cp bench-airports.db bench-c${C}.db
  uv run python -m adsbtrack.bench \
    --hex ${HEX} --start ${START} --end ${END} \
    --source ${SRC} --concurrency ${C} --rate 0.5 \
    --db bench-c${C}.db
done
```

To actually see parallelism help, run a second pass at `--rate 0.1` against a source you know tolerates it. The floor drops from ~25 s to ~5 s and per-request latency has a chance to exceed the rate spacing. If `c=4` beats `c=1` at that rate, concurrency is overlapping real in-flight time; if it doesn't, the source is fast enough that no concurrency can beat the lock. The second outcome is still a merge pass - the infrastructure is correct, just idle on this source - but it's worth knowing which regime you're in.

### Expected wall-time and error-rate behaviour

| Signal | c=1 | c=2 | c=4 | Interpretation |
|--------|-----|-----|-----|----------------|
| Wall time | ~25.0 s | T(c=1) / 1.5 | T(c=1) / 2 | Healthy - concurrency is helping. |
| Wall time | ~25.0 s | ~25.0 s | ~25.0 s | Healthy - rate limit is binding. Effective concurrency is 1 on this source, and lowering `--rate` is the lever if you want speed. |
| Wall time | **< 25.0 s** | any | any | Bug - rate-limit lock is broken. 50 starts at 0.5 s spacing is a mathematical floor at 25.0 s. |
| Wall time | ~25.0 s | > 25.0 s | > 25.0 s | Bug - concurrency is *hurting*, likely lock contention or an implicit 429 storm. |
| 429 count | baseline | <= baseline + 1 | <= baseline + 1 | Healthy - rate limit is respected. |
| 429 count | baseline | > baseline + 3 | any | Bug - rate limit is too loose under concurrency. |
| 403 count | baseline | > baseline | any | Bug - the CDN is starting to distinguish bot traffic. Back off `--concurrency` or `--rate`. |
| Orphan 200s | 0 | 0 | 0 | Prerequisite for a clean run. Anything non-zero means DB-writer task died or SIGINT recovery broke. |

### SIGINT resilience

The writer task pattern is the reason SIGINT is safe. The protocol is: send SIGINT, the workers cancel, the writer drains its queue and commits whatever rows it has, and the next run with the same args picks up from the committed state via `get_fetched_dates`. Verify with:

```bash
cp bench-airports.db bench-sigint.db
uv run python -m adsbtrack.bench \
  --hex ${HEX} --start 2024-06-01 --end 2024-06-20 \
  --source ${SRC} --concurrency 4 --rate 0.5 --db bench-sigint.db &
PID=$!
sleep 5
kill -INT ${PID}
wait ${PID} 2>/dev/null

# Orphan check (the bench script does this on success; run it by hand on cancel):
sqlite3 bench-sigint.db \
  "SELECT COUNT(*) FROM fetch_log f WHERE f.status = 200
   AND NOT EXISTS (SELECT 1 FROM trace_days t
                   WHERE t.icao = f.icao AND t.date = f.date AND t.source = f.source);"
# Must print 0.

# Resume must pick up where it left off without re-fetching committed days.
uv run python -m adsbtrack.bench \
  --hex ${HEX} --start 2024-06-01 --end 2024-06-20 \
  --source ${SRC} --concurrency 4 --rate 0.5 --db bench-sigint.db
```

## Trace merging

When multiple data sources are fetched for the same aircraft, traces are merged by absolute timestamp and deduplicated (points within 1 second and 0.001 degrees of each other are collapsed). Different receiver networks catch different points for the same flight, so combining them improves coverage.

The single-source path runs through the same sort + dedupe pipeline, so trace files that contain out-of-order or "phantom" points (cache glitches in readsb's `trace_full` output that occasionally write prior-day leakage with deeply negative offsets) are reordered into chronological order before the state machine sees them. Without this, a phantom point can overwrite the pending flight's `last_point_ts` with a timestamp earlier than its `first_point_ts` and produce a negative `duration_minutes`.

## Flight extraction

The extractor walks through the merged trace points in chronological order and runs a state machine (`None` -> `ground` -> `airborne` -> `post_landing` -> `ground`...) that detects takeoff and landing transitions.

Ground-vs-airborne for each point is decided by fusing:

- **Barometric altitude** (the `'ground'` sentinel or an int in feet)
- **Geometric altitude** (GPS-derived, in feet)
- **Ground speed** in knots
- **Barometric vertical rate** in ft/min

This catches tricky cases the old "baro says ground" heuristic missed:

- **Baro encoder errors** on helicopters like the Bell 407: the barometric encoder frequently reports `'ground'` while the aircraft is hovering at 300-500 ft AGL. Geometric altitude disagrees, so the classifier treats the point as airborne and flags the flight as `altitude_error` when the ratio gets high enough.
- **Speed override**: a `'ground'` altitude at flight speed (>80 kt) is always a glitch, not a landing.
- **OpenSky data with no ground speed**: requires two consecutive ground points before confirming a landing, to avoid false touchdowns from altitude glitches.

Additional state machine behaviors:

- **Intra-trace gap splitting**: any gap longer than 30 minutes between consecutive points (absolute value - a backwards-in-time jump also triggers a close) finalizes the pending flight.
- **Post-landing window**: after a ground transition, the flight stays "open" for up to 60 seconds or 5 more ground points to collect landing-quality metrics.
- **Touch-and-go detection**: an airborne point inside the post-landing window finalizes the current flight and immediately opens a new one.
- **Short-movement filter**: flights shorter than 5 minutes that travel less than 5 km are filtered out as taxi movements. Single-point "flights" left over from phantom trace points are also dropped.

## Fragment stitching

After extraction, a post-processing pass walks each aircraft's flights chronologically and merges pairs where a previous flight ended without a landing and the next flight starts with `takeoff_type = found_mid_flight`, within:

- **Type-endurance-aware time gap**: `max(stitch_max_gap_minutes, endurance_for(type_code) * stitch_endurance_ratio)`. The default `stitch_max_gap_minutes` is 90, which is the right window for light GA. For long-endurance types that regularly have multi-hour coverage gaps during one operational mission (KC-135R at 720 min, KC-46 at 780 min, C-5M at 900 min, GLF6 at 900 min, etc.), the effective window scales up automatically. With the default `stitch_endurance_ratio = 0.4`, a KC-135R gets a 288-minute stitch window while a Cessna 172 stays at 96 minutes. Without this scaling, a tanker orbit over restricted airspace shows up as two signal-lost fragments instead of one continuous flight.
- **Great-circle distance** less than `cruise_speed * time_gap * 1.2` (with 300 kt as the upper bound)
- **Altitude delta** under 3000 ft

The stitched flight inherits the original takeoff position and time, which recovers the actual origin airport for flights that would otherwise be classified as mid-flight fragments. Duration is recomputed after merging so the wall-clock span covers the coverage gap.

## Altitude cross-validation

`max_altitude` uses a three-layer defence against corrupt altitude spikes from pressure-datum swaps and geometric-altitude errors:

1. **AP-validated persistence filter.** Only samples where `nav_altitude_mcp` (autopilot target) is present AND agrees with the candidate altitude (within 5,000 ft) enter the persisted peak tracker. Squawk is not used -- it's always present on operating transponders and does not correlate with altitude validity. On B748 06a1e4, the correlation was perfect: 4/4 ceiling violations had squawk set but AP NULL; 198/198 normal flights had AP set.

2. **Raw fallback.** Flights without AP data fall back to the raw max (all samples tracked unconditionally). This ensures every flight gets a max_altitude even with sparse mode-S data.

3. **Per-type ceiling cap.** `TYPE_CEILINGS` maps each type code to its book service ceiling. Flights with coherent AP data (AP present and agreeing with max_altitude within 5,000 ft) get 10% tolerance; flights without coherent AP cap at exactly the book ceiling. This prevents uncorroborated spikes from exceeding physical limits while preserving legitimate high-altitude operations.

The same persistence filter pattern applies to `max_gs_kt`, with per-type caps from `TYPE_MAX_GS`.

## Helipad clustering and naming

After flight extraction, landing coordinates that don't match any airport are clustered using DBSCAN (200 m epsilon) to identify recurring helipad sites. Each cluster becomes a row in the `helipads` table with a centroid and landing count.

Helipad names are enriched in two passes: (1) an OurAirports CSV join that matches helipad centroids against `type='heliport'` entries within 500 m, and (2) a manual override dictionary for known facilities not in external databases (hospitals, HEMS bases, international helipads). The enrichment runs once per `extract` call and only updates helipads that still have generic names.

Flight-helipad linkage (`origin_helipad_id`, `destination_helipad_id`) is back-filled after all flights are inserted, matching takeoff/landing coordinates against helipad centroids within 200 m.

## Airport matching

Origin and destination are matched via a bounding-box query against the OurAirports database (~47k airports) followed by haversine distance calculation. Origin/destination ICAO codes are only populated when the match is within 2 km (on-field). Farther hits (up to 10 km) populate diagnostic `nearest_origin_icao` / `nearest_destination_icao` columns so the information isn't lost but helicopters and offshore ops don't get false-attributed to nearby civil airports. Matches are skipped entirely for `signal_lost` and `dropped_on_approach` flights.

## FAA registry parser

`registry update` downloads `ReleasableAircraft.zip` from `registry.faa.gov` (Akamai bot-managed; uses `curl_cffi` with `impersonate="chrome"` when the `faa` extra is installed, else falls back to httpx and usually 503s), extracts the three target files by basename (handling both flat and nested zips), and bulk-imports inside a single transaction.

Each file has a distinct schema:

* **MASTER.txt** -- 34 columns including 5 `OTHER NAMES(1..5)` slots and a precomputed `MODE S CODE HEX` column. The parser prefers the file-supplied hex and falls back to converting the octal `MODE S CODE`.
* **DEREG.txt** -- different column set (dash-separated names, separate MAIL / PHYSICAL addresses, `CANCEL-DATE` instead of `EXPIRATION DATE`, no `TYPE AIRCRAFT` / `TYPE ENGINE`). We project it onto the `faa_deregistered` schema by preferring PHYSICAL addresses with MAIL fallback and mapping CANCEL-DATE -> expiration_date.
* **ACFTREF.txt** -- manufacturer/model lookup keyed on `CODE`.

All files ship with a UTF-8 BOM on the first byte and latin-1 / cp1252 owner names. The parser decodes as latin-1 (lossless) and strips the BOM manually.

Each import validates the file's header line against a required-columns list before reading rows; a drift in column names fails fast with a clear error message rather than silently producing all-NULL rows.

## Hex cross-reference merge

`enrich all` walks every ICAO present in `trace_days` or `flights` that's missing from `hex_crossref` and merges three sources in preference order:

1. **FAA registry** -- authoritative for N-numbered civil aircraft, read from `faa_registry` by hex.
2. **Mictronics DB** -- community-maintained; bulk JSON files (`aircrafts.json`, `types.json`, `operators.json`) cached under `.cache/mictronics/` and kept in memory for the duration of the enrich loop.
3. **hexdb.io REST API** -- live fallback, per-minute throttled, treats both HTTP 404 and 200-with-`{status: "404"}` bodies as misses.

Conflicts (differing registrations or type codes between sources) are reported in the return value but don't block the write. An independent check against `mil_hex_ranges` runs on every hex and stamps `is_military` / `mil_country` / `mil_branch` regardless of which civilian source supplied the row, so e.g. a hex with a Mictronics registration can still be flagged as military when it sits in a DoD allocation block.

## SQL in docs

Any committed document containing SQL that queries the adsbtrack schema must execute cleanly against the current schema at commit time. "Renders as markdown" is not enough; "parses as SQL" is not enough. The query has to run.

The canonical case today is `docs/datasette-metadata.json`, which ships five pre-canned queries that Datasette loads into its web UI. These queries use specific column names (`emergency_flag`, `signal_gap_secs`, `destination_helipad_id`, etc.) and specific enum values (`mission_type = 'offshore'`, `landing_type = 'confirmed'`). Hallucinated column names or enum values are invisible at review time and only surface the first time a reader actually clicks the canned query, where they produce SQL errors that look like the project is broken.

The check is simple: before merging any doc change that adds or modifies SQL against the schema, run it against a real `adsbtrack.db` with representative data. If there's no convenient way to do that, write a pytest that parses the SQL out of the doc, runs it against a fixture DB, and asserts no error. Either works. Silent drift of column names between `adsbtrack/db.py` and docs is the failure mode we're ruling out.
