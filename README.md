# adsbtrack

Pull historical ADS-B trace data from multiple tracking networks and turn it into a structured flight history for any aircraft. Give it an ICAO hex code and a date range and it will fetch every day of trace data, extract individual flights, match takeoff/landing coordinates to airports, classify each landing by quality, and give you travel pattern statistics.

Built for OSINT and aviation nerds who want to go beyond live tracking and dig into where an aircraft has actually been over months or years - with enough signal quality metadata to trust the answers.

## Install

Requires Python 3.12+. Using [uv](https://github.com/astral-sh/uv):

```
git clone https://github.com/frankea/adsbtrack.git
cd adsbtrack
uv sync
```

## Usage

### Fetch trace data

```
uv run python -m adsbtrack.cli fetch --hex a66ad3 --start 2020-01-01
```

Downloads daily traces, then auto-extracts flights. Options: `--source` (adsbx, adsbfi, airplaneslive, adsblol, theairtraffic, opensky, all), `--end`, `--rate`, `--concurrency`, `--db`, `--tail` (converts N-number to hex), `--include-unhealthy` (with `--source all`, fetch every readsb source even if its recent history looks dead; ignored for a single named source). Skips dates already fetched - days that only ever failed with retryable errors (403/429/5xx) do not count and are retried on the next run. Omitting `--start` auto-resumes from the day after the last fetched day; under `--source all` each source resumes from its own last success, capped at 90 days behind (`Config.resume_max_lookback_days`, with a warning if clamped). Pass `--since-last` to require resume behavior explicitly (errors if there's no fetch history yet). WAL mode lets multiple fetches run in parallel.

### View statistics

```
uv run python -m adsbtrack.cli status --hex a66ad3
```

```
Status for a66ad3

  Registration:  N512WB
  Type:          Pilatus PC-XII 45

  Date range:    2020-01-01 to 2026-04-10
  Days w/ data:  392
  Total flights: 628

Data quality:

  Confirmed landings:     361 (54%)
  Signal lost:             74 (11%)

Utilization:

  Total hours:      1843.5
  Avg flight:       166.4 min
  Distinct airports: 47
```

Shows data quality breakdown, mission type distribution, utilization rollup, night/emergency indicators, and top airports. Pass `--json` to emit a single JSON document instead of the table.

### View flight history

```
uv run python -m adsbtrack.cli trips --hex a66ad3 --from 2026-03-27
```

```
                                       Flights for a66ad3
 Date       | From  | To    | Duration | Callsign | Mission | Conf | Type
 2026-03-27 | 67FL  | KSPG  | 10m      | N512WB   | XFER    |  76% | OK
 2026-03-27 | KSPG  | KHKY  | 1h 53m   | N512WB   | XFER    |  89% | OK
 2026-03-28 | KHKY  | KVNC  | 2h 41m   | N512WB   | XFER    |  86% | OK
 2026-03-29 | KSPG  | ~KFXE | 12m      | N512WB   |         |   0% | DROP
```

`Conf` is landing confidence (0-100%), `Type` is landing classification (OK/SIG LOST/DROP/UNCERT/ALT ERR). Options: `--from`, `--to`, `--airport`. Pass `--json` to emit a single JSON document instead of the table.

### Route fingerprint

```
uv run python -m adsbtrack.cli route --hex a66ad3
```

```
2026-03-27 KSPG -> KHKY  SHAWZ (15m) -> KEEMO (8m) -> CLT (3m)
```

Prints one line per flight with a navaid track: the ordered chain of VORs/NDBs/fixes the ground track pointed straight at for a sustained stretch, a compact fingerprint of the enroute routing. Requires `navaids refresh` to have been run at least once; otherwise this is always empty. Options: `--tail`.

### Signal gaps

```
uv run python -m adsbtrack.cli gaps --hex a66ad3
```

Finds within-flight ADS-B signal gaps and classifies each as `likely_transponder_off`, `coverage_hole`, or `unknown` based on altitude, surrounding coverage, and proximity to a known airport. Options: `--min-gap-secs` (default 300), `--classification` (filter to one bucket). Pass `--json` to emit a single JSON document instead of the table.

### Event log

```
uv run python -m adsbtrack.cli events --hex a66ad3
```

Chronological log of emergency squawks (7500/7600/7700), emergency flags, off-airport landings, sustained hover (>= 5 min), and multiple go-arounds (>= 2 per flight) - all read from pre-computed columns on the `flights` table, no new heuristics. Options: `--since` (filter from a date), `--severity` (`emergency`, `unusual`, or `all`). Pass `--json` to emit a single JSON document instead of the table.

### Day forensics

```
uv run python -m adsbtrack.cli inspect --hex a66ad3 --date 2026-03-27
```

Deep-dive on one aircraft-day: splits each source's raw trace into fragments on inter-point gaps (`--gap-secs`, default 300), and reports per-fragment point count, position, altitude/speed range, DO-260B v2/sil0/nic0 integrity counts, and the callsigns/squawks seen. Also prints the squawk and callsign change-point timeline across every source merged into one chronological stream. Pass `--airport <ident>` to add a closest-approach line (distance, time, altitude) against a known airport. `--source` limits to one source; `--json` emits a single JSON document instead of the tables.

### Re-extract flights

```
uv run python -m adsbtrack.cli extract --hex a66ad3 --reprocess
```

Rebuilds the flight table from raw trace data after code changes.

`--since 2026-04-08` rebuilds only the trace days that new data on that date can affect and leaves the earlier flights alone - what `fetch` does automatically for the days it just downloaded. See [Incremental extraction](docs/internals.md#incremental-extraction) for how far back "can affect" reaches and when it falls back to a full rebuild.

## FAA aircraft registry

Load the FAA bulk registry (`ReleasableAircraft.zip`) so hex codes resolve to registrant name, address, certificate dates, and deregistration history. Install with the `faa` extra so the download can bypass the Akamai TLS-fingerprint block:

```
uv sync --extra faa
uv run python -m adsbtrack.cli registry update
```

If you don't install `curl_cffi` (the `faa` extra) the live download will usually 503. Fall back to downloading the zip in your browser and pointing the command at it:

```
uv run python -m adsbtrack.cli registry update --zip /path/to/ReleasableAircraft.zip
```

Then query:

```
uv run python -m adsbtrack.cli registry lookup --hex a66ad3       # full record by hex
uv run python -m adsbtrack.cli registry lookup --tail N512WB      # or by N-number
uv run python -m adsbtrack.cli registry owner --name "BANK OF UTAH"  # LIKE match
uv run python -m adsbtrack.cli registry address --state MT --city BILLINGS
```

The `status` command surfaces FAA registrant, address, and certificate info inline when the registry is loaded, and flags aircraft found only in `faa_deregistered`.

## Runway and navaid reference data

Load [OurAirports](https://ourairports.com/data/) runway geometry and navaid (VOR/NDB/fix) reference tables, used internally for takeoff-runway identification, ILS-alignment detection, and the `route` navaid fingerprint:

```
uv run python -m adsbtrack.cli runways refresh
uv run python -m adsbtrack.cli navaids refresh
```

Both download the relevant CSV from OurAirports and upsert it; re-running is idempotent (`runways refresh` overwrites rows keyed by airport ident + runway name, `navaids refresh` by ident + lat/lon). Pass `--csv <path>` to use a local file instead of downloading.

## ACARS ingestion

Pull ACARS / VDL2 / HFDL messages for an aircraft from [airframes.io](https://app.airframes.io) and correlate OOOI events onto its flights:

```
export AIRFRAMES_API_KEY=...     # or put "airframesApiKey" in credentials.json
uv run python -m adsbtrack.cli acars --hex a66ad3 --start 2026-01-01
```

The fetcher resolves hex to airframes.io's numeric airframe id, walks each flight in range, and inserts the raw messages. When OOOI-bearing messages (labels 14 / 44 / 4T / H1) land inside a flight window, the parser fills `acars_out`, `acars_off`, `acars_on`, `acars_in` on the flight row.

`trips` shows an ACARS column with message count and a green OOOI badge when present; `status` shows a per-aircraft ACARS summary block.

## Hex cross-reference enrichment

Merge FAA + [Mictronics](https://github.com/Mictronics/readsb-protobuf/tree/dev/webapp/src/db) + [hexdb.io](https://hexdb.io) into a single `hex_crossref` table so every hex in your DB has a best-effort identity, and flag aircraft in known military allocation blocks.

```
uv run python -m adsbtrack.cli enrich all --download-mictronics   # backfill everything
uv run python -m adsbtrack.cli enrich hex --hex a66ad3            # one at a time
uv run python -m adsbtrack.cli mil hex --hex ae1234               # check mil range
uv run python -m adsbtrack.cli mil scan                           # flag every mil hex
```

Merge order is FAA (preferred) -> Mictronics -> hexdb.io; the enricher flags conflicts so you can see where sources disagree. 25 well-documented military allocation ranges (US DoD, UK RAF, Luftwaffe, JASDF, RAAF, RCAF, VKS, etc.) seed automatically on DB init; extend the `mil_hex_ranges` table with your own rows for better coverage.

## Finding hex codes

Convert US N-numbers directly:

```
uv run python -m adsbtrack.cli lookup --tail N512WB
```

Or use `--tail` instead of `--hex` on any command:

```
uv run python -m adsbtrack.cli fetch --tail N512WB --start 2020-01-01
```

External lookup sites: [aircraftdata.org](https://aircraftdata.org), [FAA Aircraft Registry](https://registry.faa.gov/aircraftinquiry), [ADS-B Exchange](https://globe.adsbexchange.com/)

## Generate trace URLs

```
uv run python -m adsbtrack.cli links --hex a66ad3
```

```
2026-03-27 67FL -> KSPG  https://globe.adsbexchange.com/?icao=a66ad3&showTrace=2026-03-27
2026-03-27 KSPG -> KHKY  https://globe.adsbexchange.com/?icao=a66ad3&showTrace=2026-03-27
```

Pass `--urls-only` for one raw URL per line (no prefix or markup), suitable for piping into shell loops.

## Multiple data sources

Fetch from different networks for better coverage:

```
uv run python -m adsbtrack.cli fetch --hex a66ad3 --source adsbfi --start 2020-01-01
```

Traces from multiple sources are automatically merged during extraction. `--source all` fetches from every readsb source in parallel (plus OpenSky when credentials are configured), with one progress line per source and a per-source summary at the end. A source whose recent fetch attempts all failed (403/429/5xx) is skipped with a warning as unhealthy; pass `--include-unhealthy` to force it back in. Requests older than a source's known archive retention window get a note in the output that a 404 there may mean "expired" rather than "no data."

| Source | Flag | Notes |
|--------|------|-------|
| [ADS-B Exchange](https://globe.adsbexchange.com/) | `--source adsbx` | Default |
| [adsb.fi](https://globe.adsb.fi/) | `--source adsbfi` | |
| [airplanes.live](https://globe.airplanes.live/) | `--source airplaneslive` | |
| [adsb.lol](https://adsb.lol/) | `--source adsblol` | |
| [TheAirTraffic](https://globe.theairtraffic.com/) | `--source theairtraffic` | |
| [OpenSky Network](https://opensky-network.org/) | `--source opensky` | OAuth2 API client credentials (see below) |
| Custom | `--url <base_url>` | Any readsb globe_history instance |

### OpenSky credentials

OpenSky retired HTTP Basic auth; the current flow is OAuth2 client-credentials. Create an API client on your [opensky-network.org](https://opensky-network.org/) account page (these are API client credentials, not your website login), then either export them or add them to `credentials.json`:

```
export OPENSKY_CLIENT_ID=...       # or put "clientId" / "clientSecret" in credentials.json
export OPENSKY_CLIENT_SECRET=...
```

The fetcher exchanges these for a Bearer token (~30 minute lifetime, refreshed automatically) and paces requests at `Config.opensky_rate_limit` since the authenticated REST quota is credit-based. `--source all` includes OpenSky automatically whenever credentials exist. OpenSky's REST API is not a readsb archive: flight metadata is available for any date, but detailed track waypoints only for roughly the last 30 days - older days are logged as checked with no trace stored. Synthesized OpenSky traces carry no ground speed, so flights extracted purely from OpenSky data land with lower confidence than readsb-source days.

## Watching a hex list

Run a lightweight fetch/extract/alert cycle over a list of hexes, meant to be driven by a **daily** cron:

```
uv run python -m adsbtrack.cli watch --hex a66ad3 --hex adf64f
```

Each run fetches from every healthy readsb source through *yesterday*, never today - archives take time to finalize after UTC midnight, so a source can hand back a terminal "no data" answer for today that's really just "not posted yet," and that answer would otherwise be recorded as fetched and never retried. This caps alert latency at roughly a day; running more often than daily doesn't get you fresher data, just redundant fetches against a window that hasn't moved.

The run then extracts and compares each aircraft's state before and after. Three things fire an alert: an aircraft going quiet for `watch_dormancy_days` (default 30) and then reappearing, a new flight carrying an emergency squawk (7500/7600/7700) or the `had_emergency` flag, and a new row landing in `spoofed_broadcasts`. A hex with no prior trace history baselines silently on its first run instead of flooding on a backfill, and a reactivation alert additionally requires a fetch_log row from an *earlier* run somewhere inside the gap - so pointing `watch` at a database `fetch` already populated doesn't misread watch's own first look as a dormancy period.

`--watchlist <path>` reads one hex per line from a file and unions it with any `--hex` flags (duplicates deduped); blank lines and `#` comments (wherever they start on the line) are ignored:

```
# rotating watchlist
a66ad3   # G650 tail N512WB
adf64f
```

Exit code is `3` when any alert fired, `0` otherwise, so a cron entry only needs to react to failure:

```
0 2 * * * adsbtrack watch --watchlist ~/.config/adsbtrack/watchlist.txt --webhook https://ntfy.sh/mytopic
```

`--webhook <url>` POSTs the run's alerts as JSON to that URL, but only when at least one fired. `--dormancy-days N` overrides the reactivation threshold for one run. `--json` prints a single machine-readable document (`generated_at`, `alerts`, per-hex `hexes` status) instead of the status lines and table; anything the run would otherwise print to the terminal goes to stderr instead, so stdout stays valid JSON.

## Interactive surfaces

### Terminal UI

```
uv sync --extra tui
uv run python -m adsbtrack.cli tui --db adsbtrack.db
```

Launches a Textual TUI over the local database: aircraft list, flight timeline, event feed, spoofed-broadcasts audit, map, and status views, plus an ops pane that wraps the DB-writing commands.

### Static HTML explorer

```
uv run python -m adsbtrack.cli gui --hex a66ad3
```

Writes a static three-column HTML explorer to `--out` (default `gui-export`): `index.html` plus a small JS/CSS bundle, including a `data.js` snapshot loaded via a plain `<script>` tag (not `fetch`) so it works from `file://`. Renders the aircraft list, flight timeline, events, and spoofed-broadcasts audit. Open `index.html` directly in a browser - no local server needed. Read-only; rerun the command to refresh the snapshot. Options: `--out`, `--hex` (focus the initial view on one aircraft).

### MCP server

```
uv sync --extra mcp
uv run python -m adsbtrack.cli mcp-serve --db adsbtrack.db
```

Runs a read-only MCP server over stdio, exposing aircraft stats, flights, events, gaps, and registry lookup tools to MCP-compatible LLM clients such as Claude Desktop and Claude Code. No fetch or write path is exposed.

## Database maintenance

```
uv run python -m adsbtrack.cli db optimize
```

Backfills legacy `trace_days` rows written before the compressed trace storage and materialized integrity-stat columns existed: compresses `trace_json` and fills the `v2_samples`/`v2_sil0`/`v2_nic0`/`v2_callsigns` columns that the spoof-detection and events path reads instead of decoding every trace. Processes rows in batches and is safe to interrupt and re-run - it skips rows already compressed and stat-filled. Pass `--vacuum` to run `VACUUM` afterward (rewrites the whole database file; needs free disk space roughly the size of the final DB).

## Configuration file

`Config` thresholds (rate limits, match distances, endurance caps, etc.) can be overridden with a TOML file instead of editing `adsbtrack/config.py`. The CLI resolves the file in this order: an explicit path > `$ADSBTRACK_CONFIG` > `~/.config/adsbtrack/config.toml` > built-in defaults if none of those exist. Keys map 1:1 onto `Config` field names; an unrecognized key is rejected. Separately, every command's `--db` option honors `$ADSBTRACK_DB`, so you can point a shell at a database once instead of repeating the flag.

```toml
# ~/.config/adsbtrack/config.toml
airport_match_threshold_km = 15.0
rate_limit = 1.0
```

## Documentation

Detailed reference docs for contributors and analysts:

- **[Database schema](docs/schema.md)** - full column reference for every table (traces / flights / registry / stats / airports / helipads / FAA registry / ACARS / hex crossref / mil ranges)
- **[Features and scoring](docs/features.md)** - landing types, confidence scoring algorithm, all derived per-flight columns, mission classification rules, signal budget, ACARS OOOI, position-source breakdown
- **[Internals](docs/internals.md)** - how the extractor works: trace merging, flight extraction state machine, fragment stitching, airport matching, FAA registry parser, hex crossref merge
- **[Datasette pairing](docs/datasette.md)** - explore `adsbtrack.db` through a web UI with five starter canned queries (emergencies, military activity, signal gaps, off-airport landings, top-aircraft-by-flights)

## Development

```
uv sync --extra dev --extra tui --extra mcp
uv run pytest
uv run ruff check .
uv run ruff format .
uv run mypy adsbtrack
```

The `tui` and `mcp` extras are needed to run the full test suite (the Textual Pilot smoke tests and MCP server tests skip or fail without them); CI installs all three extras and runs on push and pull requests (Python 3.12 and 3.13).

## Notes

- Data availability depends on ADS-B receiver coverage. Flights over oceans or remote areas will have gaps - those show up as `signal_lost` or `dropped_on_approach` rather than as missing flights.
- Different receiver networks have different coverage, so fetching from multiple sources gives the best results.
- Rate limiting is adaptive: 429s increase the delay, consecutive successes recover it.
- `extract --reprocess` clears and rebuilds all flights from raw traces. Schema migrates automatically. `fetch` no longer does that on every run: it re-extracts from the earliest day it actually downloaded, and prints why when it has to fall back to a full rebuild.
