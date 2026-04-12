# adsbtrack

Pull historical ADS-B trace data from multiple tracking networks and turn it into a structured flight history for any aircraft. Give it an ICAO hex code and a date range and it will fetch every day of trace data, extract individual flights, match takeoff/landing coordinates to airports, classify each landing by quality, and give you travel pattern statistics.

Built for OSINT and aviation nerds who want to go beyond live tracking and dig into where an aircraft has actually been over months or years - with enough signal quality metadata to trust the answers.

## What it does

1. **Fetch** - Downloads daily trace files from any readsb-based tracking network (adsbx, adsbfi, airplanes.live, adsb.lol, theairtraffic, OpenSky)
2. **Extract** - Parses raw trace points into discrete flights using a state machine that fuses barometric altitude, geometric altitude, ground speed, and vertical rate. Splits flights on multi-hour coverage gaps; keeps flights open for a post-landing window to validate the touchdown.
3. **Classify** - Every flight gets a landing type (`confirmed`, `signal_lost`, `dropped_on_approach`, `uncertain`, `altitude_error`) plus independent takeoff and landing confidence scores from 0.0 to 1.0. The classifier handles tricky cases like Bell 407 hover-at-altitude baro errors and military Mode S altitude spoofing.
4. **Stitch** - A post-processing pass merges signal-lost + mid-flight fragments that are plausibly the same continuous flight with a coverage hole, recovering the original takeoff context.
5. **Match** - Matches takeoff/landing coordinates to the nearest airport using the OurAirports database, skipping matches on `signal_lost` / `dropped_on_approach` to prevent false attributions. For dropped flights, a probable destination is inferred from the last-seen position with a separate confidence score.
6. **Derive** - Pulls everything else out of each trace point: mission classification (EMS/HEMS, offshore, exec charter, training, survey, pattern, transport), path length, loiter ratio, path efficiency, climb/cruise/descent time budget, peak climb and descent rates, hover episodes (rotorcraft only), go-around count, takeoff and landing headings, day/night classification, callsign history, squawk transitions including emergency squawks, DO-260B category, and autopilot target altitude.
7. **Roll up** - Maintains an `aircraft_registry` table with authoritative metadata (resolving drift across daily fetches) and an `aircraft_stats` materialized rollup with total hours, cycles, distinct airports, distinct callsigns, average flight minutes, and busiest day per aircraft.
8. **Analyze** - Shows flight history with confidence coloring, mission columns, probable destinations for dropped flights, top airports, mission breakdown, utilization rollup, and night/emergency indicators per aircraft.

Everything is stored in a local SQLite database with WAL mode enabled, so multiple fetch sessions can run concurrently. Fetches are logged so you can resume interrupted runs without re-downloading.

## Install

Requires Python 3.12+. Using [uv](https://github.com/astral-sh/uv):

```
git clone https://github.com/frankea/adsbtrack.git
cd adsbtrack
uv sync
```

Check version:

```
uv run adsbtrack --version
```

## Usage

### Fetch trace data

```
uv run python -m adsbtrack.cli fetch --hex a66ad3 --start 2020-01-01
```

```
Fetching a66ad3 from 2020-01-01 to 2026-04-11
Fetching a66ad3 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 2163/2163 0:00:00

Done! Fetched: 2163, With data: 391, Skipped (already fetched): 127, Errors: 0

Extracting flights...
Found 665 flights
```

Options:
- `--hex` - ICAO hex code (required). You can look these up from tail numbers on sites like [aircraftdata.org](https://aircraftdata.org)
- `--start` - Start date, YYYY-MM-DD (default: 2025-01-01)
- `--end` - End date, YYYY-MM-DD (default: today)
- `--rate` - Seconds between requests (default: 0.5)
- `--db` - Database path (default: adsbtrack.db)

The first run will automatically download the airport database (~47k airports). Fetching skips dates that have already been downloaded, so you can safely re-run to extend the date range or retry after interruptions. WAL mode is enabled on the SQLite database, so you can run multiple `fetch` or `extract` commands in parallel from different terminals without database-locked errors.

### View statistics

```
uv run python -m adsbtrack.cli status --hex a66ad3
```

```
Status for a66ad3

  Registration:  N512WB
  Type:          Pilatus PC-XII 45
  Owner:         None

  Date range:    2020-01-01 to 2026-04-10
  Days checked:  2290
  Days w/ data:  392
  Total flights: 665

Data quality:

  Confirmed landings:     361 (54%)
  Signal lost:             74 (11%)
  Dropped on approach:     86 (13%)
  Uncertain:               39 (6%)
  Altitude errors:         11 (2%)

Mission breakdown:

  exec_charter       412 (62%)
  transport          198 (30%)
  pattern             34 (5%)
  unknown             21 (3%)

Utilization:

  Total hours:      1843.5
  Cycles:           361
  Avg flight:       166.4 min
  Distinct airports: 47
  Distinct callsigns: 1
  Busiest day:      2023-04-12 (8 flights)

Indicators:

  Night flights:    142

Top airports:

┏━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┓
┃ Airport ┃ Name                                      ┃ Visits ┃
┡━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━┩
│ KSRQ    │ Sarasota Bradenton International Airport  │    443 │
│ 67FL    │ Bald Eagle Airstrip                       │     72 │
│ KVNC    │ Venice Municipal Airport                  │     54 │
│ KFPR    │ Treasure Coast International Airport      │     34 │
│ KEYW    │ Key West International Airport            │     31 │
│ KFXE    │ Fort Lauderdale Executive Airport         │     26 │
│ KFFC    │ Peachtree City Falcon Field               │     21 │
│ KCRG    │ Jacksonville Executive at Craig Airport   │     15 │
│ MYAM    │ Leonard M. Thompson International Airport │     14 │
│ KTRI    │ Tri-Cities Regional TN/VA Airport         │     13 │
└─────────┴───────────────────────────────────────────┴────────┘
```

The Data quality breakdown summarizes how each aircraft's flights classify. See [Flight quality and confidence scoring](#flight-quality-and-confidence-scoring) below for what each label means. The Mission breakdown groups flights by inferred mission_type. The Utilization rollup is materialized in the `aircraft_stats` table and refreshed on every `extract --reprocess` call. Indicators surface night flying and any emergency squawks (7500/7600/7700) that were ever observed.

### View flight history

```
uv run python -m adsbtrack.cli trips --hex a66ad3 --from 2026-03-25
```

```
                                       Flights for a66ad3
┏━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━┳━━━━━━┳━━━━━━━━┓
┃ Date       ┃ From      ┃ To        ┃ Duration ┃ Callsign ┃ Mission ┃ Conf ┃ Type   ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━╇━━━━━━╇━━━━━━━━┩
│ 2026-03-27 │ 67FL      │ KSPG      │ 10m      │ N512WB   │ XFER    │  76% │ OK     │
│ 2026-03-27 │ KSPG      │ KHKY      │ 1h 53m   │ N512WB   │ XFER    │  89% │ OK     │
│ 2026-03-28 │ KHKY      │ KVNC      │ 2h 41m   │ N512WB   │ XFER    │  86% │ OK     │
│ 2026-03-28 │ KVNC      │ KHKY      │ 2h 19m   │ N512WB   │ XFER    │  94% │ OK     │
│ 2026-03-29 │ KHKY      │ KSPG      │ 2h 23m   │ N512WB   │ XFER    │  73% │ OK     │
│ 2026-03-29 │ KSPG      │ ~KFXE     │ 12m      │ N512WB   │         │  0%  │ DROP   │
└────────────┴───────────┴───────────┴──────────┴──────────┴─────────┴──────┴────────┘

Total: 6 flights
```

The `Conf` column is the landing confidence score (0-100%), `Mission` is the inferred
mission type (`EMS`, `OFFSH`, `CHRT`, `TRAIN`, `SRVY`, `PATRN`, `XFER`), and `Type` is
the landing classification - `OK` for confirmed landings, `SIG LOST` for airborne at
last contact, `DROP` for dropped on approach (with the `To` column showing the inferred
probable destination prefixed by `~`), `UNCERT` for ambiguous, `ALT ERR` for broken
baro encoder. Confidence is color-coded: green ≥80%, yellow 50-79%, red <50%.

Options:
- `--from` - Filter from date
- `--to` - Filter to date
- `--airport` - Filter by airport ICAO code (e.g. `--airport KSRQ`)

### Re-extract flights

```
uv run python -m adsbtrack.cli extract --hex a66ad3 --reprocess
```

Useful if you want to rebuild the flight table from raw trace data (e.g. after code changes to the parser).

## How it works

ADS-B Exchange and sibling trackers store daily trace files for every aircraft they've seen. Each trace is a series of timestamped position reports with lat/lon, barometric altitude, ground speed, vertical rate, and geometric altitude. The fetcher downloads these day by day and stores them in SQLite.

### Trace merging

When multiple data sources are fetched for the same aircraft, traces are merged by absolute timestamp and deduplicated (points within 1 second and 0.001° of each other are collapsed). Different receiver networks catch different points for the same flight, so combining them improves coverage.

The single-source path runs through the same sort + dedupe pipeline, so trace files that contain out-of-order or "phantom" points (cache glitches in readsb's `trace_full` output that occasionally write prior-day leakage with deeply negative offsets) are reordered into chronological order before the state machine sees them. Without this, a phantom point can overwrite the pending flight's `last_point_ts` with a timestamp earlier than its `first_point_ts` and produce a negative `duration_minutes`.

### Flight extraction

The extractor walks through the merged trace points in chronological order and runs a state machine (`None` → `ground` → `airborne` → `post_landing` → `ground`...) that detects takeoff and landing transitions.

Ground-vs-airborne for each point is decided by fusing:

- **Barometric altitude** (the `'ground'` sentinel or an int in feet)
- **Geometric altitude** (GPS-derived, in feet)
- **Ground speed** in knots
- **Barometric vertical rate** in ft/min

This catches tricky cases the old "baro says ground" heuristic missed:

- **Baro encoder errors** on helicopters like the Bell 407: the barometric encoder frequently reports `'ground'` while the aircraft is hovering at 300-500 ft AGL. Geometric altitude disagrees, so the classifier treats the point as airborne and flags the flight as `altitude_error` when the ratio gets high enough.
- **Speed override**: a `'ground'` altitude at flight speed (>80 kt) is always a glitch, not a landing.
- **OpenSky data with no ground speed**: requires two consecutive ground points before confirming a landing, to avoid false touchdowns from altitude glitches.

A few other things the state machine does:

- **Intra-trace gap splitting**: any gap longer than 30 minutes between consecutive points (absolute value - a backwards-in-time jump also triggers a close) finalizes the pending flight. Real operations have 3.5 min median max gaps; multi-hour gaps are coverage holes that should not be stitched across.
- **Post-landing window**: after a ground transition, the flight stays "open" for up to 60 seconds or 5 more ground points. This populates landing-quality metrics (`ground_points_at_landing`, per-sample coordinate stability) with real data instead of always being 1/0.
- **Touch-and-go detection**: an airborne point inside the post-landing window finalizes the current flight and immediately opens a new one.
- **Short-movement filter**: flights shorter than 5 minutes that travel less than 5 km are filtered out as taxi movements. Single-point "flights" left over from phantom trace points are also dropped.

### Fragment stitching

After extraction, a post-processing pass walks each aircraft's flights chronologically and merges pairs where a previous flight ended without a landing and the next flight starts with `takeoff_type = found_mid_flight`, within:

- **Type-endurance-aware time gap**: `max(stitch_max_gap_minutes, endurance_for(type_code) × stitch_endurance_ratio)`. The default `stitch_max_gap_minutes` is 90, which is the right window for light GA. For long-endurance types that regularly have multi-hour coverage gaps during one operational mission (KC-135R at 720 min, KC-46 at 780 min, C-5M at 900 min, GLF6 at 900 min, etc.), the effective window scales up automatically. With the default `stitch_endurance_ratio = 0.4`, a KC-135R gets a 288-minute stitch window while a Cessna 172 stays at 96 minutes. Without this scaling, a tanker orbit over restricted airspace shows up as two signal-lost fragments instead of one continuous flight.
- **Great-circle distance** less than `cruise_speed × time_gap × 1.2` (with 300 kt as the upper bound)
- **Altitude delta** under 3000 ft

The stitched flight inherits the original takeoff position and time, which recovers the actual origin airport for flights that would otherwise be classified as mid-flight fragments. Duration is recomputed after merging so the wall-clock span covers the coverage gap.

### Airport matching

Origin and destination are matched via a bounding-box query against the OurAirports database (~47k airports) followed by haversine distance calculation, with a 10 km threshold. Matches are skipped entirely for `signal_lost` and `dropped_on_approach` flights to prevent false attributions at the nearest private strip when the aircraft just lost coverage mid-air.

## Flight quality and confidence scoring

Every extracted flight carries two independent confidence scores (`takeoff_confidence`, `landing_confidence`) in [0.0, 1.0] plus a landing type:

| Landing type | Meaning |
|--------------|---------|
| `confirmed` | Clean landing: clear descent, low final speed, low final altitude, ground points collected, and stable coordinates |
| `signal_lost` | Aircraft was airborne at last contact - coverage dropped mid-flight |
| `dropped_on_approach` | Signal lost but the last few samples show sustained descent below 5000 ft. The landing probably happened at a nearby airport but we never saw it |
| `uncertain` | Ambiguous - duration exceeds max endurance for the type (likely a data gap artifact), or low/slow but no landing transition |
| `altitude_error` | The barometric encoder is clearly broken for this flight (Bell 407 hover pathology or similar) |

Takeoff type similarly distinguishes `observed` (we saw the ground→airborne transition) from `found_mid_flight` (first trace point was already airborne). `found_mid_flight` flights cap their takeoff confidence at 0.30 because we never observed the actual origin.

**Landing confidence** is a weighted geometric mean across seven factors:

- **Descent signature** over a 30-180 sec pre-flare window (skips the flare itself so clean ILS approaches score high)
- **Approach speed** (120-150 kt for jets, 25-50 kt for helicopters, both rewarded)
- **Final altitude** (lower = better)
- **Airport proximity** at the landing point
- **Per-sample coordinate stability** (normal taxi motion is fine; only sudden >500 m jumps count as receiver noise)
- **Post-landing ground points** (how many samples confirmed the touchdown)
- **Duration plausibility** (penalized for flights approaching multi-day data gaps)

The geometric mean lets any single failing factor drag the whole score down, which catches "this looks like a landing by 5 metrics but the descent trace is missing" cases that a simple average would gloss over.

**Max endurance is per aircraft type**: a 240 min global cap would reject legitimate Gulfstream transcons, so the classifier consults a type_code lookup (B407=180, S92=300, PC12=420, GLF6=900, KC-135R=720, C-17=720, KC-46=780, C-5M=900, etc.). Flights longer than the type's endurance become `uncertain` rather than `confirmed`. The same lookup feeds the type-endurance-aware fragment stitcher, so long-endurance types can merge across the wider coverage gaps that are normal on their operational missions.

## Derived per-flight features

Beyond classification and confidence, every extracted flight is tagged with a set of derived features that turn "a flight happened" into something you can query against in detail. These come from a single pass that accumulates raw counters during trace processing and a post-classification pass that turns those counters into per-flight columns.

**Mission classification.** `mission_type` is one of `ems_hems`, `offshore`, `exec_charter`, `training`, `survey`, `pattern`, `transport`, or `unknown`. Resolved by a callsign prefix lookup table (TWY/GLF → exec_charter, PHM/PHI/ERA → offshore, N911 / *MT suffix → ems_hems, etc.) followed by physics rules: high loiter ratio + low cruise speed → survey, same-airport low-altitude → pattern, distinct origin/destination → transport.

**Path metrics.** `path_length_km` is the haversine sum of all in-flight segments (skipping coverage holes > 60 s). `max_distance_km` is the max distance ever reached from the takeoff point. `loiter_ratio = path_length / (2 * max_distance)` - a value of 1.0 is a straight there-and-back, 3+ is a survey or holding pattern, 5+ is dedicated orbiting. `path_efficiency = great_circle / path_length` is populated only when origin and destination are different airports.

**Phase of flight.** `climb_secs`, `cruise_secs`, `descent_secs`, `level_secs` partition the flight into climb (rate > +250 fpm), descent (rate < -250 fpm), and level. Cruise is the level subset above 70 % of `max_altitude`, with `cruise_alt_ft` and `cruise_gs_kt` averaging over those samples. Short flights and flights with peak altitude under 500 ft return NULL cruise fields.

**Peak rates.** `peak_climb_fpm` and `peak_descent_fpm` are the best mean rate observed over a 30-second rolling window (not point-to-point - filtering out single-point glitches). Useful per-type envelope: B407 typically peaks ~1500 / -2300, GLF6 peaks ~5000 / -3000.

**Hover detection.** Helicopters only. `max_hover_secs` and `hover_episodes` count contiguous windows ≥ 20 s where the aircraft was airborne with `gs < 5 kt` and `|baro_rate| < 100 fpm`. Useful for distinguishing scene response (hovers) from hospital-helipad landings.

**Go-around detection.** `go_around_count` is the number of "approach → climb → approach" sequences in the final 600 s before touchdown. Walks the approach altitude history backward from the landing transition, finding local min A → local max B with `(B - A) >= 400 ft` followed by a final descent. Only runs on confirmed landings.

**Headings.** `takeoff_heading_deg` is the circular mean of the ground-track samples in the first 60 s after the airborne transition (filtered to `gs > 40 kt` to exclude taxi pollution). `landing_heading_deg` is the same for the last 60 s before the landing transition. Both are circular means, not arithmetic - so a [350°, 10°] sample correctly returns ~0°, not 180°.

**Day / night.** `takeoff_is_night`, `landing_is_night`, and `night_flight` (≥50 % of in-flight points after civil sunset). Computed inline using a NOAA solar-position approximation; no external dependency. Cached by 5-minute / 0.1° buckets to keep cost down on long flights.

**Squawks.** `squawk_first` / `squawk_last` are the first and last observed transponder codes. `squawk_changes` counts the number of transitions (not distinct values). `emergency_squawk` is the most severe of any 7500 / 7600 / 7700 ever observed (priority 7500 > 7700 > 7600). `vfr_flight = 1` when ≥ 80 % of squawks were 1200.

**Callsigns history.** `callsigns` is a JSON array of all distinct callsigns seen during the flight, sorted lexicographically. `callsign_changes` counts transitions. Useful for catching aircraft that swap operator/IFR callsigns mid-flight (TWY501 ↔ GS501 on the same N999YY airframe is the canonical example).

**Detail extras.** `category_do260` is the most common DO-260B category (A0-B7) - more reliable than `type_code` from trace metadata. `autopilot_target_alt_ft` is the last `nav_altitude_mcp` observed before the first sustained descent (proxy for "intended cruise"). `emergency_flag` is the latest non-"none" `detail.emergency` value seen (e.g. `lifeguard`, `general_emergency`).

**Probable destination inference.** For `signal_lost` and `dropped_on_approach` flights, the parser searches for any airport within 25 nm of `last_seen_lat/lon` and populates `probable_destination_icao`, `probable_destination_distance_km`, `probable_destination_confidence`. The confidence is a weighted average of low last-seen altitude, distance to airport, and final descent rate - higher when the aircraft was clearly on approach.

**Altitude-error gating.** Flights classified `altitude_error` (broken baro encoder) leave their altitude-derived columns NULL: peak rates, climb/descent/cruise seconds, cruise alt/gs are all gated. Path length, hover, and callsigns still populate.

## Aircraft registry and stats

Two materialized tables track per-aircraft state across all extracts:

**`aircraft_registry`** is the authoritative metadata for each ICAO. The registry is populated at the start of every `extract` call by picking the most recently fetched `trace_days` row as the source of truth, then flagging metadata drift (`metadata_drift_count`, `metadata_drift_values` JSON) when other rows disagree on type_code, description, or registration. This is how the parser resolves the canonical type_code for `N999YY` even though its trace metadata reports it variously as `GLF6`, `GA8C`, `GVI`, and `BD-700-1A10` depending on which day you fetch.

**`aircraft_stats`** is a rollup table refreshed at the end of every extract: `total_flights`, `confirmed_flights`, `total_hours`, `total_cycles`, `distinct_airports`, `distinct_callsigns`, `avg_flight_minutes`, `busiest_day_date`, `busiest_day_count`. Populated via SQL aggregation over the `flights` table - free in cost, useful for at-a-glance fleet utilization.

Both tables are surfaced in the `status` command and queryable directly.

## Finding hex codes

You can convert US N-numbers to hex codes directly:

```
uv run python -m adsbtrack.cli lookup --tail N512WB
```

Or use `--tail` instead of `--hex` on any command:

```
uv run python -m adsbtrack.cli fetch --tail N512WB --start 2020-01-01
```

External lookup sites:
- [aircraftdata.org](https://aircraftdata.org) - search by N-number, shows Mode S hex
- [FAA Aircraft Registry](https://registry.faa.gov/aircraftinquiry) - official source
- [ADS-B Exchange](https://globe.adsbexchange.com/) - search box accepts tail numbers

## Generate trace URLs

Generate clickable ADS-B Exchange URLs for each flight to view the trace on the map:

```
uv run python -m adsbtrack.cli links --hex a66ad3
```

```
2026-03-27 67FL -> KSPG  https://globe.adsbexchange.com/?icao=a66ad3&showTrace=2026-03-27
2026-03-27 KSPG -> KHKY  https://globe.adsbexchange.com/?icao=a66ad3&showTrace=2026-03-27
2026-03-28 KHKY -> KVNC  https://globe.adsbexchange.com/?icao=a66ad3&showTrace=2026-03-28
```

Pass `--urls-only` to emit one raw URL per line with no prefix or markup, which makes it easy to pipe into shell loops or other tools:

```
uv run python -m adsbtrack.cli links --hex a66ad3 --urls-only
```

```
https://globe.adsbexchange.com/?icao=a66ad3&showTrace=2026-03-27
https://globe.adsbexchange.com/?icao=a66ad3&showTrace=2026-03-27
https://globe.adsbexchange.com/?icao=a66ad3&showTrace=2026-03-28
```

Useful for walking through every flight on a hex code interactively:

```bash
uv run python -m adsbtrack.cli links --hex a66ad3 --urls-only | while IFS= read -r url; do
  printf '\n%s\n' "$url"
  read -rp "[enter] next: " _ < /dev/tty
  xdg-open "$url" >/dev/null 2>&1 &
done
```

## Multiple data sources

Fetch from different receiver networks for better coverage. All sources use the same [readsb](https://github.com/wiedehopf/readsb) globe_history format:

```
uv run python -m adsbtrack.cli fetch --hex a66ad3 --source adsbfi --start 2020-01-01
uv run python -m adsbtrack.cli fetch --hex a66ad3 --source airplaneslive --start 2020-01-01
```

Traces from multiple sources are automatically merged during flight extraction.

Supported sources:

| Source | Flag | Network |
|--------|------|---------|
| [ADS-B Exchange](https://globe.adsbexchange.com/) | `--source adsbx` | Default |
| [adsb.fi](https://globe.adsb.fi/) | `--source adsbfi` | |
| [airplanes.live](https://globe.airplanes.live/) | `--source airplaneslive` | |
| [adsb.lol](https://adsb.lol/) | `--source adsblol` | |
| [TheAirTraffic](https://globe.theairtraffic.com/) | `--source theairtraffic` | |
| [OpenSky Network](https://opensky-network.org/) | `--source opensky` | Requires API credentials |
| Custom | `--url <base_url>` | Any readsb instance |

### OpenSky credentials

OpenSky requires API credentials. Set them as environment variables (preferred):

```
export OPENSKY_CLIENT_ID="your-client-id"
export OPENSKY_CLIENT_SECRET="your-client-secret"
```

Or create a `credentials.json` file in the project directory:

```json
{"clientId": "your-client-id", "clientSecret": "your-client-secret"}
```

Environment variables take priority over the file if both are present.

### Custom sources

You can also point at any readsb-compatible instance with `--url`:

```
uv run python -m adsbtrack.cli fetch --hex a66ad3 --url https://your-instance/globe_history
```

## Database schema

All data is stored in a local SQLite database (`adsbtrack.db`). WAL mode is enabled so multiple fetch/extract sessions can run concurrently from different terminals. Schema migrations run automatically on open.

**trace_days** - Raw daily trace data per aircraft per source
| Column | Type | Description |
|--------|------|-------------|
| icao | TEXT | ICAO hex code |
| date | TEXT | Date (YYYY-MM-DD) |
| source | TEXT | Data source (adsbx, adsbfi, airplaneslive, etc.) |
| registration | TEXT | Tail number from trace metadata |
| type_code | TEXT | Aircraft type code (B407, S92, GLF6, etc.) |
| description | TEXT | Aircraft type description |
| owner_operator | TEXT | Owner from trace metadata |
| timestamp | REAL | Base Unix timestamp for the day |
| trace_json | TEXT | Raw trace points as JSON array |
| point_count | INTEGER | Number of trace points |

**flights** - Extracted flights with airport matching, quality classification, confidence scoring, and v3 derived features
| Column | Type | Description |
|--------|------|-------------|
| icao | TEXT | ICAO hex code |
| takeoff_time | TEXT | ISO timestamp |
| takeoff_lat/lon | REAL | Takeoff coordinates |
| takeoff_date | TEXT | Source trace date (YYYY-MM-DD) |
| landing_time | TEXT | ISO timestamp (null if signal lost) |
| landing_lat/lon | REAL | Landing coordinates (null if signal lost) |
| landing_date | TEXT | Source trace date of landing |
| origin_icao | TEXT | Nearest airport ICAO code to takeoff |
| origin_name | TEXT | Airport name |
| origin_distance_km | REAL | Haversine distance from takeoff fix |
| destination_icao | TEXT | Nearest airport ICAO code to landing |
| destination_name | TEXT | Airport name |
| destination_distance_km | REAL | Haversine distance from landing fix |
| duration_minutes | REAL | Flight duration (populated on every flight, computed from first/last trace point) |
| callsign | TEXT | Callsign from ADS-B |
| landing_type | TEXT | `confirmed` / `signal_lost` / `dropped_on_approach` / `uncertain` / `altitude_error` |
| takeoff_type | TEXT | `observed` / `found_mid_flight` |
| takeoff_confidence | REAL | [0.0, 1.0] |
| landing_confidence | REAL | [0.0, 1.0] - weighted geometric mean of seven factors |
| data_points | INTEGER | Trace points recorded for this flight |
| sources | TEXT | Comma-separated data sources that contributed |
| max_altitude | INTEGER | Peak altitude (feet) |
| ground_points_at_takeoff | INTEGER | Ground points collected before takeoff transition |
| ground_points_at_landing | INTEGER | Ground points collected after landing transition (post-landing window) |
| baro_error_points | INTEGER | Count of points where the barometric encoder disagreed with geometric altitude or ground speed |
| last_seen_lat/lon | REAL | Last observed position (useful for signal_lost flights - this is where coverage actually dropped) |
| last_seen_alt_ft | INTEGER | Last observed altitude |
| last_seen_time | TEXT | Last observed timestamp (ISO) |
| squawk_first / squawk_last | TEXT | First and last transponder code observed |
| squawk_changes | INTEGER | Number of transitions between distinct squawks |
| emergency_squawk | TEXT | Most severe of any 7500/7600/7700 observed; NULL otherwise |
| vfr_flight | INTEGER | 1 when ≥80% of squawks were 1200 |
| mission_type | TEXT | `ems_hems` / `offshore` / `exec_charter` / `training` / `survey` / `pattern` / `transport` / `unknown` |
| category_do260 | TEXT | DO-260B category (A0-B7) - most common across the flight |
| autopilot_target_alt_ft | INTEGER | Last `nav_altitude_mcp` before first sustained descent |
| emergency_flag | TEXT | Latest non-"none" `detail.emergency` value (e.g. lifeguard) |
| path_length_km | REAL | Sum of haversine between consecutive points (skipping coverage holes) |
| max_distance_km | REAL | Max distance reached from the takeoff point |
| loiter_ratio | REAL | path_length / (2 * max_distance) - 1.0 = straight, 3+ = orbiting |
| path_efficiency | REAL | great_circle / path_length, only when origin != destination |
| max_hover_secs | INTEGER | Longest contiguous hover (rotorcraft only, NULL otherwise) |
| hover_episodes | INTEGER | Count of hover episodes ≥ 20 s (rotorcraft only) |
| go_around_count | INTEGER | Approach → climb → approach sequences in the final 600 s |
| takeoff_heading_deg | REAL | Circular mean of track in first 60 s of takeoff (gs > 40 kt) |
| landing_heading_deg | REAL | Circular mean of track in last 60 s before landing (gs > 40 kt) |
| climb_secs / descent_secs / level_secs / cruise_secs | INTEGER | Phase-of-flight time budget |
| cruise_alt_ft / cruise_gs_kt | INTEGER | Mean altitude / gs during cruise (NULL on short flights) |
| peak_climb_fpm / peak_descent_fpm | INTEGER | Best 30-s rolling-window mean climb / descent rate |
| takeoff_is_night / landing_is_night | INTEGER | 1 if sun was below -6° at takeoff/landing |
| night_flight | INTEGER | 1 if ≥50% of in-flight points were at night |
| callsigns | TEXT | JSON array of distinct callsigns seen |
| callsign_changes | INTEGER | Number of transitions between distinct callsigns |
| probable_destination_icao | TEXT | Inferred destination for dropped/signal-lost flights (NULL otherwise) |
| probable_destination_distance_km | REAL | Distance from last_seen position to inferred destination |
| probable_destination_confidence | REAL | [0.0, 1.0] confidence in the inference |

**aircraft_registry** - Authoritative metadata per ICAO (resolves drift across daily fetches)
| Column | Type | Description |
|--------|------|-------------|
| icao | TEXT | ICAO hex code (primary key) |
| registration | TEXT | Authoritative tail number (most recent fetch wins) |
| type_code | TEXT | Authoritative type code |
| description | TEXT | Type description |
| owner_operator | TEXT | Owner string |
| year | TEXT | Year of manufacture |
| last_updated | TEXT | When the registry row was last refreshed |
| metadata_drift_count | INTEGER | Number of trace_days rows that disagreed with the latest |
| metadata_drift_values | TEXT | JSON list of conflicting (type_code, description, count) entries |

**aircraft_stats** - Materialized rollup of utilization per aircraft (refreshed on every extract)
| Column | Type | Description |
|--------|------|-------------|
| icao | TEXT | ICAO hex code (primary key) |
| registration | TEXT | From aircraft_registry |
| type_code | TEXT | From aircraft_registry |
| first_seen / last_seen | TEXT | Earliest / latest takeoff_date in flights |
| total_flights | INTEGER | Count of all flights |
| confirmed_flights | INTEGER | Count where landing_type = 'confirmed' |
| total_hours | REAL | Sum of duration_minutes / 60 |
| total_cycles | INTEGER | Cycles (= confirmed_flights) |
| distinct_airports | INTEGER | Distinct origin or destination ICAOs |
| distinct_callsigns | INTEGER | Distinct callsigns observed |
| avg_flight_minutes | REAL | Mean flight duration |
| busiest_day_date | TEXT | Date with the most flights |
| busiest_day_count | INTEGER | Number of flights on busiest_day_date |
| updated_at | TEXT | When the rollup was last refreshed |

**fetch_log** - Tracks which dates have been fetched per source
| Column | Type | Description |
|--------|------|-------------|
| icao | TEXT | ICAO hex code |
| date | TEXT | Date checked |
| source | TEXT | Data source |
| status | INTEGER | HTTP status (200, 404, etc.) |

**airports** - OurAirports database (~47k airports)
| Column | Type | Description |
|--------|------|-------------|
| ident | TEXT | ICAO code (primary key) |
| type | TEXT | large_airport / medium_airport / small_airport / heliport / closed |
| name | TEXT | Airport name |
| latitude_deg/longitude_deg | REAL | Coordinates |
| elevation_ft | INTEGER | Elevation |
| iso_country | TEXT | ISO country code |
| iso_region | TEXT | ISO region code |
| municipality | TEXT | City |
| iata_code | TEXT | IATA code |

## Development

Install dev dependencies:

```
uv sync --extra dev
```

Run tests:

```
uv run pytest
```

Lint and format:

```
uv run ruff check .
uv run ruff format .
```

Type check:

```
uv run mypy adsbtrack
```

CI runs automatically on push and pull requests via GitHub Actions (Python 3.12 and 3.13).

## Notes

- Data availability depends on ADS-B receiver coverage. Flights over oceans or remote areas will have gaps - those show up as `signal_lost` or `dropped_on_approach` in the classifier rather than as missing flights.
- All readsb-based sources (adsbx, adsbfi, airplanes.live, adsb.lol, theairtraffic) use the same globe_history endpoint format. Different networks have different receiver coverage, so fetching from multiple sources and letting the trace merger combine them gives the best results.
- Rate limiting is adaptive - if the server returns 429, the delay between requests automatically increases and then gradually recovers after consecutive successes.
- When re-running `extract --reprocess` after upgrading, all flights for the specified aircraft are cleared and rebuilt from the raw trace data. The schema migrates automatically, so older databases pick up new quality columns without manual intervention.
