# adsbtrack

Pull historical ADS-B trace data from multiple tracking networks and turn it into a structured flight history for any aircraft. Give it an ICAO hex code and a date range and it will fetch every day of trace data, extract individual flights, match takeoff/landing coordinates to airports, classify each landing by quality, and give you travel pattern statistics.

Built for OSINT and aviation nerds who want to go beyond live tracking and dig into where an aircraft has actually been over months or years - with enough signal quality metadata to trust the answers.

## What it does

1. **Fetch** - Downloads daily trace files from any readsb-based tracking network (adsbx, adsbfi, airplanes.live, adsb.lol, theairtraffic, OpenSky)
2. **Extract** - Parses raw trace points into discrete flights using a state machine that fuses barometric altitude, geometric altitude, ground speed, and vertical rate. Splits flights on multi-hour coverage gaps; keeps flights open for a post-landing window to validate the touchdown.
3. **Classify** - Every flight gets a landing type (`confirmed`, `signal_lost`, `dropped_on_approach`, `uncertain`, `altitude_error`) plus independent takeoff and landing confidence scores from 0.0 to 1.0. The classifier handles tricky cases like Bell 407 hover-at-altitude baro errors and military Mode S altitude spoofing.
4. **Stitch** - A post-processing pass merges signal-lost + mid-flight fragments that are plausibly the same continuous flight with a coverage hole, recovering the original takeoff context.
5. **Match** - Matches takeoff/landing coordinates to the nearest airport using the OurAirports database, skipping matches on `signal_lost` / `dropped_on_approach` to prevent false attributions.
6. **Analyze** - Shows flight history with confidence coloring, top airports, and a data quality breakdown per aircraft.

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
Fetching a66ad3 from 2020-01-01 to 2026-04-08
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

  Date range:    2020-01-01 to 2026-04-08
  Days checked:  2290
  Days w/ data:  392
  Total flights: 665

Data quality:

  Confirmed landings:   361 (54%)
  Signal lost:           74 (11%)
  Uncertain:             39 (6%)
  Altitude errors:       11 (2%)

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

The Data quality breakdown summarizes how each aircraft's flights classify. See [Flight quality and confidence scoring](#flight-quality-and-confidence-scoring) below for what each label means.

### View flight history

```
uv run python -m adsbtrack.cli trips --hex a66ad3 --from 2026-03-25
```

```
                                Flights for a66ad3
┏━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━┳━━━━━━━━┓
┃ Date       ┃ From      ┃ To        ┃ Duration ┃ Callsign ┃ Conf ┃ Type   ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━╇━━━━━━━━┩
│ 2026-03-27 │ 67FL      │ KSPG      │ 10m      │ N512WB   │  76% │ OK     │
│ 2026-03-27 │ KSPG      │ KHKY      │ 1h 53m   │ N512WB   │  89% │ OK     │
│ 2026-03-28 │ KHKY      │ KVNC      │ 2h 41m   │ N512WB   │  86% │ OK     │
│ 2026-03-28 │ KVNC      │ KHKY      │ 2h 19m   │ N512WB   │  94% │ OK     │
│ 2026-03-29 │ KHKY      │ KSPG      │ 2h 23m   │ N512WB   │  73% │ OK     │
│ 2026-03-29 │ KSPG      │ uncertain │ 12m      │ N512WB   │   0% │ UNCERT │
└────────────┴───────────┴───────────┴──────────┴──────────┴──────┴────────┘

Total: 6 flights
```

The `Conf` column is the landing confidence score (0-100%) and `Type` is the
landing classification - `OK` for confirmed landings, `SIG LOST` for airborne
at last contact, `UNCERT` for ambiguous, `ALT ERR` for broken baro encoder.
Confidence is color-coded: green ≥80%, yellow 50-79%, red <50%.

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

- **Intra-trace gap splitting**: any gap longer than 30 minutes between consecutive points closes the pending flight. Real operations have 3.5 min median max gaps; multi-hour gaps are coverage holes that should not be stitched across.
- **Post-landing window**: after a ground transition, the flight stays "open" for up to 60 seconds or 5 more ground points. This populates landing-quality metrics (`ground_points_at_landing`, per-sample coordinate stability) with real data instead of always being 1/0.
- **Touch-and-go detection**: an airborne point inside the post-landing window finalizes the current flight and immediately opens a new one.
- **Short-movement filter**: flights shorter than 5 minutes that travel less than 5 km are filtered out as taxi movements.

### Fragment stitching

After extraction, a post-processing pass walks each aircraft's flights chronologically and merges pairs where a previous flight ended without a landing and the next flight starts with `takeoff_type = found_mid_flight`, within:

- **90 minutes** between the previous flight's last-seen point and the next flight's first point
- **Great-circle distance** less than `cruise_speed × time_gap × 1.2` (with 300 kt as the upper bound)
- **Altitude delta** under 3000 ft

The stitched flight inherits the original takeoff position and time, which recovers the actual origin airport for flights that would otherwise be classified as mid-flight fragments.

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

**Max endurance is per aircraft type**: a 240 min global cap would reject legitimate Gulfstream transcons, so the classifier consults a type_code lookup (B407=180, S92=300, PC12=420, GLF6=900, etc.). Flights longer than the type's endurance become `uncertain` rather than `confirmed`.

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

**flights** - Extracted flights with airport matching, quality classification, and confidence scoring
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
