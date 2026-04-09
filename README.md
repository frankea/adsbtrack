# adsbtrack

Pull historical ADS-B trace data from multiple tracking networks and turn it into a structured flight history for any aircraft. Give it an ICAO hex code and a date range and it will fetch every day of trace data, extract individual flights, match takeoff/landing coordinates to airports, and give you travel pattern statistics.

Built for OSINT and aviation nerds who want to go beyond live tracking and dig into where an aircraft has actually been over months or years.

## What it does

1. **Fetch** - Downloads daily trace files from any readsb-based tracking network
2. **Extract** - Parses raw trace points into discrete flights (takeoff/landing detection, ground speed hysteresis filtering)
3. **Match** - Matches takeoff/landing coordinates to the nearest airport using the OurAirports database
4. **Analyze** - Shows flight history, top airports, and database statistics

Everything is stored in a local SQLite database. Fetches are logged so you can resume interrupted runs without re-downloading.

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

```
Fetching a66ad3 from 2020-01-01 to 2026-04-08
Fetching a66ad3 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 2163/2163 0:00:00

Done! Fetched: 2163, With data: 391, Skipped (already fetched): 127, Errors: 0

Extracting flights...
Found 305 flights
```

Options:
- `--hex` - ICAO hex code (required). You can look these up from tail numbers on sites like [aircraftdata.org](https://aircraftdata.org)
- `--start` - Start date, YYYY-MM-DD (default: 2025-01-01)
- `--end` - End date, YYYY-MM-DD (default: today)
- `--rate` - Seconds between requests (default: 0.5)
- `--db` - Database path (default: adsbtrack.db)

The first run will automatically download the airport database (~10k airports). Fetching skips dates that have already been downloaded, so you can safely re-run to extend the date range or retry after interruptions.

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
  Total flights: 305

Top airports:

┏━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┓
┃ Airport ┃ Name                                     ┃ Visits ┃
┡━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━┩
│ KSRQ    │ Sarasota Bradenton International Airport │    130 │
│ KVNC    │ Venice Municipal Airport                 │     54 │
│ 67FL    │ Bald Eagle Airstrip                      │     26 │
│ KFXE    │ Fort Lauderdale Executive Airport        │     16 │
│ KFFC    │ Peachtree City Falcon Field              │     16 │
│ KTRI    │ Tri-Cities Regional TN/VA Airport        │     14 │
│ KBAZ    │ New Braunfels National Airport           │     11 │
│ KPBI    │ Palm Beach International Airport         │     10 │
│ KOPF    │ Miami-Opa Locka Executive Airport        │     10 │
│ KBNA    │ Nashville International Airport          │     10 │
└─────────┴──────────────────────────────────────────┴────────┘
```

### View flight history

```
uv run python -m adsbtrack.cli trips --hex a66ad3 --from 2026-03-25
```

```
                               Flights for a66ad3
┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┓
┃ Date       ┃ From                ┃ To                  ┃ Duration ┃ Callsign ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━┩
│ 2026-03-27 │ 67FL (Bald Eagle    │ KSPG (Albert        │ 10m      │ N512WB   │
│            │ Airstrip)           │ Whitted Airport)    │          │          │
│ 2026-03-27 │ KSPG (Albert        │ KHKY (Hickory       │ 3h 1m    │ N512WB   │
│            │ Whitted Airport)    │ Regional Airport)   │          │          │
│ 2026-03-28 │ KHKY (Hickory       │ KVNC (Venice        │ 4h 17m   │ N512WB   │
│            │ Regional Airport)   │ Municipal Airport)  │          │          │
│ 2026-03-28 │ KVNC (Venice        │ KHKY (Hickory       │ 2h 19m   │ N512WB   │
│            │ Municipal Airport)  │ Regional Airport)   │          │          │
│ 2026-03-29 │ KHKY (Hickory       │ KSPG (Albert        │ 2h 23m   │ N512WB   │
│            │ Regional Airport)   │ Whitted Airport)    │          │          │
│ 2026-03-29 │ KSPG (Albert        │ in flight?          │          │ N512WB   │
│            │ Whitted Airport)    │                     │          │          │
└────────────┴─────────────────────┴─────────────────────┴──────────┴──────────┘

Total: 6 flights
```

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

ADS-B Exchange stores daily trace files for every aircraft they've seen. Each trace is a series of timestamped position reports with lat/lon, altitude, ground speed, and metadata. The fetcher downloads these day by day and stores them in SQLite.

The flight extractor walks through the trace points in chronological order and detects state transitions between ground and airborne. It uses altitude ("ground" flag) and ground speed (>80kt hysteresis) to determine takeoff and landing events. Flights shorter than 5 minutes that travel less than 5km are filtered out as taxi movements.

Airport matching uses a bounding-box query against the OurAirports database followed by haversine distance calculation, with a 10km threshold.

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

You can also point at any readsb-compatible instance with `--url`:

```
uv run python -m adsbtrack.cli fetch --hex a66ad3 --url https://your-instance/globe_history
```

## Database schema

All data is stored in a local SQLite database (`adsbtrack.db`).

**trace_days** - Raw daily trace data per aircraft per source
| Column | Type | Description |
|--------|------|-------------|
| icao | TEXT | ICAO hex code |
| date | TEXT | Date (YYYY-MM-DD) |
| source | TEXT | Data source (adsbx, adsbfi, airplaneslive, etc.) |
| registration | TEXT | Tail number from trace metadata |
| description | TEXT | Aircraft type |
| owner_operator | TEXT | Owner from trace metadata |
| timestamp | REAL | Base Unix timestamp for the day |
| trace_json | TEXT | Raw trace points as JSON array |
| point_count | INTEGER | Number of trace points |

**flights** - Extracted flights with airport matching
| Column | Type | Description |
|--------|------|-------------|
| icao | TEXT | ICAO hex code |
| takeoff_time | TEXT | ISO timestamp |
| takeoff_lat/lon | REAL | Takeoff coordinates |
| landing_time | TEXT | ISO timestamp (null if signal lost) |
| landing_lat/lon | REAL | Landing coordinates |
| origin_icao | TEXT | Nearest airport ICAO code |
| destination_icao | TEXT | Nearest airport ICAO code |
| duration_minutes | REAL | Flight duration |
| callsign | TEXT | Callsign from ADS-B |

**fetch_log** - Tracks which dates have been fetched per source
| Column | Type | Description |
|--------|------|-------------|
| icao | TEXT | ICAO hex code |
| date | TEXT | Date checked |
| source | TEXT | Data source |
| status | INTEGER | HTTP status (200, 404, etc.) |

**airports** - OurAirports database (~10k airports)
| Column | Type | Description |
|--------|------|-------------|
| ident | TEXT | ICAO code (primary key) |
| name | TEXT | Airport name |
| latitude_deg/longitude_deg | REAL | Coordinates |
| municipality | TEXT | City |
| iata_code | TEXT | IATA code |

## Notes

- Data availability depends on ADS-B receiver coverage. Flights over oceans or remote areas may have gaps.
- All readsb-based sources (adsbx, adsbfi, airplanes.live, adsb.lol, theairtraffic) use the same globe_history endpoint format. Different networks have different receiver coverage, so fetching from multiple sources gives the best results.
- Rate limiting is adaptive - if the server returns 429, the delay between requests automatically increases and then gradually recovers after consecutive successes.
