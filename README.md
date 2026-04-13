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

Downloads daily traces, then auto-extracts flights. Options: `--source` (adsbx, adsbfi, airplaneslive, adsblol, theairtraffic, opensky), `--end`, `--rate`, `--db`, `--tail` (converts N-number to hex). Skips dates already fetched. WAL mode lets multiple fetches run in parallel.

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

Shows data quality breakdown, mission type distribution, utilization rollup, night/emergency indicators, and top airports.

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

`Conf` is landing confidence (0-100%), `Type` is landing classification (OK/SIG LOST/DROP/UNCERT/ALT ERR). Options: `--from`, `--to`, `--airport`.

### Re-extract flights

```
uv run python -m adsbtrack.cli extract --hex a66ad3 --reprocess
```

Rebuilds the flight table from raw trace data after code changes.

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

Traces from multiple sources are automatically merged during extraction.

| Source | Flag | Notes |
|--------|------|-------|
| [ADS-B Exchange](https://globe.adsbexchange.com/) | `--source adsbx` | Default |
| [adsb.fi](https://globe.adsb.fi/) | `--source adsbfi` | |
| [airplanes.live](https://globe.airplanes.live/) | `--source airplaneslive` | |
| [adsb.lol](https://adsb.lol/) | `--source adsblol` | |
| [TheAirTraffic](https://globe.theairtraffic.com/) | `--source theairtraffic` | |
| [OpenSky Network](https://opensky-network.org/) | `--source opensky` | Requires `OPENSKY_CLIENT_ID` + `OPENSKY_CLIENT_SECRET` env vars |
| Custom | `--url <base_url>` | Any readsb globe_history instance |

## Documentation

Detailed reference docs for contributors and analysts:

- **[Database schema](docs/schema.md)** - full column reference for all 5 tables (trace_days, flights, aircraft_registry, aircraft_stats, fetch_log, airports)
- **[Features and scoring](docs/features.md)** - landing types, confidence scoring algorithm, all derived per-flight columns, mission classification rules, signal budget, aircraft registry/stats
- **[Internals](docs/internals.md)** - how the extractor works: trace merging, flight extraction state machine, fragment stitching, airport matching

## Development

```
uv sync --extra dev
uv run pytest
uv run ruff check .
uv run ruff format .
uv run mypy adsbtrack
```

CI runs on push and pull requests (Python 3.12 and 3.13).

## Notes

- Data availability depends on ADS-B receiver coverage. Flights over oceans or remote areas will have gaps - those show up as `signal_lost` or `dropped_on_approach` rather than as missing flights.
- Different receiver networks have different coverage, so fetching from multiple sources gives the best results.
- Rate limiting is adaptive: 429s increase the delay, consecutive successes recover it.
- `extract --reprocess` clears and rebuilds all flights from raw traces. Schema migrates automatically.
