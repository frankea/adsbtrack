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

- **[Database schema](docs/schema.md)** - full column reference for every table (traces / flights / registry / stats / airports / helipads / FAA registry / ACARS / hex crossref / mil ranges)
- **[Features and scoring](docs/features.md)** - landing types, confidence scoring algorithm, all derived per-flight columns, mission classification rules, signal budget, ACARS OOOI, position-source breakdown
- **[Internals](docs/internals.md)** - how the extractor works: trace merging, flight extraction state machine, fragment stitching, airport matching, FAA registry parser, hex crossref merge

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
