# adsbtrack

Pull historical ADS-B trace data from [ADS-B Exchange](https://globe.adsbexchange.com/) and turn it into a structured flight history for any aircraft. Give it an ICAO hex code and a date range and it will fetch every day of trace data, extract individual flights, match takeoff/landing coordinates to airports, and give you travel pattern statistics.

Built for OSINT and aviation nerds who want to go beyond live tracking and dig into where an aircraft has actually been over months or years.

## What it does

1. **Fetch** - Downloads daily trace files from the ADS-B Exchange globe_history endpoint
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
uv run python -m adsbtrack.cli fetch --hex adf64f --start 2020-01-01
```

Options:
- `--hex` - ICAO hex code (required). You can look these up from tail numbers on sites like [aircraftdata.org](https://aircraftdata.org)
- `--start` - Start date, YYYY-MM-DD (default: 2025-01-01)
- `--end` - End date, YYYY-MM-DD (default: today)
- `--rate` - Seconds between requests (default: 0.5)
- `--db` - Database path (default: adsbtrack.db)

The first run will automatically download the airport database (~10k airports). Fetching skips dates that have already been downloaded, so you can safely re-run to extend the date range or retry after interruptions.

### View flight history

```
uv run python -m adsbtrack.cli trips --hex adf64f
```

Options:
- `--from` - Filter from date
- `--to` - Filter to date
- `--airport` - Filter by airport ICAO code (e.g. `--airport KSRQ`)

### View statistics

```
uv run python -m adsbtrack.cli status --hex adf64f
```

Shows registration, aircraft type, owner, date range, flight count, and top airports with visit counts.

### Re-extract flights

```
uv run python -m adsbtrack.cli extract --hex adf64f --reprocess
```

Useful if you want to rebuild the flight table from raw trace data (e.g. after code changes to the parser).

## How it works

ADS-B Exchange stores daily trace files for every aircraft they've seen. Each trace is a series of timestamped position reports with lat/lon, altitude, ground speed, and metadata. The fetcher downloads these day by day and stores them in SQLite.

The flight extractor walks through the trace points in chronological order and detects state transitions between ground and airborne. It uses altitude ("ground" flag) and ground speed (>80kt hysteresis) to determine takeoff and landing events. Flights shorter than 5 minutes that travel less than 5km are filtered out as taxi movements.

Airport matching uses a bounding-box query against the OurAirports database followed by haversine distance calculation, with a 10km threshold.

## Finding hex codes

To look up the ICAO hex code for a US tail number (N-number):
- [aircraftdata.org](https://aircraftdata.org) - search by N-number, shows Mode S hex
- [FAA Aircraft Registry](https://registry.faa.gov/aircraftinquiry) - official source
- [ADS-B Exchange](https://globe.adsbexchange.com/) - search box accepts tail numbers

## Notes

- Data availability depends on ADS-B receiver coverage. Flights over oceans or remote areas may have gaps.
- The globe_history endpoint serves the same data as the "previous day" button in the ADS-B Exchange web UI.
- Rate limiting is adaptive - if the server returns 429, the delay between requests automatically increases and then gradually recovers after consecutive successes.
