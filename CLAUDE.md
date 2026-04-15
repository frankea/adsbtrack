# CLAUDE.md

## Project overview

adsbtrack is a Python CLI that pulls historical ADS-B trace data from multiple tracking networks and extracts structured flight histories for individual aircraft. It stores everything in a local SQLite database.

## Commands

All commands use `uv run`:

```bash
uv run python -m adsbtrack.cli fetch --hex <icao> --start <date>   # Download traces
uv run python -m adsbtrack.cli extract --hex <icao> --reprocess    # Re-extract flights
uv run python -m adsbtrack.cli status --hex <icao>                 # View stats
uv run python -m adsbtrack.cli trips --hex <icao>                  # View flight history
```

## Development

```bash
uv sync --extra dev          # Install dependencies
uv run pytest                # Run tests (164 tests)
uv run ruff check .          # Lint
uv run ruff format .         # Format
uv run mypy adsbtrack        # Type check (informational)
```

## Before committing

Always run:
```bash
uv run ruff check . && uv run ruff format --check . && uv run pytest
```

CI runs ruff check, ruff format --check, pytest, and mypy on Python 3.12 and 3.13.

## Code conventions

- Line length: 120 characters
- Python 3.12+ features (type unions with `|`, etc.)
- Use regular hyphens, not em dashes, in all text output
- Prefer editing existing files over creating new ones
- Commit messages: concise, focus on "why" not "what"

## Architecture

- `adsbtrack/parser.py` -- main extraction pipeline (trace -> flights)
- `adsbtrack/classifier.py` -- ground/airborne state machine, FlightMetrics accumulator
- `adsbtrack/features.py` -- derived per-flight features (phase budget, cruise GS, hover, etc.)
- `adsbtrack/db.py` -- SQLite schema, migrations, queries
- `adsbtrack/config.py` -- type ceilings, GS caps, endurance tables, thresholds
- `adsbtrack/airports.py` -- airport matching, helipad name enrichment
- `adsbtrack/models.py` -- Flight and AirportMatch dataclasses
- `adsbtrack/cli.py` -- Click CLI commands
- `adsbtrack/fetcher.py` -- HTTP trace fetching from multiple sources

## Key design decisions

- max_altitude uses AP-validated persistence filtering (only samples with nav_altitude_mcp that agrees within 5,000 ft enter the persisted peak tracker)
- cruise_gs_kt is a time-weighted median of cruise-phase GS samples with 2-sigma outlier rejection
- Type-specific ceiling caps applied without tolerance when AP data is absent or disagrees
- Helipad names enriched from OurAirports CSV + manual overrides
- 13 regression gates validated across rounds
