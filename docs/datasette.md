# Datasette pairing

[Datasette](https://datasette.io) is a zero-config web UI for SQLite. `adsbtrack.db` is a plain SQLite file, which means you can pair it with Datasette for interactive exploration in about 60 seconds. No changes to the extractor or database schema; Datasette reads the file as-is.

## Install

Using [uv](https://github.com/astral-sh/uv):

```
uv tool install datasette
```

Or pipx:

```
pipx install datasette
```

## Quick start

Point Datasette at a database file that `adsbtrack` has already populated:

```
datasette adsbtrack.db
```

Then open <http://localhost:8001>. You get a browsable UI over every table (`flights`, `trace_days`, `aircraft_registry`, `aircraft_stats`, `airports`, `helipads`, `navaids`, `runways`, `faa_registry`, `faa_deregistered`, `faa_aircraft_ref`, `hex_crossref`, `mil_hex_ranges`, `oooi_messages`, `fetch_log`), plus the full schema browser, full-text search where indexes support it, and a SQL window for ad-hoc queries.

## Canned queries

This repo ships a [`docs/datasette-metadata.json`](./datasette-metadata.json) with five starter queries that exercise signals adsbtrack produces but that aren't surfaced by the CLI:

| Query | What it shows |
|-------|---------------|
| **Emergency-flagged flights** | Individual flights with a non-null `emergency_flag` (e.g. `nordo`) or a 7500/7600/7700 squawk captured in `emergency_squawk` |
| **Top 10 aircraft this year** | Most active hexes this calendar year, joined to `aircraft_registry` for registration / type / operator |
| **Military activity (last 7 days)** | Flights whose ICAO hex falls in a curated military allocation range (`mil_hex_ranges`) within the last week |
| **Flights with signal gaps > 10 min** | Within-flight ADS-B coverage holes exceeding 10 minutes; ranked by gap duration |
| **Off-airport landings** | Confirmed landings where neither airport nor helipad matched — helicopters on private pads, fixed-wing on unregistered strips |

Load it with:

```
datasette adsbtrack.db -m docs/datasette-metadata.json
```

The queries then appear on the database homepage under "Queries" and as individual URLs (e.g. `http://localhost:8001/adsbtrack/emergency_flights`).

## Extending

Add your own canned queries by editing the `queries` block in `docs/datasette-metadata.json`. Datasette supports parameters (`:param_name` in the SQL), descriptions, and titles — see the [Datasette canned-queries docs](https://docs.datasette.io/en/stable/sql_queries.html#canned-queries) for the full surface.

For plotting, install the `datasette-cluster-map` or `datasette-vega` plugins. The `takeoff_lat` / `takeoff_lon` / `landing_lat` / `landing_lon` columns on `flights` work out of the box with `datasette-cluster-map`.

## Read-only deployment

If you want to publish a snapshot of your `adsbtrack.db` as a static read-only website:

```
datasette publish cloudrun adsbtrack.db --service=my-adsbtrack
```

See [Datasette publish docs](https://docs.datasette.io/en/stable/publish.html) for Fly, Heroku, Vercel, and Google Cloud Run targets.
