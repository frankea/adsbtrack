import os
from datetime import date
from importlib.metadata import version as pkg_version
from pathlib import Path
from urllib.parse import urlparse

import click
from rich.console import Console
from rich.table import Table

from .airports import download_airports, enrich_helipad_names
from .config import SOURCE_URLS, Config
from .db import Database
from .fetcher import fetch_traces, fetch_traces_opensky
from .nnumber import nnumber_to_icao
from .parser import extract_flights

ALL_SOURCES = list(SOURCE_URLS.keys()) + ["opensky"]
# "all" fetches from every readsb source (excludes opensky which needs creds)
ALL_SOURCES_WITH_ALL = ["all"] + ALL_SOURCES

console = Console()


def get_db_and_config(db_path: str) -> tuple[Database, Config]:
    config = Config(db_path=Path(db_path))
    db = Database(config.db_path)
    return db, config


def ensure_airports(db: Database, config: Config):
    if db.airport_count() == 0:
        console.print("[yellow]Airport database empty, downloading...[/]")
        count = download_airports(db, config)
        console.print(f"[green]Loaded {count} airports[/]")


def _resolve_hex(hex_code: str | None, tail_number: str | None) -> str:
    """Resolve an ICAO hex code from --hex or --tail options.

    Exactly one of hex_code or tail_number must be provided.
    """
    if hex_code and tail_number:
        raise click.UsageError("Provide either --hex or --tail, not both.")
    if not hex_code and not tail_number:
        raise click.UsageError("Provide either --hex or --tail.")
    if tail_number:
        try:
            hex_code = nnumber_to_icao(tail_number)
        except ValueError as e:
            raise click.BadParameter(str(e), param_hint="--tail") from e
        console.print(f"[dim]Converted {tail_number} to hex {hex_code}[/]")
    return hex_code.lower()


def _get_version() -> str:
    try:
        return pkg_version("adsbtrack")
    except Exception:
        return "0.1.0"


@click.group()
@click.version_option(version=_get_version(), prog_name="adsbtrack")
def cli():
    """Track private plane travel history using ADS-B Exchange data."""
    pass


@cli.command()
@click.option("--hex", "hex_code", default=None, help="ICAO hex code (e.g. adf64f)")
@click.option("--tail", "tail_number", default=None, help="FAA N-number (e.g. N512WB), converted to hex automatically")
@click.option(
    "--source",
    type=click.Choice(ALL_SOURCES_WITH_ALL),
    default="adsbx",
    help="Data source, or 'all' to fetch from every readsb source (default: adsbx)",
)
@click.option("--url", "custom_url", default=None, help="Custom readsb globe_history base URL")
@click.option("--start", "start_date", default="2025-01-01", help="Start date (YYYY-MM-DD)")
@click.option("--end", "end_date", default=None, help="End date (YYYY-MM-DD), defaults to today")
@click.option("--rate", default=0.5, help="Seconds between requests")
@click.option("--db", "db_path", default="adsbtrack.db", help="Database path")
def fetch(hex_code, tail_number, source, custom_url, start_date, end_date, rate, db_path):
    """Download trace data from ADS-B data sources."""
    hex_code = _resolve_hex(hex_code, tail_number)

    with Database(Path(db_path)) as db:
        config = Config(db_path=Path(db_path))
        config.rate_limit = rate

        ensure_airports(db, config)

        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date) if end_date else date.today()

        if custom_url:
            parsed = urlparse(custom_url)
            if not parsed.scheme or not parsed.netloc:
                raise click.BadParameter(
                    f"Invalid URL: {custom_url}. Must be a full URL like https://example.com/globe_history",
                    param_hint="--url",
                )
            source_name = parsed.netloc.replace(".", "_")
            SOURCE_URLS[source_name] = custom_url
            sources_to_fetch = [source_name]
            console.print(f"Fetching [bold]{hex_code}[/] from {start} to {end} via [cyan]{custom_url}[/]")
        elif source == "all":
            # Fetch from every readsb source + opensky if credentials exist
            sources_to_fetch = list(SOURCE_URLS.keys())
            opensky_available = bool(os.environ.get("OPENSKY_CLIENT_ID") and os.environ.get("OPENSKY_CLIENT_SECRET"))
            if not opensky_available:
                # Check credentials.json fallback
                creds_path = config.credentials_path
                if creds_path.exists():
                    import json

                    try:
                        creds = json.loads(creds_path.read_text())
                        opensky_available = bool(creds.get("clientId") and creds.get("clientSecret"))
                    except Exception:
                        pass
            if opensky_available:
                sources_to_fetch.append("opensky")
            console.print(
                f"Fetching [bold]{hex_code}[/] from {start} to {end} via "
                f"[cyan]all {len(sources_to_fetch)} sources[/]"
                + (" (incl. OpenSky)" if opensky_available else " (OpenSky skipped: no credentials)")
            )
        else:
            sources_to_fetch = [source]
            console.print(f"Fetching [bold]{hex_code}[/] from {start} to {end} via [cyan]{source}[/]")

        total_stats = {"fetched": 0, "with_data": 0, "skipped": 0, "errors": 0}
        if len(sources_to_fetch) > 1:
            # Parallel fetch: each source in its own thread with its own
            # DB connection (SQLite WAL supports concurrent writers).
            import threading

            lock = threading.Lock()

            def _fetch_one(src: str) -> None:
                with Database(Path(db_path)) as thread_db:
                    thread_config = Config(db_path=Path(db_path))
                    thread_config.rate_limit = rate
                    if src == "opensky":
                        stats = fetch_traces_opensky(thread_db, thread_config, hex_code, start, end)
                    else:
                        stats = fetch_traces(thread_db, thread_config, hex_code, start, end, source=src)
                    with lock:
                        for k in total_stats:
                            total_stats[k] += stats[k]

            threads = [threading.Thread(target=_fetch_one, args=(src,)) for src in sources_to_fetch]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        else:
            src = sources_to_fetch[0]
            if src == "opensky":
                stats = fetch_traces_opensky(db, config, hex_code, start, end)
            else:
                stats = fetch_traces(db, config, hex_code, start, end, source=src)
            for k in total_stats:
                total_stats[k] += stats[k]

        console.print(
            f"\n[green]Done![/] Fetched: {total_stats['fetched']}, "
            f"With data: {total_stats['with_data']}, "
            f"Skipped (already fetched): {total_stats['skipped']}, "
            f"Errors: {total_stats['errors']}"
        )

        # Auto-extract flights
        console.print("\nExtracting flights...")
        count = extract_flights(db, config, hex_code, reprocess=True)
        console.print(f"[green]Found {count} flights[/]")
        try:
            enriched = enrich_helipad_names(db, config)
            if enriched:
                console.print(f"[green]Enriched {enriched} helipad names[/]")
        except Exception:
            pass


@cli.command()
@click.option("--hex", "hex_code", required=True, help="ICAO hex code")
@click.option("--reprocess", is_flag=True, help="Clear and rebuild all flights")
@click.option("--db", "db_path", default="adsbtrack.db")
def extract(hex_code, reprocess, db_path):
    """Process raw traces into flights."""
    hex_code = hex_code.lower()
    with Database(Path(db_path)) as db:
        config = Config(db_path=Path(db_path))
        ensure_airports(db, config)
        count = extract_flights(db, config, hex_code, reprocess=reprocess)
        console.print(f"[green]Extracted {count} flights[/]")
        # v12 N13: enrich generic helipad names from OurAirports heliport data.
        try:
            enriched = enrich_helipad_names(db, config)
            if enriched:
                console.print(f"[green]Enriched {enriched} helipad names[/]")
        except Exception:
            pass


@cli.command()
@click.option("--hex", "hex_code", required=True, help="ICAO hex code")
@click.option("--from", "from_date", default=None, help="Filter from date (YYYY-MM-DD)")
@click.option("--to", "to_date", default=None, help="Filter to date (YYYY-MM-DD)")
@click.option("--airport", default=None, help="Filter by airport ICAO code")
@click.option("--db", "db_path", default="adsbtrack.db")
def trips(hex_code, from_date, to_date, airport, db_path):
    """Show flight history."""
    hex_code = hex_code.lower()
    with Database(Path(db_path)) as db:
        flights = db.get_flights(hex_code, from_date, to_date, airport)

        if not flights:
            console.print("[yellow]No flights found[/]")
            return

        table = Table(title=f"Flights for {hex_code}")
        table.add_column("Date", style="cyan")
        table.add_column("From", style="green")
        table.add_column("To", style="green")
        table.add_column("Duration", style="yellow")
        table.add_column("Callsign", style="dim")
        table.add_column("Mission", style="magenta")
        table.add_column("Conf", justify="right")
        table.add_column("Type", style="dim")

        mission_display = {
            "ems_hems": "EMS",
            "offshore": "OFFSH",
            "exec_charter": "CHRT",
            "training": "TRAIN",
            "survey": "SRVY",
            "pattern": "PATRN",
            "transport": "XFER",
            "unknown": "",
        }

        def _col(row, name, default=None):
            try:
                return row[name]
            except (KeyError, IndexError):
                return default

        for f in flights:
            takeoff = f["takeoff_time"][:10] if f["takeoff_time"] else "?"
            origin = f["origin_icao"] or f"({f['takeoff_lat']:.2f}, {f['takeoff_lon']:.2f})"
            if f["origin_name"]:
                origin = f"{f['origin_icao']} ({f['origin_name']})"

            landing_type = f["landing_type"] or "unknown"

            if f["destination_icao"]:
                dest = f"{f['destination_icao']} ({f['destination_name']})"
            elif landing_type == "dropped_on_approach" and _col(f, "probable_destination_icao"):
                dest = f"[yellow]~{_col(f, 'probable_destination_icao')}[/]"
            elif landing_type == "signal_lost":
                dest = "[red]signal lost[/]"
            elif f["landing_lat"] is not None:
                dest = f"({f['landing_lat']:.2f}, {f['landing_lon']:.2f})"
            else:
                dest = "[yellow]uncertain[/]"

            duration = ""
            if f["duration_minutes"]:
                hours = int(f["duration_minutes"] // 60)
                mins = int(f["duration_minutes"] % 60)
                duration = f"{hours}h {mins}m" if hours else f"{mins}m"

            callsign = f["callsign"] or ""

            mission = _col(f, "mission_type") or ""
            mission = mission_display.get(mission, mission)

            # Confidence display
            conf = f["landing_confidence"]
            if conf is not None:
                pct = int(conf * 100)
                if conf >= 0.8:
                    conf_str = f"[green]{pct}%[/]"
                elif conf >= 0.5:
                    conf_str = f"[yellow]{pct}%[/]"
                else:
                    conf_str = f"[red]{pct}%[/]"
            else:
                conf_str = "[dim]--[/]"

            # Landing type display
            type_display = {
                "confirmed": "[green]OK[/]",
                "signal_lost": "[red]SIG LOST[/]",
                "dropped_on_approach": "[yellow]DROP[/]",
                "uncertain": "[yellow]UNCERT[/]",
                "altitude_error": "[red]ALT ERR[/]",
                "unknown": "[dim]--[/]",
            }.get(landing_type, "[dim]--[/]")

            table.add_row(takeoff, origin, dest, duration, callsign, mission, conf_str, type_display)

        console.print(table)
        console.print(f"\nTotal: {len(flights)} flights")


@cli.command()
@click.option("--hex", "hex_code", required=True, help="ICAO hex code")
@click.option("--db", "db_path", default="adsbtrack.db")
def status(hex_code, db_path):
    """Show database statistics."""
    hex_code = hex_code.lower()
    with Database(Path(db_path)) as db:
        total_fetched = db.get_total_days_fetched(hex_code)
        days_with_data = db.get_days_with_data(hex_code)
        flight_count = db.get_flight_count(hex_code)
        first_date, last_date = db.get_date_range(hex_code)
        top_airports = db.get_top_airports(hex_code)

        console.print(f"\n[bold]Status for {hex_code}[/]\n")

        # Get aircraft info from first trace day
        trace_days = db.get_trace_days(hex_code)
        if trace_days:
            td = trace_days[0]
            console.print(f"  Registration:  {td['registration']}")
            console.print(f"  Type:          {td['description']}")
            console.print(f"  Owner:         {td['owner_operator']}")
            console.print()

        console.print(f"  Date range:    {first_date or 'N/A'} to {last_date or 'N/A'}")
        console.print(f"  Days checked:  {total_fetched}")
        console.print(f"  Days w/ data:  {days_with_data}")
        console.print(f"  Total flights: {flight_count}")

        # Data quality summary
        quality = db.get_flight_quality_summary(hex_code)
        if quality and any(k != "unknown" for k in quality):
            console.print("\n[bold]Data quality:[/]\n")
            type_labels = {
                "confirmed": ("green", "Confirmed landings"),
                "signal_lost": ("red", "Signal lost"),
                "dropped_on_approach": ("yellow", "Dropped on approach"),
                "uncertain": ("yellow", "Uncertain"),
                "altitude_error": ("red", "Altitude errors"),
                "unknown": ("dim", "Unclassified"),
            }
            for lt, (color, label) in type_labels.items():
                if lt in quality:
                    q = quality[lt]
                    pct = q["count"] / flight_count * 100 if flight_count > 0 else 0
                    console.print(f"  [{color}]{label}:{' ' * (22 - len(label))}{q['count']:>4} ({pct:.0f}%)[/]")

        # v3: mission type breakdown
        mission_rows = db.conn.execute(
            "SELECT mission_type, COUNT(*) as cnt FROM flights WHERE icao = ? GROUP BY mission_type ORDER BY cnt DESC",
            (hex_code,),
        ).fetchall()
        if mission_rows and any(r["mission_type"] for r in mission_rows):
            console.print("\n[bold]Mission breakdown:[/]\n")
            for row in mission_rows:
                mt = row["mission_type"] or "(none)"
                pct = row["cnt"] / flight_count * 100 if flight_count > 0 else 0
                console.print(f"  {mt:<18}{row['cnt']:>4} ({pct:.0f}%)")

        # v3: aircraft_stats rollup
        try:
            stats_row = db.conn.execute("SELECT * FROM aircraft_stats WHERE icao = ?", (hex_code,)).fetchone()
        except Exception:
            stats_row = None
        if stats_row:
            console.print("\n[bold]Utilization:[/]\n")
            console.print(f"  Total hours:      {stats_row['total_hours'] or 0:.1f}")
            console.print(f"  Cycles:           {stats_row['total_cycles'] or 0}")
            console.print(f"  Avg flight:       {stats_row['avg_flight_minutes'] or 0:.1f} min")
            console.print(f"  Distinct airports: {stats_row['distinct_airports'] or 0}")
            console.print(f"  Distinct callsigns: {stats_row['distinct_callsigns'] or 0}")
            if stats_row["busiest_day_date"]:
                console.print(
                    f"  Busiest day:      {stats_row['busiest_day_date']} ({stats_row['busiest_day_count']} flights)"
                )

        # v3: emergency / night indicators
        night_count = db.conn.execute(
            "SELECT COUNT(*) FROM flights WHERE icao = ? AND night_flight = 1", (hex_code,)
        ).fetchone()[0]
        emergency_count = db.conn.execute(
            "SELECT COUNT(*) FROM flights WHERE icao = ? AND emergency_squawk IS NOT NULL", (hex_code,)
        ).fetchone()[0]
        if night_count > 0 or emergency_count > 0:
            console.print("\n[bold]Indicators:[/]\n")
            if night_count > 0:
                console.print(f"  Night flights:    {night_count}")
            if emergency_count > 0:
                console.print(f"  [red]Emergency squawks: {emergency_count}[/]")

        if top_airports:
            console.print("\n[bold]Top airports:[/]\n")
            table = Table(show_header=True)
            table.add_column("Airport", style="cyan")
            table.add_column("Name", style="green")
            table.add_column("Visits", style="yellow", justify="right")
            for ap in top_airports:
                table.add_row(ap["airport"], ap["name"], str(ap["visits"]))
            console.print(table)


@cli.command()
@click.option("--tail", "tail_number", required=True, help="FAA N-number (e.g. N512WB)")
def lookup(tail_number):
    """Convert an FAA N-number to an ICAO hex code."""
    try:
        hex_code = nnumber_to_icao(tail_number)
    except ValueError as e:
        raise click.BadParameter(str(e), param_hint="--tail") from e
    console.print(hex_code)


@cli.command()
@click.option("--hex", "hex_code", default=None, help="ICAO hex code")
@click.option("--tail", "tail_number", default=None, help="FAA N-number")
@click.option("--db", "db_path", default="adsbtrack.db")
@click.option(
    "--urls-only",
    is_flag=True,
    default=False,
    help=(
        "Print only one URL per line with no date/origin/destination prefix "
        "and no markup. Suitable for piping into shell loops."
    ),
)
def links(hex_code, tail_number, db_path, urls_only):
    """Generate ADS-B Exchange trace URLs for each flight."""
    hex_code = _resolve_hex(hex_code, tail_number)
    with Database(Path(db_path)) as db:
        flights = db.get_flights(hex_code)

        if not flights:
            if not urls_only:
                console.print("[yellow]No flights found[/]")
            return

        for f in flights:
            flight_date = f["takeoff_time"][:10]
            url = f"https://globe.adsbexchange.com/?icao={hex_code}&showTrace={flight_date}"
            if urls_only:
                # Bypass rich formatting so shell pipelines get a clean stream.
                click.echo(url)
                continue
            origin = f["origin_icao"] or "?"
            dest = f["destination_icao"] or "?"
            console.print(f"[cyan]{flight_date}[/] {origin} -> {dest}  [dim]{url}[/]")


@cli.group()
def registry():
    """FAA aircraft registry import and lookup."""


@registry.command("update")
@click.option(
    "--zip",
    "zip_path",
    type=click.Path(path_type=Path, dir_okay=False),
    default=None,
    help="Use a local ReleasableAircraft.zip instead of downloading.",
)
@click.option("--db", "db_path", default="adsbtrack.db", help="Database path")
def registry_update(zip_path, db_path):
    """Download the FAA ReleasableAircraft.zip and (re)import MASTER/DEREG/ACFTREF."""
    from .registry import refresh_faa_registry

    cfg = Config(db_path=Path(db_path))
    with Database(cfg.db_path) as db:
        stats = refresh_faa_registry(db, cfg, local_zip=zip_path)
    console.print(
        f"[green]FAA registry loaded:[/] MASTER {stats['master']}, DEREG {stats['dereg']}, ACFTREF {stats['acftref']}"
    )


if __name__ == "__main__":
    cli()
