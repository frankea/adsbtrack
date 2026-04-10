from datetime import date
from importlib.metadata import version as pkg_version
from pathlib import Path
from urllib.parse import urlparse

import click
from rich.console import Console
from rich.table import Table

from .airports import download_airports
from .config import SOURCE_URLS, Config
from .db import Database
from .fetcher import fetch_traces, fetch_traces_opensky
from .nnumber import nnumber_to_icao
from .parser import extract_flights

ALL_SOURCES = list(SOURCE_URLS.keys()) + ["opensky"]

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
@click.option("--source", type=click.Choice(ALL_SOURCES), default="adsbx", help="Data source (default: adsbx)")
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
            # Validate URL format
            parsed = urlparse(custom_url)
            if not parsed.scheme or not parsed.netloc:
                raise click.BadParameter(
                    f"Invalid URL: {custom_url}. Must be a full URL like https://example.com/globe_history",
                    param_hint="--url",
                )
            source_name = parsed.netloc.replace(".", "_")
            SOURCE_URLS[source_name] = custom_url
            source = source_name
            console.print(f"Fetching [bold]{hex_code}[/] from {start} to {end} via [cyan]{custom_url}[/]")
        else:
            console.print(f"Fetching [bold]{hex_code}[/] from {start} to {end} via [cyan]{source}[/]")

        if source == "opensky":
            stats = fetch_traces_opensky(db, config, hex_code, start, end)
        else:
            stats = fetch_traces(db, config, hex_code, start, end, source=source)

        console.print(
            f"\n[green]Done![/] Fetched: {stats['fetched']}, "
            f"With data: {stats['with_data']}, "
            f"Skipped (already fetched): {stats['skipped']}, "
            f"Errors: {stats['errors']}"
        )

        # Auto-extract flights
        console.print("\nExtracting flights...")
        count = extract_flights(db, config, hex_code, reprocess=True)
        console.print(f"[green]Found {count} flights[/]")


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
        table.add_column("Conf", justify="right")
        table.add_column("Type", style="dim")

        for f in flights:
            takeoff = f["takeoff_time"][:10] if f["takeoff_time"] else "?"
            origin = f["origin_icao"] or f"({f['takeoff_lat']:.2f}, {f['takeoff_lon']:.2f})"
            if f["origin_name"]:
                origin = f"{f['origin_icao']} ({f['origin_name']})"

            landing_type = f["landing_type"] or "unknown"

            if f["destination_icao"]:
                dest = f"{f['destination_icao']} ({f['destination_name']})"
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
                "uncertain": "[yellow]UNCERT[/]",
                "altitude_error": "[red]ALT ERR[/]",
                "unknown": "[dim]--[/]",
            }.get(landing_type, "[dim]--[/]")

            table.add_row(takeoff, origin, dest, duration, callsign, conf_str, type_display)

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
                "uncertain": ("yellow", "Uncertain"),
                "altitude_error": ("red", "Altitude errors"),
                "unknown": ("dim", "Unclassified"),
            }
            for lt, (color, label) in type_labels.items():
                if lt in quality:
                    q = quality[lt]
                    pct = q["count"] / flight_count * 100 if flight_count > 0 else 0
                    console.print(f"  [{color}]{label}:{' ' * (20 - len(label))}{q['count']:>4} ({pct:.0f}%)[/]")

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
def links(hex_code, tail_number, db_path):
    """Generate ADS-B Exchange trace URLs for each flight."""
    hex_code = _resolve_hex(hex_code, tail_number)
    with Database(Path(db_path)) as db:
        flights = db.get_flights(hex_code)

        if not flights:
            console.print("[yellow]No flights found[/]")
            return

        for f in flights:
            flight_date = f["takeoff_time"][:10]
            origin = f["origin_icao"] or "?"
            dest = f["destination_icao"] or "?"
            url = f"https://globe.adsbexchange.com/?icao={hex_code}&showTrace={flight_date}"
            console.print(f"[cyan]{flight_date}[/] {origin} -> {dest}  [dim]{url}[/]")


if __name__ == "__main__":
    cli()
