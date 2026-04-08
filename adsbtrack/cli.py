from datetime import date
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from .airports import download_airports
from .config import Config
from .db import Database
from .fetcher import fetch_traces
from .parser import extract_flights

console = Console()


def get_db_and_config(db_path: str, cookies: str) -> tuple[Database, Config]:
    config = Config(db_path=Path(db_path), cookies_path=Path(cookies))
    db = Database(config.db_path)
    return db, config


def ensure_airports(db: Database, config: Config):
    if db.airport_count() == 0:
        console.print("[yellow]Airport database empty, downloading...[/]")
        count = download_airports(db, config)
        console.print(f"[green]Loaded {count} airports[/]")


@click.group()
def cli():
    """Track private plane travel history using ADS-B Exchange data."""
    pass


@cli.command()
@click.option("--hex", "hex_code", required=True, help="ICAO hex code (e.g. adf64f)")
@click.option("--start", "start_date", default="2025-01-01", help="Start date (YYYY-MM-DD)")
@click.option("--end", "end_date", default=None, help="End date (YYYY-MM-DD), defaults to today")
@click.option("--rate", default=0.5, help="Seconds between requests")
@click.option("--db", "db_path", default="adsbtrack.db", help="Database path")
def fetch(hex_code, start_date, end_date, rate, db_path):
    """Download trace data from ADS-B Exchange globe_history API."""
    db, config = get_db_and_config(db_path, "cookies.json")
    config.rate_limit = rate

    ensure_airports(db, config)

    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date) if end_date else date.today()

    console.print(f"Fetching [bold]{hex_code}[/] from {start} to {end}")
    stats = fetch_traces(db, config, hex_code, start, end)

    console.print(f"\n[green]Done![/] Fetched: {stats['fetched']}, "
                  f"With data: {stats['with_data']}, "
                  f"Skipped (already fetched): {stats['skipped']}, "
                  f"Errors: {stats['errors']}")

    # Auto-extract flights
    console.print("\nExtracting flights...")
    count = extract_flights(db, config, hex_code, reprocess=True)
    console.print(f"[green]Found {count} flights[/]")

    db.close()


@cli.command()
@click.option("--hex", "hex_code", required=True, help="ICAO hex code")
@click.option("--reprocess", is_flag=True, help="Clear and rebuild all flights")
@click.option("--db", "db_path", default="adsbtrack.db")
@click.option("--cookies", default="cookies.json")
def extract(hex_code, reprocess, db_path, cookies):
    """Process raw traces into flights."""
    db, config = get_db_and_config(db_path, cookies)
    ensure_airports(db, config)

    count = extract_flights(db, config, hex_code, reprocess=reprocess)
    console.print(f"[green]Extracted {count} flights[/]")
    db.close()


@cli.command()
@click.option("--hex", "hex_code", required=True, help="ICAO hex code")
@click.option("--from", "from_date", default=None, help="Filter from date (YYYY-MM-DD)")
@click.option("--to", "to_date", default=None, help="Filter to date (YYYY-MM-DD)")
@click.option("--airport", default=None, help="Filter by airport ICAO code")
@click.option("--db", "db_path", default="adsbtrack.db")
def trips(hex_code, from_date, to_date, airport, db_path):
    """Show flight history."""
    db, _ = get_db_and_config(db_path, "cookies.json")
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

    for f in flights:
        takeoff = f["takeoff_time"][:10] if f["takeoff_time"] else "?"
        origin = f["origin_icao"] or f"({f['takeoff_lat']:.2f}, {f['takeoff_lon']:.2f})"
        if f["origin_name"]:
            origin = f"{f['origin_icao']} ({f['origin_name']})"

        if f["destination_icao"]:
            dest = f"{f['destination_icao']} ({f['destination_name']})"
        elif f["landing_lat"] is not None:
            dest = f"({f['landing_lat']:.2f}, {f['landing_lon']:.2f})"
        else:
            dest = "[dim]in flight?[/]"

        duration = ""
        if f["duration_minutes"]:
            hours = int(f["duration_minutes"] // 60)
            mins = int(f["duration_minutes"] % 60)
            duration = f"{hours}h {mins}m" if hours else f"{mins}m"

        callsign = f["callsign"] or ""
        table.add_row(takeoff, origin, dest, duration, callsign)

    console.print(table)
    console.print(f"\nTotal: {len(flights)} flights")
    db.close()


@cli.command()
@click.option("--hex", "hex_code", required=True, help="ICAO hex code")
@click.option("--db", "db_path", default="adsbtrack.db")
def status(hex_code, db_path):
    """Show database statistics."""
    db, _ = get_db_and_config(db_path, "cookies.json")

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

    if top_airports:
        console.print(f"\n[bold]Top airports:[/]\n")
        table = Table(show_header=True)
        table.add_column("Airport", style="cyan")
        table.add_column("Name", style="green")
        table.add_column("Visits", style="yellow", justify="right")
        for ap in top_airports:
            table.add_row(ap["airport"], ap["name"], str(ap["visits"]))
        console.print(table)

    db.close()


if __name__ == "__main__":
    cli()
