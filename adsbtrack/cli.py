import os
from datetime import date
from importlib.metadata import version as pkg_version
from pathlib import Path
from urllib.parse import urlparse

import click
from rich.console import Console
from rich.table import Table

from .acars import fetch_acars
from .airframes import AirframesClient
from .airports import download_airports, enrich_helipad_names
from .config import SOURCE_URLS, Config
from .db import Database
from .fetcher import fetch_traces, fetch_traces_opensky
from .nnumber import nnumber_to_icao
from .parser import extract_flights
from .runways import refresh_runways

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


def _load_airframes_api_key(config: Config) -> str:
    """Load the airframes.io API key from AIRFRAMES_API_KEY env var or the
    credentials.json file (key: ``airframesApiKey``). Raises click.UsageError
    with a helpful message when neither is set."""
    key = os.environ.get("AIRFRAMES_API_KEY")
    if key:
        return key
    if config.credentials_path.exists():
        import json

        try:
            creds = json.loads(config.credentials_path.read_text())
            if isinstance(creds, dict) and creds.get("airframesApiKey"):
                return str(creds["airframesApiKey"])
        except Exception:
            pass
    raise click.UsageError(
        "airframes.io API key not configured. "
        "Set the AIRFRAMES_API_KEY environment variable, "
        f'or add {{"airframesApiKey": "..."}} to {config.credentials_path}.'
    )


@cli.command()
@click.option("--hex", "hex_code", default=None, help="ICAO hex code")
@click.option("--tail", "tail_number", default=None, help="Tail/registration (resolved via aircraft_registry)")
@click.option("--start", "start_date", required=True, help="Start date (YYYY-MM-DD)")
@click.option("--end", "end_date", default=None, help="End date (YYYY-MM-DD), defaults to today")
@click.option("--db", "db_path", default="adsbtrack.db", help="Database path")
def acars(hex_code, tail_number, start_date, end_date, db_path):
    """Fetch ACARS / VDL2 / HFDL messages from airframes.io for a given aircraft.

    Either --hex or --tail must be given. --tail resolves through the local
    aircraft_registry, so you must have fetched ADS-B traces for that
    aircraft first.
    """
    if bool(hex_code) == bool(tail_number):
        raise click.UsageError("Provide exactly one of --hex or --tail.")
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date) if end_date else date.today()

    config = Config(db_path=Path(db_path))
    api_key = _load_airframes_api_key(config)

    with Database(Path(db_path)) as db:
        if tail_number:
            row = db.conn.execute(
                "SELECT icao FROM aircraft_registry WHERE registration = ? COLLATE NOCASE",
                (tail_number,),
            ).fetchone()
            if not row:
                raise click.UsageError(
                    f"Tail {tail_number!r} not found in aircraft_registry. Fetch ADS-B traces for this aircraft first."
                )
            hex_code = row["icao"]
        hex_code = hex_code.lower()

        console.print(f"Fetching ACARS for [bold]{hex_code}[/] from {start} to {end}")
        with AirframesClient(api_key=api_key) as client:
            stats = fetch_acars(db, client, hex_code, start_date=start, end_date=end)

        console.print(
            f"[green]Done.[/] Flights fetched: {stats['flights_fetched']}, "
            f"messages inserted: {stats['messages_inserted']}, "
            f"flights skipped (already fetched): {stats['flights_skipped']}, "
            f"flights tagged with OOOI: {stats['flights_with_oooi']}"
        )


@cli.command()
@click.option("--hex", "hex_code", required=True, help="ICAO hex code")
@click.option("--from", "from_date", default=None, help="Filter from date (YYYY-MM-DD)")
@click.option("--to", "to_date", default=None, help="Filter to date (YYYY-MM-DD)")
@click.option("--airport", default=None, help="Filter by airport ICAO code")
@click.option(
    "--alignment/--no-alignment",
    "show_alignment",
    default=False,
    help=(
        "Force-show the ILS alignment column even when no flight in the result set has "
        "alignment data (column auto-shows when data is present)."
    ),
)
@click.option(
    "--show-squawk/--no-show-squawk",
    "show_squawk",
    default=False,
    help="Show the primary squawk code held by each flight.",
)
@click.option("--db", "db_path", default="adsbtrack.db")
def trips(hex_code, from_date, to_date, airport, show_alignment, show_squawk, db_path):
    """Show flight history."""
    hex_code = hex_code.lower()
    with Database(Path(db_path)) as db:
        flights = db.get_flights(hex_code, from_date, to_date, airport)

        if not flights:
            console.print("[yellow]No flights found[/]")
            return

        def _col(row, name, default=None):
            try:
                return row[name]
            except (KeyError, IndexError):
                return default

        # Only render the ACARS column when the aircraft has any ACARS data
        # at all, so users who haven't run `acars` don't see an empty column.
        acars_row = db.conn.execute("SELECT COUNT(*) AS c FROM acars_messages WHERE icao = ?", (hex_code,)).fetchone()
        has_acars = acars_row and acars_row["c"] > 0

        # Auto-show the alignment column when any row has alignment data,
        # mirroring the ACARS auto-detect behavior.
        has_alignment_data = any(_col(f, "aligned_runway") is not None for f in flights)
        show_alignment_col = show_alignment or has_alignment_data

        show_squawk_col = show_squawk

        table = Table(title=f"Flights for {hex_code}")
        table.add_column("Date", style="cyan")
        table.add_column("From", style="green")
        table.add_column("To", style="green")
        table.add_column("Duration", style="yellow")
        table.add_column("Callsign", style="dim")
        table.add_column("Mission", style="magenta")
        table.add_column("Conf", justify="right")
        table.add_column("Type", style="dim")
        if has_acars:
            table.add_column("ACARS", justify="right", style="cyan")
        if show_alignment_col:
            table.add_column("Aligned", justify="right", style="cyan")
        if show_squawk_col:
            table.add_column("Squawk", justify="right", style="cyan")

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

        for f in flights:
            takeoff = f["takeoff_time"][:10] if f["takeoff_time"] else "?"
            rwy = _col(f, "takeoff_runway")
            origin_icao = f["origin_icao"]
            origin_suffix = f"/{rwy}" if rwy else ""
            if origin_icao and f["origin_name"]:
                origin = f"{origin_icao}{origin_suffix} ({f['origin_name']})"
            elif origin_icao:
                origin = f"{origin_icao}{origin_suffix}"
            else:
                origin = f"({f['takeoff_lat']:.2f}, {f['takeoff_lon']:.2f})"

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

            row_cells = [takeoff, origin, dest, duration, callsign, mission, conf_str, type_display]
            if has_acars:
                # Count ACARS messages whose timestamp falls in this flight's
                # window. OOOI marker appears when any of acars_out/off/on/in
                # is populated - an OOOI-tagged flight is highlighted.
                msg_count_row = db.conn.execute(
                    """SELECT COUNT(*) AS c FROM acars_messages
                       WHERE icao = ? AND timestamp BETWEEN ? AND ?""",
                    (
                        hex_code,
                        f["takeoff_time"],
                        f["landing_time"] or f["last_seen_time"] or f["takeoff_time"],
                    ),
                ).fetchone()
                msg_count = msg_count_row["c"] if msg_count_row else 0
                has_oooi = any(_col(f, k) for k in ("acars_out", "acars_off", "acars_on", "acars_in"))
                if msg_count > 0 and has_oooi:
                    acars_cell = f"[green]{msg_count} OOOI[/]"
                elif msg_count > 0:
                    acars_cell = str(msg_count)
                else:
                    acars_cell = "[dim]--[/]"
                row_cells.append(acars_cell)
            if show_alignment_col:
                runway = _col(f, "aligned_runway")
                seconds = _col(f, "aligned_seconds")
                # int(round(...)) uses banker's rounding (round-half-to-even); sub-second
                # precision isn't meaningful for ADS-B samples (~1s cadence) so this is
                # display-only and the minor round-half-to-even quirk is intentional.
                if runway and seconds is not None:
                    alignment_cell = f"[green]RWY {runway} / {int(round(seconds))}s[/]"
                else:
                    alignment_cell = "[dim]--[/]"
                row_cells.append(alignment_cell)
            if show_squawk_col:
                squawk_cell = _col(f, "primary_squawk") or "[dim]--[/]"
                row_cells.append(squawk_cell)
            table.add_row(*row_cells)

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

        # FAA registry block: show registrant, address, cert info when
        # we have FAA data loaded. Also flag deregistered hexes so the
        # user knows the aircraft was pulled from the registry (common
        # in the ghost-helicopter pattern).
        faa_reg = db.get_faa_registry_by_hex(hex_code)
        faa_dereg = db.get_faa_deregistered_by_hex(hex_code)
        if faa_reg or faa_dereg:
            source = faa_reg or faa_dereg
            label = "FAA registry" if faa_reg else "FAA registry (DEREGISTERED)"
            color = "cyan" if faa_reg else "red"
            console.print(f"\n[bold {color}]{label}[/]\n")
            tail = f"N{source['n_number']}" if source["n_number"] else "-"
            console.print(f"  Tail:          {tail}")
            console.print(f"  Registrant:    {source['name'] or '-'}")
            street_line, city_state_zip = _format_faa_address(source)
            console.print(f"  Address:       {street_line}")
            if city_state_zip:
                console.print(f"                 {city_state_zip}")
            console.print(f"  Cert issued:   {source['cert_issue_date'] or '-'}")
            console.print(f"  Last action:   {source['last_action_date'] or '-'}")
            console.print(f"  Expiration:    {source['expiration_date'] or '-'}")
            # Second line of deregistration context when both are present.
            if faa_reg and faa_dereg:
                console.print("  [dim yellow]Note: prior deregistration record also on file[/]")

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

        # Emergency-squawk breakdown: per-code counts. Only rendered when
        # at least one flight in the scope has an emergency.
        try:
            emergency_rows = db.conn.execute(
                """SELECT emergency_squawk, COUNT(*) AS cnt FROM flights
                   WHERE icao = ? AND emergency_squawk IS NOT NULL
                   GROUP BY emergency_squawk ORDER BY emergency_squawk""",
                (hex_code,),
            ).fetchall()
        except Exception:
            emergency_rows = []
        if emergency_rows:
            parts = ", ".join(f"{row['cnt']} ({row['emergency_squawk']})" for row in emergency_rows)
            console.print(f"  [red]Emergencies:{' ' * (22 - len('Emergencies:'))}{parts}[/]")

        # Average squawk changes per flight. Rendered when any flight has
        # a non-null squawk_changes value.
        try:
            avg_row = db.conn.execute(
                "SELECT AVG(squawk_changes) AS avg_changes, COUNT(squawk_changes) AS n FROM flights WHERE icao = ?",
                (hex_code,),
            ).fetchone()
        except Exception:
            avg_row = None
        if avg_row and avg_row["n"] and avg_row["avg_changes"] is not None:
            console.print(f"  Squawk changes per flight (avg): {avg_row['avg_changes']:.1f}")

        # Go-around + pattern-work counters. Wrapped in try/except so a
        # pre-migration DB without the new columns degrades gracefully
        # (the whole section simply doesn't render).
        try:
            counts_row = db.conn.execute(
                """SELECT
                       SUM(CASE WHEN had_go_around = 1 THEN 1 ELSE 0 END) AS go_arounds,
                       SUM(CASE WHEN pattern_cycles >= 2 THEN 1 ELSE 0 END) AS pattern_flights
                   FROM flights WHERE icao = ?""",
                (hex_code,),
            ).fetchone()
        except Exception:
            counts_row = None
        if counts_row and (counts_row["go_arounds"] or counts_row["pattern_flights"]):
            console.print("\n[bold]Approach behaviour:[/]\n")
            console.print(f"  Go-arounds:     {counts_row['go_arounds'] or 0}")
            console.print(f"  Pattern work:   {counts_row['pattern_flights'] or 0} flights")

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

        # Position source breakdown (readsb type/src field). Weight each
        # flight's percentage by its data_points so the total matches the
        # true per-point mix rather than an unweighted average.
        src_row = db.conn.execute(
            """SELECT
                   SUM(data_points) AS total_points,
                   SUM(adsb_pct * data_points) / NULLIF(SUM(data_points), 0) AS adsb,
                   SUM(mlat_pct * data_points) / NULLIF(SUM(data_points), 0) AS mlat,
                   SUM(tisb_pct * data_points) / NULLIF(SUM(data_points), 0) AS tisb
               FROM flights
               WHERE icao = ? AND data_points > 0
                 AND (adsb_pct IS NOT NULL OR mlat_pct IS NOT NULL OR tisb_pct IS NOT NULL)""",
            (hex_code,),
        ).fetchone()
        if src_row and src_row["total_points"]:
            adsb_pct = src_row["adsb"] or 0.0
            mlat_pct = src_row["mlat"] or 0.0
            tisb_pct = src_row["tisb"] or 0.0
            other_pct = max(0.0, 100.0 - adsb_pct - mlat_pct - tisb_pct)
            console.print("\n[bold]Position sources:[/]\n")
            console.print(f"  ADS-B:  {adsb_pct:>5.1f}%")
            console.print(f"  MLAT:   {mlat_pct:>5.1f}%")
            console.print(f"  TIS-B:  {tisb_pct:>5.1f}%")
            if other_pct > 0.05:
                console.print(f"  Other:  {other_pct:>5.1f}%")

        # ACARS summary (only shown when there are any stored messages)
        acars_total_row = db.conn.execute(
            "SELECT COUNT(*) AS c FROM acars_messages WHERE icao = ?", (hex_code,)
        ).fetchone()
        acars_total = acars_total_row["c"] if acars_total_row else 0
        if acars_total:
            acars_flight_row = db.conn.execute(
                "SELECT COUNT(*) AS c FROM acars_flights WHERE icao = ?", (hex_code,)
            ).fetchone()
            acars_flights = acars_flight_row["c"] if acars_flight_row else 0
            oooi_flights = db.conn.execute(
                """SELECT COUNT(*) AS c FROM flights WHERE icao = ?
                   AND (acars_out IS NOT NULL OR acars_off IS NOT NULL
                        OR acars_on IS NOT NULL OR acars_in IS NOT NULL)""",
                (hex_code,),
            ).fetchone()["c"]
            console.print("\n[bold]ACARS:[/]\n")
            console.print(f"  Total messages: {acars_total}")
            console.print(f"  Flights fetched: {acars_flights}")
            console.print(f"  Flights with OOOI data: {oooi_flights}")
            # Top labels for context
            label_rows = db.conn.execute(
                """SELECT label, COUNT(*) AS c FROM acars_messages
                   WHERE icao = ? AND label IS NOT NULL
                   GROUP BY label ORDER BY c DESC LIMIT 6""",
                (hex_code,),
            ).fetchall()
            if label_rows:
                top = ", ".join(f"{r['label']}({r['c']})" for r in label_rows)
                console.print(f"  Top labels: {top}")

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
    import sqlite3
    import zipfile

    import httpx

    from .registry import refresh_faa_registry

    cfg = Config(db_path=Path(db_path))
    try:
        with Database(cfg.db_path) as db:
            stats = refresh_faa_registry(db, cfg, local_zip=zip_path)
    except httpx.HTTPError as e:
        raise click.ClickException(f"failed to download FAA registry: {e}") from e
    except zipfile.BadZipFile as e:
        raise click.ClickException(f"FAA registry zip is corrupt: {e}") from e
    except FileNotFoundError as e:
        raise click.ClickException(str(e)) from e
    except sqlite3.DatabaseError as e:
        raise click.ClickException(f"database error: {e}") from e
    except OSError as e:
        raise click.ClickException(f"filesystem error: {e}") from e
    console.print(
        f"[green]FAA registry loaded:[/] MASTER {stats['master']}, DEREG {stats['dereg']}, ACFTREF {stats['acftref']}"
    )


def _format_faa_address(row) -> tuple[str, str]:
    """Return (street_line, city_state_zip_line) formatted for display.

    street_line is '-' when street/street2 are both empty.
    city_state_zip_line is '' when all three components are missing; callers
    should skip printing the second line in that case.
    """
    street_line = row["street"] or ""
    if row["street2"]:
        street_line = (street_line + " " + row["street2"]).strip()
    street_line = street_line or "-"
    city_state = ", ".join(p for p in (row["city"], row["state"]) if p)
    zip_part = row["zip_code"] or ""
    city_state_zip = f"{city_state} {zip_part}".strip()
    return street_line, city_state_zip


def _print_faa_registry_row(row, *, deregistered: bool) -> None:
    """Pretty-print a faa_registry / faa_deregistered sqlite3.Row."""
    heading = "Deregistered aircraft" if deregistered else "Registered aircraft"
    color = "red" if deregistered else "green"
    console.print(f"\n[bold {color}]{heading}[/]")
    tail_display = f"N{row['n_number']}" if row["n_number"] else "(unknown)"
    console.print(f"  Tail:            {tail_display}")
    console.print(f"  ICAO hex:        {row['mode_s_code_hex']}")
    console.print(f"  Serial:          {row['serial_number'] or '-'}")
    console.print(f"  Registrant:      {row['name'] or '-'}")
    street_line, city_state_zip = _format_faa_address(row)
    console.print(f"  Address:         {street_line}")
    if city_state_zip:
        console.print(f"                   {city_state_zip}")
    console.print(f"  Country:         {row['country'] or '-'}")
    console.print(f"  Cert issued:     {row['cert_issue_date'] or '-'}")
    console.print(f"  Last action:     {row['last_action_date'] or '-'}")
    console.print(f"  Airworthy date:  {row['air_worth_date'] or '-'}")
    console.print(f"  Expiration:      {row['expiration_date'] or '-'}")
    console.print(f"  Status code:     {row['status_code'] or '-'}")
    console.print(f"  MFR/MDL code:    {row['mfr_mdl_code'] or '-'}")


@registry.command("lookup")
@click.option("--hex", "hex_code", default=None, help="ICAO hex code")
@click.option("--tail", "tail_number", default=None, help="FAA N-number (with or without leading N)")
@click.option("--db", "db_path", default="adsbtrack.db")
def registry_lookup(hex_code, tail_number, db_path):
    """Show full FAA registration for an aircraft, including deregistration status."""
    if bool(hex_code) == bool(tail_number):
        raise click.UsageError("Provide exactly one of --hex or --tail.")
    with Database(Path(db_path)) as db:
        if hex_code:
            reg = db.get_faa_registry_by_hex(hex_code)
            dereg = db.get_faa_deregistered_by_hex(hex_code)
        else:
            reg = db.get_faa_registry_by_n_number(tail_number)
            dereg = db.get_faa_deregistered_by_n_number(tail_number)

        if not reg and not dereg:
            query = hex_code or tail_number
            console.print(f"[yellow]No record for {query}[/]")
            return

        if reg:
            _print_faa_registry_row(reg, deregistered=False)
            # Also mention if a deregistered row exists (common when an
            # aircraft was reregistered with a new owner).
            if dereg:
                console.print("\n[dim]Prior deregistration record also on file[/]")
        else:
            _print_faa_registry_row(dereg, deregistered=True)


def _print_registry_summary_rows(rows, *, empty_message: str) -> None:
    """Shared table renderer for owner/address searches."""
    if not rows:
        console.print(f"[yellow]{empty_message}[/]")
        return
    table = Table(show_header=True)
    table.add_column("Tail", style="cyan")
    table.add_column("ICAO hex", style="dim")
    table.add_column("Registrant", style="green")
    table.add_column("City, State", style="yellow")
    table.add_column("MFR/MDL", style="dim")
    for r in rows:
        tail = f"N{r['n_number']}" if r["n_number"] else "-"
        city_state = ", ".join(p for p in (r["city"], r["state"]) if p) or "-"
        table.add_row(
            tail,
            r["mode_s_code_hex"] or "-",
            r["name"] or "-",
            city_state,
            r["mfr_mdl_code"] or "-",
        )
    console.print(table)
    console.print(f"\n{len(rows)} aircraft")


@registry.command("owner")
@click.option("--name", required=True, help="Owner name to search (LIKE match, case-insensitive)")
@click.option("--limit", default=500, show_default=True, help="Max rows to return")
@click.option("--db", "db_path", default="adsbtrack.db")
def registry_owner(name, limit, db_path):
    """Search faa_registry by registrant name (LIKE match)."""
    with Database(Path(db_path)) as db:
        rows = db.search_faa_registry_by_name(name, limit=limit)
        _print_registry_summary_rows(rows, empty_message=f"No aircraft match name {name!r}")


@registry.command("address")
@click.option("--street", default=None, help="Street substring match (case-insensitive)")
@click.option("--city", default=None, help="Exact city match (case-insensitive)")
@click.option("--state", default=None, help="Exact state abbreviation match")
@click.option("--limit", default=500, show_default=True, help="Max rows to return")
@click.option("--db", "db_path", default="adsbtrack.db")
def registry_address(street, city, state, limit, db_path):
    """Search faa_registry by address. Provide at least one filter."""
    if not any([street, city, state]):
        raise click.UsageError("Provide at least one of --street, --city, --state.")
    with Database(Path(db_path)) as db:
        rows = db.search_faa_registry_by_address(street=street, city=city, state=state, limit=limit)
        filters = []
        if street:
            filters.append(f"street ~ {street!r}")
        if city:
            filters.append(f"city = {city!r}")
        if state:
            filters.append(f"state = {state!r}")
        msg = "No aircraft match " + ", ".join(filters)
        _print_registry_summary_rows(rows, empty_message=msg)


@cli.group()
def runways():
    """OurAirports runway geometry ingestion."""


@runways.command("refresh")
@click.option(
    "--csv",
    "csv_path",
    type=click.Path(exists=True, path_type=Path, dir_okay=False),
    default=None,
    help="Use a local runways.csv instead of downloading from OurAirports.",
)
@click.option("--db", "db_path", default="adsbtrack.db", help="Database path")
def runways_refresh(csv_path, db_path):
    """Download OurAirports runways.csv and upsert runway geometry.

    Idempotent - re-running overwrites existing rows keyed by
    (airport_ident, runway_name).
    """
    import httpx

    cfg = Config(db_path=Path(db_path))
    try:
        with Database(cfg.db_path) as db:
            count = refresh_runways(db, cfg, local_csv=csv_path)
    except httpx.HTTPError as e:
        raise click.ClickException(f"failed to download runways.csv: {e}") from e
    except FileNotFoundError as e:
        raise click.ClickException(str(e)) from e
    except OSError as e:
        raise click.ClickException(f"filesystem error: {e}") from e
    console.print(f"[green]Runway geometry loaded:[/] {count} runway ends")


# -----------------------------------------------------------------------------
# Hex cross-reference enrichment
# -----------------------------------------------------------------------------


def _print_hex_crossref_row(row) -> None:
    """Pretty-print a hex_crossref sqlite3.Row."""
    source = row["source"] or "-"
    if row["is_military"]:
        console.print(f"\n[bold red]Military aircraft ({source})[/]")
    else:
        console.print(f"\n[bold green]Aircraft identity ({source})[/]")
    console.print(f"  ICAO hex:        {row['icao']}")
    console.print(f"  Registration:    {row['registration'] or '-'}")
    console.print(f"  Type code:       {row['type_code'] or '-'}")
    console.print(f"  Type:            {row['type_description'] or '-'}")
    console.print(f"  Operator:        {row['operator'] or '-'}")
    if row["is_military"]:
        console.print(f"  [yellow]Mil country:    {row['mil_country'] or '-'}[/]")
        console.print(f"  [yellow]Mil branch:     {row['mil_branch'] or '-'}[/]")
    console.print(f"  Last updated:    {row['last_updated'] or '-'}")


@cli.group()
def enrich():
    """Hex cross-reference enrichment (FAA / Mictronics / hexdb.io)."""


@enrich.command("hex")
@click.option("--hex", "hex_code", required=True, help="ICAO hex code")
@click.option("--db", "db_path", default="adsbtrack.db")
@click.option(
    "--mictronics-dir",
    type=click.Path(path_type=Path, file_okay=False),
    default=None,
    help="Directory holding Mictronics JSON files (defaults to config path).",
)
@click.option("--no-hexdb", is_flag=True, help="Skip the hexdb.io live lookup.")
def enrich_hex_cmd(hex_code, db_path, mictronics_dir, no_hexdb):
    """Enrich a single ICAO hex. Prefers FAA registry, then Mictronics, then hexdb.io."""
    from .hex_crossref import HexdbClient, _load_mictronics_files, enrich_hex

    cfg = Config(db_path=Path(db_path))
    resolved_mictronics = mictronics_dir or cfg.mictronics_cache_dir
    mictronics_cache = None
    if (resolved_mictronics / "aircrafts.json").exists():
        aircrafts, types, operators, _ = _load_mictronics_files(resolved_mictronics)
        mictronics_cache = (aircrafts, types, operators)

    hexdb_client: HexdbClient | None = None
    if not no_hexdb:
        hexdb_client = HexdbClient(base_url=cfg.hexdb_base_url, rate_limit_per_min=cfg.hexdb_rate_limit_per_min)

    try:
        with Database(cfg.db_path) as db:
            row, conflicts = enrich_hex(
                db,
                hex_code,
                hexdb_client=hexdb_client,
                mictronics_cache=mictronics_cache,
            )
    finally:
        if hexdb_client is not None:
            hexdb_client.close()

    if row is None:
        console.print(f"[yellow]No data found for {hex_code}[/]")
        return
    _print_hex_crossref_row(row)
    if conflicts:
        console.print("\n[bold yellow]Source conflicts:[/]")
        for note in conflicts:
            console.print(f"  - {note}")


@enrich.command("all")
@click.option("--db", "db_path", default="adsbtrack.db")
@click.option(
    "--mictronics-dir",
    type=click.Path(path_type=Path, file_okay=False),
    default=None,
    help="Directory holding Mictronics JSON files (defaults to config path).",
)
@click.option("--no-hexdb", is_flag=True, help="Skip hexdb.io live lookups (Mictronics only).")
@click.option("--download-mictronics", is_flag=True, help="Refresh the Mictronics cache before running.")
def enrich_all_cmd(db_path, mictronics_dir, no_hexdb, download_mictronics):
    """Backfill hex_crossref for every icao in trace_days / flights missing an entry."""
    from .hex_crossref import download_mictronics as dl_mictronics
    from .hex_crossref import enrich_all

    cfg = Config(db_path=Path(db_path))
    resolved_mictronics = mictronics_dir or cfg.mictronics_cache_dir
    if download_mictronics:
        console.print(f"Downloading Mictronics DB into {resolved_mictronics}...")
        dl_mictronics(cfg, cache_dir=resolved_mictronics)

    with Database(cfg.db_path) as db:
        stats = enrich_all(
            db,
            cfg=cfg,
            mictronics_cache_dir=resolved_mictronics,
            use_hexdb=not no_hexdb,
        )
    console.print(
        f"[green]Enrich complete.[/] Processed {stats['processed']}, "
        f"wrote {stats['written']}, no_data {stats['no_data']}, "
        f"conflicts {stats['conflicts']}"
    )


# -----------------------------------------------------------------------------
# Military hex range checks
# -----------------------------------------------------------------------------


@cli.group()
def mil():
    """Check ICAO hex codes against known military allocation ranges."""


@mil.command("hex")
@click.option("--hex", "hex_code", required=True, help="ICAO hex code")
@click.option("--db", "db_path", default="adsbtrack.db")
def mil_hex_cmd(hex_code, db_path):
    """Check whether a single hex falls into a known military range."""
    with Database(Path(db_path)) as db:
        row = db.lookup_mil_hex_range(hex_code)
    if row is None:
        console.print(f"[green]{hex_code.lower()} is not in any known military range.[/]")
        return
    console.print(f"\n[bold red]Military hex: {hex_code.lower()}[/]")
    console.print(f"  Range:    {row['range_start']}-{row['range_end']}")
    console.print(f"  Country:  {row['country']}")
    console.print(f"  Branch:   {row['branch']}")
    console.print(f"  Notes:    {row['notes']}")


@mil.command("scan")
@click.option("--db", "db_path", default="adsbtrack.db")
def mil_scan_cmd(db_path):
    """Scan every icao in trace_days / flights against military ranges.

    Prints a table of matches. Useful for finding government / military
    aircraft hiding in an otherwise-civilian trace dataset.
    """
    from rich.table import Table

    with Database(Path(db_path)) as db:
        icaos = db.get_all_icaos()
        matches = []
        for icao in icaos:
            row = db.lookup_mil_hex_range(icao)
            if row is not None:
                matches.append((icao, row["country"], row["branch"], row["notes"]))

    if not matches:
        console.print(f"[green]No military hexes found across {len(icaos)} aircraft.[/]")
        return

    table = Table(title=f"Military hexes ({len(matches)} of {len(icaos)} aircraft)")
    table.add_column("ICAO", style="cyan")
    table.add_column("Country", style="yellow")
    table.add_column("Branch", style="magenta")
    table.add_column("Notes", style="dim")
    for icao, country, branch, notes in matches:
        table.add_row(icao, country or "-", branch or "-", notes or "-")
    console.print(table)


if __name__ == "__main__":
    cli()
