import dataclasses
import json
import os
import re
import sys
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from importlib.metadata import version as pkg_version
from pathlib import Path
from urllib.parse import urlparse

import click
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn, TimeRemainingColumn
from rich.table import Table

from .acars import fetch_acars
from .airframes import AirframesClient
from .airports import download_airports, enrich_helipad_names
from .config import SOURCE_URLS, Config, is_retryable_fetch_status
from .db import Database, iter_parsed_trace_days
from .events import collect_events
from .fetcher import fetch_traces, fetch_traces_opensky
from .forensics import (
    DEFAULT_FRAGMENT_GAP_SECS,
    callsign_timeline,
    closest_approach,
    squawk_timeline,
    summarize_fragments,
)
from .gaps import detect_gaps
from .models import LandingType
from .navaids import refresh_navaids as _refresh_navaids
from .nnumber import nnumber_to_icao
from .parser import extract_flights
from .runways import refresh_runways
from .watch import WatchAlert, _post_webhook, evaluate, snapshot_state

ALL_SOURCES = list(SOURCE_URLS.keys()) + ["opensky"]
# "all" fetches from every readsb source (excludes opensky which needs creds)
ALL_SOURCES_WITH_ALL = ["all"] + ALL_SOURCES

console = Console()


def _load_config(db_path: str) -> Config:
    """Load Config from an on-disk config file (see Config.load), then apply
    the CLI's --db/$ADSBTRACK_DB path on top -- that flag is always explicit
    (it has its own default and envvar via _db_option), so it takes
    precedence over whatever db_path a config file names. Every CLI command
    builds its Config through this one function so a config file's overrides
    apply uniformly instead of only to whichever command happens to call it."""
    config = Config.load()
    config.db_path = Path(db_path)
    return config


def ensure_airports(db: Database, config: Config):
    if db.airport_count() == 0:
        console.print("[yellow]Airport database empty, downloading...[/]")
        count = download_airports(db, config)
        console.print(f"[green]Loaded {count} airports[/]")


def _db_option(help_text: str = "Database path.") -> Callable:
    """Shared --db option. Keeps the ADSBTRACK_DB envvar wiring and the default
    path in one place instead of repeating it across ~20 command definitions."""
    return click.option(
        "--db",
        "db_path",
        default="adsbtrack.db",
        envvar="ADSBTRACK_DB",
        help=f"{help_text} Reads $ADSBTRACK_DB if set (default: ./adsbtrack.db).",
    )


def _json_option() -> Callable:
    """Shared --json flag for read commands. Emits a single JSON document to
    stdout via click.echo (no Rich markup/ANSI) instead of the Rich table."""
    return click.option(
        "--json",
        "output_json",
        is_flag=True,
        default=False,
        help="Emit a single JSON document to stdout instead of the table.",
    )


def _default_fetch_start() -> date:
    """Fallback start date for `fetch` when there's no --start and no prior
    fetch history to resume from. January 1 of the previous calendar year,
    computed at runtime rather than frozen to a hardcoded date."""
    return date(date.today().year - 1, 1, 1)


def _resume_starts_per_source(db: Database, hex_code: str) -> dict[str, str]:
    """Last success-filtered fetch date per readsb source. Sources with no
    history are absent from the dict (a missing source resumes from the
    earliest peer's start so it can catch up; see the fetch command)."""
    starts: dict[str, str] = {}
    for src in SOURCE_URLS:
        fetched_dates = db.get_fetched_dates(hex_code, source=src)
        if fetched_dates:
            starts[src] = max(fetched_dates)
    return starts


def _source_is_unhealthy(db: Database, source: str, config: Config) -> tuple[bool, int]:
    """(unhealthy, leading_failures): unhealthy when the source's most recent
    outcomes are an unbroken run of at least source_health_skip_threshold
    retryable failures (403/429/5xx)."""
    outcomes = db.recent_source_outcomes(source, limit=config.source_health_window)
    leading = 0
    for status in outcomes:
        if is_retryable_fetch_status(status):
            leading += 1
        else:
            break
    return leading >= config.source_health_skip_threshold, leading


def _warn_retention_gaps(
    config: Config, sources_to_fetch: list[str], per_source_start: dict[str, date], end: date
) -> None:
    """Print a dim note for any source whose start predates its known
    archive retention window (config.source_retention_days), so a 404 out
    there reads as "probably expired" instead of "aircraft not seen"."""
    for src in sources_to_fetch:
        retention = config.source_retention_days.get(src)
        if retention is None:
            continue
        start = per_source_start.get(src)
        if start is None or (end - start).days <= retention:
            continue
        console.print(
            f"[dim]{src}: fetching from {start}, more than ~{retention} days before {end}. "
            f"{src}'s archive may not retain data that old, so 404s past that point could mean "
            "expired rather than not seen.[/]"
        )


_HEX_RE = re.compile(r"[0-9a-f]{6}")

TAIL_HELP = (
    "Tail/registration. FAA N-numbers are converted algorithmically; "
    "other registrations (G-, D-, VP-*) are resolved via aircraft_registry "
    "or hex_crossref if the aircraft has been observed or cross-referenced."
)


def _validate_hex(ctx: click.Context, param: click.Parameter, value: str | None) -> str | None:
    """Click callback for every --hex option: lowercase, strip, and validate
    it's 6 hex digits. Applied uniformly so no command can accept a
    malformed hex code and fail later with a confusing DB-level error."""
    if value is None:
        return None
    normalized = value.strip().lower()
    if not _HEX_RE.fullmatch(normalized):
        raise click.BadParameter(
            f"{value!r} is not a valid ICAO hex code; expected exactly 6 hex digits (0-9, a-f), e.g. adf64f."
        )
    return normalized


def _validate_hex_multi(ctx: click.Context, param: click.Parameter, value: tuple[str, ...]) -> tuple[str, ...]:
    """Click callback for a --hex option with multiple=True: apply
    _validate_hex's normalization/error to each element of the tuple Click
    hands back for a repeatable option."""
    return tuple(_validate_hex(ctx, param, v) for v in value)


def _resolve_hex_db(db: Database, hex_code: str | None, tail_number: str | None) -> str:
    """Resolve an ICAO hex code from --hex or --tail options.

    Exactly one of hex_code or tail_number must be provided. --tail first
    tries algorithmic FAA N-number conversion, then falls back through
    aircraft_registry then hex_crossref when the tail isn't a valid FAA
    N-number. Useful for non-US registrations (G-, D-, VP-*) once the
    aircraft has been observed or the cross-reference tables have been
    populated.

    Multi-match: if a tail appears on multiple ICAO hexes (reg
    reassigned across aircraft over time), pick the row with the
    newest `last_updated` and warn. Analysts who want a specific
    aircraft can pass --hex explicitly.
    """
    if hex_code and tail_number:
        raise click.UsageError("Provide either --hex or --tail, not both.")
    if not hex_code and not tail_number:
        raise click.UsageError("Provide either --hex or --tail.")
    if hex_code:
        return hex_code.lower()

    # Algorithmic N-number conversion. Works for any syntactically
    # valid N-number regardless of DB state.
    try:
        resolved = nnumber_to_icao(tail_number)
        console.print(f"[dim]Converted {tail_number} to hex {resolved}[/]")
        return resolved.lower()
    except ValueError:
        pass  # not an N-number; try DB lookups

    rows = db.conn.execute(
        "SELECT icao, last_updated FROM aircraft_registry "
        "WHERE registration = ? COLLATE NOCASE "
        "ORDER BY COALESCE(last_updated, '') DESC",
        (tail_number,),
    ).fetchall()
    if not rows:
        rows = db.conn.execute(
            "SELECT icao, last_updated FROM hex_crossref "
            "WHERE registration = ? COLLATE NOCASE "
            "ORDER BY COALESCE(last_updated, '') DESC",
            (tail_number,),
        ).fetchall()

    if not rows:
        raise click.UsageError(
            f"Could not resolve tail {tail_number!r}. Options:\n"
            f"  - Pass --hex <icao> directly if you know the ICAO hex\n"
            f"  - Run `adsbtrack registry update` to populate the FAA registry\n"
            f"  - Fetch by --hex first; aircraft_registry is populated as a side effect"
        )

    distinct_icaos = sorted({row["icao"] for row in rows})
    if len(distinct_icaos) > 1:
        console.print(
            f"[yellow]Tail {tail_number!r} resolved to multiple hexes: "
            f"{distinct_icaos}. Using newest ({rows[0]['icao']}). "
            f"Pass --hex to disambiguate.[/]"
        )
    chosen = rows[0]["icao"]
    console.print(f"[dim]Resolved {tail_number} to hex {chosen}[/]")
    return chosen.lower()


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
@click.option("--hex", "hex_code", default=None, callback=_validate_hex, help="ICAO hex code (e.g. adf64f)")
@click.option("--tail", "tail_number", default=None, help=TAIL_HELP)
@click.option(
    "--source",
    type=click.Choice(ALL_SOURCES_WITH_ALL),
    default="adsbx",
    help="Data source, or 'all' to fetch from every readsb source (default: adsbx)",
)
@click.option("--url", "custom_url", default=None, help="Custom readsb globe_history base URL")
@click.option(
    "--start",
    "start_date",
    default=None,
    help="Start date (YYYY-MM-DD). If omitted: resumes the day after the last fetched "
    "day when this hex+source has prior fetch history (same as --since-last), otherwise "
    "defaults to January 1 of last year.",
)
@click.option(
    "--since-last",
    is_flag=True,
    default=False,
    help="Resume from the day after the last successfully fetched day for this hex+source. "
    "Errors if no prior fetch history exists. Implied when --start is omitted and history exists.",
)
@click.option("--end", "end_date", default=None, help="End date (YYYY-MM-DD), defaults to today")
@click.option("--rate", default=0.5, help="Seconds between requests")
@click.option(
    "--concurrency",
    default=4,
    type=int,
    help="Parallel in-flight requests per source (default: 4). Rate limit still "
    "caps request-start spacing; concurrency only helps when request latency "
    "exceeds --rate. Set to 1 for byte-identical serial behavior.",
)
@click.option(
    "--include-unhealthy",
    is_flag=True,
    default=False,
    help="With --source all, fetch every readsb source even if its recent history "
    "looks like a dead source (all retryable failures). Ignored for a single named source.",
)
@_db_option()
def fetch(
    hex_code,
    tail_number,
    source,
    custom_url,
    start_date,
    since_last,
    end_date,
    rate,
    concurrency,
    include_unhealthy,
    db_path,
):
    """Download trace data from ADS-B data sources."""
    with Database(Path(db_path)) as db:
        hex_code = _resolve_hex_db(db, hex_code, tail_number)
        config = _load_config(db_path)
        config.rate_limit = rate
        config.fetch_concurrency = concurrency

        ensure_airports(db, config)

        if start_date and since_last:
            raise click.UsageError("Provide either --start or --since-last, not both.")
        if since_last and custom_url:
            raise click.UsageError("--since-last is not supported with --url; pass --start explicitly.")

        end = date.fromisoformat(end_date) if end_date else date.today()

        if source == "all" and not custom_url:
            # Fetch from every readsb source + opensky if credentials exist.
            # sources_to_fetch is computed first, then filtered by source
            # health (below), then per-source start dates.
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

            if not include_unhealthy:
                unhealthy = {}
                for src in sources_to_fetch:
                    if src not in SOURCE_URLS:
                        continue  # opensky isn't tracked in fetch_log by readsb source name
                    is_unhealthy, leading = _source_is_unhealthy(db, src, config)
                    if is_unhealthy:
                        unhealthy[src] = leading
                if unhealthy:
                    remaining_readsb = [s for s in sources_to_fetch if s in SOURCE_URLS and s not in unhealthy]
                    if not remaining_readsb:
                        console.print(
                            "[yellow]Every readsb source looks unhealthy; fetching all of them anyway "
                            "instead of skipping everything.[/]"
                        )
                    else:
                        for src, leading in unhealthy.items():
                            console.print(
                                f"[yellow]Skipping {src}: last {leading} attempts all failed (403/429/5xx); "
                                "pass --include-unhealthy to force[/]"
                            )
                        sources_to_fetch = [s for s in sources_to_fetch if s not in unhealthy]

            if start_date:
                # Explicit --start is user intent: every source uses it
                # verbatim, unclamped.
                explicit_start = date.fromisoformat(start_date)
                per_source_start: dict[str, date] = dict.fromkeys(sources_to_fetch, explicit_start)
            else:
                last = _resume_starts_per_source(db, hex_code)
                if not last:
                    if since_last:
                        raise click.UsageError(
                            f"--since-last requested but no prior fetches found for {hex_code} via any of the "
                            f"readsb sources under --source all ({', '.join(sorted(SOURCE_URLS))})."
                        )
                    per_source_start = dict.fromkeys(sources_to_fetch, _default_fetch_start())
                else:
                    # A source with no history resumes from the earliest
                    # peer's start so it can catch up, instead of being
                    # skipped forever or stalling at the default start.
                    fallback = min(date.fromisoformat(d) for d in last.values()) + timedelta(days=1)
                    # Aligned to the (health-filtered) readsb members of
                    # sources_to_fetch, not the full SOURCE_URLS -- a
                    # skipped source must not leave a dangling entry that
                    # feeds the "uniform start" banner below or gets
                    # clamped for nothing.
                    per_source_start = {
                        src: (date.fromisoformat(last[src]) + timedelta(days=1) if src in last else fallback)
                        for src in sources_to_fetch
                        if src in SOURCE_URLS
                    }
                    for src, resume_start in list(per_source_start.items()):
                        if (end - resume_start).days > config.resume_max_lookback_days:
                            clamped = end - timedelta(days=config.resume_max_lookback_days)
                            console.print(
                                f"[yellow]{src}: resume date {resume_start} is more than "
                                f"{config.resume_max_lookback_days} days behind; clamping to {clamped} "
                                "(older days skipped -- pass --start to backfill)[/]"
                            )
                            per_source_start[src] = clamped
                    if "opensky" in sources_to_fetch:
                        # opensky isn't a readsb source (not in SOURCE_URLS),
                        # so it has no history of its own to resume from --
                        # use the min of the (already clamped) readsb starts.
                        per_source_start["opensky"] = min(per_source_start.values())

            starts = set(per_source_start.values())
            opensky_suffix = " (incl. OpenSky)" if opensky_available else " (OpenSky skipped: no credentials)"
            if len(starts) == 1:
                (uniform_start,) = starts
                console.print(
                    f"Fetching [bold]{hex_code}[/] from {uniform_start} to {end} via "
                    f"[cyan]all {len(sources_to_fetch)} sources[/]{opensky_suffix}"
                )
            else:
                console.print(
                    f"Fetching [bold]{hex_code}[/] to {end} via "
                    f"[cyan]all {len(sources_to_fetch)} sources[/]{opensky_suffix}"
                )
                for src in sources_to_fetch:
                    console.print(f"  [dim]{src}: from {per_source_start[src]}[/]")
        else:
            # --url resolves its own source name (from the URL's netloc)
            # below; resuming here against --source's fetch history would
            # look up the wrong source, so resume/--since-last only applies
            # to a plain --source fetch.
            last_fetched = None
            if not custom_url:
                fetched_dates = db.get_fetched_dates(hex_code, source=source)
                last_fetched = max(fetched_dates) if fetched_dates else None

            if since_last:
                if last_fetched is None:
                    raise click.UsageError(
                        f"--since-last requested but no prior fetches found for {hex_code} via source {source!r}."
                    )
                start = date.fromisoformat(last_fetched) + timedelta(days=1)
            elif start_date:
                start = date.fromisoformat(start_date)
            elif last_fetched is not None:
                start = date.fromisoformat(last_fetched) + timedelta(days=1)
                console.print(f"[dim]Resuming from {start} (last fetched day; pass --start to override)[/]")
            else:
                start = _default_fetch_start()

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
                per_source_start = {source_name: start}
                console.print(f"Fetching [bold]{hex_code}[/] from {start} to {end} via [cyan]{custom_url}[/]")
            else:
                sources_to_fetch = [source]
                per_source_start = {source: start}
                console.print(f"Fetching [bold]{hex_code}[/] from {start} to {end} via [cyan]{source}[/]")

        _warn_retention_gaps(config, sources_to_fetch, per_source_start, end)

        # Marks this run's trace_days rows, whichever source writes them, so
        # the auto-extract below knows where its new data starts.
        run_started_at = datetime.now(UTC).isoformat()

        total_stats = {"fetched": 0, "with_data": 0, "skipped": 0, "errors": 0, "failed_days": []}

        def _accumulate(stats: dict) -> None:
            for k in total_stats:
                if k == "failed_days":
                    total_stats[k].extend(stats.get("failed_days", []))
                else:
                    total_stats[k] += stats[k]

        if len(sources_to_fetch) > 1:
            # Parallel fetch: each source in its own thread with its own
            # DB connection (SQLite WAL supports concurrent writers). All
            # readsb-source threads share one Progress so each source
            # renders as its own task line instead of racing to start
            # separate Live displays on the same console.
            import threading

            lock = threading.Lock()
            per_source_stats: dict[str, dict] = {}

            with Progress(
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TimeRemainingColumn(),
            ) as shared_progress:

                def _fetch_one(src: str) -> None:
                    with Database(Path(db_path)) as thread_db:
                        thread_config = _load_config(db_path)
                        thread_config.rate_limit = rate
                        thread_config.fetch_concurrency = concurrency
                        src_start = per_source_start[src]
                        if src == "opensky":
                            stats = fetch_traces_opensky(thread_db, thread_config, hex_code, src_start, end)
                        else:
                            stats = fetch_traces(
                                thread_db,
                                thread_config,
                                hex_code,
                                src_start,
                                end,
                                source=src,
                                progress=shared_progress,
                            )
                        with lock:
                            per_source_stats[src] = stats
                            _accumulate(stats)

                threads = [threading.Thread(target=_fetch_one, args=(src,)) for src in sources_to_fetch]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()

            for src in sources_to_fetch:
                s = per_source_stats.get(src)
                if s is None:
                    continue
                console.print(
                    f"  [dim]{src}:[/] {s['fetched']} fetched, {s['with_data']} with data, "
                    f"{s['skipped']} skipped, {s['errors']} errors"
                )
        else:
            src = sources_to_fetch[0]
            src_start = per_source_start[src]
            if src == "opensky":
                stats = fetch_traces_opensky(db, config, hex_code, src_start, end)
            else:
                stats = fetch_traces(db, config, hex_code, src_start, end, source=src)
            _accumulate(stats)

        console.print(
            f"\n[green]Done![/] Fetched: {total_stats['fetched']}, "
            f"With data: {total_stats['with_data']}, "
            f"Skipped (already fetched): {total_stats['skipped']}, "
            f"Errors: {total_stats['errors']}"
        )
        if total_stats["failed_days"]:
            failed_str = ", ".join(f"{d} ({status})" for d, status in total_stats["failed_days"])
            console.print(f"[yellow]Failed days (will retry on next run):[/] {failed_str}")

        # Auto-extract flights, re-processing only what the new trace days
        # can affect. A day that landed nothing (204/404, or every day
        # already fetched) changes no flight, so there is nothing to redo.
        earliest_new = db.get_earliest_trace_date_since(hex_code, run_started_at)
        if earliest_new is None:
            console.print("\n[dim]No new trace days; leaving flights as they are.[/]")
            return
        console.print("\nExtracting flights...")
        count = extract_flights(db, config, hex_code, since_date=date.fromisoformat(earliest_new))
        console.print(f"[green]Found {count} flights[/]")
        try:
            enriched = enrich_helipad_names(db, config)
            if enriched:
                console.print(f"[green]Enriched {enriched} helipad names[/]")
        except Exception:
            pass


@cli.command()
@click.option("--hex", "hex_code", default=None, callback=_validate_hex, help="ICAO hex code")
@click.option("--tail", "tail_number", default=None, help=TAIL_HELP)
@click.option("--reprocess", is_flag=True, help="Clear and rebuild all flights")
@click.option(
    "--since",
    "since_str",
    default=None,
    help="Rebuild only the trace days that new data on this date (YYYY-MM-DD) can affect, "
    "keeping the flights before them. Falls back to a full reprocess when the history "
    "cannot be extended safely.",
)
@_db_option()
def extract(hex_code, tail_number, reprocess, since_str, db_path):
    """Process raw traces into flights."""
    if reprocess and since_str:
        raise click.UsageError("Provide either --reprocess or --since, not both.")
    since_date = None
    if since_str:
        try:
            since_date = date.fromisoformat(since_str)
        except ValueError:
            console.print(f"[red]Invalid --since '{since_str}'; use YYYY-MM-DD.[/]")
            return
    with Database(Path(db_path)) as db:
        hex_code = _resolve_hex_db(db, hex_code, tail_number)
        config = _load_config(db_path)
        ensure_airports(db, config)
        count = extract_flights(db, config, hex_code, reprocess=reprocess, since_date=since_date)
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
@click.option("--hex", "hex_code", default=None, callback=_validate_hex, help="ICAO hex code")
@click.option("--tail", "tail_number", default=None, help="Tail/registration (resolved via aircraft_registry)")
@click.option("--start", "start_date", required=True, help="Start date (YYYY-MM-DD)")
@click.option("--end", "end_date", default=None, help="End date (YYYY-MM-DD), defaults to today")
@_db_option()
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

    config = _load_config(db_path)
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
        with (
            AirframesClient(api_key=api_key) as client,
            Progress(
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TimeRemainingColumn(),
            ) as progress,
        ):
            task_id = progress.add_task(f"ACARS {hex_code}", total=None)

            def _on_progress(done: int, total: int) -> None:
                description = f"ACARS {hex_code}"
                minute_remaining = getattr(client, "minute_remaining", None)
                daily_remaining = getattr(client, "daily_remaining", None)
                if minute_remaining is not None or daily_remaining is not None:
                    description += f" (minute remaining: {minute_remaining}, daily remaining: {daily_remaining})"
                progress.update(task_id, completed=done, total=total, description=description)

            stats = fetch_acars(db, client, hex_code, start_date=start, end_date=end, progress_callback=_on_progress)

        console.print(
            f"[green]Done.[/] Flights fetched: {stats['flights_fetched']}, "
            f"messages inserted: {stats['messages_inserted']}, "
            f"flights skipped (already fetched): {stats['flights_skipped']}, "
            f"flights tagged with OOOI: {stats['flights_with_oooi']}"
        )


@cli.command()
@click.option("--hex", "hex_code", default=None, callback=_validate_hex, help="ICAO hex code")
@click.option("--tail", "tail_number", default=None, help=TAIL_HELP)
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
@_json_option()
@_db_option()
def trips(hex_code, tail_number, from_date, to_date, airport, show_alignment, show_squawk, output_json, db_path):
    """Show flight history."""
    with Database(Path(db_path)) as db:
        hex_code = _resolve_hex_db(db, hex_code, tail_number)
        flights = db.get_flights(hex_code, from_date, to_date, airport)

        if not flights:
            if output_json:
                click.echo(json.dumps([], indent=2))
            else:
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

        def _flight_acars(f):
            """Per-flight ACARS message count + OOOI presence. Single source of
            truth for this query -- called from both the --json row builder and
            the table row builder so the two output paths never diverge."""
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
            return msg_count, has_oooi

        # Auto-show the alignment column when any row has alignment data,
        # mirroring the ACARS auto-detect behavior.
        has_alignment_data = any(_col(f, "aligned_runway") is not None for f in flights)
        show_alignment_col = show_alignment or has_alignment_data

        show_squawk_col = show_squawk

        if output_json:
            rows = []
            for f in flights:
                row = {
                    "id": f["id"],
                    "icao": hex_code,
                    "date": f["takeoff_time"][:10] if f["takeoff_time"] else None,
                    "takeoff_time": f["takeoff_time"],
                    "landing_time": f["landing_time"],
                    "origin_icao": f["origin_icao"],
                    "origin_name": f["origin_name"],
                    "nearest_origin_icao": _col(f, "nearest_origin_icao"),
                    "nearest_origin_distance_km": _col(f, "nearest_origin_distance_km"),
                    "takeoff_runway": _col(f, "takeoff_runway"),
                    "takeoff_lat": f["takeoff_lat"],
                    "takeoff_lon": f["takeoff_lon"],
                    "destination_icao": f["destination_icao"],
                    "destination_name": f["destination_name"],
                    "landing_lat": f["landing_lat"],
                    "landing_lon": f["landing_lon"],
                    "landing_type": f["landing_type"] or "unknown",
                    "landing_confidence": f["landing_confidence"],
                    "probable_destination_icao": _col(f, "probable_destination_icao"),
                    "probable_destination_distance_km": _col(f, "probable_destination_distance_km"),
                    "duration_minutes": f["duration_minutes"],
                    "callsign": f["callsign"],
                    "mission_type": _col(f, "mission_type"),
                }
                if has_acars:
                    msg_count, has_oooi = _flight_acars(f)
                    row["acars_message_count"] = msg_count
                    row["acars_oooi"] = has_oooi
                if show_alignment_col:
                    row["aligned_runway"] = _col(f, "aligned_runway")
                    row["aligned_seconds"] = _col(f, "aligned_seconds")
                if show_squawk_col:
                    row["primary_squawk"] = _col(f, "primary_squawk")
                rows.append(row)
            click.echo(json.dumps(rows, indent=2))
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
            elif _col(f, "nearest_origin_icao"):
                origin = f"[yellow]~{_col(f, 'nearest_origin_icao')}[/]"
            else:
                origin = f"({f['takeoff_lat']:.2f}, {f['takeoff_lon']:.2f})"

            landing_type = f["landing_type"] or "unknown"

            if f["destination_icao"]:
                dest = f"{f['destination_icao']} ({f['destination_name']})"
            elif landing_type in (LandingType.DROPPED_ON_APPROACH, LandingType.SIGNAL_LOST) and _col(
                f, "probable_destination_icao"
            ):
                dest = f"[yellow]~{_col(f, 'probable_destination_icao')}[/]"
            elif landing_type == LandingType.SIGNAL_LOST:
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
                # OOOI marker appears when any of acars_out/off/on/in is
                # populated - an OOOI-tagged flight is highlighted.
                msg_count, has_oooi = _flight_acars(f)
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
@click.option("--hex", "hex_code", default=None, callback=_validate_hex, help="ICAO hex code (6 chars)")
@click.option("--tail", "tail_number", default=None, help="Tail number; resolved to hex")
@_db_option()
def route(hex_code, tail_number, db_path):
    """Print the navaid track fingerprint for each flight of an aircraft."""
    import json as _json

    cfg = _load_config(db_path)
    with Database(cfg.db_path) as db:
        resolved = _resolve_hex_db(db, hex_code, tail_number)
        rows = db.conn.execute(
            "SELECT takeoff_date, origin_icao, destination_icao,"
            "       nearest_origin_icao, nearest_destination_icao, navaid_track"
            "  FROM flights"
            " WHERE icao = ?"
            "   AND navaid_track IS NOT NULL"
            " ORDER BY takeoff_time",
            (resolved,),
        ).fetchall()

    if not rows:
        console.print(f"No navaid track data for [cyan]{resolved}[/]")
        return

    for row in rows:
        try:
            payload = _json.loads(row["navaid_track"])
        except (ValueError, TypeError):
            continue
        if not payload:
            continue
        origin = row["origin_icao"] or (f"({row['nearest_origin_icao']})" if row["nearest_origin_icao"] else "-")
        destination = row["destination_icao"] or (
            f"({row['nearest_destination_icao']})" if row["nearest_destination_icao"] else "-"
        )
        chain_parts = []
        for seg in payload:
            dur_secs = float(seg.get("end_ts", 0.0)) - float(seg.get("start_ts", 0.0))
            label = "<1m" if dur_secs < 60.0 else f"{int(round(dur_secs / 60.0))}m"
            chain_parts.append(f"{seg['navaid_ident']} ({label})")
        chain = " -> ".join(chain_parts)
        console.print(f"{row['takeoff_date']} {origin} -> {destination}  {chain}")


@cli.command()
@click.option("--hex", "hex_code", default=None, callback=_validate_hex, help="ICAO hex code")
@click.option("--tail", "tail_number", default=None, help=TAIL_HELP)
@_json_option()
@_db_option()
def status(hex_code, tail_number, output_json, db_path):
    """Show database statistics."""
    with Database(Path(db_path)) as db:
        hex_code = _resolve_hex_db(db, hex_code, tail_number)
        total_fetched = db.get_total_days_fetched(hex_code)
        days_with_data = db.get_days_with_data(hex_code)
        flight_count = db.get_flight_count(hex_code)
        first_date, last_date = db.get_date_range(hex_code)
        top_airports = db.get_top_airports(hex_code)

        # Row dict built once; the table path below prints from the same
        # variables instead of re-querying, and the --json path serializes
        # this dict directly.
        payload: dict = {
            "hex": hex_code,
            "registration": None,
            "type": None,
            "owner": None,
            "date_range": {"first": first_date, "last": last_date},
            "days_checked": total_fetched,
            "days_with_data": days_with_data,
            "total_flights": flight_count,
            "quality": {},
            "utilization": None,
            "top_airports": [
                {"airport": ap["airport"], "name": ap["name"], "visits": ap["visits"]} for ap in top_airports
            ],
            "acars": None,
        }

        if not output_json:
            console.print(f"\n[bold]Status for {hex_code}[/]\n")

        # Get aircraft info from first trace day. get_trace_days is a
        # generator (P7); next() pulls just the first row without pulling
        # the whole (possibly multi-year) trace history into memory.
        td = next(db.get_trace_days(hex_code), None)
        if td:
            payload["registration"] = td["registration"]
            payload["type"] = td["description"]
            payload["owner"] = td["owner_operator"]
            if not output_json:
                console.print(f"  Registration:  {td['registration']}")
                console.print(f"  Type:          {td['description']}")
                console.print(f"  Owner:         {td['owner_operator']}")
                console.print()

        # FAA registry block: show registrant, address, cert info when
        # we have FAA data loaded. Also flag deregistered hexes so the
        # user knows the aircraft was pulled from the registry (common
        # in the ghost-helicopter pattern). Table-only; not part of the
        # --json payload by design.
        faa_reg = db.get_faa_registry_by_hex(hex_code)
        faa_dereg = db.get_faa_deregistered_by_hex(hex_code)
        if (faa_reg or faa_dereg) and not output_json:
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

        if not output_json:
            console.print(f"  Date range:    {first_date or 'N/A'} to {last_date or 'N/A'}")
            console.print(f"  Days checked:  {total_fetched}")
            console.print(f"  Days w/ data:  {days_with_data}")
            console.print(f"  Total flights: {flight_count}")

        # Data quality summary
        quality = db.get_flight_quality_summary(hex_code)
        payload["quality"] = quality
        if quality and any(k != "unknown" for k in quality) and not output_json:
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
        if emergency_rows and not output_json:
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
        if avg_row and avg_row["n"] and avg_row["avg_changes"] is not None and not output_json:
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
        if counts_row and (counts_row["go_arounds"] or counts_row["pattern_flights"]) and not output_json:
            console.print("\n[bold]Approach behaviour:[/]\n")
            console.print(f"  Go-arounds:     {counts_row['go_arounds'] or 0}")
            console.print(f"  Pattern work:   {counts_row['pattern_flights'] or 0} flights")

        # v3: mission type breakdown
        mission_rows = db.conn.execute(
            "SELECT mission_type, COUNT(*) as cnt FROM flights WHERE icao = ? GROUP BY mission_type ORDER BY cnt DESC",
            (hex_code,),
        ).fetchall()
        if mission_rows and any(r["mission_type"] for r in mission_rows) and not output_json:
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
            payload["utilization"] = {
                "total_hours": stats_row["total_hours"],
                "total_cycles": stats_row["total_cycles"],
                "avg_flight_minutes": stats_row["avg_flight_minutes"],
                "distinct_airports": stats_row["distinct_airports"],
                "distinct_callsigns": stats_row["distinct_callsigns"],
                "busiest_day_date": stats_row["busiest_day_date"],
                "busiest_day_count": stats_row["busiest_day_count"],
            }
            if not output_json:
                console.print("\n[bold]Utilization:[/]\n")
                console.print(f"  Total hours:      {stats_row['total_hours'] or 0:.1f}")
                console.print(f"  Cycles:           {stats_row['total_cycles'] or 0}")
                console.print(f"  Avg flight:       {stats_row['avg_flight_minutes'] or 0:.1f} min")
                console.print(f"  Distinct airports: {stats_row['distinct_airports'] or 0}")
                console.print(f"  Distinct callsigns: {stats_row['distinct_callsigns'] or 0}")
                if stats_row["busiest_day_date"]:
                    console.print(
                        f"  Busiest day:      {stats_row['busiest_day_date']} "
                        f"({stats_row['busiest_day_count']} flights)"
                    )

        # Position source breakdown (readsb type/src field). Weight each
        # flight's percentage by its data_points so the total matches the
        # true per-point mix rather than an unweighted average. Prefers the
        # explicit other_pct/adsc_pct columns when present; legacy rows that
        # only carried adsb/mlat/tisb are reconstructed via 100-minus-sum.
        src_row = db.conn.execute(
            """SELECT
                   SUM(data_points) AS total_points,
                   SUM(adsb_pct * data_points) / NULLIF(SUM(data_points), 0) AS adsb,
                   SUM(mlat_pct * data_points) / NULLIF(SUM(data_points), 0) AS mlat,
                   SUM(tisb_pct * data_points) / NULLIF(SUM(data_points), 0) AS tisb,
                   SUM(other_pct * data_points) / NULLIF(SUM(data_points), 0) AS other,
                   SUM(adsc_pct * data_points) / NULLIF(SUM(data_points), 0) AS adsc,
                   SUM(CASE WHEN other_pct IS NOT NULL OR adsc_pct IS NOT NULL
                            THEN data_points ELSE 0 END) AS typed_points
               FROM flights
               WHERE icao = ? AND data_points > 0
                 AND (adsb_pct IS NOT NULL OR mlat_pct IS NOT NULL OR tisb_pct IS NOT NULL)""",
            (hex_code,),
        ).fetchone()
        if src_row and src_row["total_points"] and not output_json:
            adsb_pct = src_row["adsb"] or 0.0
            mlat_pct = src_row["mlat"] or 0.0
            tisb_pct = src_row["tisb"] or 0.0
            # Use explicit adsc/other columns when every contributing row has
            # them populated; otherwise fall back to 100-minus-sum so legacy
            # rows still render a non-negative Other bucket.
            have_explicit = src_row["typed_points"] and src_row["typed_points"] >= src_row["total_points"]
            if have_explicit:
                adsc_pct = src_row["adsc"] or 0.0
                other_pct = src_row["other"] or 0.0
            else:
                adsc_pct = 0.0
                other_pct = max(0.0, 100.0 - adsb_pct - mlat_pct - tisb_pct)
            console.print("\n[bold]Position sources:[/]\n")
            console.print(f"  ADS-B:  {adsb_pct:>5.1f}%")
            console.print(f"  MLAT:   {mlat_pct:>5.1f}%")
            console.print(f"  TIS-B:  {tisb_pct:>5.1f}%")
            if adsc_pct > 0.05:
                console.print(f"  ADS-C:  {adsc_pct:>5.1f}%")
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
            # Top labels for context
            label_rows = db.conn.execute(
                """SELECT label, COUNT(*) AS c FROM acars_messages
                   WHERE icao = ? AND label IS NOT NULL
                   GROUP BY label ORDER BY c DESC LIMIT 6""",
                (hex_code,),
            ).fetchall()
            payload["acars"] = {
                "total_messages": acars_total,
                "flights_fetched": acars_flights,
                "flights_with_oooi": oooi_flights,
                "top_labels": [{"label": r["label"], "count": r["c"]} for r in label_rows],
            }
            if not output_json:
                console.print("\n[bold]ACARS:[/]\n")
                console.print(f"  Total messages: {acars_total}")
                console.print(f"  Flights fetched: {acars_flights}")
                console.print(f"  Flights with OOOI data: {oooi_flights}")
                if label_rows:
                    top = ", ".join(f"{r['label']}({r['c']})" for r in label_rows)
                    console.print(f"  Top labels: {top}")

        # v3: emergency / night indicators. Table-only; not part of the
        # --json payload by design.
        night_count = db.conn.execute(
            "SELECT COUNT(*) FROM flights WHERE icao = ? AND night_flight = 1", (hex_code,)
        ).fetchone()[0]
        emergency_count = db.conn.execute(
            "SELECT COUNT(*) FROM flights WHERE icao = ? AND emergency_squawk IS NOT NULL", (hex_code,)
        ).fetchone()[0]
        if (night_count > 0 or emergency_count > 0) and not output_json:
            console.print("\n[bold]Indicators:[/]\n")
            if night_count > 0:
                console.print(f"  Night flights:    {night_count}")
            if emergency_count > 0:
                console.print(f"  [red]Emergency squawks: {emergency_count}[/]")

        if top_airports and not output_json:
            console.print("\n[bold]Top airports:[/]\n")
            table = Table(show_header=True)
            table.add_column("Airport", style="cyan")
            table.add_column("Name", style="green")
            table.add_column("Visits", style="yellow", justify="right")
            for ap in top_airports:
                table.add_row(ap["airport"], ap["name"], str(ap["visits"]))
            console.print(table)

        if output_json:
            click.echo(json.dumps(payload, indent=2))


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
@click.option("--hex", "hex_code", default=None, callback=_validate_hex, help="ICAO hex code")
@click.option("--tail", "tail_number", default=None, help="FAA N-number")
@_db_option()
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
    with Database(Path(db_path)) as db:
        hex_code = _resolve_hex_db(db, hex_code, tail_number)
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
@_db_option()
def registry_update(zip_path, db_path):
    """Download the FAA ReleasableAircraft.zip and (re)import MASTER/DEREG/ACFTREF."""
    import sqlite3
    import zipfile

    import httpx

    from .registry import refresh_faa_registry

    cfg = _load_config(db_path)
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
@click.option("--hex", "hex_code", default=None, callback=_validate_hex, help="ICAO hex code")
@click.option("--tail", "tail_number", default=None, help="FAA N-number (with or without leading N)")
@_db_option()
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
@_db_option()
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
@_db_option()
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
@_db_option()
def runways_refresh(csv_path, db_path):
    """Download OurAirports runways.csv and upsert runway geometry.

    Idempotent - re-running overwrites existing rows keyed by
    (airport_ident, runway_name).
    """
    import httpx

    cfg = _load_config(db_path)
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


@cli.group()
def navaids():
    """OurAirports navaid reference data (VOR / NDB / fixes)."""


@navaids.command("refresh")
@click.option(
    "--csv",
    "csv_path",
    type=click.Path(exists=True, path_type=Path, dir_okay=False),
    default=None,
    help="Use a local navaids.csv instead of downloading from OurAirports.",
)
@_db_option()
def navaids_refresh(csv_path, db_path):
    """Download OurAirports navaids.csv and upsert global navaid reference data.

    Idempotent - re-running replaces existing rows.
    """
    import httpx

    cfg = _load_config(db_path)
    try:
        with Database(cfg.db_path) as db:
            count = _refresh_navaids(db, cfg, local_csv=csv_path)
    except httpx.HTTPError as e:
        raise click.ClickException(f"failed to download navaids.csv: {e}") from e
    except FileNotFoundError as e:
        raise click.ClickException(str(e)) from e
    except OSError as e:
        raise click.ClickException(f"filesystem error: {e}") from e
    console.print(f"[green]Navaid reference data loaded:[/] {count} navaids")


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
@click.option("--hex", "hex_code", required=True, callback=_validate_hex, help="ICAO hex code")
@_db_option()
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

    cfg = _load_config(db_path)
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
@_db_option()
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

    cfg = _load_config(db_path)
    resolved_mictronics = mictronics_dir or cfg.mictronics_cache_dir
    if download_mictronics:
        console.print(f"Downloading Mictronics DB into {resolved_mictronics}...")
        dl_mictronics(cfg, cache_dir=resolved_mictronics)

    with (
        Database(cfg.db_path) as db,
        Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
        ) as progress,
    ):
        task_id = progress.add_task("Enriching hex_crossref", total=None)

        def _on_progress(done: int, total: int) -> None:
            progress.update(task_id, completed=done, total=total)

        stats = enrich_all(
            db,
            cfg=cfg,
            mictronics_cache_dir=resolved_mictronics,
            use_hexdb=not no_hexdb,
            progress_callback=_on_progress,
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
@click.option("--hex", "hex_code", required=True, callback=_validate_hex, help="ICAO hex code")
@_db_option()
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
@_db_option()
def mil_scan_cmd(db_path):
    """Scan every icao in trace_days / flights against military ranges.

    Prints a table of matches. Useful for finding government / military
    aircraft hiding in an otherwise-civilian trace dataset.
    """
    from rich.table import Table

    from .mil_hex import match_in_ranges

    with Database(Path(db_path)) as db:
        icaos = db.get_all_icaos()
        ranges = db.all_mil_hex_ranges()
        matches = []
        for icao in icaos:
            row = match_in_ranges(icao, ranges)
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


@cli.command()
@click.option("--hex", "hex_code", default=None, callback=_validate_hex, help="ICAO hex code")
@click.option("--tail", "tail_number", default=None, help=TAIL_HELP)
@_db_option()
@click.option(
    "--min-gap-secs",
    type=int,
    default=300,
    show_default=True,
    help="Minimum gap duration in seconds (below this is normal data sparseness).",
)
@click.option(
    "--classification",
    type=click.Choice(["all", "likely_transponder_off", "coverage_hole", "unknown"]),
    default="all",
    show_default=True,
    help="Filter output by classification bucket.",
)
@_json_option()
def gaps(hex_code, tail_number, db_path, min_gap_secs, classification, output_json):
    """Find within-flight ADS-B signal gaps and classify each.

    A gap is only tagged as likely_transponder_off when all of:
    altitude >= FL150, ADS-B coverage strong on both sides, and gap
    position is within 200 nm of a known airport. Ambiguous gaps
    default to "unknown" rather than a confident mislabel.
    """
    from datetime import UTC, datetime

    with Database(Path(db_path)) as db:
        hex_code = _resolve_hex_db(db, hex_code, tail_number)
        all_gaps = detect_gaps(db, hex_code, min_gap_secs=float(min_gap_secs))

    filtered = [g for g in all_gaps if g.classification == classification] if classification != "all" else all_gaps

    if output_json:
        rows = [
            {
                "gap_start": datetime.fromtimestamp(g.gap_start_ts, UTC).isoformat(),
                "gap_end": datetime.fromtimestamp(g.gap_end_ts, UTC).isoformat(),
                "duration_secs": g.duration_secs,
                "start_lat": g.start_lat,
                "start_lon": g.start_lon,
                "end_lat": g.end_lat,
                "end_lon": g.end_lon,
                "start_alt_ft": g.start_alt_ft,
                "end_alt_ft": g.end_alt_ft,
                "nearest_airport_nm": g.nearest_airport_nm,
                "pre_source_mix": g.pre_source_mix,
                "post_source_mix": g.post_source_mix,
                "classification": g.classification,
                "classification_reason": g.classification_reason,
            }
            for g in filtered
        ]
        click.echo(json.dumps(rows, indent=2))
        return

    if not filtered:
        console.print(f"[green]No gaps found for {hex_code} (min_gap_secs={min_gap_secs}, filter={classification}).[/]")
        return

    color_by_class = {
        "likely_transponder_off": "red",
        "coverage_hole": "dim",
        "unknown": "yellow",
    }
    table = Table(title=f"ADS-B gaps for {hex_code} (>= {min_gap_secs}s, {len(filtered)} of {len(all_gaps)} shown)")
    table.add_column("Start (UTC)", style="cyan")
    table.add_column("Dur", justify="right")
    table.add_column("Alt", justify="right")
    table.add_column("Position")
    table.add_column("Nearest apt")
    table.add_column("Pre→Post source")
    table.add_column("Classification")

    for g in filtered:
        started = datetime.fromtimestamp(g.gap_start_ts, UTC).strftime("%Y-%m-%d %H:%M")
        dur = f"{g.duration_secs / 60:.1f}m"
        alt = f"{g.start_alt_ft} ft" if g.start_alt_ft is not None else "-"
        pos = f"{g.start_lat:.2f},{g.start_lon:.2f}"
        apt = f"{g.nearest_airport_nm:.0f} nm" if g.nearest_airport_nm is not None else "-"
        pre_pct = _pct(g.pre_source_mix, "adsb")
        post_pct = _pct(g.post_source_mix, "adsb")
        sources = f"{pre_pct}→{post_pct} ADS-B"
        cls_color = color_by_class.get(g.classification, "white")
        cls_cell = f"[{cls_color}]{g.classification}[/]"
        table.add_row(started, dur, alt, pos, apt, sources, cls_cell)

    console.print(table)

    summary = {"likely_transponder_off": 0, "coverage_hole": 0, "unknown": 0}
    for g in all_gaps:
        summary[g.classification] = summary.get(g.classification, 0) + 1
    console.print(
        f"\n[bold]Summary:[/] {len(all_gaps)} gaps total -- "
        f"[red]{summary['likely_transponder_off']} likely_transponder_off[/], "
        f"[dim]{summary['coverage_hole']} coverage_hole[/], "
        f"[yellow]{summary['unknown']} unknown[/]"
    )


def _pct(mix: dict, key: str) -> str:
    total = sum(mix.values()) if mix else 0
    if total == 0:
        return "-"
    return f"{100 * mix.get(key, 0) // total}%"


@cli.command("inspect")
@click.option("--hex", "hex_code", callback=_validate_hex, help="ICAO hex code")
@click.option("--tail", "tail_number", help=TAIL_HELP)
@click.option("--date", "date_str", required=True, help="Day to inspect (YYYY-MM-DD)")
@click.option("--source", default=None, help="Limit to one source (default: every source with data)")
@click.option("--gap-secs", default=None, type=float, help="Fragment split gap in seconds (default 300)")
@click.option("--airport", default=None, help="Airport ident for closest-approach (e.g. EGOV, KTYS)")
@click.option("--json", "as_json", is_flag=True, help="Machine-readable output")
@_db_option()
def inspect_cmd(hex_code, tail_number, date_str, source, gap_secs, airport, as_json, db_path):
    """Forensic view of one aircraft-day: fragments, integrity, squawks.

    Splits each source's trace into fragments on inter-point gaps, reports
    per-fragment integrity (v2/sil0/nic0) and identity (callsigns, squawks)
    counts, and shows the squawk/callsign timeline and (with --airport)
    closest-approach distance across every source's points merged into one
    chronological stream.
    """
    gap_threshold = gap_secs if gap_secs is not None else DEFAULT_FRAGMENT_GAP_SECS

    with Database(Path(db_path)) as db:
        hex_code = _resolve_hex_db(db, hex_code, tail_number)
        config = _load_config(db_path)

        rows = db.get_trace_day(hex_code, date_str)
        if source:
            rows = [row for row in rows if row["source"] == source]
        parsed_rows = list(iter_parsed_trace_days(rows, hex_code))

        sources: dict[str, list] = {}
        combined_points: list[tuple[float, list]] = []
        for row, trace in parsed_rows:
            sources[row["source"]] = summarize_fragments(row["source"], row["timestamp"], trace, gap_threshold)
            for point in trace:
                if isinstance(point, list):
                    combined_points.append((row["timestamp"] + point[0], point))

        combined_points.sort(key=lambda item: item[0])
        combined_base_ts = combined_points[0][0] if combined_points else 0.0
        combined_trace = []
        for abs_ts, point in combined_points:
            shifted = list(point)
            shifted[0] = abs_ts - combined_base_ts
            combined_trace.append(shifted)

        sq_timeline = squawk_timeline(combined_base_ts, combined_trace)
        cs_timeline = callsign_timeline(combined_base_ts, combined_trace)

        approach = None
        if airport:
            ensure_airports(db, config)
            ident = airport.strip().upper()
            apt_row = db.conn.execute(
                "SELECT latitude_deg, longitude_deg FROM airports WHERE ident = ?", (ident,)
            ).fetchone()
            if apt_row is None:
                raise click.UsageError(f"Unknown airport ident {ident!r}.")
            approach = closest_approach(
                combined_base_ts, combined_trace, apt_row["latitude_deg"], apt_row["longitude_deg"]
            )

    if not sources:
        suffix = f" (source={source})" if source else ""
        if as_json:
            click.echo(
                json.dumps(
                    {
                        "hex": hex_code,
                        "date": date_str,
                        "sources": {},
                        "squawk_timeline": [],
                        "callsign_timeline": [],
                        "closest_approach": None,
                    },
                    indent=2,
                )
            )
        else:
            console.print(f"[yellow]No trace data for {hex_code} on {date_str}{suffix}.[/]")
        return

    if as_json:
        payload = {
            "hex": hex_code,
            "date": date_str,
            "sources": {src: [dataclasses.asdict(frag) for frag in frags] for src, frags in sources.items()},
            "squawk_timeline": sq_timeline,
            "callsign_timeline": cs_timeline,
            "closest_approach": {"dist_km": approach[0], "ts": approach[1], "alt": approach[2]} if approach else None,
        }
        click.echo(json.dumps(payload, indent=2))
        return

    for src, fragments in sources.items():
        table = Table(title=f"{hex_code} {date_str} -- source={src}")
        table.add_column("FRAG", justify="right")
        table.add_column("START Z")
        table.add_column("END Z")
        table.add_column("PTS", justify="right")
        table.add_column("FROM")
        table.add_column("TO")
        table.add_column("ALT")
        table.add_column("GS")
        table.add_column("V2/SIL0/NIC0")
        table.add_column("CS")
        table.add_column("SQ")
        table.add_column("SRC-MIX")

        for i, frag in enumerate(fragments, start=1):
            start_z = datetime.fromtimestamp(frag.start_ts, UTC).strftime("%H:%M:%SZ")
            end_z = datetime.fromtimestamp(frag.end_ts, UTC).strftime("%H:%M:%SZ")
            alt = (
                f"{frag.alt_min:.0f}-{frag.alt_max:.0f} ft"
                if frag.alt_min is not None and frag.alt_max is not None
                else "-"
            )
            gs_known = frag.gs_min is not None and frag.gs_max is not None
            gs = f"{frag.gs_min:.0f}-{frag.gs_max:.0f} kt" if gs_known else "-"
            integrity = f"{frag.v2_samples}/{frag.v2_sil0}/{frag.v2_nic0}"
            cs = ", ".join(frag.callsigns) if frag.callsigns else "-"
            sq = ", ".join(frag.squawks) if frag.squawks else "-"
            src_mix = (
                ", ".join(f"{k}:{v}" for k, v in sorted(frag.position_sources.items()))
                if frag.position_sources
                else "-"
            )
            table.add_row(
                str(i),
                start_z,
                end_z,
                str(frag.n_points),
                f"{frag.start_lat:.4f},{frag.start_lon:.4f}",
                f"{frag.end_lat:.4f},{frag.end_lon:.4f}",
                alt,
                gs,
                integrity,
                cs,
                sq,
                src_mix,
            )
        console.print(table)

    if sq_timeline:
        console.print("\n[bold]Squawk timeline:[/]")
        for ts, val in sq_timeline:
            console.print(f"  {datetime.fromtimestamp(ts, UTC).strftime('%H:%M:%SZ')}  {val}")

    if cs_timeline:
        console.print("\n[bold]Callsign timeline:[/]")
        for ts, val in cs_timeline:
            console.print(f"  {datetime.fromtimestamp(ts, UTC).strftime('%H:%M:%SZ')}  {val}")

    if approach:
        dist_km, ts, alt = approach
        alt_str = f"{alt:.0f} ft" if alt is not None else "unknown alt"
        console.print(
            f"\n[bold]Closest approach to {ident}:[/] "
            f"{dist_km:.2f} km at {datetime.fromtimestamp(ts, UTC).strftime('%H:%M:%SZ')} ({alt_str})"
        )


@cli.command()
@click.option("--hex", "hex_code", default=None, callback=_validate_hex, help="ICAO hex code")
@click.option("--tail", "tail_number", default=None, help=TAIL_HELP)
@_db_option()
@click.option(
    "--since",
    "since_str",
    default=None,
    help="Only show events from takeoff_time on or after this date (YYYY-MM-DD).",
)
@click.option(
    "--severity",
    type=click.Choice(["all", "emergency", "unusual"]),
    default="all",
    show_default=True,
    help="Filter by severity tier.",
)
@_json_option()
def events(hex_code, tail_number, db_path, since_str, severity, output_json):
    """Show a chronological event log for an aircraft.

    Surfaces: emergency squawks (7500/7600/7700), emergency flags,
    off-airport landings, sustained hover (>= 5 min), and multiple
    go-arounds (>= 2 per flight). All signals are read directly from
    pre-computed columns on the flights table; no new heuristics.
    """
    from datetime import UTC
    from datetime import datetime as dt_cls

    since = None
    if since_str:
        try:
            since = dt_cls.fromisoformat(since_str).replace(tzinfo=UTC)
        except ValueError:
            console.print(f"[red]Invalid --since '{since_str}'; use YYYY-MM-DD.[/]")
            return

    with Database(Path(db_path)) as db:
        hex_code = _resolve_hex_db(db, hex_code, tail_number)
        evts = collect_events(db, hex_code, since=since, severity=severity)

    if output_json:
        rows = [
            {
                "ts": e.ts.isoformat(),
                "icao": e.icao,
                "callsign": e.callsign,
                "event_type": e.event_type,
                "severity": e.severity,
                "summary": e.summary,
            }
            for e in evts
        ]
        click.echo(json.dumps(rows, indent=2))
        return

    if not evts:
        console.print(f"[green]No events for {hex_code} (filter={severity}).[/]")
        return

    color_by_severity = {"emergency": "red", "unusual": "yellow"}
    title = f"Events for {hex_code} ({len(evts)} shown, filter={severity})"
    if since:
        title += f", since {since.date().isoformat()}"
    table = Table(title=title)
    table.add_column("Date (UTC)", style="cyan", no_wrap=True)
    table.add_column("Callsign")
    table.add_column("Severity")
    table.add_column("Event")
    table.add_column("Detail")

    for e in evts:
        sev_color = color_by_severity.get(e.severity, "white")
        table.add_row(
            e.ts.strftime("%Y-%m-%d %H:%M"),
            e.callsign or "-",
            f"[{sev_color}]{e.severity}[/]",
            e.event_type,
            e.summary,
        )
    console.print(table)

    summary = {"emergency": 0, "unusual": 0}
    for e in evts:
        summary[e.severity] = summary.get(e.severity, 0) + 1
    console.print(
        f"\n[bold]Summary:[/] [red]{summary['emergency']} emergency[/], [yellow]{summary['unusual']} unusual[/]"
    )


# -----------------------------------------------------------------------------
# watch (#24)
# -----------------------------------------------------------------------------


def _parse_watchlist(path: Path) -> list[str]:
    """Parse a `watch --watchlist` file: one hex per line, blank lines
    ignored, and a '#' -- whether it starts the line or trails a hex --
    starting a comment that runs to end of line. Each surviving line is
    normalized and validated the same way --hex is."""
    hexes: list[str] = []
    for raw_line in path.read_text().splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        normalized = line.lower()
        if not _HEX_RE.fullmatch(normalized):
            raise click.UsageError(
                f"{line!r} in watchlist {path} is not a valid ICAO hex code; expected exactly 6 hex digits (0-9, a-f)."
            )
        hexes.append(normalized)
    return hexes


def _collect_watch_hexes(hex_codes: tuple[str, ...], watchlist_path: str | None) -> list[str]:
    """Union of --hex values and watchlist-file entries, deduped preserving
    first-seen order. Raises click.UsageError when the union is empty."""
    hexes: list[str] = []
    seen: set[str] = set()
    for h in hex_codes:
        if h not in seen:
            seen.add(h)
            hexes.append(h)
    if watchlist_path:
        for h in _parse_watchlist(Path(watchlist_path)):
            if h not in seen:
                seen.add(h)
                hexes.append(h)
    if not hexes:
        raise click.UsageError("Provide at least one --hex or a --watchlist file with at least one hex code.")
    return hexes


@cli.command("watch")
@click.option("--hex", "hex_codes", multiple=True, callback=_validate_hex_multi, help="Hex to watch (repeatable)")
@click.option(
    "--watchlist",
    "watchlist_path",
    type=click.Path(exists=True),
    default=None,
    help="File with one hex per line; '#' comments and blank lines ignored",
)
@click.option("--webhook", default=None, help="POST alerts as JSON to this URL when any fire")
@click.option("--dormancy-days", type=int, default=None, help="Override Config.watch_dormancy_days")
@click.option("--json", "as_json", is_flag=True, help="Machine-readable output")
@_db_option()
def watch_cmd(hex_codes, watchlist_path, webhook, dormancy_days, as_json, db_path):
    """Fetch a hex watchlist and alert on reactivation, emergencies, and new spoof quarantines."""
    hexes = _collect_watch_hexes(hex_codes, watchlist_path)

    config = _load_config(db_path)
    if dormancy_days is not None:
        config.watch_dormancy_days = dormancy_days

    end = date.today()
    hex_statuses: dict[str, str] = {}
    all_alerts: list[WatchAlert] = []

    with Database(Path(db_path)) as db:
        for hex_code in hexes:
            run_started_at = datetime.now(UTC).isoformat()
            pre = snapshot_state(db, hex_code)
            try:
                last = _resume_starts_per_source(db, hex_code)
                for src in SOURCE_URLS:
                    is_unhealthy, leading = _source_is_unhealthy(db, src, config)
                    if is_unhealthy:
                        console.print(f"[dim]{hex_code}: skipping {src} (unhealthy, last {leading} attempts failed)[/]")
                        continue
                    if src in last:
                        start = date.fromisoformat(last[src]) + timedelta(days=1)
                    else:
                        start = end - timedelta(days=config.resume_max_lookback_days)
                    if (end - start).days > config.resume_max_lookback_days:
                        start = end - timedelta(days=config.resume_max_lookback_days)
                    fetch_traces(db, config, hex_code, start, end, source=src, progress=None)

                # Re-extract only what this run's new trace days can affect,
                # same as `fetch` does -- a run that landed nothing changes no
                # flight, so there is nothing to redo, and a quiet cron tick
                # over a long-lived aircraft's whole history stays cheap.
                earliest_new = db.get_earliest_trace_date_since(hex_code, run_started_at)
                if earliest_new is not None:
                    extract_flights(db, config, hex_code, since_date=date.fromisoformat(earliest_new))

                alerts = evaluate(db, hex_code, pre, run_started_at, config)
            except Exception as exc:
                console.print(f"[red]{hex_code}: watch run failed: {exc}[/]")
                hex_statuses[hex_code] = "error"
                continue

            all_alerts.extend(alerts)
            if not pre.has_any_trace:
                status = "baselined"
            elif alerts:
                status = f"{len(alerts)} alert(s)"
            else:
                status = "no alerts"
            hex_statuses[hex_code] = status
            if not as_json:
                console.print(f"{hex_code}: {status}")

    document = {
        "generated_at": datetime.now(UTC).isoformat(),
        "alerts": [dataclasses.asdict(a) for a in all_alerts],
        "hexes": hex_statuses,
    }

    if all_alerts and webhook:
        try:
            _post_webhook(webhook, document, config.watch_webhook_timeout_secs)
        except Exception as exc:
            console.print(f"[red]webhook POST to {webhook} failed: {exc}[/]")

    if as_json:
        click.echo(json.dumps(document, indent=2))
    elif all_alerts:
        table = Table(title=f"{len(all_alerts)} alert(s)")
        table.add_column("KIND")
        table.add_column("ICAO")
        table.add_column("SUMMARY")
        for alert in all_alerts:
            table.add_row(alert.kind, alert.icao, alert.summary)
        console.print(table)

    if all_alerts:
        sys.exit(2)


# -----------------------------------------------------------------------------
# Database maintenance
# -----------------------------------------------------------------------------


@cli.group("db")
def db_group():
    """Database maintenance commands."""


@db_group.command("optimize")
@_db_option()
@click.option(
    "--vacuum",
    is_flag=True,
    help="Run VACUUM after optimizing. Rewrites the whole database file -- needs free disk space "
    "roughly the size of the final DB.",
)
def db_optimize(db_path, vacuum):
    """Backfill legacy trace_days rows: compress trace_json (Task 11) and
    fill the materialized v2_samples/v2_sil0/v2_nic0/v2_callsigns
    integrity-stat columns (Task 12) that the spoof/events path reads
    instead of decoding every trace.

    Processes ~200 rows per transaction and is safe to interrupt and
    re-run -- it skips rows that are already compressed and stat-filled.
    """
    from .db import optimize_trace_days

    cfg = _load_config(db_path)
    with (
        Database(cfg.db_path) as db,
        Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
        ) as progress,
    ):
        task_id = progress.add_task("Optimizing trace_days", total=None)

        def _on_progress(done: int, total: int) -> None:
            progress.update(task_id, completed=done, total=total)

        stats = optimize_trace_days(db, progress_callback=_on_progress)

    if stats["total"] == 0:
        console.print("[green]Nothing to optimize -- all rows already compressed and stat-filled.[/]")
    else:
        console.print(
            f"[green]Optimize complete.[/] {stats['processed']} rows processed "
            f"({stats['compressed']} compressed, {stats['stats_filled']} stat-filled)."
        )

    if vacuum:
        console.print(
            "[yellow]Running VACUUM -- this rewrites the entire database file and needs free disk space "
            "roughly the size of the final DB.[/]"
        )
        with Database(cfg.db_path) as db:
            db.conn.execute("VACUUM")
        console.print("[green]VACUUM complete.[/]")


@cli.command("mcp-serve")
@_db_option("SQLite database path the server reads from.")
def mcp_serve(db_path):
    """Run the read-only MCP server over stdio.

    Exposes adsbtrack query tools (aircraft stats, flights, events,
    gaps, registry lookup) to MCP-compatible LLM clients such as
    Claude Desktop and Claude Code. Requires the 'mcp' extra:
    `uv sync --extra mcp`.

    All tools are read-only; no fetch or write path is exposed.
    """
    from .mcp import serve

    serve(Path(db_path))


@cli.command()
@_db_option("SQLite database path to open in the TUI.")
def tui(db_path):
    """Launch the Textual TUI over the local SQLite database.

    Six views (aircraft list, flight timeline, event feed, spoofed
    broadcasts, map, status) plus an ops pane that wraps DB-writing
    commands. Requires the 'tui' extra: `uv sync --extra tui`.
    """
    try:
        from .tui.app import AdsbtrackApp
    except ImportError as e:
        raise click.ClickException("TUI extra is not installed. Run `uv sync --extra tui` to add textual.") from e

    config = _load_config(db_path)
    AdsbtrackApp(Path(db_path), config=config).run()


@cli.command("gui")
@_db_option("SQLite database path to export from.")
@click.option(
    "--out",
    "out_dir",
    default="gui-export",
    help="Output directory for the static GUI bundle (overwritten on each run).",
)
@click.option(
    "--hex", "hex_code", default=None, callback=_validate_hex, help="Focus the initial view on one ICAO hex (optional)."
)
def gui(db_path, out_dir, hex_code):
    """Write a static three-column HTML explorer backed by a JSON data snapshot.

    Produces `index.html`, `data.js`, and the design tokens CSS into
    ``--out``. Open `index.html` directly in any modern browser, including
    via `file://` - `data.js` assigns the snapshot to `window.ADSB_DATA`
    via a plain `<script>` tag rather than `fetch`, so no local server is
    needed. Renders the aircraft list, flight timeline, events, and
    spoofed-broadcasts audit. Read-only; rerun the command to refresh.
    """
    from .gui_export import export_gui

    config = _load_config(db_path)
    written = export_gui(Path(db_path), Path(out_dir), focus_hex=hex_code, config=config)
    console.print(f"[green]Wrote {len(written)} files to {out_dir}[/]")
    console.print(f"[dim]Open {out_dir}/index.html in your browser.[/]")


if __name__ == "__main__":
    cli()
