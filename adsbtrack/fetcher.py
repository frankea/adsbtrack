import asyncio
import contextlib
import gzip
import io
import json
import os
import tarfile
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

import httpx
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn, TimeRemainingColumn

from .config import SOURCE_URLS, Config
from .db import Database

# Referer headers per source domain
_SOURCE_REFERERS = {
    "adsbx": "https://globe.adsbexchange.com/",
    "adsbfi": "https://globe.adsb.fi/",
    "airplaneslive": "https://globe.airplanes.live/",
    "adsblol": "https://adsb.lol/",
    "theairtraffic": "https://globe.theairtraffic.com/",
}


def build_url(base_url: str, hex_code: str, day: date) -> str:
    last2 = hex_code[-2:]
    return f"{base_url}/{day.year}/{day.month:02d}/{day.day:02d}/traces/{last2}/trace_full_{hex_code}.json"


def date_range(start: date, end: date) -> list[date]:
    days = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def _referer_for_source(source: str) -> str:
    """Derive a referer URL from the source name or its base URL."""
    if source in _SOURCE_REFERERS:
        return _SOURCE_REFERERS[source]
    if source in SOURCE_URLS:
        # Extract origin from base URL: https://globe.example.com/globe_history -> https://globe.example.com/
        base = SOURCE_URLS[source]
        parts = base.split("/")
        return "/".join(parts[:3]) + "/"
    return f"https://{source}/"


@dataclass
class _FetchState:
    """Shared state across concurrent fetch workers for one source.

    Every field other than ``lock`` is mutated under the lock. ``lock``
    bounds the rate-limit window (minimum wall-clock gap between request
    STARTS) and serializes 429 backoff + 403 circuit-breaker updates.
    """

    current_delay: float
    rate_limit_floor: float
    rate_limit_max: float
    rate_limit_recovery: int
    last_request_start: float  # monotonic; next request can start at last + current_delay
    successes_since_backoff: int = 0
    # Per-day terminal outcomes once a day has exhausted its retry budget
    # (or succeeded). Keyed by isoformat date string.
    day_outcomes: dict[str, str] = field(default_factory=dict)
    # to_fetch sorted list of date strings used for the "3 consecutive
    # distinct 403-exhausted days" check (see _check_403_circuit_tripped).
    sorted_days: list[str] = field(default_factory=list)
    max_403_days: int = 3
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass
class _DayResult:
    """Result of a single day's fetch, queued for the DB-writer task.

    ``status`` is the terminal HTTP code logged to fetch_log. ``data`` is
    the parsed trace JSON for status 200 with a populated trace; None for
    204 / 404 / 403 / 429-exhausted / network errors.
    """

    day: date
    status: int
    data: dict | None = None
    had_data: bool = False
    had_error: bool = False


def _acquire_rate_slot_unlocked(state: _FetchState) -> float:
    """Reserve the next request-start slot and return its required wait.

    Caller must already hold ``state.lock``. Records the new
    ``last_request_start`` so a subsequent acquirer sees the slot
    claimed. Returns the sleep duration the caller should await before
    issuing its HTTP request.
    """
    now = time.monotonic()
    wait = state.last_request_start + state.current_delay - now
    if wait < 0:
        wait = 0.0
    state.last_request_start = now + wait
    return wait


async def _acquire_rate_slot(state: _FetchState) -> None:
    """Serialize request STARTS across all workers at >= current_delay
    apart. Holds the lock for the duration of the sleep so another worker
    that sees a 429 (and grabs the lock to bump current_delay) can do so
    before the next slot is reserved."""
    async with state.lock:
        wait = _acquire_rate_slot_unlocked(state)
        if wait > 0:
            await asyncio.sleep(wait)


async def _handle_429(state: _FetchState, retry_after_secs: float) -> None:
    """Apply 429 backoff: double current_delay (capped), reset success
    counter, sleep the Retry-After. All under the state lock so other
    workers waiting on the lock pick up the new delay on their next
    slot acquisition.
    """
    async with state.lock:
        state.current_delay = min(state.current_delay * 2, state.rate_limit_max)
        state.successes_since_backoff = 0
        # Sleep inside the lock so no other worker's slot advances while
        # the server is still telling us to back off.
        if retry_after_secs > 0:
            await asyncio.sleep(retry_after_secs)


async def _record_success(state: _FetchState) -> None:
    """Bump the success counter; halve current_delay (toward the floor)
    after rate_limit_recovery successes since the last backoff."""
    async with state.lock:
        state.successes_since_backoff += 1
        if state.current_delay > state.rate_limit_floor and state.successes_since_backoff >= state.rate_limit_recovery:
            state.current_delay = max(state.current_delay / 2, state.rate_limit_floor)
            state.successes_since_backoff = 0


def _check_403_circuit_tripped(state: _FetchState) -> bool:
    """Walk sorted_days in reverse, skipping days not yet complete,
    counting contiguous 403_exhausted outcomes. Trip the circuit when 3
    consecutive completed days (by date order) all exhausted on 403.
    A non-403 terminal outcome resets the count.

    Caller must hold state.lock.
    """
    count = 0
    for day_str in reversed(state.sorted_days):
        outcome = state.day_outcomes.get(day_str)
        if outcome is None:
            continue  # not finished yet
        if outcome == "403_exhausted":
            count += 1
            if count >= state.max_403_days:
                return True
        else:
            return False
    return False


def _build_headers(source: str, hex_code: str) -> dict[str, str]:
    return {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
        "dnt": "1",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "referer": _referer_for_source(source) + f"?icao={hex_code}",
        "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Brave";v="146"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "sec-gpc": "1",
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
        ),
        "x-requested-with": "XMLHttpRequest",
    }


def _parse_trace_body(day: date, raw_bytes: bytes, text_fallback: str) -> dict:
    """Decompress (or pass through) a 200 body, parse JSON, synthesize a
    midnight-UTC timestamp if missing. Raises on unparseable content."""
    try:
        text = gzip.decompress(raw_bytes).decode("utf-8")
    except gzip.BadGzipFile:
        text = text_fallback
    data = json.loads(text)
    if "timestamp" not in data:
        data["timestamp"] = datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp()
    return data


async def _fetch_one_day(
    client: httpx.AsyncClient,
    base_url: str,
    hex_code: str,
    day: date,
    source: str,
    state: _FetchState,
    progress: Progress,
    max_retries: int = 5,
) -> _DayResult:
    """Fetch a single day, honoring the shared rate-limit + backoff state.
    Returns a _DayResult capturing the terminal outcome; the caller
    enqueues it for the DB-writer task. Never raises for HTTP status; the
    only exception propagated is asyncio.CancelledError (SIGINT).
    """
    url = build_url(base_url, hex_code, day)
    retries = 0
    while True:
        await _acquire_rate_slot(state)
        try:
            resp = await client.get(url)
        except httpx.RequestError as e:
            retries += 1
            if retries > max_retries:
                progress.console.print(f"  [yellow]Network error for {day}: {e}, giving up[/]")
                return _DayResult(day=day, status=0, had_error=True)
            wait = 2**retries
            progress.console.print(f"  [yellow]Network error for {day}: {e}, retrying in {wait}s...[/]")
            await asyncio.sleep(wait)
            continue

        status = resp.status_code
        if status == 200:
            data = _parse_trace_body(day, resp.content, resp.text)
            if "trace" not in data or not data["trace"]:
                await _record_success(state)
                return _DayResult(day=day, status=204)
            await _record_success(state)
            return _DayResult(day=day, status=200, data=data, had_data=True)

        if status == 404:
            await _record_success(state)
            return _DayResult(day=day, status=404)

        if status == 403:
            retries += 1
            if retries > max_retries:
                return _DayResult(day=day, status=403, had_error=True)
            wait = min(60 * (2 ** (retries - 1)), 300)
            progress.console.print(
                f"  [yellow]HTTP 403 for {day} (likely bot protection), "
                f"backing off {wait}s (retry {retries}/{max_retries})...[/]"
            )
            await asyncio.sleep(wait)
            continue

        if status in (429, 500, 502, 503, 504):
            retries += 1
            if retries > max_retries:
                return _DayResult(day=day, status=status, had_error=True)
            if status == 429:
                retry_after = resp.headers.get("retry-after")
                wait = float(retry_after) if retry_after and retry_after.isdigit() else 2**retries
                progress.console.print(f"  [yellow]HTTP 429 for {day}, waiting {wait}s (delay will double)[/]")
                await _handle_429(state, wait)
            else:
                wait = 2**retries
                progress.console.print(f"  [yellow]HTTP {status} for {day}, retrying in {wait}s...[/]")
                await asyncio.sleep(wait)
            continue

        # Unrecognized status: log and return as error
        return _DayResult(day=day, status=status, had_error=True)


async def _db_writer(
    queue: "asyncio.Queue[_DayResult | None]",
    db: Database,
    hex_code: str,
    source: str,
    stats: dict,
    progress: Progress,
    task_id,
    state: _FetchState,
    circuit_tripped: asyncio.Event,
) -> None:
    """Single-writer task: drains the completion queue, inserts trace_days
    and fetch_log rows under one transaction per day, commits. Also
    updates the 403 circuit-breaker state and signals ``circuit_tripped``
    when 3 distinct consecutive days exhaust on 403.
    """
    while True:
        item = await queue.get()
        try:
            if item is None:
                return  # shutdown sentinel

            day_str = item.day.isoformat()
            if item.status == 200 and item.data is not None:
                db.insert_trace_day(hex_code, day_str, item.data, source=source)
                db.insert_fetch_log(hex_code, day_str, 200, source=source)
                stats["with_data"] += 1
                outcome = "200"
            elif item.status == 204:
                db.insert_fetch_log(hex_code, day_str, 204, source=source)
                outcome = "204"
            elif item.status == 404:
                db.insert_fetch_log(hex_code, day_str, 404, source=source)
                outcome = "404"
            elif item.status == 403 and item.had_error:
                db.insert_fetch_log(hex_code, day_str, 403, source=source)
                stats["errors"] += 1
                outcome = "403_exhausted"
            elif item.status == 0 and item.had_error:
                # Network error exhausted retries; no fetch_log row so the
                # day gets re-tried on next run. Count in stats only.
                stats["errors"] += 1
                outcome = "network_exhausted"
            else:
                db.insert_fetch_log(hex_code, day_str, item.status, source=source)
                if item.had_error:
                    stats["errors"] += 1
                outcome = str(item.status)

            stats["fetched"] += 1
            # Commit after each day so every fetch_log row + matching
            # trace_days row become durable atomically. SIGINT here rolls
            # back the open transaction (either both rows land or neither).
            db.commit()
            progress.advance(task_id)

            # Update circuit-breaker state under the lock and trip the
            # event if the 3-consecutive-403 rule is met.
            async with state.lock:
                state.day_outcomes[day_str] = outcome
                if _check_403_circuit_tripped(state):
                    circuit_tripped.set()
        finally:
            queue.task_done()


async def _worker(
    day: date,
    semaphore: asyncio.Semaphore,
    client: httpx.AsyncClient,
    base_url: str,
    hex_code: str,
    source: str,
    state: _FetchState,
    queue: "asyncio.Queue[_DayResult | None]",
    progress: Progress,
    circuit_tripped: asyncio.Event,
) -> None:
    """Worker coroutine for one day. Bounded by the semaphore (concurrency
    cap), sleeps between starts via _acquire_rate_slot, enqueues result."""
    async with semaphore:
        if circuit_tripped.is_set():
            # Another worker tripped the 403 breaker; bail without fetch.
            return
        result = await _fetch_one_day(client, base_url, hex_code, day, source, state, progress)
        await queue.put(result)


async def _fetch_traces_async(
    db: Database,
    config: Config,
    hex_code: str,
    start_date: date,
    end_date: date,
    source: str,
    concurrency: int,
) -> dict:
    base_url = SOURCE_URLS[source]
    already_fetched = db.get_fetched_dates(hex_code, source=source)

    all_days = date_range(start_date, end_date)
    to_fetch = [d for d in all_days if d.isoformat() not in already_fetched]

    if not to_fetch:
        return {"fetched": 0, "with_data": 0, "skipped": len(all_days), "errors": 0}

    stats = {
        "fetched": 0,
        "with_data": 0,
        "skipped": len(all_days) - len(to_fetch),
        "errors": 0,
    }

    state = _FetchState(
        current_delay=config.rate_limit,
        rate_limit_floor=config.rate_limit,
        rate_limit_max=config.rate_limit_max,
        rate_limit_recovery=config.rate_limit_recovery,
        last_request_start=time.monotonic() - config.rate_limit,  # let first request fire immediately
        sorted_days=sorted(d.isoformat() for d in to_fetch),
    )
    circuit_tripped = asyncio.Event()
    queue: asyncio.Queue[_DayResult | None] = asyncio.Queue()
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async with (
        httpx.AsyncClient(
            http2=True,
            headers=_build_headers(source, hex_code),
            timeout=30,
            follow_redirects=True,
        ) as client,
    ):
        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
        ) as progress:
            task_id = progress.add_task(f"Fetching {hex_code} ({source})", total=len(to_fetch))
            writer = asyncio.create_task(
                _db_writer(queue, db, hex_code, source, stats, progress, task_id, state, circuit_tripped)
            )

            worker_tasks = [
                asyncio.create_task(
                    _worker(d, semaphore, client, base_url, hex_code, source, state, queue, progress, circuit_tripped)
                )
                for d in to_fetch
            ]

            try:
                # Monitor circuit_tripped: when it fires, cancel any
                # workers that haven't started yet but let the writer
                # drain whatever they've already enqueued.
                pending = set(worker_tasks)
                while pending:
                    done, pending = await asyncio.wait(pending, timeout=0.5, return_when=asyncio.FIRST_COMPLETED)
                    if circuit_tripped.is_set():
                        for t in pending:
                            t.cancel()
                        # Wait for cancellations to settle.
                        for t in pending:
                            with contextlib.suppress(asyncio.CancelledError, Exception):
                                await t
                        pending = set()
                        break
                # Signal writer to stop after draining.
                await queue.put(None)
                await writer
            except (KeyboardInterrupt, asyncio.CancelledError):
                # SIGINT or cancellation: cancel all workers, stop writer
                # after drain. Any day whose _DayResult already landed on
                # the queue commits as usual; everything else is left for
                # the next run (already_fetched reflects committed state).
                for t in worker_tasks:
                    t.cancel()
                for t in worker_tasks:
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await t
                await queue.put(None)
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await writer
                raise
            finally:
                # Idempotent: safe if already committed in the writer.
                db.commit()

            if circuit_tripped.is_set():
                progress.stop()
                raise RuntimeError(
                    f"HTTP 403 for {source} on {state.max_403_days} consecutive days. "
                    "The source is likely blocking automated requests. "
                    "Try again later, increase --rate (e.g. --rate 3), "
                    "or check if the source requires authentication."
                )

    return stats


def fetch_traces(
    db: Database,
    config: Config,
    hex_code: str,
    start_date: date,
    end_date: date,
    source: str = "adsbx",
    *,
    concurrency: int | None = None,
) -> dict:
    """Fetch readsb-format traces for ``hex_code`` from ``source``.

    Sync wrapper around the async implementation. The public contract is
    identical to the pre-async version: returns
    ``{"fetched", "with_data", "skipped", "errors"}``. ``concurrency``
    defaults to ``config.fetch_concurrency`` (4). A value of 1 is
    byte-identical to serial behavior.

    Rate limit is enforced between request STARTS, not completions, so
    on a cooperative source concurrency can overlap in-flight requests
    up to the rate-limit floor. On a rate-limit-bound source the
    effective concurrency is 1 regardless of this setting, and wall
    time stays flat (the async infrastructure is still correct, just
    not useful on that source).
    """
    if concurrency is None:
        concurrency = getattr(config, "fetch_concurrency", 4)
    return asyncio.run(_fetch_traces_async(db, config, hex_code, start_date, end_date, source, concurrency))


def fetch_traces_adsblol(db: Database, config: Config, hex_code: str, start_date: date, end_date: date) -> dict:
    """Fetch traces from adsb.lol GitHub releases (same readsb JSON format)."""
    source = "adsblol"
    already_fetched = db.get_fetched_dates(hex_code, source=source)
    all_days = date_range(start_date, end_date)
    to_fetch = [d for d in all_days if d.isoformat() not in already_fetched]

    if not to_fetch:
        return {"fetched": 0, "with_data": 0, "skipped": len(all_days), "errors": 0}

    stats = {"fetched": 0, "with_data": 0, "skipped": len(all_days) - len(to_fetch), "errors": 0}
    last2 = hex_code[-2:]
    trace_filename = f"traces/{last2}/trace_full_{hex_code}.json"

    with (
        httpx.Client(timeout=120, follow_redirects=True) as client,
        Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
        ) as progress,
    ):
        task = progress.add_task(f"Fetching {hex_code} (adsblol)", total=len(to_fetch))

        for day in to_fetch:
            year = day.year
            tag = day.isoformat()
            # adsb.lol publishes daily tarballs as GitHub release assets
            asset_url = f"https://github.com/adsblol/globe_history_{year}/releases/download/{tag}/traces.tar.gz"

            try:
                resp = client.get(asset_url)
                if resp.status_code == 200:
                    try:
                        with tarfile.open(fileobj=io.BytesIO(resp.content), mode="r:gz") as tar:
                            try:
                                member = tar.getmember(trace_filename)
                                f = tar.extractfile(member)
                                raw = f.read()
                                # File may be gzipped inside the tar
                                with contextlib.suppress(gzip.BadGzipFile):
                                    raw = gzip.decompress(raw)
                                data = json.loads(raw)
                                if "trace" in data and data["trace"]:
                                    if "timestamp" not in data:
                                        progress.console.print(
                                            f"  [dim yellow]No timestamp in {day} data, using midnight UTC[/]"
                                        )
                                        data["timestamp"] = datetime(
                                            day.year, day.month, day.day, tzinfo=UTC
                                        ).timestamp()
                                    db.insert_trace_day(hex_code, day.isoformat(), data, source=source)
                                    db.insert_fetch_log(hex_code, day.isoformat(), 200, source=source)
                                    stats["with_data"] += 1
                                else:
                                    db.insert_fetch_log(hex_code, day.isoformat(), 204, source=source)
                            except KeyError:
                                # Hex not in this day's archive
                                db.insert_fetch_log(hex_code, day.isoformat(), 404, source=source)
                    except (tarfile.TarError, EOFError):
                        db.insert_fetch_log(hex_code, day.isoformat(), 500, source=source)
                        stats["errors"] += 1
                elif resp.status_code == 404:
                    db.insert_fetch_log(hex_code, day.isoformat(), 404, source=source)
                else:
                    db.insert_fetch_log(hex_code, day.isoformat(), resp.status_code, source=source)
                    stats["errors"] += 1
            except httpx.RequestError as e:
                progress.console.print(f"  [yellow]Network error for {day}: {e}[/]")
                stats["errors"] += 1

            stats["fetched"] += 1
            progress.advance(task)
            # Commit after each day so concurrent fetches on the same db
            # don't block past the WAL busy_timeout.
            db.commit()
            time.sleep(config.rate_limit)

        db.commit()

    return stats


def _load_opensky_credentials(config: Config) -> tuple[str, str]:
    """Load OpenSky credentials from env vars or JSON file. Returns (username, password)."""
    # Prefer environment variables
    client_id = os.environ.get("OPENSKY_CLIENT_ID")
    client_secret = os.environ.get("OPENSKY_CLIENT_SECRET")
    if client_id and client_secret:
        return client_id, client_secret

    # Fall back to credentials file
    if not config.credentials_path.exists():
        raise RuntimeError(
            "OpenSky credentials not found.\n"
            "Set OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET environment variables,\n"
            f"or create {config.credentials_path} with: "
            '{"clientId": "...", "clientSecret": "..."}'
        )
    with open(config.credentials_path) as f:
        creds = json.load(f)
    return creds["clientId"], creds["clientSecret"]


def _opensky_path_to_readsb(path: list, callsign: str | None, start_time: int) -> dict:
    """Convert OpenSky track response to readsb-compatible trace format."""
    trace = []
    for wp in path:
        ts, lat, lon, baro_alt, true_track, on_ground = wp
        if lat is None or lon is None:
            continue
        time_offset = ts - start_time
        alt = "ground" if on_ground else (round(baro_alt * 3.28084) if baro_alt else None)
        gs = None  # OpenSky doesn't provide ground speed in tracks
        trace.append([time_offset, lat, lon, alt, gs, None, None, None, {}])

    return {
        "timestamp": start_time,
        "trace": trace,
        "r": callsign,  # best we have for registration
    }


def fetch_traces_opensky(db: Database, config: Config, hex_code: str, start_date: date, end_date: date) -> dict:
    """Fetch flight data from OpenSky Network API.

    Uses /flights/aircraft for historical flight metadata (no 30-day limit),
    then /tracks/all for detailed waypoints (last 30 days only).
    """
    source = "opensky"
    already_fetched = db.get_fetched_dates(hex_code, source=source)
    username, password = _load_opensky_credentials(config)

    all_days = date_range(start_date, end_date)
    to_fetch = [d for d in all_days if d.isoformat() not in already_fetched]

    if not to_fetch:
        return {"fetched": 0, "with_data": 0, "skipped": len(all_days), "errors": 0}

    stats = {"fetched": 0, "with_data": 0, "skipped": len(all_days) - len(to_fetch), "errors": 0}

    # Group into 2-day windows (OpenSky /flights/aircraft limit)
    windows = []
    i = 0
    while i < len(to_fetch):
        window_start = to_fetch[i]
        window_end = min(to_fetch[min(i + 1, len(to_fetch) - 1)], window_start + timedelta(days=1))
        window_days = [to_fetch[j] for j in range(i, min(i + 2, len(to_fetch))) if to_fetch[j] <= window_end]
        windows.append((window_start, window_end, window_days))
        i += len(window_days)

    base_url = "https://opensky-network.org/api"
    thirty_days_ago = date.today() - timedelta(days=30)

    with (
        httpx.Client(auth=(username, password), timeout=30) as client,
        Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
        ) as progress,
    ):
        task = progress.add_task(f"Fetching {hex_code} (opensky)", total=len(to_fetch))

        for window_start, window_end, window_days in windows:
            begin_ts = int(datetime(window_start.year, window_start.month, window_start.day, tzinfo=UTC).timestamp())
            end_ts = int(
                datetime(window_end.year, window_end.month, window_end.day, 23, 59, 59, tzinfo=UTC).timestamp()
            )

            try:
                # Get flights in this window
                resp = client.get(
                    f"{base_url}/flights/aircraft", params={"icao24": hex_code, "begin": begin_ts, "end": end_ts}
                )

                if resp.status_code == 403:
                    progress.stop()
                    raise RuntimeError(
                        f"OpenSky access denied: {resp.text.strip()}. "
                        "Historical data requires upgraded access at opensky-network.org"
                    )

                if resp.status_code == 200:
                    flights = resp.json()
                    if not flights:
                        for day in window_days:
                            db.insert_fetch_log(hex_code, day.isoformat(), 204, source=source)
                            stats["fetched"] += 1
                            progress.advance(task)
                        time.sleep(1)
                        continue

                    # For each flight, try to get the track if within 30 days
                    for flight in flights:
                        first_seen = flight.get("firstSeen")
                        if not first_seen:
                            continue
                        flight_date = datetime.fromtimestamp(first_seen, tz=UTC).date()
                        flight_day_str = flight_date.isoformat()

                        if flight_date >= thirty_days_ago:
                            # Can get detailed track
                            time.sleep(1)
                            track_resp = client.get(
                                f"{base_url}/tracks/all", params={"icao24": hex_code, "time": first_seen}
                            )
                            if track_resp.status_code == 200:
                                track = track_resp.json()
                                if track and track.get("path"):
                                    data = _opensky_path_to_readsb(
                                        track["path"], track.get("callsign"), track["startTime"]
                                    )
                                    db.insert_trace_day(hex_code, flight_day_str, data, source=source)
                                    db.insert_fetch_log(hex_code, flight_day_str, 200, source=source)
                                    stats["with_data"] += 1
                                    continue

                        # No detailed track available, log that we checked
                        db.insert_fetch_log(hex_code, flight_day_str, 204, source=source)

                    for _day in window_days:
                        stats["fetched"] += 1
                        progress.advance(task)

                elif resp.status_code == 404:
                    for day in window_days:
                        db.insert_fetch_log(hex_code, day.isoformat(), 404, source=source)
                        stats["fetched"] += 1
                        progress.advance(task)

                elif resp.status_code == 429:
                    retry_after = resp.headers.get("x-rate-limit-retry-after-seconds", "60")
                    wait = int(retry_after) if retry_after.isdigit() else 60
                    progress.console.print(f"  [yellow]OpenSky rate limit hit, waiting {wait}s[/]")
                    time.sleep(wait)
                    continue  # retry this window

                else:
                    for day in window_days:
                        db.insert_fetch_log(hex_code, day.isoformat(), resp.status_code, source=source)
                        stats["errors"] += 1
                        stats["fetched"] += 1
                        progress.advance(task)

            except httpx.RequestError as e:
                progress.console.print(f"  [yellow]Network error: {e}[/]")
                for _day in window_days:
                    stats["errors"] += 1
                    stats["fetched"] += 1
                    progress.advance(task)

            time.sleep(1)  # be gentle with OpenSky

        db.commit()

    return stats
