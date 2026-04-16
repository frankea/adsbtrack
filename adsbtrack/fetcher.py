import contextlib
import gzip
import io
import json
import os
import tarfile
import time
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


def fetch_traces(
    db: Database, config: Config, hex_code: str, start_date: date, end_date: date, source: str = "adsbx"
) -> dict:
    base_url = SOURCE_URLS[source]
    already_fetched = db.get_fetched_dates(hex_code, source=source)

    all_days = date_range(start_date, end_date)
    to_fetch = [d for d in all_days if d.isoformat() not in already_fetched]

    if not to_fetch:
        return {"fetched": 0, "with_data": 0, "skipped": len(all_days), "errors": 0}

    stats = {"fetched": 0, "with_data": 0, "skipped": len(all_days) - len(to_fetch), "errors": 0}

    current_delay = config.rate_limit
    successes_since_backoff = 0
    # Circuit breaker: count days where every retry returned 403. Reset whenever
    # we get any response that wasn't 403 (even a 404), since that proves the
    # origin is reachable and we aren't CDN-blocked. Raises RuntimeError once
    # we've eaten through enough days of pure 403s to conclude the source is
    # genuinely blocking us rather than throwing transient bot-challenge 403s.
    consecutive_403_days = 0
    max_403_days = 3

    headers = {
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

    with (
        httpx.Client(http2=True, headers=headers, timeout=30, follow_redirects=True) as client,
        Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
        ) as progress,
    ):
        task = progress.add_task(f"Fetching {hex_code} ({source})", total=len(to_fetch))

        for day in to_fetch:
            url = build_url(base_url, hex_code, day)
            retries = 0
            max_retries = 5

            while retries <= max_retries:
                try:
                    resp = client.get(url)

                    if resp.status_code == 200:
                        # Handle gzip - try to decompress raw bytes first
                        try:
                            text = gzip.decompress(resp.content).decode("utf-8")
                        except gzip.BadGzipFile:
                            text = resp.text
                        data = json.loads(text)

                        # Some 200 responses have no trace data (just {"icao": "..."})
                        if "trace" not in data or not data["trace"]:
                            db.insert_fetch_log(hex_code, day.isoformat(), 204, source=source)
                            consecutive_403_days = 0
                            break

                        # Synthesize timestamp if missing (midnight UTC of that day)
                        if "timestamp" not in data:
                            progress.console.print(f"  [dim yellow]No timestamp in {day} data, using midnight UTC[/]")
                            data["timestamp"] = datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp()

                        db.insert_trace_day(hex_code, day.isoformat(), data, source=source)
                        db.insert_fetch_log(hex_code, day.isoformat(), 200, source=source)
                        stats["with_data"] += 1
                        consecutive_403_days = 0
                        break

                    elif resp.status_code == 404:
                        db.insert_fetch_log(hex_code, day.isoformat(), 404, source=source)
                        consecutive_403_days = 0
                        break

                    elif resp.status_code == 403:
                        # Treat 403 as transient (Cloudflare/WAF bot challenge). Back off
                        # aggressively and retry. Only give up on the source after
                        # max_403_days consecutive days exhaust their retry budget.
                        retries += 1
                        if retries > max_retries:
                            db.insert_fetch_log(hex_code, day.isoformat(), 403, source=source)
                            stats["errors"] += 1
                            consecutive_403_days += 1
                            if consecutive_403_days >= max_403_days:
                                progress.stop()
                                raise RuntimeError(
                                    f"HTTP 403 for {source} on {consecutive_403_days} consecutive days. "
                                    "The source is likely blocking automated requests. "
                                    "Try again later, increase --rate (e.g. --rate 3), "
                                    "or check if the source requires authentication."
                                )
                            break
                        wait = min(60 * (2 ** (retries - 1)), 300)
                        progress.console.print(
                            f"  [yellow]HTTP 403 for {day} (likely bot protection), "
                            f"backing off {wait}s (retry {retries}/{max_retries})...[/]"
                        )
                        time.sleep(wait)

                    elif resp.status_code in (429, 500, 502, 503, 504):
                        retries += 1
                        if retries > max_retries:
                            db.insert_fetch_log(hex_code, day.isoformat(), resp.status_code, source=source)
                            stats["errors"] += 1
                            break
                        # On 429, also increase the base delay for future requests
                        if resp.status_code == 429:
                            retry_after = resp.headers.get("retry-after")
                            wait = int(retry_after) if retry_after and retry_after.isdigit() else 2**retries
                            current_delay = min(current_delay * 2, config.rate_limit_max)
                            successes_since_backoff = 0
                            progress.console.print(
                                f"  [yellow]HTTP 429 for {day}, waiting {wait}s "
                                f"(base delay now {current_delay:.1f}s)[/]"
                            )
                        else:
                            wait = 2**retries
                            progress.console.print(
                                f"  [yellow]HTTP {resp.status_code} for {day}, retrying in {wait}s...[/]"
                            )
                        time.sleep(wait)
                    else:
                        db.insert_fetch_log(hex_code, day.isoformat(), resp.status_code, source=source)
                        stats["errors"] += 1
                        break

                except httpx.RequestError as e:
                    retries += 1
                    if retries > max_retries:
                        stats["errors"] += 1
                        break
                    wait = 2**retries
                    progress.console.print(f"  [yellow]Network error for {day}: {e}, retrying in {wait}s...[/]")
                    time.sleep(wait)

            stats["fetched"] += 1
            progress.advance(task)

            # Commit after each day so the write transaction doesn't stay open
            # across the rate-limit sleep. In WAL mode only one writer at a time
            # can hold the lock, so holding it for >30s blocks any concurrent
            # fetch past the busy_timeout.
            db.commit()

            # Gradually recover delay after consecutive successes
            successes_since_backoff += 1
            if current_delay > config.rate_limit and successes_since_backoff >= config.rate_limit_recovery:
                current_delay = max(current_delay / 2, config.rate_limit)
                successes_since_backoff = 0
                progress.console.print(f"  [green]Rate recovering, delay now {current_delay:.1f}s[/]")

            time.sleep(current_delay)

        db.commit()

    return stats


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
