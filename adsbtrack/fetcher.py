import gzip
import io
import json
import tarfile
import time
from datetime import date, datetime, timedelta, timezone

import httpx
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn, TimeRemainingColumn

from .config import Config, SOURCE_URLS
from .db import Database


# Referer headers per source domain
_SOURCE_REFERERS = {
    "adsbx": "https://globe.adsbexchange.com/",
    "adsbfi": "https://globe.adsb.fi/",
    "airplaneslive": "https://globe.airplanes.live/",
    "adsblol": "https://adsb.lol/",
    "theairtraffic": "https://globe.theairtraffic.com/",
}


def load_cookies(config: Config) -> dict[str, str] | None:
    if not config.cookies_path.exists():
        return None
    with open(config.cookies_path) as f:
        return json.load(f)


def build_url(base_url: str, hex_code: str, day: date) -> str:
    last2 = hex_code[-2:]
    return (
        f"{base_url}/{day.year}/{day.month:02d}/{day.day:02d}"
        f"/traces/{last2}/trace_full_{hex_code}.json"
    )


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


def fetch_traces(db: Database, config: Config, hex_code: str,
                 start_date: date, end_date: date,
                 source: str = "adsbx") -> dict:
    base_url = SOURCE_URLS[source]
    already_fetched = db.get_fetched_dates(hex_code, source=source)

    all_days = date_range(start_date, end_date)
    to_fetch = [d for d in all_days if d.isoformat() not in already_fetched]

    if not to_fetch:
        return {"fetched": 0, "with_data": 0, "skipped": len(all_days), "errors": 0}

    stats = {"fetched": 0, "with_data": 0, "skipped": len(all_days) - len(to_fetch), "errors": 0}

    current_delay = config.rate_limit
    successes_since_backoff = 0

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
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
    }

    # Browser doesn't send cookies - neither should we
    cookies = None

    with httpx.Client(http2=True, headers=headers, cookies=cookies,
                      timeout=30, follow_redirects=True) as client:
        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task(
                f"Fetching {hex_code} ({source})", total=len(to_fetch)
            )

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
                                break

                            # Synthesize timestamp if missing (midnight UTC of that day)
                            if "timestamp" not in data:
                                from datetime import datetime, timezone
                                data["timestamp"] = datetime(
                                    day.year, day.month, day.day, tzinfo=timezone.utc
                                ).timestamp()

                            db.insert_trace_day(hex_code, day.isoformat(), data, source=source)
                            db.insert_fetch_log(hex_code, day.isoformat(), 200, source=source)
                            stats["with_data"] += 1
                            break

                        elif resp.status_code == 404:
                            db.insert_fetch_log(hex_code, day.isoformat(), 404, source=source)
                            break

                        elif resp.status_code == 403:
                            progress.stop()
                            raise RuntimeError(
                                f"Authentication failed (403). Update cookies in {config.cookies_path}"
                            )

                        elif resp.status_code in (429, 500, 502, 503, 504):
                            retries += 1
                            if retries > max_retries:
                                db.insert_fetch_log(hex_code, day.isoformat(), resp.status_code, source=source)
                                stats["errors"] += 1
                                break
                            # On 429, also increase the base delay for future requests
                            if resp.status_code == 429:
                                retry_after = resp.headers.get("retry-after")
                                if retry_after and retry_after.isdigit():
                                    wait = int(retry_after)
                                else:
                                    wait = 2 ** retries
                                current_delay = min(current_delay * 2, config.rate_limit_max)
                                successes_since_backoff = 0
                                progress.console.print(
                                    f"  [yellow]HTTP 429 for {day}, waiting {wait}s "
                                    f"(base delay now {current_delay:.1f}s)[/]"
                                )
                            else:
                                wait = 2 ** retries
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
                        wait = 2 ** retries
                        progress.console.print(
                            f"  [yellow]Network error for {day}: {e}, retrying in {wait}s...[/]"
                        )
                        time.sleep(wait)

                stats["fetched"] += 1
                progress.advance(task)

                # Gradually recover delay after consecutive successes
                successes_since_backoff += 1
                if (current_delay > config.rate_limit
                        and successes_since_backoff >= config.rate_limit_recovery):
                    current_delay = max(current_delay / 2, config.rate_limit)
                    successes_since_backoff = 0
                    progress.console.print(
                        f"  [green]Rate recovering, delay now {current_delay:.1f}s[/]"
                    )

                time.sleep(current_delay)

    return stats


def fetch_traces_adsblol(db: Database, config: Config, hex_code: str,
                         start_date: date, end_date: date) -> dict:
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

    with httpx.Client(timeout=120, follow_redirects=True) as client:
        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task(
                f"Fetching {hex_code} (adsblol)", total=len(to_fetch)
            )

            for day in to_fetch:
                year = day.year
                tag = day.isoformat()
                # adsb.lol publishes daily tarballs as GitHub release assets
                asset_url = (
                    f"https://github.com/adsblol/globe_history_{year}"
                    f"/releases/download/{tag}/traces.tar.gz"
                )

                try:
                    resp = client.get(asset_url)
                    if resp.status_code == 200:
                        try:
                            tar = tarfile.open(fileobj=io.BytesIO(resp.content), mode="r:gz")
                            try:
                                member = tar.getmember(trace_filename)
                                f = tar.extractfile(member)
                                raw = f.read()
                                # File may be gzipped inside the tar
                                try:
                                    raw = gzip.decompress(raw)
                                except gzip.BadGzipFile:
                                    pass
                                data = json.loads(raw)
                                if "trace" in data and data["trace"]:
                                    if "timestamp" not in data:
                                        data["timestamp"] = datetime(
                                            day.year, day.month, day.day, tzinfo=timezone.utc
                                        ).timestamp()
                                    db.insert_trace_day(hex_code, day.isoformat(), data, source=source)
                                    db.insert_fetch_log(hex_code, day.isoformat(), 200, source=source)
                                    stats["with_data"] += 1
                                else:
                                    db.insert_fetch_log(hex_code, day.isoformat(), 204, source=source)
                            except KeyError:
                                # Hex not in this day's archive
                                db.insert_fetch_log(hex_code, day.isoformat(), 404, source=source)
                            finally:
                                tar.close()
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
                time.sleep(config.rate_limit)

    return stats


def _load_opensky_credentials(config: Config) -> tuple[str, str]:
    """Load OpenSky credentials from JSON file. Returns (username, password)."""
    if not config.credentials_path.exists():
        raise RuntimeError(
            f"Credentials file not found: {config.credentials_path}\n"
            f'Create it with: {{"clientId": "...", "clientSecret": "..."}}'
        )
    with open(config.credentials_path) as f:
        creds = json.load(f)
    return creds["clientId"], creds["clientSecret"]


def _opensky_path_to_readsb(path: list, callsign: str | None,
                             start_time: int) -> dict:
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


def fetch_traces_opensky(db: Database, config: Config, hex_code: str,
                          start_date: date, end_date: date) -> dict:
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
        window_end = min(to_fetch[min(i + 1, len(to_fetch) - 1)],
                         window_start + timedelta(days=1))
        window_days = [to_fetch[j] for j in range(i, min(i + 2, len(to_fetch)))
                       if to_fetch[j] <= window_end]
        windows.append((window_start, window_end, window_days))
        i += len(window_days)

    base_url = "https://opensky-network.org/api"
    thirty_days_ago = date.today() - timedelta(days=30)

    with httpx.Client(auth=(username, password), timeout=30) as client:
        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task(
                f"Fetching {hex_code} (opensky)", total=len(to_fetch)
            )

            for window_start, window_end, window_days in windows:
                begin_ts = int(datetime(window_start.year, window_start.month,
                                        window_start.day, tzinfo=timezone.utc).timestamp())
                end_ts = int(datetime(window_end.year, window_end.month,
                                      window_end.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())

                try:
                    # Get flights in this window
                    resp = client.get(
                        f"{base_url}/flights/aircraft",
                        params={"icao24": hex_code, "begin": begin_ts, "end": end_ts}
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
                            flight_date = datetime.fromtimestamp(first_seen, tz=timezone.utc).date()
                            flight_day_str = flight_date.isoformat()

                            if flight_date >= thirty_days_ago:
                                # Can get detailed track
                                time.sleep(1)
                                track_resp = client.get(
                                    f"{base_url}/tracks/all",
                                    params={"icao24": hex_code, "time": first_seen}
                                )
                                if track_resp.status_code == 200:
                                    track = track_resp.json()
                                    if track and track.get("path"):
                                        data = _opensky_path_to_readsb(
                                            track["path"],
                                            track.get("callsign"),
                                            track["startTime"]
                                        )
                                        db.insert_trace_day(hex_code, flight_day_str, data, source=source)
                                        db.insert_fetch_log(hex_code, flight_day_str, 200, source=source)
                                        stats["with_data"] += 1
                                        continue

                            # No detailed track available, log that we checked
                            db.insert_fetch_log(hex_code, flight_day_str, 204, source=source)

                        for day in window_days:
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
                        progress.console.print(
                            f"  [yellow]OpenSky rate limit hit, waiting {wait}s[/]"
                        )
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
                    for day in window_days:
                        stats["errors"] += 1
                        stats["fetched"] += 1
                        progress.advance(task)

                time.sleep(1)  # be gentle with OpenSky

    return stats
