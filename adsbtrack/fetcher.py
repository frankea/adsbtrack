import gzip
import json
import time
from datetime import date, datetime, timedelta

import httpx
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn, TimeRemainingColumn

from .config import Config
from .db import Database


def load_cookies(config: Config) -> dict[str, str] | None:
    if not config.cookies_path.exists():
        return None
    with open(config.cookies_path) as f:
        return json.load(f)


def build_url(config: Config, hex_code: str, day: date) -> str:
    last2 = hex_code[-2:]
    return (
        f"{config.adsbx_base_url}/{day.year}/{day.month:02d}/{day.day:02d}"
        f"/traces/{last2}/trace_full_{hex_code}.json"
    )


def date_range(start: date, end: date) -> list[date]:
    days = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def fetch_traces(db: Database, config: Config, hex_code: str,
                 start_date: date, end_date: date) -> dict:
    already_fetched = db.get_fetched_dates(hex_code)

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
        "referer": "https://globe.adsbexchange.com/",
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

    with httpx.Client(http2=True, headers=headers, timeout=30, follow_redirects=True) as client:
        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task(
                f"Fetching {hex_code}", total=len(to_fetch)
            )

            for day in to_fetch:
                url = build_url(config, hex_code, day)
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
                                db.insert_fetch_log(hex_code, day.isoformat(), 204)
                                break

                            # Synthesize timestamp if missing (midnight UTC of that day)
                            if "timestamp" not in data:
                                from datetime import datetime, timezone
                                data["timestamp"] = datetime(
                                    day.year, day.month, day.day, tzinfo=timezone.utc
                                ).timestamp()

                            db.insert_trace_day(hex_code, day.isoformat(), data)
                            db.insert_fetch_log(hex_code, day.isoformat(), 200)
                            stats["with_data"] += 1
                            break

                        elif resp.status_code == 404:
                            db.insert_fetch_log(hex_code, day.isoformat(), 404)
                            break

                        elif resp.status_code == 403:
                            progress.stop()
                            raise RuntimeError(
                                f"Authentication failed (403). Update cookies in {config.cookies_path}"
                            )

                        elif resp.status_code in (429, 500, 502, 503, 504):
                            retries += 1
                            if retries > max_retries:
                                db.insert_fetch_log(hex_code, day.isoformat(), resp.status_code)
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
                            db.insert_fetch_log(hex_code, day.isoformat(), resp.status_code)
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
