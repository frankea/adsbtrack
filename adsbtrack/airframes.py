"""HTTP client for the airframes.io REST API.

The public API lives at https://api.airframes.io (no /v1 prefix despite
what the published OpenAPI spec says). This module wraps the three routes
we need for ACARS backfill:

  GET /airframes/icao/{hex}  -> airframe record with numeric id
  GET /airframes/{id}        -> airframe with `flights` array
  GET /flights/{id}          -> flight with `messages` array (capped at 200)

Auth is a static API key passed via the X-API-KEY header. Rate limits on
the paid tier are 60 requests/minute and 50k/day; the client self-throttles
to stay below the per-minute cap and retries on 429 / 5xx.
"""

from __future__ import annotations

import time

import httpx

BASE_URL = "https://api.airframes.io"

_DEFAULT_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "adsbtrack/0.1 (+https://github.com/afranke/adsbtrack)",
}


class AirframesError(RuntimeError):
    """Non-transient HTTP or protocol error from airframes.io."""


class AirframesClient:
    """Minimal synchronous client for api.airframes.io.

    Usage::

        with AirframesClient(api_key="...") as client:
            airframe = client.get_airframe_by_icao("06A0A5")
            full = client.get_airframe_by_id(airframe["id"])
            for f in full["flights"]:
                flight = client.get_flight(f["id"])
                for msg in flight["messages"]:
                    ...
    """

    MAX_RETRIES = 5

    def __init__(
        self,
        api_key: str,
        *,
        client: httpx.Client | None = None,
        rate_limit_per_min: int = 50,
        timeout: float = 60.0,
    ):
        if not api_key:
            raise ValueError("api_key required")
        self.api_key = api_key
        # rate_limit_per_min=0 disables throttling (used in tests)
        self._min_interval = 60.0 / rate_limit_per_min if rate_limit_per_min > 0 else 0.0
        self._owns_client = client is None
        self._client = client or httpx.Client(
            headers=_DEFAULT_HEADERS,
            timeout=timeout,
            http2=True,
            follow_redirects=True,
        )
        self._last_call_ts = 0.0
        self.daily_remaining: int | None = None
        self.minute_remaining: int | None = None

    # Context manager ---------------------------------------------------------

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self):
        if self._owns_client:
            self._client.close()

    # Throttling / sleeping ---------------------------------------------------

    def _sleep(self, seconds: float) -> None:
        """Extracted so tests can patch it without real delays."""
        time.sleep(seconds)

    def _throttle(self) -> None:
        if self._min_interval <= 0:
            return
        elapsed = time.monotonic() - self._last_call_ts
        if elapsed < self._min_interval:
            self._sleep(self._min_interval - elapsed)
        self._last_call_ts = time.monotonic()

    # Core GET with retry ------------------------------------------------------

    def _get(self, path: str) -> dict | list | None:
        """GET {BASE_URL}{path}. Returns parsed JSON, or None on 404.

        Retries on 429 (honoring Retry-After) and on 5xx with exponential
        backoff. Raises AirframesError when max retries are exhausted or
        the response isn't JSON as promised.
        """
        url = f"{BASE_URL}{path}"
        headers = {"X-API-KEY": self.api_key}

        for attempt in range(self.MAX_RETRIES + 1):
            self._throttle()
            try:
                response = self._client.get(url, headers=headers)
            except httpx.RequestError as exc:
                if attempt >= self.MAX_RETRIES:
                    raise AirframesError(f"network error after {attempt} retries: {exc}") from exc
                self._sleep(2**attempt)
                continue

            self._record_rate_limit(response.headers)

            status = response.status_code
            if status == 200:
                ct = response.headers.get("content-type", "")
                if "json" not in ct.lower():
                    raise AirframesError(f"expected JSON, got {ct!r} for {url}")
                return response.json()

            if status == 404:
                return None

            if status == 429:
                retry_after = self._parse_retry_after(response.headers)
                self._sleep(retry_after)
                continue

            if 500 <= status < 600:
                if attempt >= self.MAX_RETRIES:
                    raise AirframesError(f"HTTP {status} after {attempt} retries: {response.text[:200]}")
                self._sleep(2**attempt)
                continue

            # Non-retryable 4xx
            raise AirframesError(f"HTTP {status}: {response.text[:200]}")

        raise AirframesError(f"retries exhausted for {url}")

    @staticmethod
    def _parse_retry_after(headers) -> float:
        raw = headers.get("retry-after") or headers.get("Retry-After") or ""
        try:
            return max(1.0, float(raw))
        except (TypeError, ValueError):
            return 5.0

    def _record_rate_limit(self, headers) -> None:
        # airframes.io returns x-ratelimit-remaining for both per-min and
        # per-day buckets depending on which endpoint you hit. Record both
        # slots when present so callers can show progress.
        rem = headers.get("x-ratelimit-remaining") or headers.get("X-RateLimit-Remaining")
        if rem is not None:
            try:
                val = int(rem)
            except ValueError:
                return
            # Heuristic: values <= 60 are the per-minute bucket, higher are daily.
            # This matches the observed behavior where /airframes/icao/* returns
            # the 60/min counter and /messages returns the 50k/day one.
            if val <= 60:
                self.minute_remaining = val
            else:
                self.daily_remaining = val

    # Public endpoints --------------------------------------------------------

    def get_airframe_by_icao(self, icao: str) -> dict | None:
        """Resolve an ICAO hex to the airframe record (including numeric id).

        Returns None when airframes.io has no record for the hex.
        """
        return self._get(f"/airframes/icao/{icao.upper()}")

    def get_airframe_by_id(self, airframe_id: int) -> dict | None:
        """Fetch the airframe by its numeric id. The response includes a
        `flights` array with up to several hundred entries (all history).
        """
        return self._get(f"/airframes/{int(airframe_id)}")

    def get_flight(self, flight_id: int) -> dict | None:
        """Fetch a flight by id. The response includes a `messages` array
        capped by the server at ~200 items. Long flights will be truncated.
        """
        return self._get(f"/flights/{int(flight_id)}")
