"""Live callsign -> hex resolution (issue #29).

Everything in adsbtrack is hex-keyed; when casework starts from a
callsign / flight number there was no in-tool path to the airframe.
`adsbtrack resolve <callsign>` asks the open live-traffic APIs
(api.adsb.lol, then opendata.adsb.fi) which airframes are broadcasting
that callsign right now and returns hex + registration + type for each
match.

Live-only by design: an aircraft that is not currently airborne (or at
least currently broadcasting) will not be found, and historical callsign
search is out of scope for v1. airplanes.live is deliberately not
queried: its API requires a key granted on application.

Matches carrying identity are cached into hex_crossref with a
"<network>_live" source tag - but never over an existing identity row,
since live-feed fields (spoofable broadcasts) must not clobber
FAA / Mictronics / hexdb data.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from .db import Database

_HEX_RE = re.compile(r"[0-9a-f]{6}")

# LiveMatch fields fillable from a readsb aircraft dict; later networks
# fill in what earlier ones left None (adsb.lol often omits desc/ownOp
# that adsb.fi carries).
_FILL_FIELDS = ("registration", "type_code", "type_description", "operator", "alt_baro", "ground_speed")


class LiveApiError(RuntimeError):
    """Non-transient error talking to a live-traffic network."""


class LiveNetworkClient:
    """Throttled client for a readsb-style /v2/callsign live endpoint.

    Both adsb.lol and adsb.fi serve the same response envelope:
    {"ac": [readsb aircraft dicts], "total": n, ...}. The callsign field
    ("flight") comes back space-padded and 404s are treated as an empty
    result rather than an error (the networks normally answer 200 with an
    empty "ac" list for unknown callsigns).
    """

    MAX_RETRIES = 2

    def __init__(
        self,
        name: str,
        base_url: str,
        *,
        client: httpx.Client | None = None,
        rate_limit_per_min: int = 30,
        timeout: float = 15.0,
    ):
        self.name = name
        self.base_url = base_url.rstrip("/")
        self._min_interval = 60.0 / rate_limit_per_min if rate_limit_per_min > 0 else 0.0
        self._owns_client = client is None
        self._client = client or httpx.Client(
            timeout=timeout,
            headers={"Accept": "application/json", "User-Agent": "adsbtrack/0.1"},
            follow_redirects=True,
        )
        self._last_call = 0.0

    def __enter__(self) -> LiveNetworkClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def _sleep(self, seconds: float) -> None:
        """Extracted so tests can patch without real delays."""
        time.sleep(seconds)

    def _throttle(self) -> None:
        if self._min_interval <= 0:
            return
        elapsed = time.monotonic() - self._last_call
        if elapsed < self._min_interval:
            self._sleep(self._min_interval - elapsed)
        self._last_call = time.monotonic()

    def get_callsign(self, callsign: str) -> list[dict]:
        """GET /v2/callsign/{CALLSIGN}. Returns the "ac" list (possibly
        empty). Raises LiveApiError when the network is unreachable or
        keeps failing, so the caller can fall through to the next one."""
        url = f"{self.base_url}/v2/callsign/{callsign.strip().upper()}"
        for attempt in range(self.MAX_RETRIES + 1):
            self._throttle()
            try:
                response = self._client.get(url)
            except httpx.RequestError as exc:
                if attempt >= self.MAX_RETRIES:
                    raise LiveApiError(f"{self.name} network error after {attempt} retries: {exc}") from exc
                self._sleep(2**attempt)
                continue

            if response.status_code == 200:
                payload = response.json()
                ac_list = payload.get("ac") if isinstance(payload, dict) else None
                return ac_list if isinstance(ac_list, list) else []

            if response.status_code == 404:
                return []

            if response.status_code == 429:
                retry_after = response.headers.get("retry-after")
                try:
                    wait = max(1.0, float(retry_after)) if retry_after else 5.0
                except ValueError:
                    wait = 5.0
                self._sleep(wait)
                continue

            if 500 <= response.status_code < 600 and attempt < self.MAX_RETRIES:
                self._sleep(2**attempt)
                continue

            raise LiveApiError(f"{self.name} HTTP {response.status_code} for {callsign}: {response.text[:200]}")

        raise LiveApiError(f"{self.name} retries exhausted for {callsign}")


@dataclass
class LiveMatch:
    """One currently-broadcasting airframe matching the queried callsign."""

    hex_code: str
    callsign: str
    registration: str | None = None
    type_code: str | None = None
    type_description: str | None = None
    operator: str | None = None
    alt_baro: float | str | None = None  # readsb reports "ground" as a string
    ground_speed: float | None = None
    networks: list[str] = field(default_factory=list)


def _match_from_ac(ac: dict, callsign: str, network: str) -> LiveMatch:
    registration = (ac.get("r") or "").strip() or None
    return LiveMatch(
        hex_code=ac["hex"].lower(),
        callsign=callsign,
        registration=registration,
        type_code=ac.get("t") or None,
        type_description=ac.get("desc") or None,
        operator=ac.get("ownOp") or None,
        alt_baro=ac.get("alt_baro"),
        ground_speed=ac.get("gs"),
        networks=[network],
    )


def _cache_matches(db: Database, matches: list[LiveMatch]) -> None:
    """Cache identity-bearing matches into hex_crossref, tagged with the
    first network that reported them. Existing identity rows are never
    overwritten: live-feed fields come from spoofable broadcasts and must
    not clobber FAA / Mictronics / hexdb data."""
    wrote = False
    for match in matches:
        if not (match.registration or match.type_code):
            continue
        existing = db.get_hex_crossref(match.hex_code)
        if existing is not None and any(existing[key] for key in ("registration", "type_code", "operator")):
            continue
        row = {
            "icao": match.hex_code,
            "registration": match.registration,
            "type_code": match.type_code,
            "type_description": match.type_description,
            "operator": match.operator,
            "source": f"{match.networks[0]}_live",
            "is_military": False,
            "mil_country": None,
            "mil_branch": None,
            "last_updated": datetime.now(UTC).isoformat(),
        }
        mil_row = db.lookup_mil_hex_range(match.hex_code)
        if mil_row is not None:
            row["is_military"] = True
            row["mil_country"] = mil_row["country"]
            row["mil_branch"] = mil_row["branch"]
        db.upsert_hex_crossref(row)
        wrote = True
    if wrote:
        db.commit()


def resolve_callsign(
    db: Database,
    callsign: str,
    *,
    clients: list[LiveNetworkClient],
    cache: bool = True,
) -> tuple[list[LiveMatch], dict[str, str]]:
    """Ask each live network which airframes broadcast ``callsign`` right now.

    Returns (matches, errors): matches deduplicated by hex across networks
    (first network wins per field, later ones fill gaps and are recorded in
    ``networks``), errors keyed by network name for any source that failed -
    one network being down must not sink the answer the other one has.
    """
    normalized = callsign.strip().upper()
    if not normalized:
        raise ValueError("callsign must not be empty")

    matches: dict[str, LiveMatch] = {}
    errors: dict[str, str] = {}
    for client in clients:
        try:
            ac_list = client.get_callsign(normalized)
        except LiveApiError as exc:
            errors[client.name] = str(exc)
            continue
        for ac in ac_list:
            flight = (ac.get("flight") or "").strip().upper()
            if flight != normalized:
                continue
            hex_code = (ac.get("hex") or "").strip().lower()
            if not _HEX_RE.fullmatch(hex_code):
                # TIS-B / MLAT synthetic addresses ("~2e40c9") aren't real
                # ICAO allocations; nothing downstream can use them.
                continue
            existing = matches.get(hex_code)
            if existing is None:
                matches[hex_code] = _match_from_ac(ac, normalized, client.name)
                continue
            existing.networks.append(client.name)
            fresh = _match_from_ac(ac, normalized, client.name)
            for field_name in _FILL_FIELDS:
                if getattr(existing, field_name) is None:
                    setattr(existing, field_name, getattr(fresh, field_name))

    result = list(matches.values())
    if cache:
        _cache_matches(db, result)
    return result, errors
