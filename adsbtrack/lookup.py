"""Universal aircraft identity lookup (issue #27).

`adsbtrack lookup <hex|registration>` answers hex <-> registration <->
type <-> operator for any country, not just FAA-registered aircraft.

Resolution order:

1. Local first: the hex_crossref cache, then the FAA registry /
   Mictronics merge via :func:`hex_crossref.enrich_hex` (which also runs
   the hexdb.io live fallback when online use is allowed).
2. Online fallback: hexdb.io (inside enrich_hex), then adsbdb
   (api.adsbdb.com) - adsbdb covers many foreign civil and military
   registrations hexdb.io misses (e.g. UAE A6-* airframes).
3. Military-range annotation from mil_hex_ranges runs regardless, so an
   unresolvable US DoD-pool hex still gets a useful answer instead of a
   bare miss.

Online results are cached into hex_crossref with a source tag ("hexdb" /
"adsbdb") so repeat lookups stay local.
"""

from __future__ import annotations

import re
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import httpx

from .hex_crossref import HexdbClient, enrich_hex
from .nnumber import icao_to_nnumber, nnumber_to_icao

if TYPE_CHECKING:
    from .db import Database

_HEX_RE = re.compile(r"[0-9a-f]{6}")

# hex_crossref columns that constitute an actual identity. A row carrying
# none of these (e.g. a mil_range-only stub) still leaves the hex worth an
# online lookup attempt.
_IDENTITY_FIELDS = ("registration", "type_code", "operator")


class AdsbdbError(RuntimeError):
    """Non-transient error talking to adsbdb."""


class AdsbdbClient:
    """Minimal client for api.adsbdb.com with per-minute throttling.

    The /v0/aircraft/{query} endpoint accepts either a Mode-S hex or a
    registration and answers with an aircraft envelope. Unknown aircraft
    come back as HTTP 404 with an {"response": "unknown aircraft"} body;
    both spellings normalize to None. adsbdb is a free community API with
    no published hard limit; the default self-throttle keeps adsbtrack a
    polite consumer.
    """

    MAX_RETRIES = 3

    def __init__(
        self,
        *,
        base_url: str = "https://api.adsbdb.com",
        client: httpx.Client | None = None,
        rate_limit_per_min: int = 30,
        timeout: float = 15.0,
    ):
        self.base_url = base_url.rstrip("/")
        self._min_interval = 60.0 / rate_limit_per_min if rate_limit_per_min > 0 else 0.0
        self._owns_client = client is None
        self._client = client or httpx.Client(
            timeout=timeout,
            headers={"Accept": "application/json", "User-Agent": "adsbtrack/0.1"},
            follow_redirects=True,
        )
        self._last_call = 0.0

    def __enter__(self) -> AdsbdbClient:
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

    def get_aircraft(self, query: str) -> dict | None:
        """GET /v0/aircraft/{hex or registration}. Returns the aircraft
        dict out of the response envelope on 200, or None when adsbdb
        doesn't know the aircraft."""
        url = f"{self.base_url}/v0/aircraft/{query.strip().upper()}"
        for attempt in range(self.MAX_RETRIES + 1):
            self._throttle()
            try:
                response = self._client.get(url)
            except httpx.RequestError as exc:
                if attempt >= self.MAX_RETRIES:
                    raise AdsbdbError(f"adsbdb network error after {attempt} retries: {exc}") from exc
                self._sleep(2**attempt)
                continue

            if response.status_code == 200:
                payload = response.json()
                aircraft = payload.get("response") if isinstance(payload, dict) else None
                if not isinstance(aircraft, dict):
                    # {"response": "unknown aircraft"} and any other
                    # non-envelope body count as a miss.
                    return None
                aircraft = aircraft.get("aircraft")
                return aircraft if isinstance(aircraft, dict) else None

            if response.status_code == 404:
                return None

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

            raise AdsbdbError(f"adsbdb HTTP {response.status_code} for {query}: {response.text[:200]}")

        raise AdsbdbError(f"adsbdb retries exhausted for {query}")


def _adsbdb_to_crossref(payload: dict) -> dict:
    """Map an adsbdb aircraft envelope to a hex_crossref row."""
    manufacturer = payload.get("manufacturer") or ""
    type_name = payload.get("type") or ""
    description = " ".join(part for part in (manufacturer, type_name) if part) or None
    return {
        "icao": (payload.get("mode_s") or "").lower(),
        "registration": payload.get("registration") or None,
        "type_code": payload.get("icao_type") or None,
        "type_description": description,
        "operator": payload.get("registered_owner") or None,
        "source": "adsbdb",
        "is_military": False,
        "mil_country": None,
        "mil_branch": None,
        "last_updated": datetime.now(UTC).isoformat(),
    }


@dataclass
class LookupResult:
    """Everything `adsbtrack lookup` learned about one query."""

    query: str
    kind: str = "hex"  # "hex" or "registration", from the query's shape
    hex_code: str | None = None
    record: dict | None = None  # hex_crossref-shaped identity, if any
    source: str | None = None  # convenience copy of record["source"]
    country: str | None = None  # adsbdb extra; not a hex_crossref column
    derived_registration: str | None = None  # algorithmic N-number, unverified
    mil_range: dict | None = None  # matching mil_hex_ranges row, if any
    conflicts: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)  # resolution trail

    @property
    def resolved(self) -> bool:
        """True when the lookup produced an answer worth exit code 0. A
        registration query that yielded a hex is an answer even without an
        identity record (the mapping itself is what was asked for); a hex
        query needs an identity, a derived N-number, or at least a
        military-range attribution."""
        if self.hex_code is None:
            return False
        if self.kind == "registration":
            return True
        return self.record is not None or self.mil_range is not None or self.derived_registration is not None


def _row_to_dict(row: sqlite3.Row) -> dict:
    """sqlite3.Row -> plain dict. Row iterates values, so dict(row) alone
    doesn't work - and `for key in row` would iterate values too, which is
    why this zips keys explicitly."""
    return dict(zip(row.keys(), row, strict=True))


def _has_identity(record: sqlite3.Row | dict) -> bool:
    """True when the row/dict carries at least one identity field."""
    return any(record[key] for key in _IDENTITY_FIELDS)


def _overlay_mil(db: Database, record: dict) -> None:
    """Stamp is_military / mil_country / mil_branch from mil_hex_ranges."""
    row = db.lookup_mil_hex_range(record["icao"])
    if row is not None:
        record["is_military"] = True
        record["mil_country"] = row["country"]
        record["mil_branch"] = row["branch"]


def _resolve_registration(
    db: Database,
    reg: str,
    result: LookupResult,
    *,
    hexdb_client: HexdbClient | None,
    adsbdb_client: AdsbdbClient | None,
) -> tuple[str | None, dict | None]:
    """Registration -> hex. Local sources first, then hexdb.io's reg-hex
    converter, then adsbdb (whose payload is returned alongside the hex so
    the caller doesn't re-fetch the identity it just received)."""
    # 1. Algorithmic FAA N-number conversion - no DB or network needed.
    try:
        hex_code = nnumber_to_icao(reg)
    except ValueError:
        hex_code = None
    if hex_code is not None:
        result.notes.append(f"N-number {reg} converts algorithmically to {hex_code.lower()}")
        return hex_code.lower(), None

    # 2. Local tables, newest row first (registrations get reassigned).
    for table in ("hex_crossref", "aircraft_registry"):
        row = db.conn.execute(
            f"SELECT icao FROM {table} WHERE registration = ? COLLATE NOCASE "  # noqa: S608 - table name is literal
            "ORDER BY COALESCE(last_updated, '') DESC",
            (reg,),
        ).fetchone()
        if row is not None:
            result.notes.append(f"registration found locally in {table}")
            return row["icao"].lower(), None

    # 3. Online: hexdb.io reg-hex converter, then adsbdb by registration.
    if hexdb_client is not None:
        hex_code = hexdb_client.get_registration_hex(reg)
        if hex_code is not None:
            result.notes.append("registration resolved via hexdb.io reg-hex")
            return hex_code, None
    if adsbdb_client is not None:
        payload = adsbdb_client.get_aircraft(reg)
        if payload is not None and payload.get("mode_s"):
            result.notes.append("registration resolved via adsbdb")
            return payload["mode_s"].lower(), payload

    result.notes.append("registration not found locally or online")
    return None, None


def lookup_aircraft(
    db: Database,
    query: str,
    *,
    hexdb_client: HexdbClient | None = None,
    adsbdb_client: AdsbdbClient | None = None,
    mictronics_cache: tuple[dict, dict, dict] | None = None,
) -> LookupResult:
    """Resolve a hex or registration to a full identity record.

    Pass hexdb_client=None / adsbdb_client=None to skip that online
    fallback (offline mode passes neither). Successful online lookups are
    cached into hex_crossref so the next lookup answers locally.
    """
    stripped = query.strip()
    result = LookupResult(query=stripped)
    adsbdb_payload: dict | None = None
    hex_code: str | None

    if _HEX_RE.fullmatch(stripped.lower()):
        hex_code = stripped.lower()
    else:
        result.kind = "registration"
        hex_code, adsbdb_payload = _resolve_registration(
            db,
            stripped.upper(),
            result,
            hexdb_client=hexdb_client,
            adsbdb_client=adsbdb_client,
        )
        if hex_code is None:
            return result
    result.hex_code = hex_code

    cached = db.get_hex_crossref(hex_code)
    if cached is not None and _has_identity(cached):
        result.record = _row_to_dict(cached)
        result.source = result.record.get("source")
        result.notes.append("answered from hex_crossref cache")
    else:
        # FAA registry -> Mictronics -> hexdb.io, with mil overlay; writes
        # to hex_crossref as a side effect.
        record, conflicts = enrich_hex(
            db,
            hex_code,
            hexdb_client=hexdb_client,
            mictronics_cache=mictronics_cache,
        )
        result.conflicts = conflicts
        if record is not None and _has_identity(record):
            result.record = record
            result.source = record.get("source")
        else:
            # Final online fallback: adsbdb (reusing the payload when the
            # registration path already fetched it).
            if adsbdb_payload is None and adsbdb_client is not None:
                adsbdb_payload = adsbdb_client.get_aircraft(hex_code)
            if adsbdb_payload is not None:
                fetched = _adsbdb_to_crossref(adsbdb_payload)
                fetched["icao"] = hex_code  # trust our normalized hex
                _overlay_mil(db, fetched)
                db.upsert_hex_crossref(fetched)
                db.commit()
                result.record = fetched
                result.source = "adsbdb"
                result.country = adsbdb_payload.get("registered_owner_country_name")
            elif record is not None:
                # mil_range-only stub from enrich_hex - still an answer.
                result.record = record
                result.source = record.get("source")

    # Algorithmic N-number for US civil hexes nothing else resolved:
    # useful when the FAA registry isn't loaded. Marked unverified, and
    # skipped when it would just echo the queried registration back.
    if result.record is None or not result.record.get("registration"):
        try:
            derived = icao_to_nnumber(hex_code)
        except ValueError:
            derived = None
        if derived is not None and derived.upper() != stripped.upper():
            result.derived_registration = derived

    # Military-range annotation always runs, resolved or not.
    mil_row = db.lookup_mil_hex_range(hex_code)
    if mil_row is not None:
        result.mil_range = _row_to_dict(mil_row)

    return result
