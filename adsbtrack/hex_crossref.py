"""Hex cross-reference enrichment: merges FAA registry, Mictronics, and hexdb.io.

The unified `hex_crossref` table lets adsbtrack resolve any ICAO hex to a
registration / type / operator without caring which source supplied the
row. Sources are preferred in this order:

1. FAA registry (authoritative for N-numbered aircraft, already loaded
   via `registry update`)
2. Mictronics ICAO DB (community-maintained, covers foreign civilian
   and many military registrations)
3. hexdb.io REST API (live lookup, used as a fallback for hexes the
   first two sources miss)

The module also wraps the military-range lookup from `mil_hex.py` so a
single `enrich_hex()` call stamps is_military / mil_country / mil_branch
on any hex that falls into a known military allocation block.
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import httpx

from .config import Config

if TYPE_CHECKING:
    from .db import Database


# Files we pull from the Mictronics DB. dbversion.json is included so we
# can tell users whether their cache needs a refresh.
_MICTRONICS_FILES: tuple[str, ...] = (
    "aircrafts.json",
    "types.json",
    "operators.json",
    "dbversion.json",
)


class HexCrossrefError(RuntimeError):
    """Non-transient error during hex enrichment."""


# -----------------------------------------------------------------------------
# Mictronics
# -----------------------------------------------------------------------------


def download_mictronics(cfg: Config, *, cache_dir: Path | None = None) -> Path:
    """Download the four Mictronics DB files into cache_dir.

    Files are overwritten on each run; the caller is expected to invoke
    this on demand (it's ~16 MB and only needs to run when the user wants
    a refresh, not on every enrich).

    Returns the resolved cache_dir so callers can chain straight into
    :func:`import_mictronics`.
    """
    dest = cache_dir or cfg.mictronics_cache_dir
    dest.mkdir(parents=True, exist_ok=True)
    base = cfg.mictronics_base_url.rstrip("/")
    with httpx.Client(timeout=120.0, follow_redirects=True) as client:
        for name in _MICTRONICS_FILES:
            url = f"{base}/{name}"
            response = client.get(url)
            response.raise_for_status()
            (dest / name).write_bytes(response.content)
    return dest


def _load_mictronics_files(cache_dir: Path) -> tuple[dict, dict, dict, str | None]:
    """Read the four JSON files from cache_dir. Returns
    (aircrafts, types, operators, db_version)."""
    with (cache_dir / "aircrafts.json").open("r", encoding="utf-8") as fh:
        aircrafts = json.load(fh)
    with (cache_dir / "types.json").open("r", encoding="utf-8") as fh:
        types = json.load(fh)
    with (cache_dir / "operators.json").open("r", encoding="utf-8") as fh:
        operators = json.load(fh)
    db_version: str | None = None
    version_path = cache_dir / "dbversion.json"
    if version_path.exists():
        try:
            db_version = json.loads(version_path.read_text()).get("version")
        except (json.JSONDecodeError, AttributeError):
            db_version = None
    return aircrafts, types, operators, db_version


def _mictronics_row_to_crossref(
    hex_code: str,
    entry: list,
    types: dict,
    operators: dict,
    *,
    source_label: str,
) -> dict:
    """Flatten a Mictronics aircrafts.json row into a hex_crossref row.

    entry layout (positional): [registration, icao_type_code, flags_hex_str]
    The types dict maps icao_type_code -> [description, wtc, category], and
    operators maps a 3-letter operator code derived from registration
    callsign (not available in aircrafts.json) - so operator is left NULL
    here and filled in from hexdb.io when possible.
    """
    registration = entry[0] if len(entry) > 0 else None
    type_code = entry[1] if len(entry) > 1 else None
    type_description: str | None = None
    if type_code and type_code in types:
        type_description = types[type_code][0]
    return {
        "icao": hex_code.lower(),
        "registration": registration or None,
        "type_code": type_code or None,
        "type_description": type_description,
        "operator": None,  # Mictronics aircrafts.json has no operator field
        "source": source_label,
        "is_military": False,
        "mil_country": None,
        "mil_branch": None,
        "last_updated": datetime.now(UTC).isoformat(),
    }


def import_mictronics(db: Database, cache_dir: Path) -> int:
    """Load Mictronics JSON files from cache_dir and INSERT OR REPLACE
    every aircraft into hex_crossref with source='mictronics'. Rows that
    already have a stronger source (checked by caller) are not touched
    here -- this function is a bulk loader. Callers that want merge
    semantics should use :func:`enrich_hex` per-hex instead.

    Returns the number of rows written.
    """
    aircrafts, types, operators, _version = _load_mictronics_files(cache_dir)
    count = 0
    # One transaction for the whole bulk load -- 462k INSERTs committed
    # individually would take minutes; within a single transaction it
    # runs in a couple of seconds.
    with db.conn:
        for hex_code, entry in aircrafts.items():
            row = _mictronics_row_to_crossref(hex_code, entry, types, operators, source_label="mictronics")
            db.upsert_hex_crossref(row)
            count += 1
    return count


# -----------------------------------------------------------------------------
# hexdb.io
# -----------------------------------------------------------------------------


class HexdbClient:
    """Minimal REST client for hexdb.io with per-minute throttling.

    hexdb.io publicly caps the polite-use rate around 60/min. The client
    self-throttles and retries on 429 / 5xx. 404 is treated as a
    "not in database" miss and returns None rather than raising.
    """

    MAX_RETRIES = 3

    def __init__(
        self,
        *,
        base_url: str = "https://hexdb.io",
        client: httpx.Client | None = None,
        rate_limit_per_min: int = 60,
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

    def __enter__(self):
        return self

    def __exit__(self, *exc):
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

    def get_aircraft(self, hex_code: str) -> dict | None:
        """GET /api/v1/aircraft/{HEX}. Returns the parsed JSON on 200,
        or None on 404 (including 404s returned as JSON bodies)."""
        url = f"{self.base_url}/api/v1/aircraft/{hex_code.upper()}"
        for attempt in range(self.MAX_RETRIES + 1):
            self._throttle()
            try:
                response = self._client.get(url)
            except httpx.RequestError as exc:
                if attempt >= self.MAX_RETRIES:
                    raise HexCrossrefError(f"hexdb.io network error after {attempt} retries: {exc}") from exc
                self._sleep(2**attempt)
                continue

            if response.status_code == 200:
                payload = response.json()
                # hexdb.io sometimes returns 200 with a {"status":"404"}
                # body for unknown hexes. Normalize that to None.
                if isinstance(payload, dict) and str(payload.get("status", "")) == "404":
                    return None
                return payload

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

            raise HexCrossrefError(f"hexdb.io HTTP {response.status_code} for {hex_code}: {response.text[:200]}")

        raise HexCrossrefError(f"hexdb.io retries exhausted for {hex_code}")


def _hexdb_payload_to_crossref(hex_code: str, payload: dict) -> dict:
    """Map a hexdb.io payload to a hex_crossref row."""
    return {
        "icao": hex_code.lower(),
        "registration": payload.get("Registration") or None,
        "type_code": payload.get("ICAOTypeCode") or None,
        "type_description": payload.get("Type") or payload.get("Manufacturer") or None,
        "operator": payload.get("RegisteredOwners") or None,
        "source": "hexdb",
        "is_military": False,
        "mil_country": None,
        "mil_branch": None,
        "last_updated": datetime.now(UTC).isoformat(),
    }


# -----------------------------------------------------------------------------
# Merge / enrichment
# -----------------------------------------------------------------------------


def _faa_to_crossref(row, hex_code: str) -> dict:
    """Map a faa_registry sqlite3.Row to a hex_crossref row."""
    tail = row["n_number"]
    return {
        "icao": hex_code.lower(),
        "registration": f"N{tail}" if tail else None,
        "type_code": row["mfr_mdl_code"] or None,
        "type_description": None,  # FAA stores codes; full description lives in faa_aircraft_ref
        "operator": row["name"] or None,
        "source": "faa",
        "is_military": False,
        "mil_country": None,
        "mil_branch": None,
        "last_updated": datetime.now(UTC).isoformat(),
    }


def enrich_hex(
    db: Database,
    hex_code: str,
    *,
    hexdb_client: HexdbClient | None = None,
    mictronics_cache: tuple[dict, dict, dict] | None = None,
) -> tuple[dict | None, list[str]]:
    """Resolve identity for a single hex with source preference FAA -> Mictronics -> hexdb.io.

    Returns (row, conflicts):
      - row: the merged hex_crossref row written to the db, or None if no
        source produced any data and the hex isn't in a military range
      - conflicts: list of human-readable notes when two sources disagreed
        on registration or type_code. Always present (possibly empty).

    The military check is always run (cheap local table lookup) and flags
    is_military / mil_country / mil_branch regardless of which other
    source filled the row.

    `mictronics_cache`, when given, is (aircrafts, types, operators) from
    :func:`_load_mictronics_files` -- passing it in avoids repeat disk
    reads when enriching many hexes in a loop.
    """
    from .mil_hex import is_military_hex

    hex_lower = hex_code.lower()
    conflicts: list[str] = []
    chosen: dict | None = None

    # 1. FAA registry (preferred)
    faa_row = db.get_faa_registry_by_hex(hex_lower)
    if faa_row is not None:
        chosen = _faa_to_crossref(faa_row, hex_lower)

    # 2. Mictronics fallback / cross-check
    mictronics_row: dict | None = None
    if mictronics_cache is not None:
        aircrafts, types, _operators = mictronics_cache
        entry = aircrafts.get(hex_lower) or aircrafts.get(hex_code.upper())
        if entry:
            mictronics_row = _mictronics_row_to_crossref(hex_lower, entry, types, {}, source_label="mictronics")

    if chosen is None and mictronics_row is not None:
        chosen = mictronics_row
    elif chosen is not None and mictronics_row is not None:
        conflicts.extend(_diff_fields(chosen, mictronics_row, label_a="faa", label_b="mictronics"))

    # 3. hexdb.io live lookup (only when we still have no identity)
    if chosen is None and hexdb_client is not None:
        payload = hexdb_client.get_aircraft(hex_lower)
        if payload:
            chosen = _hexdb_payload_to_crossref(hex_lower, payload)

    # Military range check (always on, independent of civilian identity source)
    is_mil, country, branch = is_military_hex(db, hex_lower)
    if is_mil:
        if chosen is None:
            chosen = {
                "icao": hex_lower,
                "registration": None,
                "type_code": None,
                "type_description": None,
                "operator": None,
                "source": "mil_range",
                "is_military": True,
                "mil_country": country,
                "mil_branch": branch,
                "last_updated": datetime.now(UTC).isoformat(),
            }
        else:
            chosen["is_military"] = True
            chosen["mil_country"] = country
            chosen["mil_branch"] = branch

    if chosen is not None:
        db.upsert_hex_crossref(chosen)
        db.commit()

    return chosen, conflicts


def _diff_fields(a: dict, b: dict, *, label_a: str, label_b: str) -> list[str]:
    """Compare registration and type_code between two crossref dicts and
    return human-readable conflict notes for any mismatches."""
    notes: list[str] = []
    for field in ("registration", "type_code"):
        va, vb = a.get(field), b.get(field)
        if va and vb and _norm(va) != _norm(vb):
            notes.append(f"{field}: {label_a}={va!r} vs {label_b}={vb!r}")
    return notes


def _norm(value: str) -> str:
    """Case-normalize plus strip for conflict comparison."""
    return value.strip().upper()


def enrich_all(
    db: Database,
    *,
    cfg: Config | None = None,
    mictronics_cache_dir: Path | None = None,
    use_hexdb: bool = True,
    progress_callback=None,
) -> dict:
    """Backfill every icao in trace_days / flights missing a hex_crossref row.

    Loads Mictronics from disk once (if cache exists) and keeps a single
    HexdbClient open for the whole run. Returns a stats dict with counts.

    progress_callback, when given, is called as progress_callback(done, total).
    """
    missing = db.get_icaos_missing_crossref()
    stats = {"processed": 0, "written": 0, "no_data": 0, "conflicts": 0}
    if not missing:
        return stats

    mictronics_cache: tuple[dict, dict, dict] | None = None
    if mictronics_cache_dir is not None and (mictronics_cache_dir / "aircrafts.json").exists():
        aircrafts, types, operators, _ = _load_mictronics_files(mictronics_cache_dir)
        mictronics_cache = (aircrafts, types, operators)

    hexdb_client: HexdbClient | None = None
    if use_hexdb and cfg is not None:
        hexdb_client = HexdbClient(
            base_url=cfg.hexdb_base_url,
            rate_limit_per_min=cfg.hexdb_rate_limit_per_min,
        )

    try:
        total = len(missing)
        for idx, hex_code in enumerate(missing, start=1):
            row, conflicts = enrich_hex(
                db,
                hex_code,
                hexdb_client=hexdb_client,
                mictronics_cache=mictronics_cache,
            )
            stats["processed"] += 1
            if row is not None:
                stats["written"] += 1
            else:
                stats["no_data"] += 1
            if conflicts:
                stats["conflicts"] += 1
            if progress_callback is not None:
                progress_callback(idx, total)
    finally:
        if hexdb_client is not None:
            hexdb_client.close()

    return stats
