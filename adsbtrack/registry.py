"""FAA aircraft registry downloader, importer, and lookup helpers.

The FAA publishes a zipped bundle of comma-delimited CSV data at
https://registry.faa.gov/database/ReleasableAircraft.zip. The three
files we care about are MASTER.txt, DEREG.txt, and ACFTREF.txt; each
has a different schema, so we treat them as three separate sources.

Quirks the parser handles:
  * UTF-8 BOM on the first byte of each file
  * latin-1 / cp1252 single-byte characters scattered through owner names
  * Fixed-width padding inside the comma-delimited fields (spaces trimmed)
  * Leading-space in at least one MASTER fieldname (" KIT MODEL")
  * DEREG has a completely different column set from MASTER (separate
    mail / physical addresses, CANCEL-DATE instead of EXPIRATION DATE)

The download is routed through curl_cffi with a Chrome TLS fingerprint
when the optional dependency is installed; otherwise it falls back to
plain httpx. The FAA site sits behind Akamai Bot Manager which 503s
default Python TLS fingerprints, so curl_cffi is strongly recommended.
"""

from __future__ import annotations

import csv
import zipfile
from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory

from rich.progress import Progress

from .config import Config
from .db import Database

# FAA files ship with latin-1 / cp1252 bytes in a small fraction of rows
# (accented owner names, odd punctuation). We decode as latin-1 which
# round-trips every byte, and strip any UTF-8 BOM manually.
_FAA_ENCODING = "latin-1"
_FAA_DELIMITER = ","
# UTF-8 BOM bytes 0xEF 0xBB 0xBF decoded as latin-1 are the three chars
# below. When the file starts with this sequence we skip it before csv.
_BOM_AS_LATIN1 = "\xef\xbb\xbf"


def octal_mode_s_to_icao_hex(octal_str: str) -> str:
    """Convert FAA MODE S CODE (8-digit octal) to 6-char ICAO hex.

    Raises ValueError if the input is empty or not a valid octal number.
    The result is always 6 lowercase hex characters, zero-padded.
    """
    stripped = octal_str.strip()
    if not stripped:
        raise ValueError("empty MODE S CODE")
    value = int(stripped, 8)  # raises ValueError on non-octal digits
    return format(value, "06x")


# -----------------------------------------------------------------------------
# Row parsing
# -----------------------------------------------------------------------------

# MASTER.txt column order mirrors db.insert_faa_registry. Keep these
# lists in the same order as the table columns so positional tuples
# slot directly into executemany().
_MASTER_HEADERS: tuple[tuple[str, str], ...] = (
    ("N-NUMBER", "n_number"),
    ("SERIAL NUMBER", "serial_number"),
    ("MFR MDL CODE", "mfr_mdl_code"),
    ("ENG MFR MDL", "eng_mfr_mdl"),
    ("YEAR MFR", "year_mfr"),
    ("TYPE REGISTRANT", "type_registrant"),
    ("NAME", "name"),
    ("STREET", "street"),
    ("STREET2", "street2"),
    ("CITY", "city"),
    ("STATE", "state"),
    ("ZIP CODE", "zip_code"),
    ("REGION", "region"),
    ("COUNTY", "county"),
    ("COUNTRY", "country"),
    ("LAST ACTION DATE", "last_action_date"),
    ("CERT ISSUE DATE", "cert_issue_date"),
    ("CERTIFICATION", "certification"),
    ("TYPE AIRCRAFT", "type_aircraft"),
    ("TYPE ENGINE", "type_engine"),
    ("STATUS CODE", "status_code"),
    ("MODE S CODE", "mode_s_code"),
    ("FRACT OWNER", "fract_owner"),
    ("AIR WORTH DATE", "air_worth_date"),
    ("EXPIRATION DATE", "expiration_date"),
    ("UNIQUE ID", "unique_id"),
    ("KIT MFR", "kit_mfr"),
    ("KIT MODEL", "kit_model"),
    # mode_s_code_hex at the end, derived from MODE S CODE when present
    # or from the file's MODE S CODE HEX column when the file supplies it.
)

MASTER_COLUMNS: tuple[str, ...] = tuple(snake for _, snake in _MASTER_HEADERS) + ("mode_s_code_hex",)

# DEREG.txt uses dash-separated headers, no TYPE AIRCRAFT / TYPE ENGINE /
# FRACT OWNER columns, and splits mailing and physical addresses. We
# project it onto the MASTER-shaped faa_deregistered schema by preferring
# PHYSICAL addresses with a fallback to MAIL and leaving fields that do
# not exist in DEREG as NULL.
_DEREG_SOURCE_HEADERS: tuple[str, ...] = (
    "N-NUMBER",
    "SERIAL-NUMBER",
    "MFR-MDL-CODE",
    "STATUS-CODE",
    "NAME",
    "STREET-MAIL",
    "STREET2-MAIL",
    "CITY-MAIL",
    "STATE-ABBREV-MAIL",
    "ZIP-CODE-MAIL",
    "ENG-MFR-MDL",
    "YEAR-MFR",
    "CERTIFICATION",
    "REGION",
    "COUNTY-MAIL",
    "COUNTRY-MAIL",
    "AIR-WORTH-DATE",
    "CANCEL-DATE",
    "MODE-S-CODE",
    "LAST-ACT-DATE",
    "CERT-ISSUE-DATE",
    "STREET-PHYSICAL",
    "STREET2-PHYSICAL",
    "CITY-PHYSICAL",
    "STATE-ABBREV-PHYSICAL",
    "ZIP-CODE-PHYSICAL",
    "COUNTY-PHYSICAL",
    "COUNTRY-PHYSICAL",
    "KIT MFR",
    "KIT MODEL",
)


def _clean(value: str | None) -> str | None:
    """Strip whitespace; return None for empty strings so SQLite stores NULL."""
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped else None


def _pick(row: dict[str, str], *keys: str) -> str | None:
    """Return _clean(row[first_non_empty_key]) or None."""
    for key in keys:
        v = _clean(row.get(key))
        if v is not None:
            return v
    return None


def parse_master_row(row: dict[str, str]) -> tuple:
    """Map a MASTER.txt csv-DictReader row to the positional tuple
    consumed by db.insert_faa_registry.

    The file's optional ``MODE S CODE HEX`` column is used when present
    and non-blank; otherwise we derive the hex by converting the octal
    ``MODE S CODE`` field (raises ValueError if both are empty / bad).
    """
    values: list[str | None] = []
    for header, _snake in _MASTER_HEADERS:
        values.append(_clean(row.get(header)))
    hex_from_file = _clean(row.get("MODE S CODE HEX"))
    if hex_from_file:
        values.append(hex_from_file.lower())
    else:
        mode_s = row.get("MODE S CODE") or ""
        values.append(octal_mode_s_to_icao_hex(mode_s))  # raises on empty/bad
    return tuple(values)


def parse_dereg_row(row: dict[str, str]) -> tuple:
    """Map a DEREG.txt csv-DictReader row to the positional tuple
    consumed by db.insert_faa_deregistered.

    DEREG has a different schema from MASTER, so we project it:
      * Address fields prefer STREET-PHYSICAL / CITY-PHYSICAL / ...;
        fall back to the MAIL equivalents when the physical slot is empty.
      * TYPE REGISTRANT / TYPE AIRCRAFT / TYPE ENGINE / FRACT OWNER /
        UNIQUE ID have no equivalent in DEREG and become NULL.
      * EXPIRATION DATE is filled from CANCEL-DATE (the closest match).
      * MODE S CODE HEX is derived from MODE-S-CODE (octal).
    """
    values: list[str | None] = [
        _clean(row.get("N-NUMBER")),  # n_number
        _clean(row.get("SERIAL-NUMBER")),  # serial_number
        _clean(row.get("MFR-MDL-CODE")),  # mfr_mdl_code
        _clean(row.get("ENG-MFR-MDL")),  # eng_mfr_mdl
        _clean(row.get("YEAR-MFR")),  # year_mfr
        None,  # type_registrant (not in DEREG)
        _clean(row.get("NAME")),  # name
        _pick(row, "STREET-PHYSICAL", "STREET-MAIL"),  # street
        _pick(row, "STREET2-PHYSICAL", "STREET2-MAIL"),  # street2
        _pick(row, "CITY-PHYSICAL", "CITY-MAIL"),  # city
        _pick(row, "STATE-ABBREV-PHYSICAL", "STATE-ABBREV-MAIL"),  # state
        _pick(row, "ZIP-CODE-PHYSICAL", "ZIP-CODE-MAIL"),  # zip_code
        _clean(row.get("REGION")),  # region
        _pick(row, "COUNTY-PHYSICAL", "COUNTY-MAIL"),  # county
        _pick(row, "COUNTRY-PHYSICAL", "COUNTRY-MAIL"),  # country
        _clean(row.get("LAST-ACT-DATE")),  # last_action_date
        _clean(row.get("CERT-ISSUE-DATE")),  # cert_issue_date
        _clean(row.get("CERTIFICATION")),  # certification
        None,  # type_aircraft
        None,  # type_engine
        _clean(row.get("STATUS-CODE")),  # status_code
        _clean(row.get("MODE-S-CODE")),  # mode_s_code
        None,  # fract_owner
        _clean(row.get("AIR-WORTH-DATE")),  # air_worth_date
        _clean(row.get("CANCEL-DATE")),  # expiration_date
        None,  # unique_id
        _clean(row.get("KIT MFR")),  # kit_mfr
        _clean(row.get("KIT MODEL")),  # kit_model
    ]
    mode_s = row.get("MODE-S-CODE") or ""
    values.append(octal_mode_s_to_icao_hex(mode_s))  # raises on empty/bad
    return tuple(values)


# ACFTREF.txt headers we keep (code is the join key back to MFR MDL CODE
# on MASTER / DEREG rows).
_ACFTREF_HEADERS: tuple[tuple[str, str], ...] = (
    ("CODE", "code"),
    ("MFR", "mfr"),
    ("MODEL", "model"),
    ("TYPE-ACFT", "type_acft"),
    ("TYPE-ENG", "type_eng"),
)
ACFTREF_COLUMNS: tuple[str, ...] = tuple(snake for _, snake in _ACFTREF_HEADERS)


def parse_acftref_row(row: dict[str, str]) -> tuple:
    """Map ACFTREF row to positional tuple. Raises KeyError when the
    required CODE column is missing (prevents the silent full-NULL
    insert bug where a wrong delimiter would still 'succeed')."""
    code = _clean(row.get("CODE"))
    if code is None:
        raise KeyError("CODE column missing or empty")
    return (
        code,
        _clean(row.get("MFR")),
        _clean(row.get("MODEL")),
        _clean(row.get("TYPE-ACFT")),
        _clean(row.get("TYPE-ENG")),
    )


# -----------------------------------------------------------------------------
# File iteration
# -----------------------------------------------------------------------------


def _iter_faa_rows(path: Path):
    """Yield csv.DictReader rows from a comma-delimited FAA file.

    Strips a UTF-8 BOM if present, reads the rest as latin-1, and trims
    whitespace from every fieldname so leading-space quirks like
    `" KIT MODEL"` resolve cleanly against `_MASTER_HEADERS`.
    """
    with path.open("r", encoding=_FAA_ENCODING, newline="") as fh:
        start = fh.read(3)
        if start != _BOM_AS_LATIN1:
            fh.seek(0)
        # Read first line ourselves so we can normalize field names.
        header_line = fh.readline()
        if not header_line:
            return
        fieldnames = [name.strip() for name in next(csv.reader([header_line], delimiter=_FAA_DELIMITER))]
        reader = csv.DictReader(fh, fieldnames=fieldnames, delimiter=_FAA_DELIMITER)
        yield from reader


def _require_headers(path: Path, required: tuple[str, ...]) -> None:
    """Read just the header line and raise if any required column is missing.

    Catches schema drift (FAA renamed columns) and wrong-delimiter bugs
    before they degrade into a silent all-NULL import.
    """
    with path.open("r", encoding=_FAA_ENCODING, newline="") as fh:
        start = fh.read(3)
        if start != _BOM_AS_LATIN1:
            fh.seek(0)
        header_line = fh.readline()
    fieldnames = {name.strip() for name in next(csv.reader([header_line], delimiter=_FAA_DELIMITER))}
    missing = [h for h in required if h not in fieldnames]
    if missing:
        raise ValueError(
            f"{path.name}: missing expected columns {missing}. Got fieldnames: {sorted(fieldnames)[:10]}..."
        )


# -----------------------------------------------------------------------------
# Bulk import
# -----------------------------------------------------------------------------


def _import_rows(
    db: Database,
    path: Path,
    parse_fn: Callable[[dict], tuple],
    insert_fn: Callable[[list[tuple]], None],
) -> int:
    """Shared body: read every row, skip malformed, bulk-insert in one
    transaction. Returns rows successfully inserted."""
    parsed: list[tuple] = []
    skipped = 0
    for row in _iter_faa_rows(path):
        try:
            parsed.append(parse_fn(row))
        except (ValueError, KeyError):
            skipped += 1
    with db.conn:
        insert_fn(parsed)
    if skipped:
        print(f"  skipped {skipped} malformed rows from {path.name}")
    return len(parsed)


def import_master_from_path(db: Database, path: Path) -> int:
    _require_headers(path, required=("N-NUMBER", "MODE S CODE", "NAME"))
    return _import_rows(db, path, parse_master_row, db.insert_faa_registry)


def import_dereg_from_path(db: Database, path: Path) -> int:
    _require_headers(path, required=("N-NUMBER", "MODE-S-CODE", "NAME"))
    return _import_rows(db, path, parse_dereg_row, db.insert_faa_deregistered)


def import_acftref_from_path(db: Database, path: Path) -> int:
    _require_headers(path, required=("CODE", "MFR", "MODEL"))
    return _import_rows(db, path, parse_acftref_row, db.insert_faa_aircraft_ref)


# -----------------------------------------------------------------------------
# Download + refresh orchestration
# -----------------------------------------------------------------------------

_TARGET_FILES = ("MASTER.txt", "DEREG.txt", "ACFTREF.txt")


def download_faa_zip(cfg: Config, destination: Path | None = None) -> Path:
    """Download the FAA ReleasableAircraft.zip.

    Prefers ``curl_cffi`` with a Chrome TLS fingerprint when available,
    which bypasses the Akamai Bot Manager on registry.faa.gov. Falls
    back to ``httpx`` when curl_cffi is not installed (download will
    probably 503 in that case; instruct the user to install curl_cffi
    or download the zip manually and pass --zip).
    """
    dest = destination or cfg.faa_registry_cache_path
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        _download_with_curl_cffi(cfg.faa_registry_url, dest)
    except ImportError:
        _download_with_httpx(cfg.faa_registry_url, dest)
    return dest


def _download_with_curl_cffi(url: str, dest: Path) -> None:
    """Download via curl_cffi impersonating Chrome. Raises ImportError
    when curl_cffi is not installed so the caller can fall back.

    Note: curl_cffi's Response is not a context manager, so we close
    explicitly in a finally block."""
    # Lazy import so curl_cffi stays an optional dependency.
    from curl_cffi import requests as cffi_requests

    with Progress() as progress:
        task = progress.add_task(
            "Downloading FAA ReleasableAircraft.zip (curl_cffi, impersonate=chrome)...",
            total=None,
        )
        resp = cffi_requests.get(url, impersonate="chrome", stream=True, timeout=300)
        try:
            resp.raise_for_status()
            with dest.open("wb") as fh:
                for chunk in resp.iter_content(chunk_size=1 << 16):
                    if chunk:
                        fh.write(chunk)
        finally:
            resp.close()
        progress.update(task, completed=100)


def _download_with_httpx(url: str, dest: Path) -> None:
    """Fallback path when curl_cffi isn't installed. registry.faa.gov
    usually 503s this in practice because of Akamai Bot Manager."""
    import httpx

    with Progress() as progress:
        task = progress.add_task("Downloading FAA ReleasableAircraft.zip (httpx fallback)...", total=None)
        with httpx.stream("GET", url, follow_redirects=True, timeout=300) as resp:
            resp.raise_for_status()
            with dest.open("wb") as fh:
                for chunk in resp.iter_bytes(chunk_size=1 << 16):
                    fh.write(chunk)
        progress.update(task, completed=100)


def refresh_faa_registry(
    db: Database,
    cfg: Config,
    *,
    local_zip: Path | None = None,
) -> dict[str, int]:
    """Download (or use local_zip) the FAA bundle, extract the three files,
    truncate the local tables, and bulk-import the fresh data.

    Returns a stats dict {master, dereg, acftref} with the row counts inserted.
    """
    zip_path = local_zip or download_faa_zip(cfg)

    stats = {"master": 0, "dereg": 0, "acftref": 0}
    with TemporaryDirectory() as tmpdir:
        tmp_root = Path(tmpdir)
        with zipfile.ZipFile(zip_path) as zf:
            # Index by basename so future FAA zips that nest the files
            # under a folder (e.g. "ReleasableAircraft/MASTER.txt") still
            # resolve. Directory entries have an empty basename and are
            # harmlessly filtered out by the _TARGET_FILES lookup.
            members_by_base: dict[str, str] = {}
            for member in zf.namelist():
                base = member.rsplit("/", 1)[-1].upper()
                if base:
                    members_by_base[base] = member
            for target in _TARGET_FILES:
                if target.upper() not in members_by_base:
                    raise FileNotFoundError(f"{target} missing from {zip_path}")
                zf.extract(members_by_base[target.upper()], tmp_root)

        # Clear out prior data so re-runs don't accumulate stale rows.
        db.truncate_faa_tables()
        db.commit()

        stats["master"] = import_master_from_path(db, _resolve_case(tmp_root, "MASTER.txt"))
        stats["dereg"] = import_dereg_from_path(db, _resolve_case(tmp_root, "DEREG.txt"))
        stats["acftref"] = import_acftref_from_path(db, _resolve_case(tmp_root, "ACFTREF.txt"))
    return stats


def _resolve_case(root: Path, name: str) -> Path:
    """Find the first file matching ``name`` (case-insensitive) anywhere
    under ``root``. Prefers the shallowest match so ambiguous layouts
    resolve deterministically.

    Raises FileNotFoundError when no match exists.
    """
    matches = sorted(
        (p for p in root.rglob("*") if p.is_file() and p.name.upper() == name.upper()),
        key=lambda p: len(p.parts),
    )
    if not matches:
        raise FileNotFoundError(f"{name} missing from {root}")
    return matches[0]


# Re-export for backwards compat with tests that import the old name
_iter_pipe_rows = _iter_faa_rows  # noqa: F841  retained for test compatibility
