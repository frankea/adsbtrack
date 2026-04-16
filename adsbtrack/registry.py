"""FAA aircraft registry downloader, importer, and lookup helpers.

The FAA publishes a zipped bundle of pipe-delimited bulk data at
https://registry.faa.gov/database/ReleasableAircraft.zip. This module
downloads that bundle, extracts the three files we care about
(MASTER.txt, DEREG.txt, ACFTREF.txt) and imports them into the local
SQLite store so the rest of adsbtrack can resolve ICAO hex codes to
registrant identity, address, and deregistration history.
"""

from __future__ import annotations


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


# MASTER.txt / DEREG.txt column order mirrors db.insert_faa_registry.
# Keep these lists in the same order as the table columns so positional
# tuples slot directly into executemany().
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
    # mode_s_code_hex is derived from mode_s_code, not read from the file.
)

MASTER_COLUMNS: tuple[str, ...] = tuple(snake for _, snake in _MASTER_HEADERS) + ("mode_s_code_hex",)


def _clean(value: str | None) -> str | None:
    """Strip whitespace; return None for empty strings so SQLite stores NULL."""
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped else None


def parse_master_row(row: dict[str, str]) -> tuple:
    """Map a MASTER.txt / DEREG.txt csv-DictReader row to the positional
    tuple consumed by db.insert_faa_registry / db.insert_faa_deregistered.

    The FAA file uses dashes / spaces in the header names, so we look up
    each column by its original header and coerce blanks to NULL. The
    final field (mode_s_code_hex) is derived from MODE S CODE (octal).
    """
    values: list[str | None] = []
    for header, _snake in _MASTER_HEADERS:
        values.append(_clean(row.get(header)))
    mode_s = row.get("MODE S CODE") or ""
    mode_s_hex = octal_mode_s_to_icao_hex(mode_s)  # raises on empty/bad
    values.append(mode_s_hex)
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
    return tuple(_clean(row.get(header)) for header, _snake in _ACFTREF_HEADERS)
