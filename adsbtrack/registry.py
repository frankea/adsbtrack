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
