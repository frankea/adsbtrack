"""Military ICAO hex allocation ranges and lookup helpers.

The ICAO 24-bit aircraft address scheme assigns blocks to member states;
many states reserve a sub-block for military use. The official mapping
is not published publicly so this module ships a curated starter set
based on community-compiled sources (Mictronics DB, Wikipedia,
virtualradarserver.co.uk, and active ADS-B watchers).

Coverage is best-effort and deliberately conservative: only ranges that
are widely documented as military are included. Users can extend the
`mil_hex_ranges` table with their own entries if they have better data.

Each entry is a tuple of (range_start_hex, range_end_hex, country,
branch, notes) with hex values lowercase and zero-padded to 6 chars.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .db import Database


# Community-curated list. Sources in the notes column. Rough guiderails:
# include only ranges that are both (a) widely cited as military, and
# (b) unlikely to be reassigned to a civilian block. Dual-use civil /
# government prefixes (e.g. head-of-state aircraft in the regular civil
# pool) are intentionally excluded.
_SEED_RANGES: list[tuple[str, str, str, str, str]] = [
    ("adf7c8", "afffff", "United States", "Military (DoD)", "US DoD pool; widely documented"),
    ("ae0000", "afffff", "United States", "Military (DoD)", "legacy USDoD range; overlaps adf7c8 upper half"),
    ("43c000", "43cfff", "United Kingdom", "Military (RAF)", "Royal Air Force"),
    ("43ea00", "43eb7f", "United Kingdom", "Military (MoD)", "UK MoD supplementary"),
    ("3a8000", "3affff", "France", "Military", "Armee de l'Air + French Navy"),
    ("3f8000", "3fffff", "Germany", "Military (Luftwaffe)", "Bundeswehr"),
    ("33f000", "33ffff", "Italy", "Military", "Aeronautica Militare / Marina Militare"),
    ("868100", "86ffff", "Japan", "Military (JASDF)", "Japan Self-Defense Forces"),
    ("7c9400", "7c99ff", "Australia", "Military (RAAF)", "Royal Australian Air Force"),
    ("c01000", "c03fff", "Canada", "Military (RCAF)", "Royal Canadian Air Force"),
    ("15c000", "15ffff", "Russia", "Military (VKS)", "Russian Aerospace Forces"),
    ("3f7000", "3f7fff", "Russia", "Military (alt)", "Russian MoD supplementary"),
    ("710000", "717fff", "Saudi Arabia", "Military (RSAF)", "Royal Saudi Air Force (partial)"),
    ("50c100", "50cfff", "Poland", "Military", "Polskie Sily Powietrzne"),
    ("4a8000", "4a8fff", "Sweden", "Military (Flygvapnet)", "Swedish Air Force"),
    ("478100", "4783ff", "Denmark", "Military", "Royal Danish Air Force"),
    ("7c0000", "7c00ff", "Australia", "Military (historical)", "older RAAF assignments"),
    ("71c0d0", "71c0ef", "Korea, South", "Military (ROKAF)", "reported ROKAF assignment"),
    ("708000", "708fff", "Yemen", "Military", "Yemeni Air Force (legacy)"),
    ("76c000", "76cfff", "Singapore", "Military (RSAF)", "Republic of Singapore Air Force"),
    ("c06000", "c07fff", "Canada", "Military (CF historical)", "historical CF block"),
    ("3c8000", "3cbfff", "Germany", "Military (secondary)", "Bundeswehr additional"),
    ("4b7000", "4b7fff", "Switzerland", "Military", "Swiss Air Force"),
    ("451000", "451fff", "Iceland", "Government", "Icelandic Coast Guard / gov"),
    ("73c000", "73ffff", "India", "Military (IAF)", "Indian Air Force (partial)"),
]


def seed_mil_hex_ranges(db: Database) -> int:
    """Write the curated military ranges into the mil_hex_ranges table.

    Idempotent -- uses INSERT OR REPLACE keyed on (range_start, range_end).
    Returns the number of rows written (always equal to len(_SEED_RANGES)).
    """
    for start, end, country, branch, notes in _SEED_RANGES:
        db.insert_mil_hex_range(
            {
                "range_start": start,
                "range_end": end,
                "country": country,
                "branch": branch,
                "notes": notes,
            }
        )
    db.commit()
    return len(_SEED_RANGES)


def is_military_hex(db: Database, hex_code: str) -> tuple[bool, str | None, str | None]:
    """Check the mil_hex_ranges table for a matching range.

    Returns (is_military, country, branch). When the hex does not fall
    into any seeded range, returns (False, None, None).
    """
    row = db.lookup_mil_hex_range(hex_code)
    if row is None:
        return False, None, None
    return True, row["country"], row["branch"]
