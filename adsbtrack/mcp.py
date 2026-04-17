"""Read-only MCP server exposing adsbtrack queries to LLM assistants.

The server wraps a set of parameterless query functions (`_query_*`)
and a small FastMCP server that delegates to them. Tool responses are
always bounded (default 50 rows, cap 200) with `total_matching` and
`truncated` fields so the LLM knows when it is seeing a partial result.

No tool mutates the database. No tool triggers a fetch. This is a
pure analytical-surface exposure, suitable for being invoked from
Claude Desktop / Claude Code without additional permission scoping.

Wire up via the `adsbtrack mcp-serve --db <path>` CLI subcommand,
which sets the module-level `_DB_PATH` and calls `serve()`.
"""

from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .db import Database
from .events import collect_events
from .gaps import detect_gaps

_DB_PATH: Path = Path("adsbtrack.db")  # overridden by serve()

_MAX_LIMIT = 200

_FLIGHT_SUMMARY_COLS = (
    "icao",
    "takeoff_time",
    "landing_time",
    "takeoff_date",
    "origin_icao",
    "destination_icao",
    "duration_minutes",
    "max_altitude",
    "cruise_gs_kt",
    "mission_type",
    "callsign",
    "landing_type",
    "go_around_count",
    "max_hover_secs",
    "emergency_squawk",
    "emergency_flag",
)


# ---------------------------------------------------------------------------
# Pure query functions -- tested directly, no MCP dep required
# ---------------------------------------------------------------------------


def _query_aircraft_stats(db: Database, hex_code: str) -> dict[str, Any]:
    """Return the aircraft_stats row for a hex, or a not_found sentinel."""
    hex_code = hex_code.lower()
    row = db.conn.execute("SELECT * FROM aircraft_stats WHERE icao = ?", (hex_code,)).fetchone()
    if row is None:
        return {
            "hex": hex_code,
            "error": "not_found",
            "message": (
                f"No aircraft_stats row for hex {hex_code!r}. Fetch traces for this hex "
                f"or run `adsbtrack extract --hex {hex_code} --reprocess` to populate."
            ),
        }
    return {"hex": hex_code, "stats": dict(row)}


def _query_flights(
    db: Database,
    hex_code: str,
    *,
    from_date: str | None = None,
    to_date: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    """Return up to `limit` flight summaries for a hex, newest first.

    `from_date` / `to_date` are YYYY-MM-DD strings matched against
    `takeoff_date`. `limit` is clamped to [1, 200].
    """
    hex_code = hex_code.lower()
    limit = max(1, min(limit, _MAX_LIMIT))

    where = ["icao = ?"]
    params: list[Any] = [hex_code]
    if from_date:
        where.append("takeoff_date >= ?")
        params.append(from_date)
    if to_date:
        where.append("takeoff_date <= ?")
        params.append(to_date)
    where_clause = " AND ".join(where)

    total = db.conn.execute(f"SELECT COUNT(*) FROM flights WHERE {where_clause}", params).fetchone()[0]
    cols = ", ".join(_FLIGHT_SUMMARY_COLS)
    rows = db.conn.execute(
        f"SELECT {cols} FROM flights WHERE {where_clause} ORDER BY takeoff_time DESC LIMIT ?",
        [*params, limit],
    ).fetchall()

    flights = [dict(r) for r in rows]
    return {
        "hex": hex_code,
        "total_matching": total,
        "returned_count": len(flights),
        "truncated": total > len(flights),
        "flights": flights,
    }


def _query_events(
    db: Database,
    hex_code: str,
    *,
    since_date: str | None = None,
    severity: str = "all",
    limit: int = 50,
) -> dict[str, Any]:
    """Return noteworthy events for a hex via collect_events()."""
    hex_code = hex_code.lower()
    limit = max(1, min(limit, _MAX_LIMIT))

    since_dt: datetime | None = None
    if since_date:
        try:
            since_dt = datetime.fromisoformat(since_date)
        except ValueError:
            return {
                "hex": hex_code,
                "error": "bad_input",
                "message": f"since_date {since_date!r} is not ISO format (YYYY-MM-DD expected)",
            }

    events = collect_events(db, hex_code, since=since_dt, severity=severity)
    total = len(events)
    truncated_events = events[:limit]
    return {
        "hex": hex_code,
        "total_matching": total,
        "returned_count": len(truncated_events),
        "truncated": total > len(truncated_events),
        "events": [_event_to_dict(e) for e in truncated_events],
    }


def _query_gaps(
    db: Database,
    hex_code: str,
    *,
    min_gap_secs: float = 300.0,
    limit: int = 50,
) -> dict[str, Any]:
    """Return within-flight ADS-B gaps for a hex via detect_gaps()."""
    hex_code = hex_code.lower()
    limit = max(1, min(limit, _MAX_LIMIT))

    gaps = detect_gaps(db, hex_code, min_gap_secs=min_gap_secs)
    total = len(gaps)
    truncated_gaps = gaps[:limit]
    return {
        "hex": hex_code,
        "min_gap_secs": min_gap_secs,
        "total_matching": total,
        "returned_count": len(truncated_gaps),
        "truncated": total > len(truncated_gaps),
        "gaps": [_gap_to_dict(g) for g in truncated_gaps],
    }


def _registry_lookup(db: Database, reg_or_hex: str) -> dict[str, Any]:
    """Resolve a hex or registration string and return merged registry info.

    Input is auto-classified: 6-character hexadecimal = ICAO hex, anything
    else = registration. For a hex, looks up aircraft_registry and
    hex_crossref. For a registration, resolves to a hex via the same
    chain, then does the same lookup.
    """
    q = reg_or_hex.strip()
    if len(q) == 6 and all(c in "0123456789abcdefABCDEF" for c in q):
        return _registry_lookup_by_hex(db, q.lower())

    # Registration path: resolve to hex then look up
    hex_row = db.conn.execute(
        "SELECT icao FROM aircraft_registry WHERE registration = ? COLLATE NOCASE",
        (q,),
    ).fetchone()
    if hex_row is None:
        hex_row = db.conn.execute(
            "SELECT icao FROM hex_crossref WHERE registration = ? COLLATE NOCASE",
            (q,),
        ).fetchone()
    if hex_row is None:
        return {
            "query": reg_or_hex,
            "error": "not_found",
            "message": (
                f"No aircraft matching registration {reg_or_hex!r} in aircraft_registry or "
                f"hex_crossref. Try running `adsbtrack registry update` to load FAA data, "
                f"or fetch by --hex first."
            ),
        }
    return _registry_lookup_by_hex(db, hex_row["icao"])


def _registry_lookup_by_hex(db: Database, hex_code: str) -> dict[str, Any]:
    """Pull aircraft_registry + hex_crossref rows for a hex into one dict."""
    result: dict[str, Any] = {"hex": hex_code}
    reg_row = db.conn.execute("SELECT * FROM aircraft_registry WHERE icao = ?", (hex_code,)).fetchone()
    if reg_row:
        result["aircraft_registry"] = dict(reg_row)
    xref_row = db.conn.execute("SELECT * FROM hex_crossref WHERE icao = ?", (hex_code,)).fetchone()
    if xref_row:
        result["hex_crossref"] = dict(xref_row)
    if "aircraft_registry" not in result and "hex_crossref" not in result:
        result["error"] = "not_found"
        result["message"] = (
            f"No registry or crossref entry for hex {hex_code!r}. "
            f"Run `adsbtrack enrich hex --hex {hex_code}` or fetch traces first."
        )
    return result


# ---------------------------------------------------------------------------
# Serialization helpers: make dataclasses JSON-safe for MCP transport.
# ---------------------------------------------------------------------------


def _event_to_dict(event: Any) -> dict[str, Any]:
    d = asdict(event) if is_dataclass(event) else dict(event)
    # datetime and enum members need string coercion for JSON transport.
    if "ts" in d and hasattr(d["ts"], "isoformat"):
        d["ts"] = d["ts"].isoformat()
    return d


def _gap_to_dict(gap: Any) -> dict[str, Any]:
    d = asdict(gap) if is_dataclass(gap) else dict(gap)
    for key in ("gap_start_ts", "gap_end_ts"):
        if key in d and isinstance(d[key], float):
            # Keep as float unix timestamp; also add ISO rendering for LLM
            # readability without requiring a second tool call.
            d[f"{key}_iso"] = datetime.fromtimestamp(d[key]).isoformat() + "Z"
    return d


# ---------------------------------------------------------------------------
# MCP server bindings -- lazy imports so the dep is optional.
# ---------------------------------------------------------------------------


def serve(db_path: Path) -> None:
    """Start the MCP server over stdio. Invoked by `adsbtrack mcp-serve`.

    Requires the `mcp` optional extra: `uv sync --extra mcp`.
    """
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError as e:  # pragma: no cover - behavior is install-state-dependent
        raise RuntimeError("MCP server requires the 'mcp' optional extra. Install via `uv sync --extra mcp`.") from e

    global _DB_PATH
    _DB_PATH = db_path

    mcp = FastMCP("adsbtrack")

    @mcp.tool()
    def query_aircraft_stats(hex_code: str) -> str:
        """Return lifetime stats (total flights, hours, home base, etc.) for an aircraft by ICAO hex."""
        with Database(_DB_PATH) as db:
            return json.dumps(_query_aircraft_stats(db, hex_code), default=str)

    @mcp.tool()
    def query_flights(
        hex_code: str,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int = 50,
    ) -> str:
        """Return up to 50 flight summaries for an aircraft, newest first, optionally filtered by takeoff_date range."""
        with Database(_DB_PATH) as db:
            return json.dumps(
                _query_flights(db, hex_code, from_date=from_date, to_date=to_date, limit=limit),
                default=str,
            )

    @mcp.tool()
    def query_events(
        hex_code: str,
        since_date: str | None = None,
        severity: str = "all",
        limit: int = 50,
    ) -> str:
        """Return noteworthy events (emergency squawks, long hovers, off-airport landings, etc.) for an aircraft."""
        with Database(_DB_PATH) as db:
            return json.dumps(
                _query_events(db, hex_code, since_date=since_date, severity=severity, limit=limit),
                default=str,
            )

    @mcp.tool()
    def query_gaps(hex_code: str, min_gap_secs: float = 300.0, limit: int = 50) -> str:
        """Return within-flight ADS-B signal gaps with classification.

        Each gap is labeled coverage_hole, likely_transponder_off, or unknown.
        """
        with Database(_DB_PATH) as db:
            return json.dumps(
                _query_gaps(db, hex_code, min_gap_secs=min_gap_secs, limit=limit),
                default=str,
            )

    @mcp.tool()
    def registry_lookup(reg_or_hex: str) -> str:
        """Look up aircraft registry info by either ICAO hex (6 hex chars) or tail/registration."""
        with Database(_DB_PATH) as db:
            return json.dumps(_registry_lookup(db, reg_or_hex), default=str)

    mcp.run()
