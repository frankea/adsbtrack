"""Per-tail deliverable bundle behind the `export` CLI command (issue #25).

Read-only over the working database: assembles the package previously built
by hand for third parties (journalists, researchers) - a hex-scoped SQLite
extract, flight CSVs, fragment-level trace CSVs per named window, a README
describing every file, and an optional analysis.md identity stub.

Everything here is pure "query, decode, write": no table is written in the
source database, and compressed trace_days rows are read back through the
same decode path (db.decode_trace_json / iter_parsed_trace_days) the parser
and forensics use, so legacy TEXT rows and zlib BLOB rows both export.
"""

from __future__ import annotations

import csv
import json
import re
import sqlite3
import zipfile
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

from .db import Database, decode_trace_json, iter_parsed_trace_days

# Split "START:END" at the colon that is followed by a full date. Times
# contain colons of their own ("2026-04-21T14:00:2026-04-22"), so the
# separator is found by shape, not position: END always starts with
# YYYY-MM-DD and runs to the end of the spec.
_WINDOW_SPLIT_RE = re.compile(r"^(?P<start>.+?):(?P<end>\d{4}-\d{2}-\d{2}(?:T[\d:.]+)?)$")


@dataclass(frozen=True)
class ExportWindow:
    """One --window request, parsed. ``start``/``end`` are tz-aware UTC
    instants (inclusive on both ends); ``label`` is the filename-safe form
    used in flights_<label>.csv / trace_<label>.csv."""

    spec: str
    label: str
    start: datetime
    end: datetime


@dataclass
class ExportResult:
    """What export_bundle wrote, for the CLI summary and tests."""

    out_dir: Path
    files: list[Path] = field(default_factory=list)
    flight_count: int = 0
    trace_day_count: int = 0
    fetch_log_count: int = 0
    window_flight_counts: dict[str, int] = field(default_factory=dict)
    window_point_counts: dict[str, int] = field(default_factory=dict)
    zip_path: Path | None = None


def _parse_endpoint(token: str, *, is_end: bool) -> tuple[datetime, str]:
    """Parse one side of a window spec into (instant, label_part).

    A bare date means the whole UTC day: midnight for the start side,
    end-of-day (23:59:59.999999) for the end side, so "D:D" covers exactly
    that one day. A datetime is taken literally; a naive one is assumed UTC.
    """
    if "T" in token:
        parsed = datetime.fromisoformat(token)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed, parsed.strftime("%Y-%m-%dT%H%M%S")
    day = date.fromisoformat(token)
    boundary = time.max if is_end else time.min
    return datetime.combine(day, boundary, tzinfo=UTC), day.isoformat()


def parse_window(spec: str) -> ExportWindow:
    """Parse a --window START:END spec. START/END are dates (YYYY-MM-DD) or
    datetimes (YYYY-MM-DDTHH:MM[:SS]); raises ValueError with a usable
    message on malformed input or an inverted range."""
    match = _WINDOW_SPLIT_RE.match(spec.strip())
    if not match:
        raise ValueError(
            f"Window {spec!r} is not START:END with dates (YYYY-MM-DD) or datetimes (YYYY-MM-DDTHH:MM[:SS])."
        )
    try:
        start, start_label = _parse_endpoint(match.group("start"), is_end=False)
        end, end_label = _parse_endpoint(match.group("end"), is_end=True)
    except ValueError as exc:
        raise ValueError(f"Window {spec!r} has an unparseable endpoint: {exc}") from exc
    if start > end:
        raise ValueError(f"Window {spec!r} starts after it ends.")
    return ExportWindow(spec=spec, label=f"{start_label}_{end_label}", start=start, end=end)


def parse_windows(specs: tuple[str, ...] | list[str]) -> list[ExportWindow]:
    """Parse every spec, rejecting duplicates (two windows that would write
    the same flights_<label>.csv / trace_<label>.csv files)."""
    windows: list[ExportWindow] = []
    seen: set[str] = set()
    for spec in specs:
        window = parse_window(spec)
        if window.label in seen:
            raise ValueError(f"Window {spec!r} duplicates an earlier window ({window.label}).")
        seen.add(window.label)
        windows.append(window)
    return windows


# ---------------------------------------------------------------------------
# SQLite extract
# ---------------------------------------------------------------------------

_EXTRACT_TABLES = ("flights", "trace_days", "fetch_log")


def _write_sqlite_extract(db: Database, hex_code: str, path: Path) -> dict[str, int]:
    """Write the hex-scoped SQLite extract: flights, trace_days, fetch_log.

    Table DDL is copied from the source database's sqlite_master so the
    extract's schema tracks the working schema without a second copy to
    maintain. trace_days.trace_json is stored as plain JSON text (decoded
    via decode_trace_json), so recipients can read traces with any SQLite
    browser - no zlib step. A row whose trace_json fails to decode is
    copied byte-for-byte instead of being dropped: a deliverable must not
    silently lose rows, even damaged ones.
    """
    path.unlink(missing_ok=True)
    counts: dict[str, int] = {}
    extract = sqlite3.connect(path)
    try:
        for table in _EXTRACT_TABLES:
            ddl_row = db.conn.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
            ).fetchone()
            extract.execute(ddl_row["sql"])

            order = {"flights": "takeoff_time", "trace_days": "date, source", "fetch_log": "date, source"}[table]
            rows = db.conn.execute(f"SELECT * FROM {table} WHERE icao = ? ORDER BY {order}", (hex_code,)).fetchall()
            counts[table] = len(rows)
            if not rows:
                continue

            columns = rows[0].keys()
            insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})"
            for row in rows:
                values = list(row)
                if table == "trace_days":
                    trace_idx = list(columns).index("trace_json")
                    decoded = decode_trace_json(row["trace_json"])
                    if decoded is not None:
                        values[trace_idx] = json.dumps(decoded, separators=(",", ":"))
                extract.execute(insert_sql, values)
        extract.commit()
    finally:
        extract.close()
    return counts


# ---------------------------------------------------------------------------
# Flight CSVs
# ---------------------------------------------------------------------------


def _flight_instant(value: str | None) -> datetime | None:
    """Parse a flights-table ISO timestamp; naive values are assumed UTC
    (every writer in this codebase stores tz-aware isoformat, but a
    deliverable should not crash on a hand-edited row)."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _flight_in_window(row: sqlite3.Row, window: ExportWindow) -> bool:
    """Overlap semantics: a flight belongs to a window when any part of it
    falls inside [start, end] - takeoff before the end AND (landing, or
    takeoff when the landing is unknown) after the start."""
    takeoff = _flight_instant(row["takeoff_time"])
    if takeoff is None:
        return False
    landing = _flight_instant(row["landing_time"]) or takeoff
    return takeoff <= window.end and landing >= window.start


def _write_flights_csv(db: Database, hex_code: str, path: Path, window: ExportWindow | None = None) -> int:
    """Write one CSV of flights rows (all columns, ordered by takeoff_time),
    optionally restricted to a window. Returns the row count."""
    cursor = db.conn.execute("SELECT * FROM flights WHERE icao = ? ORDER BY takeoff_time", (hex_code,))
    header = [description[0] for description in cursor.description]
    count = 0
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        for row in cursor:
            if window is not None and not _flight_in_window(row, window):
                continue
            writer.writerow(list(row))
            count += 1
    return count


# ---------------------------------------------------------------------------
# Fragment-level trace CSVs
# ---------------------------------------------------------------------------

TRACE_CSV_HEADER = (
    "time_utc",
    "epoch_s",
    "source",
    "lat",
    "lon",
    "alt_ft",
    "on_ground",
    "gs_kt",
    "callsign",
    "squawk",
)


def _point_detail(point: list) -> dict | None:
    """The point's readsb detail dict (index 8), mirroring
    forensics._point_detail."""
    if len(point) > 8 and isinstance(point[8], dict):
        return point[8]
    return None


def _trace_csv_row(abs_ts: float, source: str, point: list) -> list:
    alt = point[3] if len(point) > 3 else None
    on_ground = 1 if alt == "ground" else 0
    alt_ft = alt if isinstance(alt, int | float) else None
    gs = point[4] if len(point) > 4 else None
    gs_kt = gs if isinstance(gs, int | float) else None
    detail = _point_detail(point)
    callsign = (detail.get("flight") or "").strip() if detail else ""
    squawk = str(detail.get("squawk") or "") if detail else ""
    return [
        datetime.fromtimestamp(abs_ts, UTC).isoformat(),
        abs_ts,
        source,
        point[1],
        point[2],
        alt_ft,
        on_ground,
        gs_kt,
        callsign,
        squawk,
    ]


def _iter_window_points(db: Database, hex_code: str, window: ExportWindow) -> list[tuple[float, str, list]]:
    """Every trace point inside the window from every source, merged into
    one chronological stream: (abs_ts, source, point). Native resolution -
    no dedup across sources, no downsampling.

    trace_days rows are keyed by UTC date, so the scan starts one day
    before the window's start date to catch a day whose points spill past
    its own midnight, then filters point-by-point on absolute timestamp.
    """
    date_lo = (window.start.date() - timedelta(days=1)).isoformat()
    date_hi = window.end.date().isoformat()
    rows = db.conn.execute(
        "SELECT * FROM trace_days WHERE icao = ? AND date BETWEEN ? AND ? ORDER BY date, source",
        (hex_code, date_lo, date_hi),
    ).fetchall()

    start_epoch = window.start.timestamp()
    end_epoch = window.end.timestamp()
    points: list[tuple[float, str, list]] = []
    for row, trace in iter_parsed_trace_days(rows, hex_code):
        base_ts = row["timestamp"]
        for point in trace:
            if not isinstance(point, list) or len(point) < 3:
                continue
            abs_ts = base_ts + point[0]
            if start_epoch <= abs_ts <= end_epoch:
                points.append((abs_ts, row["source"], point))
    points.sort(key=lambda item: (item[0], item[1]))
    return points


def _write_trace_csv(db: Database, hex_code: str, path: Path, window: ExportWindow) -> int:
    """Write the fragment-level trace CSV for one window. Returns the point
    count. A window with no coverage still writes the header, so the bundle
    file set stays deterministic."""
    points = _iter_window_points(db, hex_code, window)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(TRACE_CSV_HEADER)
        for abs_ts, source, point in points:
            writer.writerow(_trace_csv_row(abs_ts, source, point))
    return len(points)


# ---------------------------------------------------------------------------
# Identity, README, analysis stub
# ---------------------------------------------------------------------------


def _aircraft_identity(db: Database, hex_code: str) -> dict:
    """Best-effort identity merged from observed broadcast metadata
    (aircraft_registry), the cross-reference table, and the FAA registry.
    Individual keys are None when nothing is on file."""
    registry = db.get_aircraft_registry(hex_code)
    crossref = db.get_hex_crossref(hex_code)
    faa = db.get_faa_registry_by_hex(hex_code)
    faa_ref = db.get_faa_aircraft_ref(faa["mfr_mdl_code"]) if faa and faa["mfr_mdl_code"] else None

    def pick(*values: object) -> str | None:
        for value in values:
            if value:
                return str(value)
        return None

    return {
        "registration": pick(
            registry["registration"] if registry else None,
            crossref["registration"] if crossref else None,
            f"N{faa['n_number']}" if faa and faa["n_number"] else None,
        ),
        "type_code": pick(registry["type_code"] if registry else None, crossref["type_code"] if crossref else None),
        "description": pick(
            registry["description"] if registry else None,
            crossref["type_description"] if crossref else None,
        ),
        "owner_operator": pick(
            registry["owner_operator"] if registry else None,
            crossref["operator"] if crossref else None,
            faa["name"] if faa else None,
        ),
        "year": pick(registry["year"] if registry else None, faa["year_mfr"] if faa else None),
        "registry": registry,
        "crossref": crossref,
        "faa": faa,
        "faa_ref": faa_ref,
    }


def _bundle_title(hex_code: str, identity: dict) -> str:
    registration = identity["registration"]
    return f"{registration} ({hex_code})" if registration else hex_code


def _readme_lines(
    hex_code: str,
    identity: dict,
    result: ExportResult,
    windows: list[ExportWindow],
    sources: list[str],
    date_range: tuple[str | None, str | None],
    extract_name: str,
    include_analysis: bool,
    tool_version: str,
) -> list[str]:
    generated = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    first_date, last_date = date_range
    lines = [
        f"# ADS-B history bundle: {_bundle_title(hex_code, identity)}",
        "",
        f"Generated {generated} by adsbtrack {tool_version} from a local archive of",
        "crowdsourced ADS-B tracking-network history. All timestamps are UTC.",
        "",
        "## Aircraft",
        "",
        f"- ICAO hex: {hex_code}",
        f"- Registration: {identity['registration'] or 'not on file'}",
        f"- Type: {identity['type_code'] or 'not on file'}"
        + (f" ({identity['description']})" if identity["description"] else ""),
        f"- Owner/operator: {identity['owner_operator'] or 'not on file'}",
        "",
        "## Coverage",
        "",
        f"- Extracted flights: {result.flight_count}",
        f"- Trace days: {result.trace_day_count} rows across sources: {', '.join(sources) if sources else 'none'}",
        f"- Fetch log: {result.fetch_log_count} day-requests, {first_date or '?'} to {last_date or '?'}",
        "",
        "## Files",
        "",
        f"- `{extract_name}` - SQLite extract with three tables (`flights`, `trace_days`, `fetch_log`),",
        "  every row scoped to this aircraft's ICAO hex. `trace_days.trace_json` holds each day's raw",
        "  trace as plain JSON text (decompressed from the working database's zlib form), readable in",
        "  any SQLite browser.",
        "- `flights.csv` - every extracted flight, one row per flight, all columns of the `flights` table.",
    ]
    for window in windows:
        lines += [
            f"- `flights_{window.label}.csv` - flights overlapping {window.spec}"
            f" ({result.window_flight_counts.get(window.label, 0)} rows).",
            f"- `trace_{window.label}.csv` - raw trace points for {window.spec} at native resolution"
            f" ({result.window_point_counts.get(window.label, 0)} points).",
        ]
    if include_analysis:
        lines.append("- `analysis.md` - analyst notes stub with the aircraft's registry identity.")
    lines += [
        "- `README.md` - this file.",
        "",
        "## Trace CSV columns",
        "",
        "- `time_utc` / `epoch_s` - point timestamp (ISO 8601 UTC / Unix seconds).",
        "- `source` - tracking network the point came from (adsbx, adsbfi, ...). Points from every",
        "  network are merged chronologically without deduplication, so nearby duplicates across",
        "  sources are expected and are themselves evidence of independent reception.",
        "- `lat` / `lon` - position in decimal degrees.",
        "- `alt_ft` - barometric altitude in feet; empty when the aircraft reported ground or no altitude.",
        "- `on_ground` - 1 when the aircraft reported being on the ground, else 0.",
        "- `gs_kt` - ground speed in knots (empty when not reported).",
        '- `callsign` / `squawk` - only on points that carried them; blanks mean "not broadcast on',
        '  this sample", not "unknown for the flight".',
        "",
        "## Caveats",
        "",
        "- Positions are crowdsourced ADS-B receptions; coverage gaps mean no receiver heard the",
        "  aircraft, not necessarily that it was on the ground.",
        "- Flight rows are algorithmic extractions from the raw traces (takeoff/landing detection,",
        "  airport matching, quality scoring); the raw points in `trace_days` are the ground truth.",
        "- Callsigns, registrations, and owner strings originate in the aircraft's own broadcasts",
        "  or public registries and can be stale or spoofed; treat them as claims, not facts.",
    ]
    return lines


def _analysis_lines(hex_code: str, identity: dict, tool_version: str) -> list[str]:
    generated = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"# Analysis notes: {_bundle_title(hex_code, identity)}",
        "",
        f"Stub generated {generated} by adsbtrack {tool_version}. Identity below is a",
        "database snapshot; add findings under Notes.",
        "",
        "## Identity",
        "",
    ]
    faa = identity["faa"]
    if faa is not None:
        address = ", ".join(part for part in (faa["street"], faa["city"], faa["state"], faa["zip_code"]) if part)
        faa_ref = identity["faa_ref"]
        model = f"{faa_ref['mfr']} {faa_ref['model']}".strip() if faa_ref else None
        lines += [
            f"- FAA registry: N{faa['n_number']}, registrant {faa['name'] or 'unknown'}"
            + (f" ({address})" if address else ""),
            f"- Airframe: {model or 'model not on file'}, serial {faa['serial_number'] or 'unknown'},"
            f" year {faa['year_mfr'] or 'unknown'}",
            f"- Certificate issued {faa['cert_issue_date'] or 'unknown'},"
            f" expires {faa['expiration_date'] or 'unknown'}",
        ]
    else:
        lines.append("- FAA registry: no record on file for this hex.")
    registry = identity["registry"]
    if registry is not None:
        lines.append(
            f"- Observed broadcasts: registration {registry['registration'] or 'unknown'},"
            f" type {registry['type_code'] or 'unknown'},"
            f" owner/operator {registry['owner_operator'] or 'unknown'}"
        )
    crossref = identity["crossref"]
    if crossref is not None:
        mil = "yes" if crossref["is_military"] else "no"
        lines.append(
            f"- Cross-reference ({crossref['source'] or 'unknown source'}):"
            f" registration {crossref['registration'] or 'unknown'},"
            f" operator {crossref['operator'] or 'unknown'}, military: {mil}"
        )
    lines += [
        "",
        "## Notes",
        "",
        "(add findings here)",
        "",
    ]
    return lines


# ---------------------------------------------------------------------------
# Bundle assembly
# ---------------------------------------------------------------------------


def export_bundle(
    db: Database,
    hex_code: str,
    out_dir: Path,
    windows: list[ExportWindow] | None = None,
    *,
    include_analysis: bool = False,
    make_zip: bool = False,
    tool_version: str = "unknown",
) -> ExportResult:
    """Assemble the deliverable bundle for one aircraft into ``out_dir``.

    Raises ValueError when the database holds no flights AND no trace days
    for the hex - an empty deliverable is almost always a typo'd hex, and
    writing one anyway would hand a third party a misleading "no activity"
    package.
    """
    windows = windows or []
    identity = _aircraft_identity(db, hex_code)
    result = ExportResult(out_dir=out_dir)

    has_data = db.conn.execute(
        "SELECT EXISTS(SELECT 1 FROM flights WHERE icao = ?) OR EXISTS(SELECT 1 FROM trace_days WHERE icao = ?)",
        (hex_code, hex_code),
    ).fetchone()[0]
    if not has_data:
        raise ValueError(f"No flights or trace days for hex {hex_code} in this database - nothing to export.")

    out_dir.mkdir(parents=True, exist_ok=True)

    extract_path = out_dir / f"{hex_code}.sqlite"
    counts = _write_sqlite_extract(db, hex_code, extract_path)
    result.files.append(extract_path)
    result.flight_count = counts["flights"]
    result.trace_day_count = counts["trace_days"]
    result.fetch_log_count = counts["fetch_log"]

    flights_path = out_dir / "flights.csv"
    _write_flights_csv(db, hex_code, flights_path)
    result.files.append(flights_path)

    for window in windows:
        window_flights = out_dir / f"flights_{window.label}.csv"
        result.window_flight_counts[window.label] = _write_flights_csv(db, hex_code, window_flights, window)
        result.files.append(window_flights)

        window_trace = out_dir / f"trace_{window.label}.csv"
        result.window_point_counts[window.label] = _write_trace_csv(db, hex_code, window_trace, window)
        result.files.append(window_trace)

    if include_analysis:
        analysis_path = out_dir / "analysis.md"
        analysis_path.write_text("\n".join(_analysis_lines(hex_code, identity, tool_version)), encoding="utf-8")
        result.files.append(analysis_path)

    sources = [
        row["source"]
        for row in db.conn.execute(
            "SELECT DISTINCT source FROM trace_days WHERE icao = ? ORDER BY source", (hex_code,)
        ).fetchall()
    ]
    readme_path = out_dir / "README.md"
    readme_path.write_text(
        "\n".join(
            _readme_lines(
                hex_code,
                identity,
                result,
                windows,
                sources,
                db.get_date_range(hex_code),
                extract_path.name,
                include_analysis,
                tool_version,
            )
        )
        + "\n",
        encoding="utf-8",
    )
    result.files.append(readme_path)

    if make_zip:
        zip_path = out_dir.parent / (out_dir.name + ".zip")
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for file_path in sorted(result.files):
                archive.write(file_path, arcname=file_path.name)
        result.zip_path = zip_path

    return result
