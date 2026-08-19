"""Flight timeline view: flights for a single aircraft."""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass

from rich.text import Text
from textual import work
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import DataTable, Input
from textual.worker import Worker, WorkerState

from ...db import Database
from ..queries import FlightRow, list_flights
from ..widgets import (
    ACCENT_AMBER,
    ACCENT_CYAN,
    ACCENT_MAGENTA,
    ACCENT_OK,
    ACCENT_RED,
    DOT,
    FG_0,
    FG_1,
    FG_2,
    FilterBar,
    PageHeader,
    cell,
    dash,
    num_cell,
    pill_markup,
)

_LANDING_SHORT = {
    "confirmed": ("OK", ACCENT_OK),
    "signal_lost": ("SIG LOST", ACCENT_RED),
    "dropped_on_approach": ("DROP", ACCENT_AMBER),
    "uncertain": ("UNCERT", ACCENT_AMBER),
    "altitude_error": ("ALT ERR", ACCENT_RED),
}

_ICAO_RE = re.compile(r"^[A-Z]{3,4}$")


def _fmt_time(iso: str) -> str:
    if not iso:
        return "-"
    try:
        date_part, rest = iso.split("T", 1)
        return f"{date_part} {rest[:5]}Z"
    except ValueError:
        return iso


def _airport_cell(code: str | None, *, fallback_style: str = FG_2) -> Text:
    """Colour an origin/destination code per the concept's table.

    - Clean 3/4-letter ICAO: c-ok (green).
    - `~PREFIX`: amber (uncertain / off-airport approximation).
    - Literal `sig lost`: red.
    - Coordinate tuple `(lat, lon)` or missing: dim.
    """
    if not code:
        return dash()
    low = code.lower()
    if low == "sig lost" or low == "signal_lost":
        return cell("sig lost", style=ACCENT_RED)
    if code.startswith("~"):
        return cell(code, style=ACCENT_AMBER)
    if code.startswith("("):
        return cell(code, style=fallback_style)
    if _ICAO_RE.match(code.upper()):
        return cell(code, style=ACCENT_OK)
    return cell(code, style=FG_0)


def _fmt_landing(row: FlightRow) -> Text:
    code, colour = _LANDING_SHORT.get(row.landing_type, (row.landing_type.upper()[:8], FG_1))
    return cell(code, style=colour)


def _fmt_conf(row: FlightRow) -> Text:
    if row.landing_confidence is None:
        return dash()
    pct = int(row.landing_confidence * 100)
    if pct >= 80:
        style = ACCENT_OK
    elif pct >= 50:
        style = ACCENT_AMBER
    else:
        style = ACCENT_RED
    return num_cell(f"{pct}%", style=style)


def _fmt_flags(row: FlightRow) -> Text:
    parts: list[str] = []
    if row.emergency_squawk:
        parts.append(pill_markup(f"SQK {row.emergency_squawk}", ACCENT_RED))
    if row.had_go_around:
        parts.append(pill_markup("GA", ACCENT_AMBER))
    if row.max_hover_secs and row.max_hover_secs >= 300:
        parts.append(pill_markup("HOVER", ACCENT_AMBER))
    if row.landing_type == "signal_lost":
        parts.append(pill_markup("LOST", FG_2))
    return Text.from_markup(" ".join(parts)) if parts else dash()


def filter_flights(rows: Sequence[FlightRow], needle: str) -> list[FlightRow]:
    """Return the flights matching ``needle`` (case-insensitive substring).

    Pure function, no Textual/DB dependency, so the filter bar's re-filter
    path can be exercised without a running app. Matches origin,
    destination, callsign, takeoff date, and mission type -- the same
    fields the old in-view ``_matches`` helper checked.
    """
    needle_low = needle.lower() if needle else ""
    if not needle_low:
        return list(rows)
    return [r for r in rows if _flight_matches(r, needle_low)]


def _flight_matches(row: FlightRow, needle_low: str) -> bool:
    return any(
        hay and needle_low in hay.lower()
        for hay in (row.origin_icao, row.destination_icao, row.callsign, row.takeoff_date, row.mission_type)
    )


@dataclass(frozen=True)
class _FlightsResult:
    """Everything the worker fetched, applied to the UI on the event loop."""

    icao: str
    rows: list[FlightRow]
    reg_desc: str


class FlightsView(Vertical):
    """Reverse-chronological flight list for one aircraft."""

    def __init__(self) -> None:
        super().__init__(id="view-flights")
        self._icao: str | None = None
        self._rows: list[FlightRow] = []
        self._header = PageHeader(
            "flights",
            crumb="select an aircraft first",
            widget_id="flights-header",
        )
        self._filter = FilterBar(
            placeholder="filter flights (airport, callsign, date, mission)",
            widget_id="flights-filter",
        )
        self._table = DataTable(id="flights-table", zebra_stripes=True)

    def compose(self) -> ComposeResult:
        yield self._header
        yield self._filter.build()
        yield self._table

    def on_mount(self) -> None:
        self._table.cursor_type = "row"
        self._table.add_column("DATE", width=18)
        self._table.add_column("FROM", width=6)
        self._table.add_column("TO", width=10)
        self._table.add_column(Text("DUR", justify="right"), width=6)
        self._table.add_column("CALLSIGN", width=10)
        self._table.add_column("MISSION", width=8)
        self._table.add_column(Text("ALT", justify="right"), width=8)
        self._table.add_column(Text("GS", justify="right"), width=6)
        self._table.add_column(Text("CONF", justify="right"), width=6)
        self._table.add_column("TYPE", width=9)
        self._table.add_column("FLAGS")

    # --- public API ---

    def set_icao(self, icao: str) -> None:
        self._icao = icao
        self.refresh_data()

    def refresh_data(self) -> None:
        """Kick off a background worker to query flights for the current aircraft.

        This is the only path that re-queries the DB; the filter bar
        re-filters the cached ``self._rows`` via ``_apply_filter`` without
        touching the DB again (Task 14). The query itself runs off the
        event loop in ``_fetch_flights`` (Task 15); results land back here
        via ``on_worker_state_changed``.
        """
        if self._icao is None:
            self._rows = []
            self._apply_filter("")
            self._header.set_crumb("select an aircraft first")
            self._header.set_trailing("")
            return
        self.loading = True
        self._fetch_flights(self._icao)

    @work(thread=True, exclusive=True, group="flights", exit_on_error=False)
    def _fetch_flights(self, icao: str) -> _FlightsResult:
        """Run the flight + registry queries on a worker's own connection.

        Must not touch ``self.app.db`` (the main-thread connection) or any
        widget -- only DB reads and pure computation happen here.
        ``exit_on_error=False`` on the decorator keeps a raised exception
        from crashing the whole app before the ``ERROR`` branch in
        ``on_worker_state_changed`` runs.
        """
        db = self.app.db_factory()
        try:
            rows = list_flights(db, icao)
            reg_desc = self._registry_line(db, icao)
        finally:
            db.close()
        return _FlightsResult(icao=icao, rows=rows, reg_desc=reg_desc)

    def on_worker_state_changed(self, event: Worker.StateChanged) -> None:
        if event.worker.name != "_fetch_flights":
            return
        if event.state == WorkerState.SUCCESS:
            result = event.worker.result
            assert result is not None  # a SUCCESS worker always has a result
            self._rows = result.rows
            total_hours = sum((r.duration_minutes or 0) for r in self._rows) / 60
            self._header.set_title(result.icao)
            self._header.set_crumb(result.reg_desc)
            self._header.set_trailing(f"{len(self._rows):,} flights {DOT} {total_hours:,.1f} hrs")
            self._apply_filter(self._filter.input_widget.value)
            self.loading = False
        elif event.state == WorkerState.ERROR:
            self.loading = False
            self.app.notify(f"failed to load flights: {event.worker.error}", severity="error")

    def _apply_filter(self, needle: str) -> None:
        rows = filter_flights(self._rows, needle)
        self._table.clear()
        for r in rows:
            self._table.add_row(
                cell(_fmt_time(r.takeoff_time), style=FG_1),
                _airport_cell(r.origin_icao),
                _airport_cell(r.destination_icao),
                num_cell(f"{r.duration_minutes:.0f}" if r.duration_minutes is not None else "-", style=FG_0),
                cell(r.callsign or "-", style=ACCENT_CYAN if r.callsign else FG_2),
                cell((r.mission_type or "-").upper()[:7], style=ACCENT_MAGENTA if r.mission_type else FG_2),
                num_cell(f"{r.max_altitude:,}" if r.max_altitude is not None else "-", style=FG_0),
                num_cell(f"{r.cruise_gs_kt:,}" if r.cruise_gs_kt is not None else "-", style=FG_0),
                _fmt_conf(r),
                _fmt_landing(r),
                _fmt_flags(r),
            )
        self._filter.set_counts(matched=len(rows), total=len(self._rows))

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input is self._filter.input_widget:
            self._apply_filter(event.value or "")

    def focus_filter(self) -> None:
        self._filter.input_widget.focus()

    # --- helpers ---

    def _registry_line(self, db: Database, icao: str) -> str:
        try:
            row = db.conn.execute(
                "SELECT registration, type_code, description FROM aircraft_registry WHERE icao = ?",
                (icao,),
            ).fetchone()
        except Exception:
            return ""
        if not row:
            return ""
        parts = [b for b in (row["registration"], row["type_code"], row["description"]) if b]
        return f" {DOT} ".join(parts)
