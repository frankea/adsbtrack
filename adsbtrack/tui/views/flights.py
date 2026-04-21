"""Flight timeline view: flights for a single aircraft."""

from __future__ import annotations

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import DataTable, Input

from ..queries import FlightRow, list_flights
from ..widgets import (
    ACCENT_AMBER,
    ACCENT_CYAN,
    ACCENT_OK,
    ACCENT_RED,
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
    "signal_lost": ("SIG LOST", FG_2),
    "dropped_on_approach": ("DROP", ACCENT_AMBER),
    "uncertain": ("UNCERT", ACCENT_AMBER),
    "altitude_error": ("ALT ERR", ACCENT_RED),
}


def _fmt_time(iso: str) -> str:
    if not iso:
        return "-"
    try:
        date_part, rest = iso.split("T", 1)
        return f"{date_part} {rest[:5]}Z"
    except ValueError:
        return iso


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
        style = FG_2
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
        yield self._filter
        yield self._table

    def on_mount(self) -> None:
        self._table.cursor_type = "row"
        self._table.add_column("DATE", width=18)
        self._table.add_column("FROM", width=6)
        self._table.add_column("TO", width=6)
        self._table.add_column(Text("DUR", justify="right"), width=6)
        self._table.add_column("CALLSIGN", width=10)
        self._table.add_column("MISSION", width=8)
        self._table.add_column(Text("ALT", justify="right"), width=8)
        self._table.add_column(Text("GS", justify="right"), width=6)
        self._table.add_column(Text("CONF", justify="right"), width=6)
        self._table.add_column("LAND", width=9)
        self._table.add_column("FLAGS")

    # --- public API ---

    def set_icao(self, icao: str) -> None:
        self._icao = icao
        self.refresh_data("")

    def refresh_data(self, needle: str) -> None:
        db = self.app.db
        if self._icao is None:
            self._rows = []
            self._table.clear()
            self._filter.set_counts(0, 0)
            self._header.set_crumb("select an aircraft first")
            self._header.set_trailing("")
            return

        self._rows = list_flights(db, self._icao)
        needle_low = needle.lower() if needle else ""
        matched: list[FlightRow] = []
        self._table.clear()
        for r in self._rows:
            if needle_low and not self._matches(r, needle_low):
                continue
            matched.append(r)
            self._table.add_row(
                cell(_fmt_time(r.takeoff_time), style=FG_1),
                cell(r.origin_icao or "-", style=FG_0 if r.origin_icao else FG_2),
                cell(r.destination_icao or "-", style=FG_0 if r.destination_icao else FG_2),
                num_cell(f"{r.duration_minutes:.0f}" if r.duration_minutes is not None else "-", style=FG_0),
                cell(r.callsign or "-", style=ACCENT_CYAN if r.callsign else FG_2),
                cell((r.mission_type or "-").upper()[:7], style=FG_1),
                num_cell(f"{r.max_altitude:,}" if r.max_altitude is not None else "-", style=FG_0),
                num_cell(f"{r.cruise_gs_kt:,}" if r.cruise_gs_kt is not None else "-", style=FG_0),
                _fmt_conf(r),
                _fmt_landing(r),
                _fmt_flags(r),
            )
        self._filter.set_counts(matched=len(matched), total=len(self._rows))
        total_hours = sum((r.duration_minutes or 0) for r in self._rows) / 60
        reg_desc = self._registry_line(self._icao)
        self._header.set_title(self._icao)
        self._header.set_crumb(reg_desc)
        self._header.set_trailing(f"{len(self._rows):,} flights / {total_hours:,.1f} hrs")

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input is self._filter.input_widget:
            self.refresh_data(event.value or "")

    def focus_filter(self) -> None:
        self._filter.input_widget.focus()

    # --- helpers ---

    @staticmethod
    def _matches(row: FlightRow, needle: str) -> bool:
        return any(
            hay and needle in hay.lower()
            for hay in (row.origin_icao, row.destination_icao, row.callsign, row.takeoff_date, row.mission_type)
        )

    def _registry_line(self, icao: str) -> str:
        try:
            row = self.app.db.conn.execute(
                "SELECT registration, type_code, description FROM aircraft_registry WHERE icao = ?",
                (icao,),
            ).fetchone()
        except Exception:
            return ""
        if not row:
            return ""
        parts = [b for b in (row["registration"], row["type_code"], row["description"]) if b]
        return " / ".join(parts)
