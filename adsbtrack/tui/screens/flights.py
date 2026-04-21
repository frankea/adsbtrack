"""Flight timeline screen for a single aircraft."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import Screen
from textual.widgets import DataTable

from ..queries import FlightRow, list_flights
from ..widgets import FilterBar, PageHeader


def _fmt_time(iso: str) -> str:
    """Render an ISO timestamp as `YYYY-MM-DD HH:MMZ`."""
    if not iso:
        return "-"
    # Input looks like 2026-04-21T00:49:47.580000+00:00
    try:
        date, rest = iso.split("T", 1)
        hm = rest[:5]
        return f"{date} {hm}Z"
    except ValueError:
        return iso


def _fmt_landing(landing_type: str, conf: float | None) -> tuple[str, str]:
    """Return `(short_code, rich_markup_class)` for the landing column."""
    short = {
        "confirmed": "OK",
        "signal_lost": "SIG LOST",
        "dropped_on_approach": "DROP",
        "uncertain": "UNCERT",
        "altitude_error": "ALT ERR",
    }.get(landing_type, landing_type.upper()[:8])
    tier = "ok" if conf is not None and conf >= 0.8 else "amber" if conf is not None and conf >= 0.5 else "dim"
    return short, tier


def _render_flags(row: FlightRow) -> str:
    parts: list[str] = []
    if row.emergency_squawk:
        parts.append(f"[#e0433a]SQK{row.emergency_squawk}[/]")
    if row.had_go_around:
        parts.append("[#f2b136]GA[/]")
    if row.max_hover_secs and row.max_hover_secs >= 300:
        parts.append("[#f2b136]HOVER[/]")
    if row.landing_type == "signal_lost":
        parts.append("[#6b7885]LOST[/]")
    return " ".join(parts)


class FlightsScreen(Screen):
    """Per-aircraft flights in reverse-chronological order."""

    BINDINGS = [
        ("escape", "back", "Back"),
        ("/", "focus_filter", "Filter"),
    ]

    def __init__(self, icao: str) -> None:
        super().__init__()
        self._icao = icao
        self._rows: list[FlightRow] = []
        self._header = PageHeader(icao, crumb="flights")
        self._filter = FilterBar(
            placeholder="filter flights (airport, callsign, date range)",
            widget_id="flights-filter",
        )
        self._table = DataTable(id="flights-table", zebra_stripes=True)

    def compose(self) -> ComposeResult:
        with Vertical():
            yield self._header
            yield self._filter
            yield self._table

    def on_mount(self) -> None:
        self._table.add_columns(
            "DATE", "FROM", "TO", "DUR", "CALLSIGN", "MISSION", "ALT", "GS", "CONF", "LAND", "FLAGS"
        )
        self._table.cursor_type = "row"
        self._refresh()

    def on_input_changed(self, event) -> None:  # type: ignore[override]
        if event.input is self._filter.input_widget:
            self._rerender(event.value or "")

    def action_focus_filter(self) -> None:
        self._filter.input_widget.focus()

    def action_back(self) -> None:
        self.app.pop_screen()

    def _refresh(self) -> None:
        self._rows = list_flights(self.app.db, self._icao)
        self._rerender("")
        reg = None
        type_code = None
        try:
            row = self.app.db.conn.execute(
                "SELECT registration, type_code FROM aircraft_registry WHERE icao = ?",
                (self._icao,),
            ).fetchone()
            if row:
                reg = row["registration"]
                type_code = row["type_code"]
        except Exception:
            pass
        crumb_bits = [b for b in (reg, type_code) if b]
        self._header.set_crumb(" / ".join(crumb_bits) if crumb_bits else "flights")
        total_hours = sum((r.duration_minutes or 0) for r in self._rows) / 60
        self._header.set_trailing(f"{len(self._rows)} flights / {total_hours:,.1f} hours")

    def _rerender(self, needle: str) -> None:
        self._table.clear()
        nlow = needle.lower() if needle else None
        matched = []
        for r in self._rows:
            if nlow and not self._matches(r, nlow):
                continue
            matched.append(r)
            land_code, land_tier = _fmt_landing(r.landing_type, r.landing_confidence)
            conf_pct = f"{int(r.landing_confidence * 100)}%" if r.landing_confidence is not None else "-"
            self._table.add_row(
                _fmt_time(r.takeoff_time),
                r.origin_icao or "-",
                r.destination_icao or "-",
                f"{r.duration_minutes:.0f}" if r.duration_minutes is not None else "-",
                r.callsign or "-",
                (r.mission_type or "-").upper()[:6],
                f"{r.max_altitude:,}" if r.max_altitude is not None else "-",
                f"{r.cruise_gs_kt:,}" if r.cruise_gs_kt is not None else "-",
                conf_pct,
                f"[#{_tier_colour(land_tier)}]{land_code}[/]",
                _render_flags(r),
            )
        self._filter.set_counts(matched=len(matched), total=len(self._rows))

    @staticmethod
    def _matches(row: FlightRow, needle: str) -> bool:
        for hay in (
            row.origin_icao,
            row.destination_icao,
            row.callsign,
            row.takeoff_date,
            row.mission_type,
        ):
            if hay and needle in hay.lower():
                return True
        return False


def _tier_colour(tier: str) -> str:
    return {
        "ok": "4ec07a",
        "amber": "f2b136",
        "dim": "6b7885",
    }.get(tier, "e4ecf3")
