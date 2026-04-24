"""Event feed view: unified chronological stream across event types."""

from __future__ import annotations

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import DataTable, Input

from ..queries import list_events
from ..widgets import (
    ACCENT_AMBER,
    ACCENT_CYAN,
    ACCENT_RED,
    ACCENT_VIOLET,
    DOT,
    FG_0,
    FG_1,
    FG_2,
    FilterBar,
    PageHeader,
    cell,
    pill_markup,
)

# Concept-specified pill label per event_type.
_EVENT_PILLS: dict[str, tuple[str, str]] = {
    "emergency_squawk": ("EMERGENCY", ACCENT_RED),
    "emergency_flag": ("EMERGENCY", ACCENT_RED),
    "off_airport_landing": ("OFF-AIRPORT", ACCENT_AMBER),
    "long_hover": ("LONG HOVER", ACCENT_AMBER),
    "multiple_go_arounds": ("GO-AROUND", ACCENT_AMBER),
}


def _pill_for(event_type: str, severity: str) -> tuple[str, str]:
    if event_type.startswith("spoof"):
        return "SPOOF", ACCENT_VIOLET
    if event_type in _EVENT_PILLS:
        return _EVENT_PILLS[event_type]
    if severity == "emergency":
        return "EMERGENCY", ACCENT_RED
    if severity == "unusual":
        return "UNUSUAL", ACCENT_AMBER
    return event_type.upper(), FG_2


class EventsView(Vertical):
    """Chronological event stream, optionally scoped to one ICAO."""

    def __init__(self) -> None:
        super().__init__(id="view-events")
        self._icao: str | None = None
        self._rows: list = []
        self._header = PageHeader(
            "events",
            crumb="all aircraft",
            widget_id="events-header",
            crumb_prefix="›",
        )
        self._filter = FilterBar(
            placeholder="filter events (type:emergency, icao:ae, since:3d)",
            widget_id="events-filter",
        )
        self._table = DataTable(id="events-table", zebra_stripes=True)

    def compose(self) -> ComposeResult:
        yield self._header
        yield self._filter.build()
        yield self._table

    def on_mount(self) -> None:
        # Column order mirrors the concept's event-feed grid
        # (ts / hex / callsign / pill / summary).
        self._table.cursor_type = "row"
        self._table.add_column("TIME", width=18)
        self._table.add_column("ICAO", width=8)
        self._table.add_column("CALLSIGN", width=10)
        self._table.add_column("EVENT", width=14)
        self._table.add_column("SUMMARY")

    def set_icao(self, icao: str | None) -> None:
        self._icao = icao
        self.refresh_data("")

    def refresh_data(self, needle: str) -> None:
        db = self.app.db
        self._rows = list_events(db, self._icao, include_spoof_checks=True)
        counts = {"emergency": 0, "unusual": 0, "spoof": 0}
        for e in self._rows:
            if e.event_type.startswith("spoof"):
                counts["spoof"] += 1
            elif e.severity == "emergency":
                counts["emergency"] += 1
            else:
                counts["unusual"] += 1
        crumb = "all aircraft" if self._icao is None else self._icao
        self._header.set_crumb(f"{crumb} {DOT} last 7d")
        # Build the trailing severity-pills line (outlined pills, per concept).
        trailing = Text.from_markup(
            " ".join(
                [
                    pill_markup(f"emergency {counts['emergency']}", ACCENT_RED),
                    pill_markup(f"unusual {counts['unusual']}", ACCENT_AMBER),
                    pill_markup(f"spoof {counts['spoof']}", ACCENT_VIOLET),
                ]
            )
        )
        self._header.set_trailing(trailing)

        needle_low = needle.lower() if needle else ""
        self._table.clear()
        matched = 0
        for e in self._rows:
            if needle_low and not self._matches(e, needle_low):
                continue
            matched += 1
            label, colour = _pill_for(e.event_type, e.severity)
            ts_short = e.ts.strftime("%Y-%m-%d %H:%MZ") if getattr(e, "ts", None) else "-"
            self._table.add_row(
                cell(ts_short, style=FG_1),
                cell(e.icao, style=ACCENT_CYAN),
                cell(e.callsign or "-", style=FG_0 if e.callsign else FG_2),
                Text.from_markup(pill_markup(label, colour)),
                cell(e.summary or "", style=FG_1),
            )
        self._filter.set_counts(matched=matched, total=len(self._rows))

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input is self._filter.input_widget:
            self.refresh_data(event.value or "")

    def focus_filter(self) -> None:
        self._filter.input_widget.focus()

    @staticmethod
    def _matches(event, needle: str) -> bool:
        for field in ("event_type", "icao", "callsign", "summary"):
            v = getattr(event, field, None)
            if v and needle in str(v).lower():
                return True
        return False
