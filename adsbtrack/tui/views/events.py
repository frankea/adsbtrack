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
    FG_0,
    FG_1,
    FG_2,
    FilterBar,
    PageHeader,
    cell,
    pill_markup,
)

_SEV_STYLE = {
    "emergency": (ACCENT_RED, "EMERG"),
    "unusual": (ACCENT_AMBER, "UNUSL"),
}


def _sev_for(event_type: str, severity: str) -> tuple[str, str]:
    if event_type.startswith("spoof"):
        return ACCENT_VIOLET, "SPOOF"
    return _SEV_STYLE.get(severity, (FG_2, "INFO"))


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
        )
        self._filter = FilterBar(
            placeholder="filter events (type, icao, callsign)",
            widget_id="events-filter",
        )
        self._table = DataTable(id="events-table", zebra_stripes=True)

    def compose(self) -> ComposeResult:
        yield self._header
        yield self._filter
        yield self._table

    def on_mount(self) -> None:
        self._table.cursor_type = "row"
        self._table.add_column("TIME", width=18)
        self._table.add_column("SEV", width=8)
        self._table.add_column("TYPE", width=24)
        self._table.add_column("ICAO", width=8)
        self._table.add_column("CALLSIGN", width=10)
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
        self._header.set_crumb("all aircraft" if self._icao is None else self._icao)
        self._header.set_trailing(
            f"emergency {counts['emergency']}   unusual {counts['unusual']}   spoof {counts['spoof']}"
        )

        needle_low = needle.lower() if needle else ""
        self._table.clear()
        matched = 0
        for e in self._rows:
            if needle_low and not self._matches(e, needle_low):
                continue
            matched += 1
            colour, label = _sev_for(e.event_type, e.severity)
            ts_short = e.ts.strftime("%Y-%m-%d %H:%MZ") if getattr(e, "ts", None) else "-"
            self._table.add_row(
                cell(ts_short, style=FG_1),
                Text.from_markup(pill_markup(label, colour)),
                cell(e.event_type, style=FG_0),
                cell(e.icao, style=ACCENT_CYAN),
                cell(e.callsign or "-", style=FG_0 if e.callsign else FG_2),
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
