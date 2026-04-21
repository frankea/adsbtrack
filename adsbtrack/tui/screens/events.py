"""Event feed screen: unified chronological stream across the event types."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import Screen
from textual.widgets import DataTable

from ..queries import list_events
from ..widgets import FilterBar, PageHeader

_EVENT_TIER = {
    "emergency_squawk": ("EMERG", "e0433a"),
    "emergency_flag": ("EMERG", "e0433a"),
    "off_airport_landing": ("UNUSL", "f2b136"),
    "long_hover": ("UNUSL", "f2b136"),
    "multiple_go_arounds": ("UNUSL", "f2b136"),
    "spoof_bimodal_integrity": ("SPOOF", "c24bd6"),
}


class EventsScreen(Screen):
    """Chronological feed. Scoped to one ICAO when ``icao`` is provided."""

    BINDINGS = [
        ("escape", "back", "Back"),
        ("/", "focus_filter", "Filter"),
    ]

    def __init__(self, icao: str | None = None) -> None:
        super().__init__()
        self._icao = icao
        self._rows: list[dict] = []
        self._header = PageHeader("events", crumb="all aircraft" if icao is None else icao)
        self._filter = FilterBar(
            placeholder="filter events (type, icao, date)",
            widget_id="events-filter",
        )
        self._table = DataTable(id="events-table", zebra_stripes=True)

    def compose(self) -> ComposeResult:
        with Vertical():
            yield self._header
            yield self._filter
            yield self._table

    def on_mount(self) -> None:
        self._table.add_columns("TIME", "SEV", "TYPE", "ICAO", "CALLSIGN", "SUMMARY")
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
        raw = list_events(self.app.db, self._icao, include_spoof_checks=True)
        counts = {"emergency": 0, "unusual": 0, "spoof": 0}
        self._rows = []
        for e in raw:
            if e.event_type.startswith("spoof"):
                counts["spoof"] += 1
            elif e.severity == "emergency":
                counts["emergency"] += 1
            else:
                counts["unusual"] += 1
            self._rows.append(
                {
                    "event": e,
                    "ts": e.ts,
                    "type": e.event_type,
                    "icao": e.icao,
                    "callsign": e.callsign or "",
                    "summary": e.summary,
                }
            )
        self._header.set_trailing(
            f"emergency {counts['emergency']} / unusual {counts['unusual']} / spoof {counts['spoof']}"
        )
        self._rerender("")

    def _rerender(self, needle: str) -> None:
        self._table.clear()
        nlow = needle.lower() if needle else None
        matched = 0
        for row in self._rows:
            if nlow and not self._matches(row, nlow):
                continue
            matched += 1
            tier_label, tier_colour = _EVENT_TIER.get(row["type"], ("INFO", "6b7885"))
            sev_cell = f"[#{tier_colour}]{tier_label}[/]"
            ts_short = row["ts"].strftime("%Y-%m-%d %H:%MZ") if row["ts"] else "-"
            self._table.add_row(
                ts_short,
                sev_cell,
                row["type"],
                row["icao"],
                row["callsign"],
                row["summary"],
            )
        self._filter.set_counts(matched=matched, total=len(self._rows))

    @staticmethod
    def _matches(row: dict, needle: str) -> bool:
        for field in ("type", "icao", "callsign", "summary"):
            hay = row.get(field) or ""
            if needle in hay.lower():
                return True
        return False
