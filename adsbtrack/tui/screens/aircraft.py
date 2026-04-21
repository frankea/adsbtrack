"""Aircraft list screen: filterable table keyed on ICAO hex."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import Screen
from textual.widgets import DataTable

from ..queries import AircraftRow, list_aircraft
from ..widgets import FilterBar, PageHeader


class AircraftScreen(Screen):
    """The default screen: every aircraft we have data for."""

    BINDINGS = [
        ("enter", "open_flights", "Open flights"),
        ("/", "focus_filter", "Filter"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self._rows: list[AircraftRow] = []
        self._matched: list[AircraftRow] = []
        self._header = PageHeader("aircraft", crumb="all (-)", trailing="sort: last_seen desc")
        self._filter = FilterBar(
            placeholder="filter by hex / registration / type / home base",
            widget_id="aircraft-filter",
        )
        self._table = DataTable(id="aircraft-table", zebra_stripes=True)

    def compose(self) -> ComposeResult:
        with Vertical():
            yield self._header
            yield self._filter
            yield self._table

    def on_mount(self) -> None:
        self._table.add_columns("ICAO", "REG", "TYPE", "FLTS", "HRS", "HOME", "LAST SEEN", "SPF", "FLAGS")
        self._table.cursor_type = "row"
        self._refresh()

    def on_input_changed(self, event) -> None:  # type: ignore[override]
        if event.input is self._filter.input_widget:
            self._refresh(filter_substr=event.value or None)

    def action_focus_filter(self) -> None:
        self._filter.input_widget.focus()

    def action_open_flights(self) -> None:
        row = self._selected_row()
        if row is None:
            return
        self.app.post_message(OpenFlights(row.icao))

    def _selected_row(self) -> AircraftRow | None:
        if not self._matched:
            return None
        idx = self._table.cursor_row
        if idx is None or idx < 0 or idx >= len(self._matched):
            return None
        return self._matched[idx]

    def _refresh(self, *, filter_substr: str | None = None) -> None:
        db = self.app.db
        self._matched = list_aircraft(db, filter_substr=filter_substr)
        if filter_substr is None:
            self._rows = self._matched
        self._table.clear()
        for row in self._matched:
            self._table.add_row(
                row.icao,
                row.display_reg,
                row.display_type,
                f"{row.total_flights:,}",
                f"{row.total_hours:.1f}",
                row.display_home,
                row.display_last_seen,
                str(row.spoof_count) if row.spoof_count else "",
                row.flags,
                key=row.icao,
            )
        total_all = len(self._rows) if self._rows else len(self._matched)
        self._filter.set_counts(matched=len(self._matched), total=total_all)
        self._header.set_crumb(f"all ({total_all:,})")


from textual.message import Message  # noqa: E402 -- circular if imported at top


class OpenFlights(Message):
    """Request the app to switch to the Flights screen for a given ICAO."""

    def __init__(self, icao: str) -> None:
        super().__init__()
        self.icao = icao
