"""Aircraft list view: filterable table keyed on ICAO hex."""

from __future__ import annotations

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.message import Message
from textual.widgets import DataTable, Input

from ..queries import AircraftRow, list_aircraft
from ..widgets import (
    ACCENT_AMBER,
    ACCENT_CYAN,
    ACCENT_VIOLET,
    FG_0,
    FG_2,
    FilterBar,
    PageHeader,
    cell,
    dash,
    num_cell,
    pill_markup,
)


def _fmt_last_seen(s: str | None) -> Text:
    if not s:
        return dash()
    return cell(s, style=FG_2)


def _fmt_flags(row: AircraftRow) -> Text:
    """Render the trailing FLAGS cell (MIL / SPF / HOVER / TYP per concept).

    SPF is a bare badge because the count already has its own column
    to the left; doubling the number would be redundant and noisier
    than the design.
    """
    parts: list[str] = []
    if row.is_military:
        parts.append(pill_markup("MIL", ACCENT_VIOLET))
    if row.spoof_count:
        parts.append(pill_markup("SPF", ACCENT_VIOLET))
    if row.has_hover:
        parts.append(pill_markup("HOVER", ACCENT_AMBER))
    if row.has_type_override:
        parts.append(pill_markup("TYP", ACCENT_AMBER))
    return Text.from_markup(" ".join(parts)) if parts else dash()


class AircraftOpenFlights(Message):
    """Bubble up to the App when the user selects an aircraft and asks for flights."""

    def __init__(self, icao: str) -> None:
        super().__init__()
        self.icao = icao


class AircraftView(Vertical):
    """Aircraft list view. Lives inside the App's ContentSwitcher."""

    def __init__(self) -> None:
        super().__init__(id="view-aircraft")
        self._rows: list[AircraftRow] = []
        self._matched: list[AircraftRow] = []
        self._header = PageHeader(
            "aircraft",
            crumb="all (-)",
            trailing=Text.from_markup(f"[{FG_2}]sort:[/] [{ACCENT_CYAN}]last_seen ↓[/]"),
            widget_id="aircraft-header",
            crumb_prefix="›",
        )
        self._filter = FilterBar(
            placeholder="filter (fzf)   e.g.  pc-12  or  N512  or  ae6",
            widget_id="aircraft-filter",
        )
        self._table = DataTable(id="aircraft-table", zebra_stripes=True)

    def compose(self) -> ComposeResult:
        yield self._header
        yield self._filter.build()
        yield self._table

    def on_mount(self) -> None:
        self._table.cursor_type = "row"
        self._table.add_column("ICAO", width=9)
        self._table.add_column("REG", width=9)
        self._table.add_column("TYPE")
        self._table.add_column(Text("FLTS", justify="right"), width=8)
        self._table.add_column(Text("HRS", justify="right"), width=9)
        self._table.add_column("HOME", width=7)
        self._table.add_column("LAST SEEN", width=13)
        self._table.add_column(Text("SPF", justify="right"), width=6)
        self._table.add_column("FLAGS")
        self.refresh_data()

    # --- public API ---

    def refresh_data(self, *, filter_substr: str | None = None) -> None:
        db = self.app.db
        rows = list_aircraft(db, filter_substr=filter_substr)
        if filter_substr is None:
            self._rows = rows
        self._matched = rows
        self._table.clear()
        for row in rows:
            type_display = row.description or row.type_code or "(unknown)"
            self._table.add_row(
                cell(row.icao, style=ACCENT_CYAN),
                cell(row.display_reg, style=FG_0),
                cell(type_display, style=FG_2),
                num_cell(f"{row.total_flights:,}", style=FG_0),
                num_cell(f"{row.total_hours:.1f}", style=FG_0),
                cell(row.display_home, style=FG_0),
                _fmt_last_seen(row.last_seen),
                num_cell(
                    str(row.spoof_count) if row.spoof_count else "--",
                    style=ACCENT_VIOLET if row.spoof_count else FG_2,
                ),
                _fmt_flags(row),
                key=row.icao,
            )
        total_all = len(self._rows) if self._rows else len(rows)
        self._filter.set_counts(matched=len(rows), total=total_all)
        self._header.set_crumb(f"all ({total_all:,})")

    # --- event handlers ---

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input is self._filter.input_widget:
            self.refresh_data(filter_substr=event.value or None)

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.row_key is None:
            return
        icao = str(event.row_key.value)
        self.post_message(AircraftOpenFlights(icao))

    def focus_filter(self) -> None:
        self._filter.input_widget.focus()
