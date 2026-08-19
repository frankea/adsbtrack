"""Aircraft list view: filterable table keyed on ICAO hex."""

from __future__ import annotations

from collections.abc import Sequence

from rich.text import Text
from textual import work
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.message import Message
from textual.widgets import DataTable, Input
from textual.worker import Worker, WorkerState

from ..queries import AircraftRow, list_aircraft
from ..widgets import (
    ACCENT_AMBER,
    ACCENT_CYAN,
    ACCENT_VIOLET,
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


def _fmt_last_seen(s: str | None) -> Text:
    if not s:
        return dash()
    return cell(s, style=FG_1)


def _fmt_flags(row: AircraftRow) -> Text:
    """Render the trailing FLAGS cell.

    SPF is a bare badge because the count already has its own column
    to the left; doubling the number would be redundant and noisier
    than the design.
    """
    parts: list[str] = []
    if row.is_military:
        parts.append(pill_markup("MIL", ACCENT_VIOLET))
    if row.spoof_count:
        parts.append(pill_markup("SPF", ACCENT_VIOLET))
    if "HELI" in row.flags.split():
        parts.append(pill_markup("HELI", ACCENT_AMBER))
    return Text.from_markup(" ".join(parts)) if parts else dash()


def filter_aircraft(rows: Sequence[AircraftRow], needle: str) -> list[AircraftRow]:
    """Return the aircraft matching ``needle`` (case-insensitive substring).

    Pure function, no Textual/DB dependency, so the filter bar's re-filter
    path can be exercised without a running app. Matches ICAO hex,
    registration, type code, and home-base ICAO -- the same four columns
    ``queries.list_aircraft``'s SQL ``LIKE`` clause used to match before
    Task 14 moved filtering out of the query.
    """
    needle_low = needle.lower() if needle else ""
    if not needle_low:
        return list(rows)
    return [r for r in rows if _aircraft_matches(r, needle_low)]


def _aircraft_matches(row: AircraftRow, needle_low: str) -> bool:
    return any(
        hay and needle_low in str(hay).lower()
        for hay in (row.icao, row.registration, row.type_code, row.home_base_icao)
    )


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
            trailing="sort: last_seen desc",
            widget_id="aircraft-header",
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

    def refresh_data(self) -> None:
        """Kick off a background worker to query the full aircraft list.

        This is the only path that re-queries the DB; the filter bar
        re-filters the cached ``self._rows`` via ``_apply_filter`` without
        touching the DB again (Task 14). The query itself runs off the
        event loop in ``_fetch_aircraft`` (Task 15); results land back
        here via ``on_worker_state_changed``.
        """
        self.loading = True
        self._fetch_aircraft()

    @work(thread=True, exclusive=True, group="aircraft")
    def _fetch_aircraft(self) -> list[AircraftRow]:
        """Run the aircraft-list query on a worker's own connection.

        Must not touch ``self.app.db`` (the main-thread connection) or any
        widget -- only DB reads happen here.
        """
        db = self.app.db_factory()
        try:
            return list_aircraft(db)
        finally:
            db.close()

    def on_worker_state_changed(self, event: Worker.StateChanged) -> None:
        if event.worker.name != "_fetch_aircraft":
            return
        if event.state == WorkerState.SUCCESS:
            self._rows = event.worker.result or []
            self._apply_filter("")
            self.loading = False
        elif event.state == WorkerState.ERROR:
            self.loading = False
            self.app.notify(f"failed to load aircraft: {event.worker.error}", severity="error")

    def _apply_filter(self, needle: str) -> None:
        rows = filter_aircraft(self._rows, needle)
        self._matched = rows
        self._table.clear()
        for row in rows:
            self._table.add_row(
                cell(row.icao, style=ACCENT_CYAN),
                cell(row.display_reg, style=FG_0),
                cell(row.display_type, style=FG_1),
                num_cell(f"{row.total_flights:,}", style=FG_0),
                num_cell(f"{row.total_hours:.1f}", style=FG_0),
                cell(row.display_home, style=FG_0),
                _fmt_last_seen(row.last_seen),
                num_cell(
                    str(row.spoof_count) if row.spoof_count else "-", style=ACCENT_VIOLET if row.spoof_count else FG_2
                ),
                _fmt_flags(row),
                key=row.icao,
            )
        total_all = len(self._rows)
        self._filter.set_counts(matched=len(rows), total=total_all)
        self._header.set_crumb(f"all ({total_all:,})")

    # --- event handlers ---

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input is self._filter.input_widget:
            self._apply_filter(event.value or "")

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.row_key is None:
            return
        icao = str(event.row_key.value)
        self.post_message(AircraftOpenFlights(icao))

    def focus_filter(self) -> None:
        self._filter.input_widget.focus()
