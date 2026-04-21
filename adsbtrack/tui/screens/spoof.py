"""Spoofed-broadcasts audit screen."""

from __future__ import annotations

import json

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import Screen
from textual.widgets import DataTable, Static

from ..queries import SpoofedBroadcast, list_spoofed_broadcasts
from ..widgets import FilterBar, PageHeader


class SpoofScreen(Screen):
    """Table of rejected broadcasts with an expandable JSON detail pane."""

    BINDINGS = [
        ("escape", "back", "Back"),
        ("/", "focus_filter", "Filter"),
        ("enter", "toggle_detail", "Toggle detail"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self._rows: list[SpoofedBroadcast] = []
        self._matched: list[SpoofedBroadcast] = []
        self._header = PageHeader(
            "spoofed broadcasts",
            crumb="bimodal integrity audit",
            trailing="threshold v2_sil0 >= 10% / min 25 samples",
        )
        self._filter = FilterBar(
            placeholder="filter rejected flights",
            widget_id="spoof-filter",
        )
        self._table = DataTable(id="spoof-table", zebra_stripes=True)
        self._detail = Static("", id="spoof-detail")
        self._detail.display = False

    def compose(self) -> ComposeResult:
        with Vertical():
            yield self._header
            yield self._filter
            yield self._table
            yield self._detail

    def on_mount(self) -> None:
        self._table.add_columns("DATE", "ICAO", "CALLSIGN", "ALT", "V2 SMP", "SIL=0%", "NIC=0%", "REASON")
        self._table.cursor_type = "row"
        self._refresh()

    def on_input_changed(self, event) -> None:  # type: ignore[override]
        if event.input is self._filter.input_widget:
            self._rerender(event.value or "")

    def action_focus_filter(self) -> None:
        self._filter.input_widget.focus()

    def action_back(self) -> None:
        self.app.pop_screen()

    def action_toggle_detail(self) -> None:
        row = self._selected()
        if row is None:
            self._detail.display = False
            return
        if self._detail.display:
            self._detail.display = False
            return
        self._detail.update(
            f"[b #c24bd6]reason_detail[/b #c24bd6]  icao={row.icao}  date={row.takeoff_date}\n"
            + json.dumps(row.reason_detail, indent=2)
        )
        self._detail.display = True

    def _selected(self) -> SpoofedBroadcast | None:
        if not self._matched:
            return None
        idx = self._table.cursor_row
        if idx is None or idx < 0 or idx >= len(self._matched):
            return None
        return self._matched[idx]

    def _refresh(self) -> None:
        self._rows = list_spoofed_broadcasts(self.app.db)
        self._rerender("")

    def _rerender(self, needle: str) -> None:
        self._table.clear()
        nlow = needle.lower() if needle else None
        self._matched = []
        for row in self._rows:
            if nlow and not self._matches(row, nlow):
                continue
            self._matched.append(row)
            detail = row.reason_detail or {}
            v2 = detail.get("v2_samples", "-")
            sil = detail.get("v2_sil0_pct")
            nic = detail.get("v2_nic0_pct")
            sil_fmt = f"{sil:.1f}" if isinstance(sil, (int, float)) else "-"
            nic_fmt = f"{nic:.1f}" if isinstance(nic, (int, float)) else "-"
            self._table.add_row(
                row.takeoff_date,
                row.icao,
                row.callsign or "-",
                f"{row.max_altitude:,}" if row.max_altitude is not None else "-",
                f"{v2:,}" if isinstance(v2, int) else str(v2),
                sil_fmt,
                nic_fmt,
                row.reason,
            )
        self._filter.set_counts(matched=len(self._matched), total=len(self._rows))

    @staticmethod
    def _matches(row: SpoofedBroadcast, needle: str) -> bool:
        return any(hay and needle in hay.lower() for hay in (row.icao, row.takeoff_date, row.callsign, row.reason))
