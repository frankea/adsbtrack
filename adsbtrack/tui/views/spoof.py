"""Spoofed-broadcasts audit view."""

from __future__ import annotations

import json

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import DataTable, Input, Static

from ..queries import SpoofedBroadcast, list_spoofed_broadcasts
from ..widgets import (
    ACCENT_CYAN,
    ACCENT_VIOLET,
    DOT,
    FG_0,
    FG_2,
    FilterBar,
    PageHeader,
    cell,
    dash,
    num_cell,
)


def _format_source_rates(detail: dict) -> str:
    """Concept renders per-source rates as ``adsbx 41% · adsbfi 34%``."""
    rates = detail.get("source_rates") if isinstance(detail, dict) else None
    if isinstance(rates, list) and rates:
        parts: list[str] = []
        for entry in rates:
            if isinstance(entry, (list, tuple)) and len(entry) >= 2:
                name, pct = entry[0], entry[1]
                try:
                    parts.append(f"{name} {float(pct):.0f}%")
                except (TypeError, ValueError):
                    parts.append(f"{name} {pct}")
        return f" {DOT} ".join(parts)
    sources = detail.get("sources") if isinstance(detail, dict) else None
    if isinstance(sources, list):
        return ", ".join(str(s) for s in sources)
    return ""


class SpoofView(Vertical):
    """Audit table of rejected broadcasts with an expandable detail pane."""

    def __init__(self) -> None:
        super().__init__(id="view-spoof")
        self._rows: list[SpoofedBroadcast] = []
        self._matched: list[SpoofedBroadcast] = []
        self._header = PageHeader(
            "spoofed broadcasts",
            crumb="bimodal integrity audit",
            trailing=f"threshold v2_sil0 ≥ 10% {DOT} min 25 samples",
            widget_id="spoof-header",
        )
        self._filter = FilterBar(
            placeholder="filter rejected broadcasts",
            widget_id="spoof-filter",
        )
        self._table = DataTable(id="spoof-table", zebra_stripes=True)
        self._detail = Static(" ", id="spoof-detail", classes="spoof-detail")
        self._detail.display = False

    def compose(self) -> ComposeResult:
        yield self._header
        yield self._filter.build()
        yield self._table
        yield self._detail

    def on_mount(self) -> None:
        self._table.cursor_type = "row"
        self._table.add_column("DATE", width=12)
        self._table.add_column("ICAO", width=8)
        self._table.add_column("CALLSIGN", width=10)
        self._table.add_column(Text("V2 SMP", justify="right"), width=8)
        self._table.add_column(Text("SIL=0%", justify="right"), width=8)
        self._table.add_column(Text("NIC=0%", justify="right"), width=8)
        self._table.add_column("PER-SOURCE")

    # --- public API ---

    def refresh_data(self, needle: str = "") -> None:
        db = self.app.db
        self._rows = list_spoofed_broadcasts(db)
        self._rerender(needle)

    def toggle_detail(self) -> None:
        row = self._selected()
        if row is None:
            self._detail.display = False
            return
        if self._detail.display:
            self._detail.display = False
            return
        body = json.dumps(row.reason_detail, indent=2)
        self._detail.update(
            Text.from_markup(
                f"[b {ACCENT_VIOLET}]reason_detail[/]   "
                f"[{FG_2}]icao[/] {row.icao}   "
                f"[{FG_2}]date[/] {row.takeoff_date}\n"
                f"[{FG_2}]reason:[/] {row.reason}\n\n"
            )
            + Text(body, style=FG_0)
        )
        self._detail.display = True

    def focus_filter(self) -> None:
        self._filter.input_widget.focus()

    # --- event handlers ---

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input is self._filter.input_widget:
            self._rerender(event.value or "")

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        self.toggle_detail()

    # --- helpers ---

    def _selected(self) -> SpoofedBroadcast | None:
        if not self._matched:
            return None
        idx = self._table.cursor_row
        if idx is None or idx < 0 or idx >= len(self._matched):
            return None
        return self._matched[idx]

    def _rerender(self, needle: str) -> None:
        self._table.clear()
        self._detail.display = False
        needle_low = needle.lower() if needle else ""
        self._matched = []
        for row in self._rows:
            if needle_low and not self._row_matches(row, needle_low):
                continue
            self._matched.append(row)
            detail = row.reason_detail or {}
            v2 = detail.get("v2_samples")
            sil = detail.get("v2_sil0_pct")
            nic = detail.get("v2_nic0_pct")
            sil_fmt = f"{sil:.1f}" if isinstance(sil, (int, float)) else "-"
            nic_fmt = f"{nic:.1f}" if isinstance(nic, (int, float)) else "-"
            per_source = _format_source_rates(detail) or "-"
            self._table.add_row(
                cell(row.takeoff_date, style=FG_2),
                cell(row.icao, style=ACCENT_CYAN),
                cell(row.callsign or "-", style=FG_0 if row.callsign else FG_2),
                num_cell(f"{v2:,}" if isinstance(v2, int) else str(v2) if v2 is not None else "-", style=FG_0),
                num_cell(sil_fmt, style=ACCENT_VIOLET),
                num_cell(nic_fmt, style=ACCENT_VIOLET),
                cell(per_source, style=FG_2) if per_source != "-" else dash(),
            )
        self._filter.set_counts(matched=len(self._matched), total=len(self._rows))
        if not self._rows:
            self._header.set_crumb("bimodal integrity audit (no rejections yet)")
        else:
            self._header.set_crumb("bimodal integrity audit")
        self._header.set_trailing(f"threshold v2_sil0 ≥ 10% {DOT} min 25 samples")

    @staticmethod
    def _row_matches(row: SpoofedBroadcast, needle: str) -> bool:
        return any(hay and needle in hay.lower() for hay in (row.icao, row.takeoff_date, row.callsign, row.reason))


class _SpoofDetail(Static):
    """Alias so external imports stay stable if we split the widget out later."""
