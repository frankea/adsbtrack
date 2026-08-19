"""Tests for the spoof view's expandable detail rows.

Ports PR #16's expand/collapse behaviour onto the current SpoofView:
a trailing `+`/`-` indicator column, expanded-row state that survives
a filter re-render (unless the expanded row itself got filtered out),
and cursor position preserved (by underlying icao/date identity, not
raw index) across a filter re-render.
"""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("textual")  # tui extra: pyproject [project.optional-dependencies].tui

from textual.app import App, ComposeResult  # noqa: E402
from textual.widgets import DataTable  # noqa: E402

from adsbtrack.tui.queries import SpoofedBroadcast  # noqa: E402
from adsbtrack.tui.views.spoof import SpoofView  # noqa: E402


def _row(icao: str, date: str, callsign: str = "CS1") -> SpoofedBroadcast:
    return SpoofedBroadcast(
        icao=icao,
        takeoff_time=f"{date}T00:00:00+00:00",
        takeoff_date=date,
        callsign=callsign,
        max_altitude=250,
        reason="bimodal_integrity",
        reason_detail={"v2_samples": 100, "v2_sil0_pct": 25.0, "v2_nic0_pct": 30.0},
        detected_at=f"{date}T01:00:00+00:00",
    )


class _Harness(App):
    def compose(self) -> ComposeResult:
        self.view = SpoofView()
        yield self.view


def test_indicator_column_present_and_collapsed_by_default():
    async def scenario() -> None:
        app = _Harness()
        async with app.run_test():
            app.view._rows = [_row("aaa111", "2026-01-01")]
            app.view._rerender("")
            table = app.view.query_one(DataTable)
            assert table.ordered_columns[-1].label.plain == ""
            assert table.get_row_at(0)[-1].plain == "+"

    asyncio.run(scenario())


def test_toggle_detail_expands_shows_minus_and_detail_pane():
    async def scenario() -> None:
        app = _Harness()
        async with app.run_test():
            app.view._rows = [_row("aaa111", "2026-01-01")]
            app.view._rerender("")
            table = app.view.query_one(DataTable)
            table.move_cursor(row=0)
            app.view.toggle_detail()
            assert app.view._detail.display is True
            assert table.get_row_at(0)[-1].plain == "−"

    asyncio.run(scenario())


def test_toggle_detail_twice_collapses():
    async def scenario() -> None:
        app = _Harness()
        async with app.run_test():
            app.view._rows = [_row("aaa111", "2026-01-01")]
            app.view._rerender("")
            table = app.view.query_one(DataTable)
            table.move_cursor(row=0)
            app.view.toggle_detail()
            app.view.toggle_detail()
            assert app.view._detail.display is False
            assert table.get_row_at(0)[-1].plain == "+"

    asyncio.run(scenario())


def test_expanded_row_survives_a_filter_that_still_matches_it():
    async def scenario() -> None:
        app = _Harness()
        async with app.run_test():
            app.view._rows = [
                _row("aaa111", "2026-01-01", callsign="KEEP-A"),
                _row("bbb222", "2026-01-02", callsign="DROPME"),
            ]
            app.view._rerender("")
            table = app.view.query_one(DataTable)
            table.move_cursor(row=0)
            app.view.toggle_detail()
            assert app.view._detail.display is True

            app.view._rerender("keep")  # narrows to just aaa111, still expanded
            assert app.view._detail.display is True
            assert table.row_count == 1
            assert table.get_row_at(0)[-1].plain == "−"

    asyncio.run(scenario())


def test_expanded_row_collapses_when_filtered_out():
    async def scenario() -> None:
        app = _Harness()
        async with app.run_test():
            app.view._rows = [
                _row("aaa111", "2026-01-01", callsign="EXPANDME"),
                _row("bbb222", "2026-01-02", callsign="OTHER"),
            ]
            app.view._rerender("")
            table = app.view.query_one(DataTable)
            table.move_cursor(row=0)
            app.view.toggle_detail()
            assert app.view._detail.display is True

            app.view._rerender("other")  # narrows to bbb222 only; the expanded row drops out
            assert app.view._detail.display is False
            assert app.view._expanded_key is None

    asyncio.run(scenario())


def test_cursor_reseats_on_underlying_row_after_filter_shifts_indices():
    async def scenario() -> None:
        app = _Harness()
        async with app.run_test():
            app.view._rows = [
                _row("aaa111", "2026-01-01", callsign="KEEP-A"),
                _row("bbb222", "2026-01-02", callsign="DROPME"),
                _row("ccc333", "2026-01-03", callsign="KEEP-C"),
            ]
            app.view._rerender("")
            table = app.view.query_one(DataTable)
            table.move_cursor(row=2)  # sitting on ccc333 before filtering

            app.view._rerender("keep")  # drops bbb222; ccc333 shifts from index 2 to index 1
            assert table.row_count == 2
            assert table.cursor_row == 1, "cursor must follow ccc333 to its new index, not stay at the old row number"
            selected = app.view._selected()
            assert selected is not None
            assert selected.icao == "ccc333"

    asyncio.run(scenario())
