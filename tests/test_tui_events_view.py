"""Tests for the events view's table layout.

Pins the column order to TIME / ICAO / CALLSIGN / EVENT / SUMMARY -- the
concept's event-feed grid groups the two identity columns (ICAO,
CALLSIGN) together right after the timestamp, with the EVENT pill and
free-text SUMMARY trailing. The previous order put the EVENT pill
second, splitting ICAO from CALLSIGN.
"""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("textual")  # tui extra: pyproject [project.optional-dependencies].tui

from textual.app import App, ComposeResult  # noqa: E402
from textual.widgets import DataTable  # noqa: E402

from adsbtrack.tui.views.events import EventsView  # noqa: E402


def test_events_table_column_order():
    class _Harness(App):
        def compose(self) -> ComposeResult:
            self.view = EventsView()
            yield self.view

    async def scenario() -> None:
        app = _Harness()
        async with app.run_test():
            table = app.view.query_one(DataTable)
            labels = [c.label.plain for c in table.ordered_columns]
            assert labels == ["TIME", "ICAO", "CALLSIGN", "EVENT", "SUMMARY"]

    asyncio.run(scenario())
