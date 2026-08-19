"""Tests for reusable TUI chrome widgets (adsbtrack/tui/widgets.py)."""

from __future__ import annotations

import asyncio

from textual.app import App, ComposeResult

from adsbtrack.tui.views.aircraft import AircraftView
from adsbtrack.tui.views.events import EventsView
from adsbtrack.tui.views.spoof import SpoofView
from adsbtrack.tui.widgets import DOT, PageHeader


def test_page_header_default_crumb_prefix_is_dot():
    """Attribute crumbs (flights, map, status) keep the existing middle-dot
    prefix when a view doesn't opt into a scope-style prefix."""
    header = PageHeader("map", crumb="2026-03-01")
    text = header._build().plain
    assert f"{DOT} 2026-03-01" in text


def test_page_header_custom_crumb_prefix_overrides_dot():
    """Scope crumbs (aircraft, events, spoof) use a `>`-style glyph
    instead of the attribute dot."""
    header = PageHeader("aircraft", crumb="all (12)", crumb_prefix="›")
    text = header._build().plain
    assert "› all (12)" in text
    assert f"{DOT} all (12)" not in text


class _HeaderHarness(App):
    """Minimal app so ``set_crumb`` can call ``Label.update`` (needs a
    mounted app context) without pulling in a whole view."""

    def __init__(self) -> None:
        super().__init__()
        self.header = PageHeader("events", crumb="all aircraft", crumb_prefix="›")

    def compose(self) -> ComposeResult:
        yield self.header


def test_page_header_crumb_prefix_survives_set_crumb():
    """set_crumb() re-renders via the same _build() path, so a
    non-default prefix set at construction time must persist across
    later crumb updates instead of resetting to the dot."""

    async def scenario() -> None:
        app = _HeaderHarness()
        async with app.run_test():
            app.header.set_crumb("aaa111 · last 7d")
            text = app.header._build().plain
            assert "› aaa111" in text
            assert f"{DOT} aaa111" not in text

    asyncio.run(scenario())


def test_aircraft_events_spoof_views_use_scope_crumb_prefix():
    """Aircraft, events, and spoof are the PR's scope crumbs (`›`);
    flights/map/status stay on the default attribute dot."""

    class _ViewsHarness(App):
        def compose(self) -> ComposeResult:
            self.aircraft = AircraftView()
            self.events = EventsView()
            self.spoof = SpoofView()
            yield self.aircraft
            yield self.events
            yield self.spoof

    async def scenario() -> None:
        app = _ViewsHarness()
        async with app.run_test():
            assert app.aircraft._header._crumb_prefix == "›"
            assert app.events._header._crumb_prefix == "›"
            assert app.spoof._header._crumb_prefix == "›"

    asyncio.run(scenario())
