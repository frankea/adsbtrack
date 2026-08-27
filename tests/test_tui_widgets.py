"""Tests for reusable TUI chrome widgets (adsbtrack/tui/widgets.py)."""

from __future__ import annotations

import asyncio

from textual.app import App, ComposeResult

from adsbtrack.tui.views.aircraft import AircraftView
from adsbtrack.tui.views.events import EventsView
from adsbtrack.tui.views.spoof import SpoofView
from adsbtrack.tui.widgets import _OPS, _VIEWS, DOT, OVERLAY_SELECTED, PageHeader, Sidebar, StatusStrip


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


# ---------------------------------------------------------------------------
# Sidebar: exactly one row highlights per active view (#31). All five
# OPERATIONS rows used to share the "ops" key, so set_active("ops") lit
# the whole group at once.
# ---------------------------------------------------------------------------


def test_sidebar_ops_active_highlights_only_the_entry_row():
    rows = Sidebar._render_items(_OPS, highlight="ops")
    highlighted = [row for row in rows if OVERLAY_SELECTED in row]
    assert len(highlighted) == 1, "exactly one OPERATIONS row must highlight when the ops view is active"
    assert "fetch" in highlighted[0], "the highlighted row is the ops view's entry point (the `f` row)"


def test_sidebar_view_rows_highlight_exactly_one():
    for view_id, _, _ in _VIEWS:
        rows = Sidebar._render_items(_VIEWS, highlight=view_id)
        assert sum(OVERLAY_SELECTED in row for row in rows) == 1


def test_sidebar_ops_group_keys_are_unique():
    """Guards the fix's mechanism: the highlight comparison is key equality,
    so duplicate keys inside a group mean multi-row highlights."""
    keys = [key for key, _, _ in _OPS]
    assert len(keys) == len(set(keys))


# ---------------------------------------------------------------------------
# StatusStrip: on a narrow terminal the right side (job, UTC clock) must
# survive; the left side elides instead (#31). The PR #15 chrome rework
# floored the left/right gap at 1 cell, so the composed line overflowed
# the strip and the Label clipped the clock off the right edge.
# ---------------------------------------------------------------------------


class _StripHarness(App):
    def __init__(self) -> None:
        super().__init__()
        self.strip = StatusStrip(
            db_path="/Users/afranke/Projects/adsbtrack/adsbtrack.db",
            flights=12345,
            aircraft=678,
            traces=3_400_000_000,
        )

    def compose(self) -> ComposeResult:
        yield self.strip


def test_status_strip_narrow_terminal_keeps_job_and_clock_visible():
    async def scenario() -> None:
        app = _StripHarness()
        async with app.run_test(size=(70, 24)):
            app.strip.set_job("fetch a1b2c3 2026-08-01")
            built = app.strip._build()
            inner = app.strip.size.width - 2  # padding: 0 1
            assert built.cell_len <= inner, "composed line must fit the strip instead of clipping its right end"
            plain = built.plain
            assert plain.rstrip().endswith("Z"), "UTC clock must stay visible on the right"
            assert "fetch a1b2c3" in plain, "active job must stay visible"
            assert "…" in plain, "the static left half is what gets elided"

    asyncio.run(scenario())


def test_status_strip_wide_terminal_needs_no_elision():
    async def scenario() -> None:
        app = _StripHarness()
        async with app.run_test(size=(160, 24)):
            built = app.strip._build()
            plain = built.plain
            assert "…" not in plain
            assert "/Users/afranke/Projects/adsbtrack/adsbtrack.db" in plain
            assert plain.rstrip().endswith("Z")

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
