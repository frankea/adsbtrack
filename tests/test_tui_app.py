"""Headless smoke tests for the Textual TUI.

Instantiates the full ``AdsbtrackApp`` against an empty-but-initialised
DB and cycles through every view via the keyboard bindings, asserting
the ContentSwitcher flips current without raising. This catches import
errors, compose-time crashes, and per-view render failures before a
human ever launches the TUI.
"""

from __future__ import annotations

import pytest

pytest.importorskip("textual")

from adsbtrack.db import Database
from adsbtrack.tui.app import AdsbtrackApp

pytestmark = pytest.mark.asyncio


@pytest.fixture
def empty_db(tmp_path):
    db_path = tmp_path / "tui.db"
    with Database(db_path) as db:
        db.commit()
    return db_path


async def test_app_mounts_and_switches_views(empty_db):
    app = AdsbtrackApp(empty_db)
    async with app.run_test() as pilot:
        # ContentSwitcher default lands on aircraft
        from textual.widgets import ContentSwitcher

        assert app.query_one(ContentSwitcher).current == "view-aircraft"

        # Cycle through every view keyboard shortcut without crashing.
        # The ones scoped to an ICAO are skipped when no aircraft is selected
        # (a warning notification is emitted instead; not an exception).
        for key in ("3", "4", "5", "6", "f", "1"):
            await pilot.press(key)
            await pilot.pause()


async def test_jump_overlay_opens_and_dismisses(empty_db):
    app = AdsbtrackApp(empty_db)
    async with app.run_test() as pilot:
        # Trigger the binding directly so the test is keymap-independent.
        await app.run_action("jump")
        await pilot.pause()
        from adsbtrack.tui.views.jump import JumpToHex

        assert any(isinstance(s, JumpToHex) for s in app.screen_stack)
        await pilot.press("escape")
        await pilot.pause()
        assert not any(isinstance(s, JumpToHex) for s in app.screen_stack)


async def test_help_overlay_opens_and_dismisses(empty_db):
    app = AdsbtrackApp(empty_db)
    async with app.run_test() as pilot:
        await app.run_action("help")
        await pilot.pause()
        from adsbtrack.tui.views.jump import HelpScreen

        assert any(isinstance(s, HelpScreen) for s in app.screen_stack)
        await pilot.press("escape")
        await pilot.pause()
        assert not any(isinstance(s, HelpScreen) for s in app.screen_stack)


async def test_app_navigates_after_selecting_aircraft(seeded_db):
    """After an ICAO is picked, the flights/map/status views render."""
    app = AdsbtrackApp(seeded_db)
    async with app.run_test() as pilot:
        from textual.widgets import ContentSwitcher, DataTable

        from adsbtrack.tui.views.flights import FlightsView
        from adsbtrack.tui.views.status import StatusView
        from adsbtrack.tui.widgets import Sidebar

        app._open_icao("aaa111")
        await pilot.pause()
        switcher = app.query_one(ContentSwitcher)
        sidebar = app.query_one(Sidebar)
        # _open_icao routes to flights and tags the sidebar in lockstep.
        assert switcher.current == "view-flights"
        assert sidebar._active == "flights"

        # The seeded aaa111 has one confirmed flight and a full registry row;
        # assert both render through to the view content, not just the
        # switcher toggle, so a silently-swallowed set_icao would fail here.
        flights_table = app.query_one(FlightsView).query_one(DataTable)
        assert flights_table.row_count == 1

        for key, target, sidebar_active in (
            ("5", "view-map", "map"),
            ("6", "view-status", "status"),
            ("3", "view-events", "events"),
            ("4", "view-spoof", "spoof"),
            ("1", "view-aircraft", "aircraft"),
            ("2", "view-flights", "flights"),
        ):
            await pilot.press(key)
            await pilot.pause()
            assert switcher.current == target, f"after pressing {key!r}"
            assert sidebar._active == sidebar_active, f"sidebar after {key!r}"

        # After landing on status (key "6" above will have refreshed the
        # grid), walk the mounted cards and assert the seeded registration
        # from aircraft_registry made it through to a rendered card. A
        # regression that silently dropped registry merge in status_snapshot
        # would leave "N111AA" out of the grid entirely.
        await pilot.press("6")
        await pilot.pause()
        rendered = "".join(str(child.render()) for child in app.query_one(StatusView)._grid.children)
        assert "N111AA" in rendered
