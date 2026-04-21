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
