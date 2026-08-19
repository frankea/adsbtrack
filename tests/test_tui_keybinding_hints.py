"""Pins the keyboard-shortcut hints in the help overlay and action bar to
the app's *real* bindings (Task A2).

Before this fix: "/" was labeled "search" (it actually runs
action_focus_filter), "f" was labeled "filter" (it actually runs
goto('ops')), and "j" / "k" / "g g" / "G" / "e" / "m" were listed as if
they were real shortcuts when no such binding exists anywhere in the app
(checked against both AdsbtrackApp.BINDINGS and DataTable's own built-in
cursor bindings). These tests cross-check every hint so a future mislabel
fails loudly instead of silently teaching users a keymap that doesn't
exist.
"""

from __future__ import annotations

import pytest

pytest.importorskip("textual")  # tui extra: pyproject [project.optional-dependencies].tui

from textual.widgets import DataTable  # noqa: E402

from adsbtrack.tui.app import AdsbtrackApp  # noqa: E402
from adsbtrack.tui.views.jump import _HELP_ROWS  # noqa: E402
from adsbtrack.tui.widgets import _ACTION_HINTS  # noqa: E402


def _app_binding_keys() -> set[str]:
    return {b.key for b in AdsbtrackApp.BINDINGS}


def _datatable_binding_keys() -> set[str]:
    return {b.key for b in DataTable.BINDINGS}


# ---------------------------------------------------------------------------
# HelpScreen
# ---------------------------------------------------------------------------


def test_help_screen_view_switch_row_matches_app_bindings():
    """The 1-6 + f row must match app.py's real goto(...) targets exactly."""
    view_row = dict(_HELP_ROWS[2])
    assert view_row == {
        "1": "aircraft",
        "2": "flights",
        "3": "events",
        "4": "spoof",
        "5": "map",
        "6": "status",
        "f": "ops",
    }


def test_help_screen_slash_and_f_labels_are_not_the_old_wrong_labels():
    """Regression pin for the exact reported bug: '/' mislabeled "search"
    (really focus_filter) and 'f' mislabeled "filter" (really
    goto('ops'))."""
    hints: dict[str, str] = {}
    for row in _HELP_ROWS:
        for key, label in row:
            hints.setdefault(key, label)
    assert hints["/"] != "search"
    assert hints["f"] != "filter"
    assert hints["f"] == "ops"


def test_help_screen_lists_no_fabricated_bindings():
    """j / k / g g / G / e / m must not appear -- no such bindings exist."""
    fabricated = {"j", "k", "g g", "G", "e", "m"}
    hint_keys = {key for row in _HELP_ROWS for key, _ in row}
    assert hint_keys.isdisjoint(fabricated), hint_keys & fabricated


def test_help_screen_movement_hints_are_real_datatable_bindings():
    """Every hint key outside the app-level chrome set must resolve to an
    actual DataTable binding (e.g. up/down/enter/ctrl+home/ctrl+end)."""
    dt_keys = _datatable_binding_keys()
    app_level_keys = {"/", "f", ":", "esc", "?", "q", "1", "2", "3", "4", "5", "6"}
    for row in _HELP_ROWS:
        for key, _label in row:
            if key in app_level_keys:
                continue
            assert key in dt_keys, f"{key!r} is not a real DataTable binding: {sorted(dt_keys)}"


# ---------------------------------------------------------------------------
# ActionBar
# ---------------------------------------------------------------------------


def test_action_bar_hints_never_use_fabricated_keys():
    fabricated = {"j/k", "j", "k", "g g", "G", "e", "m"}
    hint_keys = {key for key, _ in _ACTION_HINTS}
    assert hint_keys.isdisjoint(fabricated), hint_keys & fabricated


def test_action_bar_slash_and_f_labels_are_not_the_old_wrong_labels():
    hints = dict(_ACTION_HINTS)
    assert hints["/"] != "search"
    assert hints["f"] != "filter"
    assert hints["f"] == "ops"


def test_action_bar_view_shortcuts_reference_real_bindings():
    """Any hint claiming to open "events" or "map" must key off the real
    app-level binding for that view (3 and 5), not an invented letter."""
    app_bindings = {b.key: b.action for b in AdsbtrackApp.BINDINGS}
    for key, label in _ACTION_HINTS:
        if label == "events":
            assert app_bindings.get(key) == "goto('events')"
        if label == "map":
            assert app_bindings.get(key) == "goto('map')"


def test_action_bar_every_view_switch_hint_matches_a_real_binding():
    """Any single-character hint key that isn't chrome (/, :, ?, q) must
    correspond to a real app-level binding."""
    app_bindings = {b.key: b.action for b in AdsbtrackApp.BINDINGS}
    chrome = {"/", ":", "?", "q"}
    movement = {"up/down", "enter"}
    for key, _label in _ACTION_HINTS:
        if key in chrome or key in movement:
            continue
        assert key in app_bindings, f"{key!r} is not a real app binding: {sorted(app_bindings)}"
