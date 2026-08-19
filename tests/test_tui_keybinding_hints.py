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

Textual's ``Binding.key`` uses its own internal names for symbol keys
(``slash``, ``colon``, ``question_mark`` for "/", ":", "?") rather than the
display glyph the hint tables show a human. A first pass at these tests
excluded "/"/":"/"?" wholesale from the "resolves to a real binding with
the correct label" check to dodge that name mismatch -- which meant the
exact key from the original bug report ("/") was never actually checked
against app.py's real binding, only against a hardcoded list of old wrong
strings. ``_DISPLAY_TO_APP_KEY`` below maps each display glyph to its real
Textual key name so those three keys go through the *same* resolve-and-
verify check as every digit/letter key, protecting against a future
rebinding (app.py changes what "/" does) or relabeling (a hint's text
drifts from what the key really does) alike.
"""

from __future__ import annotations

import pytest

pytest.importorskip("textual")  # tui extra: pyproject [project.optional-dependencies].tui

from textual.widgets import DataTable  # noqa: E402

from adsbtrack.tui.app import AdsbtrackApp  # noqa: E402
from adsbtrack.tui.views.jump import _HELP_ROWS, HelpScreen, JumpToHex  # noqa: E402
from adsbtrack.tui.widgets import _ACTION_HINTS  # noqa: E402


def _app_binding_keys() -> set[str]:
    return {b.key for b in AdsbtrackApp.BINDINGS}


def _app_bindings() -> dict[str, str]:
    return {b.key: b.action for b in AdsbtrackApp.BINDINGS}


def _datatable_binding_keys() -> set[str]:
    return {b.key for b in DataTable.BINDINGS}


def _modal_bindings() -> dict[str, str]:
    """Real key -> action for the modal-only bindings (escape/dismiss),
    which live on HelpScreen/JumpToHex rather than AdsbtrackApp."""
    out: dict[str, str] = {}
    for cls in (HelpScreen, JumpToHex):
        for b in cls.BINDINGS:
            out[b.key] = b.action
    return out


# Display glyph (what a hint table shows a human) -> Textual's internal
# Binding.key name (what the real BINDINGS lists actually key off of).
_DISPLAY_TO_APP_KEY = {
    "/": "slash",
    ":": "colon",
    "?": "question_mark",
    "esc": "escape",
}


def _resolve(display_key: str) -> str:
    """Translate a hint's display glyph into the real Binding.key name."""
    return _DISPLAY_TO_APP_KEY.get(display_key, display_key)


# Every app-level key a hint might reference, keyed by its *resolved*
# Binding.key name, with the label(s) that correctly describe it. A hint
# whose resolved key is in this table is claiming to be an app-level
# shortcut, so both its existence (real binding) and its label (correct
# description) get checked below -- this is what closes the "/"/":"/"?"
# gap: they resolve into this same table via _DISPLAY_TO_APP_KEY instead
# of being skipped.
_EXPECTED_APP_LABELS: dict[str, set[str]] = {
    "slash": {"filter"},
    "colon": {"jump", "jump to hex"},
    "question_mark": {"help"},
    "q": {"quit"},
    "f": {"ops"},
    "1": {"aircraft"},
    "2": {"flights"},
    "3": {"events"},
    "4": {"spoof"},
    "5": {"map"},
    "6": {"status"},
}

# Same idea, but for bindings that live on the modal screens themselves
# (escape/dismiss) rather than on AdsbtrackApp.
_EXPECTED_MODAL_LABELS: dict[str, set[str]] = {
    "escape": {"back"},
}


def _all_hints() -> list[tuple[str, str]]:
    """Flatten every (key, label) pair from both hint sources."""
    out: list[tuple[str, str]] = list(_ACTION_HINTS)
    for row in _HELP_ROWS:
        out.extend(row)
    return out


# ---------------------------------------------------------------------------
# Full resolution check: every app-level hint key (across BOTH tables)
# must resolve to a real AdsbtrackApp binding and carry the correct label.
# This is the check that protects "/" ":"  "?" -- not just a pinned old-
# wrong-string comparison.
# ---------------------------------------------------------------------------


def test_every_app_level_hint_resolves_to_a_real_binding_with_correct_label():
    app_bindings = _app_bindings()
    checked_resolved_keys: set[str] = set()
    for display_key, label in _all_hints():
        resolved = _resolve(display_key)
        if resolved not in _EXPECTED_APP_LABELS:
            continue
        checked_resolved_keys.add(resolved)
        assert resolved in app_bindings, (
            f"hint key {display_key!r} (resolved: {resolved!r}) is not a real "
            f"AdsbtrackApp binding: {sorted(app_bindings)}"
        )
        assert label in _EXPECTED_APP_LABELS[resolved], (
            f"hint key {display_key!r} (resolved: {resolved!r}) is labeled {label!r}, "
            f"expected one of {_EXPECTED_APP_LABELS[resolved]!r}"
        )
    # Sanity: the three symbol keys actually appeared and got checked --
    # guards against this test silently checking nothing for them if a
    # future edit removes them from the hint tables entirely.
    assert {"slash", "colon", "question_mark"} <= checked_resolved_keys


def test_slash_colon_question_mark_specifically_resolve_and_match():
    """Direct regression pin for the reviewed gap: "/" ":" "?" must each
    resolve to their real Binding.key name and match the action app.py
    actually binds them to, not merely avoid a hardcoded wrong string."""
    app_bindings = _app_bindings()
    assert app_bindings["slash"] == "focus_filter"
    assert app_bindings["colon"] == "jump"
    assert app_bindings["question_mark"] == "help"

    hints = dict(_all_hints())
    assert hints["/"] in _EXPECTED_APP_LABELS["slash"]
    assert hints[":"] in _EXPECTED_APP_LABELS["colon"]
    assert hints["?"] in _EXPECTED_APP_LABELS["question_mark"]


def test_every_modal_level_hint_resolves_to_a_real_binding_with_correct_label():
    """Same idea as the app-level check above, for "esc" -- which lives on
    HelpScreen/JumpToHex's own BINDINGS, not AdsbtrackApp's."""
    modal_bindings = _modal_bindings()
    checked: set[str] = set()
    for display_key, label in _all_hints():
        resolved = _resolve(display_key)
        if resolved not in _EXPECTED_MODAL_LABELS:
            continue
        checked.add(resolved)
        assert resolved in modal_bindings, (
            f"hint key {display_key!r} (resolved: {resolved!r}) is not a real modal binding"
        )
        assert label in _EXPECTED_MODAL_LABELS[resolved], (
            f"hint key {display_key!r} (resolved: {resolved!r}) is labeled {label!r}, "
            f"expected one of {_EXPECTED_MODAL_LABELS[resolved]!r}"
        )
    assert "escape" in checked


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
    """Every hint key that is NOT an app-level or modal-level shortcut
    (i.e. doesn't resolve into _EXPECTED_APP_LABELS or
    _EXPECTED_MODAL_LABELS) must resolve to an actual DataTable binding
    (e.g. up/down/enter/ctrl+home/ctrl+end)."""
    dt_keys = _datatable_binding_keys()
    for row in _HELP_ROWS:
        for key, _label in row:
            resolved = _resolve(key)
            if resolved in _EXPECTED_APP_LABELS or resolved in _EXPECTED_MODAL_LABELS:
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
    app_bindings = _app_bindings()
    for key, label in _ACTION_HINTS:
        if label == "events":
            assert app_bindings.get(key) == "goto('events')"
        if label == "map":
            assert app_bindings.get(key) == "goto('map')"


def test_action_bar_every_non_movement_hint_key_resolves_to_a_real_binding():
    """Every ActionBar hint key that isn't the up/down|enter movement pair
    must resolve (via _DISPLAY_TO_APP_KEY where applicable) to a real
    app-level binding -- including "/" ":" "?", not just "q" and the
    digits."""
    app_bindings = _app_bindings()
    movement = {"up/down", "enter"}
    for key, _label in _ACTION_HINTS:
        if key in movement:
            continue
        resolved = _resolve(key)
        assert resolved in app_bindings, (
            f"{key!r} (resolved: {resolved!r}) is not a real app binding: {sorted(app_bindings)}"
        )
