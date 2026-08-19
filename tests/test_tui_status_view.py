"""Tests for the status view's activity strip (Task A1).

The old strip was generated from ``sin(flight_count)`` with amber markers
at ``i % 17`` -- fake data dressed up as telemetry. These tests pin the
replacement: the strip must be built entirely from real per-day
``DailyActivity`` rows (see ``adsbtrack.tui.queries.daily_activity``), with
no synthetic fallback left in the rendered card.
"""

from __future__ import annotations

import re

import pytest

pytest.importorskip("textual")  # tui extra: pyproject [project.optional-dependencies].tui

from adsbtrack.tui.queries import DailyActivity  # noqa: E402
from adsbtrack.tui.views.status import (  # noqa: E402
    _ACTIVITY_DAYS,
    _ACTIVITY_GLYPHS,
    _activity_bar_index,
    _activity_spark_markup,
    _build_signal_body,
)
from adsbtrack.tui.widgets import ACCENT_AMBER, ACCENT_OK  # noqa: E402


def _day(date: str, flight_count: int = 0, flagged: bool = False) -> DailyActivity:
    return DailyActivity(date=date, flight_count=flight_count, flagged=flagged)


# ---------------------------------------------------------------------------
# _activity_bar_index: pure scaling helper
# ---------------------------------------------------------------------------


def test_activity_bar_index_zero_flights_is_lowest_glyph():
    assert _activity_bar_index(0, max_count=10, n_glyphs=len(_ACTIVITY_GLYPHS)) == 0


def test_activity_bar_index_zero_max_count_is_lowest_glyph():
    """No flights anywhere in the window: nothing should scale above zero."""
    assert _activity_bar_index(0, max_count=0, n_glyphs=len(_ACTIVITY_GLYPHS)) == 0


def test_activity_bar_index_any_positive_count_rises_off_baseline():
    assert _activity_bar_index(1, max_count=100, n_glyphs=len(_ACTIVITY_GLYPHS)) >= 1


def test_activity_bar_index_busiest_day_hits_top_glyph():
    n = len(_ACTIVITY_GLYPHS)
    assert _activity_bar_index(10, max_count=10, n_glyphs=n) == n - 1


def test_activity_bar_index_monotonic_in_count():
    n = len(_ACTIVITY_GLYPHS)
    low = _activity_bar_index(2, max_count=10, n_glyphs=n)
    high = _activity_bar_index(8, max_count=10, n_glyphs=n)
    assert high >= low


# ---------------------------------------------------------------------------
# _activity_spark_markup: one glyph per real day, amber where flagged
# ---------------------------------------------------------------------------


def test_activity_spark_markup_one_glyph_per_day():
    activity = [_day(f"2026-06-{d:02d}", flight_count=d % 3) for d in range(1, 30)]
    markup = _activity_spark_markup(activity)
    glyphs_found = [ch for ch in markup if ch in _ACTIVITY_GLYPHS]
    assert len(glyphs_found) == len(activity)


def test_activity_spark_markup_zero_day_is_lowest_glyph():
    activity = [_day("2026-06-01", flight_count=0)]
    markup = _activity_spark_markup(activity)
    assert _ACTIVITY_GLYPHS[0] in markup


def test_activity_spark_markup_flagged_day_is_amber():
    activity = [_day("2026-06-01", flight_count=1, flagged=True)]
    markup = _activity_spark_markup(activity)
    assert f"[{ACCENT_AMBER}]" in markup


def test_activity_spark_markup_unflagged_day_is_not_amber():
    activity = [_day("2026-06-01", flight_count=1, flagged=False)]
    markup = _activity_spark_markup(activity)
    assert ACCENT_AMBER not in markup
    assert f"[{ACCENT_OK}]" in markup


def test_activity_spark_markup_amber_count_matches_flagged_days():
    activity = [
        _day("2026-06-01", flight_count=1, flagged=True),
        _day("2026-06-02", flight_count=2, flagged=False),
        _day("2026-06-03", flight_count=0, flagged=True),
        _day("2026-06-04", flight_count=3, flagged=False),
    ]
    markup = _activity_spark_markup(activity)
    assert markup.count(f"[{ACCENT_AMBER}]") == 2
    assert markup.count(f"[{ACCENT_OK}]") == 2


def test_activity_spark_markup_empty_activity_is_empty_string():
    assert _activity_spark_markup([]) == ""


# ---------------------------------------------------------------------------
# _build_signal_body: the whole card, no synthetic component left
# ---------------------------------------------------------------------------


def test_signal_body_has_no_synthetic_placeholder_language():
    snap = {"spoof_count": 0, "activity": [_day("2026-06-01", flight_count=1)]}
    text = _build_signal_body(snap).plain
    assert "signal quality" not in text.lower()
    assert "placeholder" not in text.lower()
    assert "not real uptime" not in text.lower()


def test_signal_body_uses_honest_label_and_caption():
    snap = {"spoof_count": 0, "activity": [_day("2026-06-01", flight_count=1)]}
    text = _build_signal_body(snap).plain
    assert "ACTIVITY (52D)" in text
    assert "flights/day - amber = emergency or spoof day" in text


def test_signal_body_renders_real_bar_count():
    activity = [_day(f"2026-06-{d:02d}", flight_count=d % 4) for d in range(1, _ACTIVITY_DAYS + 1)]
    snap = {"spoof_count": 0, "activity": activity}
    text = _build_signal_body(snap).plain
    lines = text.splitlines()
    spark_line = next(line for line in lines if any(ch in _ACTIVITY_GLYPHS for ch in line))
    glyph_count = sum(1 for ch in spark_line if ch in _ACTIVITY_GLYPHS)
    assert glyph_count == len(activity)


def test_signal_body_handles_missing_activity_key_without_crashing():
    """status_snapshot's own dict may not carry an "activity" key yet in
    older call sites/tests -- the card must degrade to an empty strip
    rather than raising."""
    snap = {"spoof_count": 0}
    text = _build_signal_body(snap).plain
    assert "ACTIVITY (52D)" in text


def test_signal_body_no_fake_sine_derived_pattern():
    """Regression guard: the old body derived every bar from
    total_flights + i via sin(), so identical snaps with different
    'activity' payloads must render different strips. A leftover
    sin()-based fallback would make these two renders identical."""
    snap_a = {"spoof_count": 0, "activity": [_day("2026-06-01", flight_count=0)] * 10}
    snap_b = {"spoof_count": 0, "activity": [_day(f"2026-06-{d:02d}", flight_count=d) for d in range(1, 11)]}
    assert _build_signal_body(snap_a).plain != _build_signal_body(snap_b).plain


def test_activity_glyphs_ramp_matches_module_constant_length():
    assert re.fullmatch(r"[▁-█]+", _ACTIVITY_GLYPHS)
