"""Tests for adsbtrack.geo, primarily the shared split_on_gaps core."""

from __future__ import annotations

from adsbtrack.geo import split_on_gaps


def test_split_on_gaps_empty_input() -> None:
    assert split_on_gaps([], ts=lambda p: p[0], split_gap_secs=10.0, min_duration_secs=0.0) == []


def test_split_on_gaps_single_contiguous_segment() -> None:
    points = [(0.0,), (1.0,), (2.0,), (3.0,)]
    segments = split_on_gaps(points, ts=lambda p: p[0], split_gap_secs=5.0, min_duration_secs=0.0)
    assert segments == [points]


def test_split_on_gaps_splits_on_time_gap() -> None:
    # Gap of 20s between ts=2 and ts=22 exceeds split_gap_secs=10.
    points = [(0.0,), (1.0,), (2.0,), (22.0,), (23.0,)]
    segments = split_on_gaps(points, ts=lambda p: p[0], split_gap_secs=10.0, min_duration_secs=0.0)
    assert segments == [[(0.0,), (1.0,), (2.0,)], [(22.0,), (23.0,)]]


def test_split_on_gaps_gap_exactly_at_threshold_does_not_split() -> None:
    # cur - prev > split_gap_secs is strict; equal to threshold stays joined.
    points = [(0.0,), (10.0,)]
    segments = split_on_gaps(points, ts=lambda p: p[0], split_gap_secs=10.0, min_duration_secs=0.0)
    assert segments == [points]


def test_split_on_gaps_min_duration_filter_drops_short_segments() -> None:
    # Gap of 19s between ts=1 and ts=20 splits into [0, 1] (duration 1s) and
    # [20, 30, 40] (duration 20s, joined since each internal gap is 10s).
    points = [(0.0,), (1.0,), (20.0,), (30.0,), (40.0,)]
    segments = split_on_gaps(points, ts=lambda p: p[0], split_gap_secs=15.0, min_duration_secs=5.0)
    assert segments == [[(20.0,), (30.0,), (40.0,)]]


def test_split_on_gaps_min_duration_filter_drops_all_when_all_too_short() -> None:
    points = [(0.0,), (1.0,), (100.0,), (101.0,)]
    segments = split_on_gaps(points, ts=lambda p: p[0], split_gap_secs=5.0, min_duration_secs=10.0)
    assert segments == []


def test_split_on_gaps_single_point_segment_has_zero_duration() -> None:
    # A lone point has span 0, so it only survives when min_duration_secs is 0.
    points = [(0.0,), (100.0,)]
    segments = split_on_gaps(points, ts=lambda p: p[0], split_gap_secs=5.0, min_duration_secs=0.0)
    assert segments == [[(0.0,)], [(100.0,)]]

    segments = split_on_gaps(points, ts=lambda p: p[0], split_gap_secs=5.0, min_duration_secs=0.1)
    assert segments == []


def test_split_on_gaps_extra_predicate_filters_segments() -> None:
    # Two qualifying (by duration) segments; extra_predicate keeps only the
    # one whose minimum second element is below a threshold.
    points = [(0.0, 5.0), (1.0, 4.0), (2.0, 3.0), (20.0, 100.0), (21.0, 99.0), (22.0, 98.0)]
    segments = split_on_gaps(
        points,
        ts=lambda p: p[0],
        split_gap_secs=5.0,
        min_duration_secs=1.0,
        extra_predicate=lambda seg: min(v for _, v in seg) < 10.0,
    )
    assert segments == [[(0.0, 5.0), (1.0, 4.0), (2.0, 3.0)]]


def test_split_on_gaps_extra_predicate_receives_full_segment() -> None:
    seen: list[list[tuple[float,]]] = []

    def _record(seg: list[tuple[float,]]) -> bool:
        seen.append(list(seg))
        return True

    points = [(0.0,), (1.0,), (2.0,)]
    split_on_gaps(points, ts=lambda p: p[0], split_gap_secs=5.0, min_duration_secs=0.0, extra_predicate=_record)
    assert seen == [points]
