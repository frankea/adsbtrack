"""Landing airport-matching anchor selection.

When picking which airport a flight was headed for, the "last observed
point" is often a poor proxy: on signal-loss / dropped-on-approach
flights the aircraft may have drifted laterally or climbed back up after
a missed approach. The altitude minimum within the final N minutes of
the trace is a stronger "where the aircraft was trying to land"
estimate.

This module is a pure function over `FlightMetrics.recent_points`. No
I/O, no DB calls. Falls back to the last observed position when no
in-window sample has altitude data.
"""

from __future__ import annotations

from dataclasses import dataclass

from .classifier import FlightMetrics, _PointSample


@dataclass(frozen=True)
class LandingAnchor:
    """Result of compute_landing_anchor. `method` is either `"alt_min"`
    (anchor chosen from the altitude minimum) or `"last_point"` (fell
    back to last_seen_lat/lon)."""

    lat: float
    lon: float
    method: str  # "alt_min" | "last_point"


def _sample_altitude(sample: _PointSample) -> int | None:
    """Prefer baro altitude; fall back to geometric. Returns None when
    neither is present (sample contributes nothing to alt_min selection)."""
    if sample.baro_alt is not None:
        return sample.baro_alt
    if sample.geom_alt is not None:
        return sample.geom_alt
    return None


def compute_landing_anchor(
    metrics: FlightMetrics,
    *,
    window_minutes: float = 10.0,
) -> LandingAnchor | None:
    """Choose the landing airport-matching anchor from a flight's metrics.

    Walks the tail of `metrics.recent_points` within the final
    `window_minutes` and returns the lowest-altitude sample (tie-broken
    by latest timestamp). Falls back to `metrics.last_seen_lat` /
    `metrics.last_seen_lon` with `method="last_point"` when no sample in
    the window has altitude data.

    Returns None when the metrics carry no usable position data at all
    (empty recent_points AND no last_seen coordinates).
    """
    window_secs = float(window_minutes) * 60.0

    # last_point_ts is the reference; fall back to last_seen_ts then to the
    # most recent sample ts. This lets us compute a window even on metrics
    # that haven't had post-close bookkeeping done.
    ref_ts: float | None = metrics.last_point_ts or metrics.last_seen_ts
    if ref_ts is None and metrics.recent_points:
        ref_ts = metrics.recent_points[-1].ts

    best: _PointSample | None = None
    if ref_ts is not None:
        cutoff = ref_ts - window_secs
        # Iterate newest-first so we can short-circuit on the first sample
        # older than the window cutoff.
        for sample in reversed(metrics.recent_points):
            if sample.ts < cutoff:
                break
            alt = _sample_altitude(sample)
            if alt is None or sample.lat is None or sample.lon is None:
                continue
            if best is None:
                best = sample
                continue
            best_alt = _sample_altitude(best)
            # best_alt is non-None because we only set best when alt was non-None.
            assert best_alt is not None
            if alt < best_alt or (alt == best_alt and sample.ts > best.ts):
                best = sample

    if best is not None:
        return LandingAnchor(lat=best.lat, lon=best.lon, method="alt_min")

    # Fallback: last_seen_*
    if metrics.last_seen_lat is not None and metrics.last_seen_lon is not None:
        return LandingAnchor(
            lat=metrics.last_seen_lat,
            lon=metrics.last_seen_lon,
            method="last_point",
        )
    return None
