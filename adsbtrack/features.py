"""Per-flight derived features.

Pure functions that take a ``FlightMetrics`` (and sometimes a ``Flight``
plus small primitives like ``type_code``) and return derived values to
populate on the Flight row. No database, no I/O, no ``Flight`` mutation
except the convenience ``derive_all`` at the bottom which applies every
feature to a Flight in one pass.

The keystone data-extraction work happens in ``classifier.FlightMetrics``
during the trace walk in ``parser.extract_flights``. This module is the
"now turn all those raw counters into the columns the user sees" half.
"""

from __future__ import annotations

import json
import math
import statistics
from collections import Counter
from datetime import datetime
from typing import TYPE_CHECKING

from .classifier import _haversine_m, descent_score
from .ils_alignment import _bearing_deg
from .models import LandingType, MissionType
from .solar import is_night_at

if TYPE_CHECKING:
    from .classifier import FlightMetrics
    from .config import Config
    from .models import Flight


# ----------------------------------------------------------------------
# Mission classification (§2)
# ----------------------------------------------------------------------


def classify_mission(
    *,
    callsign: str | None,
    origin_icao: str | None,
    destination_icao: str | None,
    max_altitude: int | None,
    loiter_ratio: float | None,
    cruise_gs_kt: int | None,
    config: Config,
    owner_operator: str | None = None,
    type_code: str | None = None,
) -> str:
    """Return a mission_type enum string for the flight.

    v4 (§3.6): added owner_operator and type_code inputs so the offshore
    classifier can fall back to operator name (PHI/ERA/Bristow), and the
    `training` enum value is now distinct from `survey`.
    """
    # 1. Operator-based offshore: catches PHI/ERA/Bristow flights even when
    #    the callsign is just a tail number. Runs before pattern/transport
    #    rules so it doesn't lose to a same-airport check.
    if owner_operator:
        op_upper = owner_operator.upper()
        for kw in config.offshore_operator_keywords:
            if kw in op_upper:
                return MissionType.OFFSHORE

    # 2. Callsign-based classification
    if callsign:
        cs = callsign.strip().upper()
        for prefix, mission in config.callsign_prefix_missions.items():
            if cs.startswith(prefix):
                return mission
        if cs.startswith("N911"):
            return MissionType.EMS_HEMS
        if cs.endswith("MT") and not cs.endswith(("LMT", "RMT", "AMT")):
            return MissionType.EMS_HEMS

    # 3. Physics rules
    # exempt large jets / VIP aircraft from survey and pattern.
    # A B748 BBJ or GLF6 with high loiter is in ATC holding or doing a
    # VIP security orbit, not flying a survey grid.
    _exempt_survey_pattern = ("B748", "GLF5", "GLF6", "GLF4", "SJ30", "FA7X", "CL60")

    # Survey: high loiter + low cruise speed (geometric pattern)
    if (
        loiter_ratio is not None
        and loiter_ratio > 3.0
        and cruise_gs_kt is not None
        and cruise_gs_kt < 120
        and type_code not in _exempt_survey_pattern
    ):
        return MissionType.SURVEY

    # Training: same airport + low altitude on a known primary trainer.
    is_trainer_type = type_code in ("C150", "C152", "C172", "C162", "PA28", "DA20", "DA40")
    if (
        origin_icao is not None
        and destination_icao is not None
        and origin_icao == destination_icao
        and max_altitude is not None
        and max_altitude < 5000
        and is_trainer_type
    ):
        return MissionType.TRAINING

    if (
        origin_icao is not None
        and destination_icao is not None
        and origin_icao == destination_icao
        and max_altitude is not None
        and max_altitude < 3000
        and type_code not in _exempt_survey_pattern
    ):
        return MissionType.PATTERN

    if origin_icao is not None and destination_icao is not None and origin_icao != destination_icao:
        return MissionType.TRANSPORT

    # 4. Tail-only callsign with at least one airport assigned: bias to
    #    transport so we don't dump it in unknown by default.
    if callsign and (origin_icao is not None or destination_icao is not None):
        cs_upper = callsign.strip().upper()
        if cs_upper.startswith("N") and cs_upper[1:].isalnum():
            return MissionType.TRANSPORT

    return MissionType.UNKNOWN


# ----------------------------------------------------------------------
# Path metrics (§3, §9)
# ----------------------------------------------------------------------


def compute_path_metrics(
    metrics: FlightMetrics,
    *,
    origin_icao: str | None,
    destination_icao: str | None,
    takeoff_lat: float,
    takeoff_lon: float,
    landing_lat: float | None,
    landing_lon: float | None,
) -> dict:
    """Return a dict of {path_length_km, max_distance_km, loiter_ratio, path_efficiency}."""
    path_length_km = round(metrics.path_length_km, 2) if metrics.path_length_km > 0 else None
    max_distance_km = round(metrics.max_distance_from_origin_km, 2) if metrics.max_distance_from_origin_km > 0 else None

    loiter_ratio: float | None = None
    if path_length_km is not None and max_distance_km is not None and max_distance_km > 0:
        loiter_ratio = round(path_length_km / (2.0 * max_distance_km), 3)

    # v7 H2+F2: redefine path_efficiency as max_distance / path_length.
    # This needs only coordinates (100% coverage) instead of requiring both
    # origin_icao and destination_icao (6.1% coverage after D1 tightening).
    # Measures displacement efficiency: how much of the path was net
    # progress vs backtracking/orbiting. 1.0 = straight line, <0.5 = lots
    # of loitering or return-to-origin.
    path_efficiency: float | None = None
    if path_length_km is not None and path_length_km > 1.0 and max_distance_km is not None and max_distance_km > 0:
        path_efficiency = round(min(1.0, max_distance_km / path_length_km), 3)

    return {
        "path_length_km": path_length_km,
        "max_distance_km": max_distance_km,
        "loiter_ratio": loiter_ratio,
        "path_efficiency": path_efficiency,
    }


# ----------------------------------------------------------------------
# Signal budget (v5 F1)
# ----------------------------------------------------------------------


def compute_signal_budget(metrics: FlightMetrics, *, duration_secs: float) -> dict:
    """Compute active_minutes, signal_gap_secs, signal_gap_count.

    ``duration_secs`` is the wall-clock flight duration (from B1's single
    source of truth). ``active_secs`` is the sum of the phase-of-flight
    counters from the online classifier (climb + descent + level). The gap
    is the difference, clamped to >= 0.
    """
    active_secs = metrics.climb_secs + metrics.descent_secs + metrics.level_secs
    active_secs = min(active_secs, duration_secs)  # can't exceed wall-clock
    gap_secs = max(0, int(round(duration_secs - active_secs)))
    return {
        "active_minutes": round(active_secs / 60.0, 1),
        "signal_gap_secs": gap_secs,
        "signal_gap_count": metrics.signal_gap_count,
    }


# ----------------------------------------------------------------------
# Phase of flight budget (§7)
# ----------------------------------------------------------------------


def compute_phase_budget(metrics: FlightMetrics, *, config: Config, wall_clock_secs: float = 0.0) -> dict:
    """Reconcile level samples into cruise/non-cruise using final max_altitude.

    climb/descent/level seconds are already accumulated online; this function
    splits the level bucket into cruise (alt > ratio * max) and plain level.
    Short flights and low-ceiling flights return NULL cruise fields.

    ``wall_clock_secs`` is the B1 wall-clock duration from parser. When
    provided, the B2 rescale caps the phase sum at this value so the
    rescaled bins match what active_minutes will be clamped to.
    """
    climb_secs = int(round(metrics.climb_secs))
    descent_secs = int(round(metrics.descent_secs))
    level_total = metrics.level_secs

    max_alt = metrics.max_altitude or 0
    min_secs = config.phase_short_flight_min_secs
    min_alt = config.phase_short_flight_min_alt
    ratio = config.phase_cruise_alt_ratio

    # v6 B2 fix: prefer the wall-clock duration from parser (B1's single
    # source of truth) so the rescale target matches what active_minutes
    # will be clamped to. Fall back to metric span for backward compat.
    duration_secs = wall_clock_secs
    if duration_secs <= 0 and metrics.first_point_ts is not None and metrics.last_point_ts is not None:
        duration_secs = metrics.last_point_ts - metrics.first_point_ts

    if max_alt < min_alt or duration_secs < min_secs or not metrics.level_buf:
        return {
            "climb_secs": climb_secs,
            "descent_secs": descent_secs,
            "level_secs": int(round(level_total)),
            "cruise_secs": None,
            "cruise_alt_ft": None,
            "cruise_gs_kt": None,
        }

    cruise_threshold = max_alt * ratio
    cruise_secs_val = 0.0
    cruise_alt_sum = 0.0
    cruise_alt_count = 0
    cruise_gs_sum = 0.0
    cruise_gs_count = 0
    level_secs_val = 0.0
    for dt, alt, gs in metrics.level_buf:
        if alt >= cruise_threshold:
            cruise_secs_val += dt
            cruise_alt_sum += alt * dt
            cruise_alt_count += int(round(dt))
            if gs is not None:
                cruise_gs_sum += gs * dt
                cruise_gs_count += int(round(dt))
        else:
            level_secs_val += dt

    # cap cruise_alt_ft at max_altitude. Cruise can never exceed
    # the peak. Also catches residual N1 cases where altitude source mixing
    # (baro for max, geom fallback in level_buf) produces cruise > max.
    cruise_alt_ft: int | None = None
    if cruise_secs_val > 0 and cruise_alt_count > 0:
        cruise_alt_ft = int(round(cruise_alt_sum / max(1, cruise_secs_val)))
        if max_alt > 0 and cruise_alt_ft > max_alt:
            cruise_alt_ft = max_alt

    # time-weighted median for cruise_gs_kt. Previous v10 used an
    # unweighted median of level_buf gs values, which gave equal weight to
    # each sample point regardless of its duration (dt). Brief tailwind
    # bursts with many closely-spaced trace points were over-represented,
    # producing 319 B407 flights at 150-176 kt (above Vne 140 kt). The
    # time-weighted median expands each sample by its integer-second
    # duration so the statistic reflects the typical cruise speed held
    # over time, not per-sample. 2-sigma rejection still trims tails.
    cruise_gs_kt: int | None = None
    if cruise_gs_count > 0:
        # Build time-weighted gs list: repeat each gs value by its dt in seconds
        weighted_gs: list[float] = []
        for _dt, _alt, _gs in metrics.level_buf:
            if _alt >= cruise_threshold and _gs is not None:
                repeats = max(1, int(round(_dt)))
                weighted_gs.extend([_gs] * repeats)
        if weighted_gs:
            n = len(weighted_gs)
            if n > 2:
                mean_gs = statistics.fmean(weighted_gs)
                stdev = statistics.stdev(weighted_gs)
                lower = mean_gs - 2.0 * max(stdev, 5.0)
                upper = mean_gs + 2.0 * max(stdev, 5.0)
                trimmed = [g for g in weighted_gs if lower <= g <= upper]
            else:
                trimmed = weighted_gs
            cruise_gs_kt = int(round(statistics.median(trimmed if trimmed else weighted_gs)))
            # cap cruise_gs at the persistence-filtered max_gs.
            # Cruise GS is a subset of all GS samples, so the median
            # cannot exceed the max. The v13 removal of this cap caused
            # 3,134 flights (13.8%) to violate the cruise <= max
            # invariant because level_buf stores raw floats while
            # max_gs uses int() with persistence filtering. Capping
            # here (before the type cap in parser.py) ensures both
            # stats agree on the physical peak.
            max_gs = metrics.max_gs_kt
            if max_gs > 0 and cruise_gs_kt is not None and cruise_gs_kt > max_gs:
                cruise_gs_kt = max_gs

    # proportional rescale so the four bins sum to exactly the
    # on-signal active time. The online classifier can double-count
    # boundary points between bins, causing the raw sum to overshoot.
    # active_secs is clamped to duration_secs so we never claim more
    # active time than the wall-clock allows.
    raw_bins = [float(climb_secs), float(descent_secs), level_secs_val, cruise_secs_val]
    raw_total = sum(raw_bins)
    active_secs = min(raw_total, duration_secs) if duration_secs > 0 else raw_total
    if raw_total > 0 and active_secs > 0:
        scale = active_secs / raw_total
        scaled = [b * scale for b in raw_bins]
        # Round with a leftover-drip pass so the integer outputs sum exactly.
        floored = [int(s) for s in scaled]
        remainder = int(round(active_secs)) - sum(floored)
        fracs = [(scaled[i] - floored[i], i) for i in range(4)]
        fracs.sort(reverse=True)
        for j in range(min(abs(remainder), 4)):
            floored[fracs[j][1]] += 1 if remainder > 0 else -1
        climb_secs, descent_secs_out, level_secs_out, cruise_secs_out = floored
    else:
        descent_secs_out = descent_secs
        level_secs_out = int(round(level_secs_val))
        cruise_secs_out = int(round(cruise_secs_val))

    return {
        "climb_secs": climb_secs,
        "descent_secs": descent_secs_out,
        "level_secs": level_secs_out,
        "cruise_secs": cruise_secs_out,
        "cruise_alt_ft": cruise_alt_ft,
        "cruise_gs_kt": cruise_gs_kt,
    }


# ----------------------------------------------------------------------
# Peak climb/descent (§8)
# ----------------------------------------------------------------------


def compute_peak_rates(metrics: FlightMetrics) -> dict:
    """Read peak climb/descent from online accumulators.

    v6 D4: hard-cap at sanity limits. Biz jets cap at ~6,000 fpm climb;
    anything above 10,000 is a sample glitch that survived the rolling-
    window outlier filter. Clamp rather than NULL so the direction is
    preserved.
    """
    _MAX_CLIMB = 10_000
    _MAX_DESCENT = -10_000
    peak_climb = int(round(metrics.peak_climb_fpm)) if metrics.peak_climb_fpm > 0 else None
    peak_descent = int(round(metrics.peak_descent_fpm)) if metrics.peak_descent_fpm < 0 else None
    if peak_climb is not None and peak_climb > _MAX_CLIMB:
        peak_climb = _MAX_CLIMB
    if peak_descent is not None and peak_descent < _MAX_DESCENT:
        peak_descent = _MAX_DESCENT
    return {"peak_climb_fpm": peak_climb, "peak_descent_fpm": peak_descent}


# ----------------------------------------------------------------------
# Hover detection (§4)
# ----------------------------------------------------------------------


def compute_hover(metrics: FlightMetrics, *, type_code: str | None, config: Config) -> dict:
    """Emit hover stats only for rotorcraft. NULL on everything else.

    Any ongoing hover at end-of-flight is folded into the totals.
    """
    if type_code is None or type_code not in config.helicopter_types:
        return {"max_hover_secs": None, "hover_episodes": None}

    max_hover = metrics.max_hover_secs
    episodes = metrics.hover_episodes

    # Fold any ongoing hover at end of flight
    if metrics._hover_start_ts is not None and metrics.last_point_ts is not None:
        ongoing = metrics.last_point_ts - metrics._hover_start_ts
        if ongoing >= config.hover_min_duration_secs:
            episodes += 1
            if ongoing > max_hover:
                max_hover = ongoing

    return {
        "max_hover_secs": int(round(max_hover)) if max_hover > 0 else 0,
        "hover_episodes": episodes,
    }


# ----------------------------------------------------------------------
# Go-around detection (§5)
# ----------------------------------------------------------------------


def compute_go_around(metrics: FlightMetrics, *, landing_type: str, config: Config) -> int:
    """Count go-arounds in the final approach window.

    Only runs on confirmed landings - otherwise the "final descent" anchor
    doesn't exist and the algorithm is meaningless.
    """
    if landing_type != LandingType.CONFIRMED:
        return 0
    if not metrics.approach_alts or metrics.landing_transition_ts is None:
        return 0

    # Restrict to the final lookback window before landing.
    cutoff = metrics.landing_transition_ts - config.go_around_lookback_secs
    window = [(ts, alt) for (ts, alt) in metrics.approach_alts if cutoff <= ts <= metrics.landing_transition_ts]
    if len(window) < 6:
        return 0

    min_rebound = config.go_around_min_rebound_ft
    extremum_sep = config.go_around_local_extremum_sep_ft
    extremum_window = config.go_around_local_extremum_window_secs

    # Find local minima and maxima with neighbor-separation gating to reject
    # baro noise. A point P is a local min if there exists at least one point
    # within ±extremum_window seconds that is >= P.alt + extremum_sep; symmetric
    # for local max.
    #
    # Two-pointer sliding window: since ``window`` is sorted by ts, left and
    # right both advance monotonically across the n iterations, so the total
    # inner work is O(n * avg_neighbors) instead of O(n^2). At typical ADS-B
    # cadences (~5 s) the 30 s extremum window holds ~6 points, so this
    # reduces ~360 k comparisons to ~4 k on a full 600-point window.
    n = len(window)
    is_min = [False] * n
    is_max = [False] * n
    left = 0
    right = 0
    for i in range(n):
        ts_i, alt_i = window[i]
        while left < n and window[left][0] < ts_i - extremum_window:
            left += 1
        while right < n and window[right][0] <= ts_i + extremum_window:
            right += 1
        lower = None
        upper = None
        for j in range(left, right):
            if j == i:
                continue
            alt_j = window[j][1]
            if lower is None or alt_j < lower:
                lower = alt_j
            if upper is None or alt_j > upper:
                upper = alt_j
        if lower is not None and upper is not None:
            if upper - alt_i >= extremum_sep and alt_i <= lower + 1e-6:
                is_min[i] = True
            if alt_i - lower >= extremum_sep and alt_i >= upper - 1e-6:
                is_max[i] = True

    # Walk forward: consume each (min A, max B) pair where B-A >= min_rebound
    count = 0
    i = 0
    while i < n:
        if not is_min[i]:
            i += 1
            continue
        min_alt = window[i][1]
        # Find next local max after i
        j = i + 1
        while j < n and not is_max[j]:
            j += 1
        if j >= n:
            break
        max_alt = window[j][1]
        if max_alt - min_alt >= min_rebound:
            count += 1
        i = j + 1

    return count


# ----------------------------------------------------------------------
# Takeoff / landing heading (§6)
# ----------------------------------------------------------------------


def _circular_mean_deg(headings: list[float]) -> float | None:
    if not headings:
        return None
    xs = sum(math.sin(math.radians(h)) for h in headings)
    ys = sum(math.cos(math.radians(h)) for h in headings)
    if xs == 0 and ys == 0:
        return None
    deg = math.degrees(math.atan2(xs, ys))
    return (deg + 360.0) % 360.0


def compute_headings(metrics: FlightMetrics, *, config: Config) -> dict:
    min_gs = config.heading_min_gs_kts
    heading_window = config.heading_window_secs

    # Takeoff: use already-collected takeoff_tracks buffer, filter by gs
    takeoff_tracks = [h for (_ts, h, gs) in metrics.takeoff_tracks if gs is None or gs > min_gs]
    takeoff_heading = _circular_mean_deg(takeoff_tracks)

    # Landing heading (v4 fix §1.4): the round-3 spec only looked at the
    # last 60 s before touchdown filtered to gs > 40 kt. Helicopters land
    # vertically with gs ≈ 0 for the final 30-60 s, so the median was over
    # an empty set on 1,922 confirmed rotorcraft landings. Fix: walk
    # backwards in widening windows (60s, 120s, 240s, 600s) until we find
    # at least 3 qualifying tracks. Drop the gs threshold to >10 kt for
    # the fallback windows since helicopters approach slowly.
    landing_heading: float | None = None
    if metrics.landing_tracks and metrics.landing_transition_ts is not None:
        # added (600s, 0.0) final fallback so helicopter landings
        # with very low GS (hover approach) still get a heading when enough
        # track samples exist. The v4 fix stopped at gs > 10 kt which still
        # missed 358 B407 confirmed landings.
        # use >= for the zero-floor window so gs=0 points are included,
        # and lower minimum to 1 for the final fallback (a single track sample
        # is still a real heading observation).
        windows = [(heading_window, min_gs, 3), (120.0, 10.0, 3), (240.0, 10.0, 3), (600.0, 10.0, 3), (600.0, 0.0, 1)]
        for window_secs, gs_floor, min_tracks in windows:
            cutoff = metrics.landing_transition_ts - window_secs
            final_tracks = [
                h
                for (ts, h, gs) in metrics.landing_tracks
                if cutoff <= ts <= metrics.landing_transition_ts and (gs is None or gs >= gs_floor)
            ]
            if len(final_tracks) >= min_tracks:
                landing_heading = _circular_mean_deg(final_tracks)
                if landing_heading is not None:
                    break

    # v9 N7 / v10 N11: position-based bearing fallback. When track data is
    # unavailable (coverage hole during approach, or helicopter hover with
    # no track field), compute approach bearing from an earlier airborne
    # position to the landing coordinates. Walk backward through the
    # position buffer to find a point > 50m from touchdown -- helicopters
    # descend nearly vertically so the last few positions can be directly
    # overhead the pad.
    if landing_heading is None and metrics._recent_positions:
        land_lat: float | None = None
        land_lon: float | None = None
        if metrics.landing_lats and metrics.landing_lons:
            land_lat = metrics.landing_lats[0]
            land_lon = metrics.landing_lons[0]
        if land_lat is not None and land_lon is not None:
            for i in range(len(metrics._recent_positions) - 1, -1, -1):
                pos = metrics._recent_positions[i]
                dist = _haversine_m(pos[1], pos[2], land_lat, land_lon)
                if dist > 50:
                    landing_heading = _bearing_deg(pos[1], pos[2], land_lat, land_lon)
                    break
        elif len(metrics._recent_positions) >= 2:
            # No landing coords: use earliest vs latest position in buffer
            first = metrics._recent_positions[0]
            last = metrics._recent_positions[-1]
            dist = _haversine_m(first[1], first[2], last[1], last[2])
            if dist > 50:
                landing_heading = _bearing_deg(first[1], first[2], last[1], last[2])

    # wrap to [0, 360) after rounding so a 359.95 that rounds to
    # 360.0 becomes 0.0. _circular_mean_deg already normalizes but
    # round() can nudge the edge case past the boundary.
    to_hdg = round(takeoff_heading, 1) % 360.0 if takeoff_heading is not None else None
    ldg_hdg = round(landing_heading, 1) % 360.0 if landing_heading is not None else None
    return {
        "takeoff_heading_deg": to_hdg,
        "landing_heading_deg": ldg_hdg,
    }


# ----------------------------------------------------------------------
# DO-260 category (§10)
# ----------------------------------------------------------------------


def classify_category_do260(metrics: FlightMetrics) -> str | None:
    if not metrics.category_counts:
        return None
    counter = Counter(metrics.category_counts)
    # most_common returns sorted by count desc; break ties lexicographically
    top = sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))
    return top[0][0]


# ----------------------------------------------------------------------
# Squawk summary (§1)
# ----------------------------------------------------------------------


def compute_squawk_summary(metrics: FlightMetrics, *, config: Config) -> dict:
    # Flush the final open squawk run before reading durations. Idempotent:
    # safe to call multiple times.
    metrics.flush_open_squawk()

    emergency = None
    if metrics.emergency_squawks_seen:
        # Pick most severe by priority
        prio = config.emergency_squawk_priority
        emergency = max(metrics.emergency_squawks_seen, key=lambda s: prio.get(s, 0))

    vfr = None
    if metrics.squawk_total_count > 0:
        vfr = 1 if metrics.squawk_1200_count / metrics.squawk_total_count >= 0.8 else 0

    had_emergency = 1 if metrics.emergency_squawks_seen else 0
    observed = sorted(metrics.squawk_durations.keys())
    squawks_observed = json.dumps(observed, ensure_ascii=True) if observed else None
    primary_squawk: str | None = None
    if metrics.squawk_durations:
        # sorted() for deterministic tie-break: longest duration first,
        # alphabetically smallest code breaks ties.
        sorted_items = sorted(metrics.squawk_durations.items(), key=lambda kv: (-kv[1], kv[0]))
        primary_squawk = sorted_items[0][0]

    return {
        "squawk_first": metrics.squawk_first,
        "squawk_last": metrics.squawk_last,
        "squawk_changes": metrics.squawk_changes if metrics.squawk_total_count > 0 else None,
        "emergency_squawk": emergency,
        "vfr_flight": vfr,
        "squawks_observed": squawks_observed,
        "had_emergency": had_emergency,
        "primary_squawk": primary_squawk,
    }


# ----------------------------------------------------------------------
# Callsigns history (§12)
# ----------------------------------------------------------------------


def compute_callsigns_summary(metrics: FlightMetrics) -> dict:
    if not metrics.callsigns_seen:
        return {"callsigns": None, "callsign_changes": None, "callsign_count": None}
    unique = sorted(set(metrics.callsigns_seen))
    # cap callsign_changes at max(0, distinct - 1). The online
    # counter in FlightMetrics tracks real transitions between the last-
    # observed value, but legacy data or edge cases could still over-count.
    # The capped value means "how many distinct callsigns were used beyond
    # the first one" which is what analysts actually want.
    capped_changes = min(metrics.callsign_changes, max(0, len(unique) - 1))
    return {
        "callsigns": json.dumps(unique, ensure_ascii=True),
        "callsign_changes": capped_changes,
        "callsign_count": len(unique),
    }


# ----------------------------------------------------------------------
# Day / night (§11)
# ----------------------------------------------------------------------


def compute_day_night(
    *,
    takeoff_time: datetime,
    takeoff_lat: float,
    takeoff_lon: float,
    landing_time: datetime | None,
    landing_lat: float | None,
    landing_lon: float | None,
    metrics: FlightMetrics,
    config: Config,
) -> dict:
    takeoff_night = (
        1
        if is_night_at(
            takeoff_time,
            takeoff_lat,
            takeoff_lon,
            threshold_deg=config.night_sun_altitude_deg,
            lat_lon_quant_deg=config.solar_cache_lat_lon_quant,
            ts_bucket_secs=config.solar_cache_ts_bucket_secs,
        )
        else 0
    )

    landing_night: int | None = None
    if landing_time is not None and landing_lat is not None and landing_lon is not None:
        landing_night = (
            1
            if is_night_at(
                landing_time,
                landing_lat,
                landing_lon,
                threshold_deg=config.night_sun_altitude_deg,
                lat_lon_quant_deg=config.solar_cache_lat_lon_quant,
                ts_bucket_secs=config.solar_cache_ts_bucket_secs,
            )
            else 0
        )

    # Per-point aggregate using recent_points. This is only the tail of the
    # flight (recent_points deque), which is a reasonable approximation for
    # "most of the flight" on short flights and for "the cruise + descent"
    # on long ones. For perfect fidelity we'd keep a full list; we accept the
    # approximation as the cost of bounded memory.
    night_count = 0
    day_count = 0
    for sample in metrics.recent_points:
        if metrics.last_seen_lat is None or metrics.last_seen_lon is None:
            break
        try:
            dt = datetime.fromtimestamp(sample.ts)
        except (OSError, OverflowError, ValueError):
            continue
        if is_night_at(
            dt,
            metrics.last_seen_lat,
            metrics.last_seen_lon,
            threshold_deg=config.night_sun_altitude_deg,
            lat_lon_quant_deg=config.solar_cache_lat_lon_quant,
            ts_bucket_secs=config.solar_cache_ts_bucket_secs,
        ):
            night_count += 1
        else:
            day_count += 1
    total = night_count + day_count
    night_flight: int | None = None
    if total > 0:
        # redefine night_flight as "any phase of the flight occurs
        # during night" per FAR 91.205(c). If either endpoint is night the
        # flight is a night flight. This resolves the inconsistency where
        # long flights could have night_flight=1 but both endpoints day (or
        # vice versa) because the old >=50% sample threshold disagreed with
        # the endpoint flags.
        night_flight = 1 if (takeoff_night == 1 or landing_night == 1) else 0

    return {
        "takeoff_is_night": takeoff_night,
        "landing_is_night": landing_night,
        "night_flight": night_flight,
    }


# ----------------------------------------------------------------------
# Destination inference (§15)
# ----------------------------------------------------------------------


def infer_destination(
    *,
    flight: Flight,
    metrics: FlightMetrics,
    candidates: list,
    config: Config,
    anchor_lat: float | None = None,
    anchor_lon: float | None = None,
) -> dict:
    """Compute probable destination for signal_lost / dropped_on_approach.

    ``candidates`` is a list of airport rows (as returned by
    ``db.find_nearby_airports``) that parser.py has already queried around
    the anchor position. This function is pure - no db calls.

    ``anchor_lat`` / ``anchor_lon`` override the proximity reference point.
    When omitted, the function falls back to ``flight.last_seen_lat`` /
    ``flight.last_seen_lon`` (the historical behavior).
    """
    if flight.landing_type not in (LandingType.SIGNAL_LOST, LandingType.DROPPED_ON_APPROACH):
        return {
            "probable_destination_icao": None,
            "probable_destination_distance_km": None,
            "probable_destination_confidence": None,
        }

    ref_lat = anchor_lat if anchor_lat is not None else flight.last_seen_lat
    ref_lon = anchor_lon if anchor_lon is not None else flight.last_seen_lon
    if ref_lat is None or ref_lon is None or not candidates:
        return {
            "probable_destination_icao": None,
            "probable_destination_distance_km": None,
            "probable_destination_confidence": None,
        }

    max_km = config.prob_dest_max_distance_km
    best = None
    best_dist = float("inf")
    for ap in candidates:
        d_m = _haversine_m(ref_lat, ref_lon, ap["latitude_deg"], ap["longitude_deg"])
        d_km = d_m / 1000.0
        if d_km <= max_km and d_km < best_dist:
            best = ap
            best_dist = d_km

    if best is None:
        return {
            "probable_destination_icao": None,
            "probable_destination_distance_km": None,
            "probable_destination_confidence": None,
        }

    # Confidence factors (altitude factor still uses flight.last_seen_alt_ft
    # - that is a property of the trace-end, not the anchor - and descent
    # score is computed from metrics, both unchanged).
    alt = flight.last_seen_alt_ft or 5000
    alt_factor = max(0.0, min(1.0, (5000.0 - alt) / 4500.0))  # 500ft->1.0, 5000ft->0.0
    prox_factor = max(0.0, min(1.0, 1.0 - best_dist / max_km))
    descent_factor = descent_score(metrics.recent_points)

    confidence = (
        alt_factor * config.prob_dest_alt_weight
        + prox_factor * config.prob_dest_prox_weight
        + descent_factor * config.prob_dest_descent_weight
    )

    return {
        "probable_destination_icao": best["ident"],
        "probable_destination_distance_km": round(best_dist, 2),
        "probable_destination_confidence": round(confidence, 2),
    }


# ----------------------------------------------------------------------
# One-call application
# ----------------------------------------------------------------------


def derive_all(
    flight: Flight,
    metrics: FlightMetrics,
    *,
    config: Config,
    type_code: str | None,
    owner_operator: str | None = None,
) -> None:
    """Mutate ``flight`` in-place with all derived v3 features.

    Must be called AFTER ``classify_landing`` has set ``flight.landing_type``
    and airport matching has populated origin/destination.

    v4 (§3.6): owner_operator is now passed through to the mission classifier
    so PHI/ERA/Bristow flights are correctly tagged offshore even when their
    callsign is just a tail number.

    Destination inference is NOT run here - it needs a candidates list from
    the database. Call ``infer_destination`` separately in the parser.
    """
    # signal budget (active_minutes, signal_gap_secs, signal_gap_count).
    # Must run before phase budget so B2's rescale can clamp to active_secs.
    duration_secs = (flight.duration_minutes or 0.0) * 60.0
    sig = compute_signal_budget(metrics, duration_secs=duration_secs)
    flight.active_minutes = sig["active_minutes"]
    flight.signal_gap_secs = sig["signal_gap_secs"]
    flight.signal_gap_count = sig["signal_gap_count"]

    # fragment count passthrough
    flight.fragments_stitched = metrics.fragments_stitched

    # max_gs_kt cap is deferred to after phase budget so H1 military
    # type override can detect fixed-wing flights and use jet caps.

    # Path metrics
    path = compute_path_metrics(
        metrics,
        origin_icao=flight.origin_icao,
        destination_icao=flight.destination_icao,
        takeoff_lat=flight.takeoff_lat,
        takeoff_lon=flight.takeoff_lon,
        landing_lat=flight.landing_lat,
        landing_lon=flight.landing_lon,
    )
    flight.path_length_km = path["path_length_km"]
    flight.max_distance_km = path["max_distance_km"]
    flight.loiter_ratio = path["loiter_ratio"]
    flight.path_efficiency = path["path_efficiency"]

    # Phase budget (gate altitude-derived fields for altitude_error flights)
    if flight.landing_type != LandingType.ALTITUDE_ERROR:
        phase = compute_phase_budget(metrics, config=config, wall_clock_secs=duration_secs)
        flight.climb_secs = phase["climb_secs"]
        flight.descent_secs = phase["descent_secs"]
        flight.level_secs = phase["level_secs"]
        flight.cruise_secs = phase["cruise_secs"]
        flight.cruise_alt_ft = phase["cruise_alt_ft"]
        flight.cruise_gs_kt = phase["cruise_gs_kt"]

        # cruise_detected flag. 1 when the phase budget found a
        # real cruise segment, 0 when cruise_alt_ft is NULL or set via the
        # fallback below. Makes the NULL informative for analytics.
        flight.cruise_detected = 1 if phase["cruise_alt_ft"] is not None else 0

        # fallback cruise_alt_ft = max_altitude when no stable
        # cruise was detected but the flight is long enough (>10 min).
        # This closes the NULL gap where max_altitude is always populated
        # but cruise_alt_ft can be NULL on constantly-climbing flights.
        if flight.cruise_alt_ft is None and flight.max_altitude is not None:
            dur_min = flight.duration_minutes or 0.0
            if dur_min > 10.0 and flight.max_altitude > 0:
                flight.cruise_alt_ft = flight.max_altitude

        # recompute active_minutes from the rescaled phase secs so
        # the stored columns satisfy climb+cruise+descent+level == active*60
        # by construction. The earlier compute_signal_budget used raw metrics
        # which can diverge from the rescaled values.
        rescaled_active = (
            (flight.climb_secs or 0) + (flight.cruise_secs or 0) + (flight.descent_secs or 0) + (flight.level_secs or 0)
        )
        active_min = round(rescaled_active / 60.0, 1)
        # clamp so active never exceeds wall-clock duration.
        # floor at the metric span so flights with data never show 0.
        dur_min = flight.duration_minutes or 0.0
        if dur_min > 0:
            active_min = min(active_min, dur_min)
        if active_min == 0.0 and metrics.first_point_ts is not None and metrics.last_point_ts is not None:
            metric_span = (metrics.last_point_ts - metrics.first_point_ts) / 60.0
            active_min = round(min(metric_span, dur_min) if dur_min > 0 else metric_span, 1)
        flight.active_minutes = active_min
        flight.signal_gap_secs = max(0, int(round(duration_secs - active_min * 60.0)))

        peak = compute_peak_rates(metrics)
        flight.peak_climb_fpm = peak["peak_climb_fpm"]
        flight.peak_descent_fpm = peak["peak_descent_fpm"]
    else:
        # altitude_error flights skip the phase budget entirely.
        # Set cruise_detected=0 so the column is never NULL.
        flight.cruise_detected = 0

    # heavy signal gap advisory flag. Flights where < 50% of the
    # duration was observed should be excluded from speed analyses.
    dur = flight.duration_minutes or 0.0
    act = flight.active_minutes or 0.0
    flight.heavy_signal_gap = 1 if dur > 0 and (act / dur) < 0.5 else 0

    # Hover (rotorcraft only)
    hov = compute_hover(metrics, type_code=type_code, config=config)
    flight.max_hover_secs = hov["max_hover_secs"]
    flight.hover_episodes = hov["hover_episodes"]

    # Go-around (confirmed landings only)
    flight.go_around_count = compute_go_around(metrics, landing_type=flight.landing_type, config=config)

    # Headings
    headings = compute_headings(metrics, config=config)
    flight.takeoff_heading_deg = headings["takeoff_heading_deg"]
    flight.landing_heading_deg = headings["landing_heading_deg"]

    # per-flight MIL_FW type override for ae69xx ICAOs. These are
    # registered as H60 (Black Hawk) but some flights show fixed-wing
    # profiles (FL350, 400+ kt) from C-17/KC-135/etc sharing the ICAO
    # block. Use OR logic: if EITHER cruise metric exceeds H60 capability,
    # override the effective type to MIL_FW for this flight. This also
    # resolves 13 of the 18 R4 ceiling violations where the H60 ceiling
    # was being applied to jets.
    effective_type = type_code
    if flight.icao.startswith("ae69"):
        cruise_alt = flight.cruise_alt_ft or 0
        cruise_gs = flight.cruise_gs_kt or 0
        # loosened from 15,000/250 to 12,000/220. ae69xx are
        # single military ICAOs logging mixed rotary/fixed-wing profiles;
        # the lower gate catches flights at 10,000-19,000 ft / 180+ kt
        # that are clearly fixed-wing but fell below the old threshold.
        # added max_altitude > 15,000 for flights where no stable
        # cruise was detected (cruise_alt=None) but altitude alone proves
        # fixed-wing -- e.g. ae69d7 at 20,025 ft with 840 data points.
        max_alt = flight.max_altitude or 0
        if cruise_alt > 12_000 or cruise_gs > 220 or max_alt > 15_000:
            effective_type = "MIL_FW"
            flight.type_override = "MIL_FW"

    flight.max_gs_kt = int(metrics.max_gs_kt) if metrics.max_gs_kt > 0 else None

    # Squawks
    sq = compute_squawk_summary(metrics, config=config)
    flight.squawk_first = sq["squawk_first"]
    flight.squawk_last = sq["squawk_last"]
    flight.squawk_changes = sq["squawk_changes"]
    flight.emergency_squawk = sq["emergency_squawk"]
    flight.vfr_flight = sq["vfr_flight"]
    flight.squawks_observed = sq["squawks_observed"]
    flight.had_emergency = sq["had_emergency"]
    flight.primary_squawk = sq["primary_squawk"]

    # Callsigns
    cs = compute_callsigns_summary(metrics)
    flight.callsigns = cs["callsigns"]
    flight.callsign_changes = cs["callsign_changes"]
    flight.callsign_count = cs["callsign_count"]

    # DO-260 category
    flight.category_do260 = classify_category_do260(metrics)

    # Autopilot + detail emergency. H60 / ae69xx are excluded because their
    # nav_altitude_mcp readouts are not reliable cruise-altitude selections.
    if effective_type == "H60" or flight.icao.startswith("ae69"):
        flight.autopilot_target_alt_ft = None
    else:
        flight.autopilot_target_alt_ft = metrics.autopilot_target_alt_ft
    flight.emergency_flag = metrics.emergency_flag

    # v7 R1 final cap: cruise_alt_ft must not exceed the type-ceiling-
    # capped flight.max_altitude. The compute_phase_budget cap uses
    # metrics.max_altitude which is pre-type-ceiling.
    if (
        flight.cruise_alt_ft is not None
        and flight.max_altitude is not None
        and flight.cruise_alt_ft > flight.max_altitude
    ):
        flight.cruise_alt_ft = flight.max_altitude

    # Mission (v4 §3.6: now consults owner_operator and type_code)
    flight.mission_type = classify_mission(
        callsign=flight.callsign,
        origin_icao=flight.origin_icao,
        destination_icao=flight.destination_icao,
        max_altitude=flight.max_altitude,
        loiter_ratio=flight.loiter_ratio,
        cruise_gs_kt=flight.cruise_gs_kt,
        config=config,
        owner_operator=owner_operator,
        type_code=type_code,
    )

    # Day / night.
    # v4 fix (§1.5): COALESCE landing_time/lat with last_seen so dropped /
    # signal_lost / uncertain flights still get a landing_is_night value.
    eff_landing_time = flight.landing_time or flight.last_seen_time
    eff_landing_lat = flight.landing_lat if flight.landing_lat is not None else flight.last_seen_lat
    eff_landing_lon = flight.landing_lon if flight.landing_lon is not None else flight.last_seen_lon
    day_night = compute_day_night(
        takeoff_time=flight.takeoff_time,
        takeoff_lat=flight.takeoff_lat,
        takeoff_lon=flight.takeoff_lon,
        landing_time=eff_landing_time,
        landing_lat=eff_landing_lat,
        landing_lon=eff_landing_lon,
        metrics=metrics,
        config=config,
    )
    flight.takeoff_is_night = day_night["takeoff_is_night"]
    flight.landing_is_night = day_night["landing_is_night"]
    flight.night_flight = day_night["night_flight"]
