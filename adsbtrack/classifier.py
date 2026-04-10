"""Classify flight endings and score confidence.

Determines whether a flight ended with a confirmed landing, signal loss,
dropped on approach, altitude encoding error, or is uncertain. Computes
confidence scores for takeoff and landing data quality.

v2 changes (Apr 2026):
- Uses geometric altitude (trace index 10) in addition to barometric (index 3)
  to detect the Bell 407 hover-at-altitude-with-baro=ground pathology.
- Uses barometric vertical rate (trace index 7) for descent detection with
  a wall-clock time window instead of a point-count window.
- Landing confidence uses a weighted geometric mean so any single failing
  factor drags the whole score down instead of being averaged away.
- Per-type endurance cap (Config.type_endurance_minutes) replaces the
  global max_endurance_minutes for flights where the type is known.
- Adds dropped_on_approach landing type for signal-lost flights that show
  a clear descent trajectory at the last observed point.
- takeoff_type distinguishes "observed" (saw a ground-to-airborne transition)
  from "found_mid_flight" (first trace point was already airborne).
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field


@dataclass
class _PointSample:
    """A lightweight snapshot of a trace point kept for descent scoring."""

    ts: float  # absolute unix timestamp
    baro_alt: int | None  # None when trace reports 'ground'
    geom_alt: int | None
    gs: float | None
    baro_rate: float | None


@dataclass
class FlightMetrics:
    """Raw signal metrics accumulated during trace processing."""

    data_points: int = 0
    total_ground_points: int = 0  # any point where classify_ground_state said "ground"
    baro_error_points: int = 0  # baro=ground but geom disagreed (airborne override)
    sources: set[str] = field(default_factory=set)
    max_altitude: int = 0
    last_airborne_alt: int | None = None  # last airborne baro altitude
    last_airborne_geom: int | None = None
    last_airborne_gs: float | None = None
    last_airborne_baro_rate: float | None = None
    ground_points_at_takeoff: int = 0
    ground_points_at_landing: int = 0
    ground_speed_while_ground: int = 0  # baro=ground with gs > landing threshold
    landing_lats: list[float] = field(default_factory=list)
    landing_lons: list[float] = field(default_factory=list)
    # Rolling wall-clock window of recent points for descent analysis.
    # Only the last ~300 seconds are needed; we keep the last 40 points as a cap.
    recent_points: deque = field(default_factory=lambda: deque(maxlen=40))
    # Takeoff category: "observed" (saw ground -> airborne) or "found_mid_flight"
    takeoff_type: str = "unknown"

    def record_point(
        self,
        *,
        baro_alt: int | str | None,
        geom_alt: int | None,
        gs: float | None,
        baro_rate: float | None,
        lat: float,
        lon: float,
        ts: float,
        ground_state: str,
        ground_reason: str,
        landing_speed_threshold: float = 80.0,
    ) -> None:
        """Record a single trace point into the running metrics.

        ground_state is the output of classify_ground_state ("ground" / "airborne" / "unknown").
        ground_reason is "ok" / "baro_error" / "insufficient".
        """
        self.data_points += 1

        # Rolling wall-clock window sample for descent analysis
        sample_baro_alt = None
        if isinstance(baro_alt, (int, float)):
            sample_baro_alt = int(baro_alt)
        self.recent_points.append(
            _PointSample(
                ts=ts,
                baro_alt=sample_baro_alt,
                geom_alt=int(geom_alt) if isinstance(geom_alt, (int, float)) else None,
                gs=gs,
                baro_rate=baro_rate,
            )
        )

        if ground_state == "ground":
            self.total_ground_points += 1
            if ground_reason == "baro_error":
                # Shouldn't happen: baro_error forces airborne. But count defensively.
                self.baro_error_points += 1
        elif ground_state == "airborne" and ground_reason == "baro_error":
            self.baro_error_points += 1

        # Track last airborne signals for confidence scoring
        if ground_state == "airborne":
            if isinstance(baro_alt, (int, float)):
                self.last_airborne_alt = int(baro_alt)
                if baro_alt > self.max_altitude:
                    self.max_altitude = int(baro_alt)
            elif isinstance(geom_alt, (int, float)):
                # Fall back to geom when baro is 'ground' but we know we're airborne
                self.last_airborne_alt = int(geom_alt)
                if geom_alt > self.max_altitude:
                    self.max_altitude = int(geom_alt)
            if isinstance(geom_alt, (int, float)):
                self.last_airborne_geom = int(geom_alt)
            if gs is not None:
                self.last_airborne_gs = gs
            if baro_rate is not None:
                self.last_airborne_baro_rate = baro_rate

        # Legacy altitude_error heuristic: baro=ground + high gs
        if baro_alt == "ground" and gs is not None and gs > landing_speed_threshold:
            self.ground_speed_while_ground += 1

    def record_landing_ground_point(self, lat: float, lon: float) -> None:
        self.ground_points_at_landing += 1
        self.landing_lats.append(lat)
        self.landing_lons.append(lon)

    def landing_coord_spread(self) -> float:
        """Max spread in degrees across landing ground points."""
        if len(self.landing_lats) < 2:
            return 0.0
        lat_spread = max(self.landing_lats) - min(self.landing_lats)
        lon_spread = max(self.landing_lons) - min(self.landing_lons)
        return max(lat_spread, lon_spread)


# ----------------------------------------------------------------------
# Point classification (baro + geom fusion)
# ----------------------------------------------------------------------


def classify_ground_state(
    baro_alt: int | str | None,
    geom_alt: int | None,
    gs: float | None,
    *,
    landing_speed_threshold: float = 80.0,
    baro_error_geom_threshold: float = 300.0,
) -> tuple[str, str]:
    """Classify a single trace point as ground / airborne / unknown.

    Returns (state, reason) where state is one of:
      - "ground"   - aircraft is on the surface
      - "airborne" - aircraft is flying (including hover)
      - "unknown"  - insufficient data to decide

    And reason is:
      - "ok"              - agreement between available signals
      - "baro_error"      - baro reports ground but geom altitude disagrees
      - "speed_override"  - baro reports ground but ground speed is high
      - "insufficient"    - no usable altitude data
    """
    baro_is_ground = baro_alt == "ground"
    baro_low = isinstance(baro_alt, (int, float)) and baro_alt < 50
    geom_low = isinstance(geom_alt, (int, float)) and geom_alt < 200
    geom_high = isinstance(geom_alt, (int, float)) and geom_alt > baro_error_geom_threshold

    # Bell 407 pathology: baro reports ground but geometric altitude is well
    # above ground level. The aircraft is actually hovering or in the pattern.
    if baro_is_ground and geom_high:
        return ("airborne", "baro_error")

    # Speed override: baro says ground but ground speed is clearly above
    # landing threshold. Strict greater-than so gs exactly at the threshold
    # is still treated as a valid landing (matches historical behavior).
    if baro_is_ground and gs is not None and gs > landing_speed_threshold:
        return ("airborne", "speed_override")

    # Strong ground: baro says ground (and no overriding signals)
    if baro_is_ground:
        return ("ground", "ok")

    # Strong ground: both altitudes low
    if baro_low and (geom_low or geom_alt is None):
        return ("ground", "ok")

    # Strong airborne: baro clearly above ground
    if isinstance(baro_alt, (int, float)) and baro_alt >= 50:
        return ("airborne", "ok")

    # Fallback to geom
    if isinstance(geom_alt, (int, float)) and geom_alt >= 200:
        return ("airborne", "ok")
    if isinstance(geom_alt, (int, float)) and geom_alt < 200:
        return ("ground", "ok")

    return ("unknown", "insufficient")


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _lerp(value: float, low: float, high: float) -> float:
    """Linear interpolation clamped to [0, 1]. Returns 0.0 at low, 1.0 at high.
    If high < low, reverses the mapping (useful for "lower is better")."""
    if high == low:
        return 1.0 if value >= high else 0.0
    if high > low:
        return max(0.0, min(1.0, (value - low) / (high - low)))
    # Reversed: high < low means lower input = higher output
    return max(0.0, min(1.0, (low - value) / (low - high)))


def descent_score(
    recent_points: deque,
    *,
    window_secs: float = 120.0,
) -> float:
    """Return a score in [0, 1] where 1.0 = strong descent, 0.0 = climbing/level.

    Uses barometric vertical rate (ft/min) when available, averaged over a
    wall-clock window. Falls back to a simple first/last altitude difference
    if no vertical rate data is present.
    """
    if not recent_points:
        return 0.5

    points = list(recent_points)
    cutoff = points[-1].ts - window_secs
    recent = [p for p in points if p.ts >= cutoff]
    if not recent:
        return 0.5

    rates = [p.baro_rate for p in recent if p.baro_rate is not None]
    if rates:
        # Weight newer points slightly higher via their position in the window
        total_w = 0.0
        acc = 0.0
        for i, p in enumerate(recent):
            if p.baro_rate is None:
                continue
            w = (i + 1.0)
            total_w += w
            acc += p.baro_rate * w
        if total_w == 0:
            return 0.5
        avg_rate = acc / total_w  # ft/min
        # -800 ft/min or better = strong descent (1.0). 0 = level (0.0). Climbing = 0.0.
        return max(0.0, min(1.0, -avg_rate / 800.0))

    # No vertical rate: use altitude delta across window
    alts = [p.baro_alt for p in recent if p.baro_alt is not None]
    if len(alts) < 2:
        return 0.5
    delta = alts[-1] - alts[0]  # negative means descending
    span_secs = max(1.0, recent[-1].ts - recent[0].ts)
    rate = (delta / span_secs) * 60.0  # ft/min
    return max(0.0, min(1.0, -rate / 800.0))


def endurance_for(
    type_code: str | None,
    type_endurance_minutes: dict[str, float],
    default: float = 240.0,
) -> float:
    """Look up max endurance for a Mode S type code."""
    if not type_code:
        return default
    return type_endurance_minutes.get(type_code, default)


# ----------------------------------------------------------------------
# Classification
# ----------------------------------------------------------------------


def classify_landing(
    metrics: FlightMetrics,
    has_landing: bool,
    *,
    duration_minutes: float | None = None,
    type_code: str | None = None,
    type_endurance_minutes: dict[str, float] | None = None,
    default_endurance_minutes: float = 240.0,
) -> str:
    """Classify how a flight ended.

    Returns one of:
      - 'confirmed'           - clean landing with good supporting signals
      - 'signal_lost'         - aircraft was airborne at last contact (dropout)
      - 'dropped_on_approach' - signal lost with clear descent trajectory
      - 'uncertain'           - ambiguous, duration artifact, or no data
      - 'altitude_error'      - baro altimeter clearly broken (Bell 407 pathology)
    """
    # Altitude error detection. Two triggers:
    # (a) Legacy: baro=ground + high gs (catches mid-cruise encoder glitches).
    # (b) New: baro_error_points (hover-at-altitude) > 20% of ground-state points.
    # Use total_ground_points as the denominator so short flights with mixed
    # airborne data are not diluted.
    if metrics.data_points >= 10:
        gs_ground_ratio = metrics.ground_speed_while_ground / max(1, metrics.total_ground_points)
        baro_error_ratio = metrics.baro_error_points / max(1, metrics.data_points)
        if gs_ground_ratio > 0.20 or baro_error_ratio > 0.20:
            return "altitude_error"

    # Flight with no landing transition: signal loss or taxi-like
    if not has_landing:
        last_alt = metrics.last_airborne_alt
        last_gs = metrics.last_airborne_gs

        looks_airborne = (
            (last_alt is not None and last_alt > 2000)
            or (last_gs is not None and last_gs > 100)
            or metrics.max_altitude > 3000
        )
        if looks_airborne:
            # Distinguish mid-cruise dropout from on-approach dropout by descent signature
            d_score = descent_score(metrics.recent_points)
            if d_score > 0.5 and last_alt is not None and last_alt < 5000:
                return "dropped_on_approach"
            return "signal_lost"
        return "uncertain"

    # Duration sanity check. Per-type cap beats the global default.
    endurance_cap = default_endurance_minutes
    if type_endurance_minutes is not None:
        endurance_cap = endurance_for(type_code, type_endurance_minutes, default_endurance_minutes)
    if duration_minutes is not None and duration_minutes > endurance_cap:
        return "uncertain"

    # Flight has a landing transition. Score it on multiple factors.
    factors = []

    # Factor 1: last airborne altitude (lower = better landing)
    last_alt_ft = metrics.last_airborne_alt or 0
    alt_signal = _lerp(last_alt_ft, 500, 5000)  # 0.0 at 500, 1.0 at 5000
    factors.append((alt_signal, 3.0))

    # Factor 2: last airborne ground speed (slower = better)
    gs_signal = 0.0
    if metrics.last_airborne_gs is not None:
        gs_signal = _lerp(metrics.last_airborne_gs, 30, 150)
    factors.append((gs_signal, 2.5))

    # Factor 3: ground points collected at landing
    gp = metrics.ground_points_at_landing
    gp_signal = 1.0 if gp == 0 else (0.3 if gp == 1 else (0.1 if gp == 2 else 0.0))
    factors.append((gp_signal, 3.0))

    # Factor 4: descent trend via baro_rate window
    # _descent_trend semantics: 0 = descending (good), 1 = climbing (bad).
    descent_signal = 1.0 - descent_score(metrics.recent_points)
    factors.append((descent_signal, 2.0))

    # Factor 5: coordinate stability at landing
    spread = metrics.landing_coord_spread()
    coord_signal = _lerp(spread, 0.001, 0.01)
    factors.append((coord_signal, 1.5))

    total_weight = sum(w for _, w in factors)
    score = sum(f * w for f, w in factors) / total_weight if total_weight > 0 else 0.5

    if score > 0.6:
        return "signal_lost"
    return "confirmed"


def score_confidence(
    metrics: FlightMetrics,
    has_landing: bool,
    landing_type: str,
    *,
    origin_distance_km: float | None = None,
    dest_distance_km: float | None = None,
    duration_minutes: float | None = None,
) -> tuple[float, float]:
    """Compute takeoff and landing confidence scores in [0.0, 1.0].

    Landing confidence uses a weighted geometric mean across independent
    factors. Any single factor near zero drags the whole score down, which
    is the desired behavior: "one bad signal means we do not trust it."
    """

    # ---- Takeoff confidence ----
    if metrics.takeoff_type == "found_mid_flight":
        # We never observed an actual takeoff transition - conservatively
        # score low regardless of where we first saw the aircraft.
        takeoff_conf = 0.3
    else:
        takeoff_factors = []
        gp = metrics.ground_points_at_takeoff
        gp_score = 0.2 if gp == 0 else (0.5 if gp == 1 else (0.7 if gp == 2 else 1.0))
        takeoff_factors.append((gp_score, 2.0))

        if origin_distance_km is not None:
            prox = 1.0 - _lerp(origin_distance_km, 0, 10)
            takeoff_factors.append((prox, 1.5))

        t_total = sum(w for _, w in takeoff_factors)
        takeoff_conf = sum(f * w for f, w in takeoff_factors) / t_total if t_total > 0 else 0.5

    # ---- Landing confidence ----
    if not has_landing or landing_type in ("signal_lost", "dropped_on_approach"):
        landing_conf = 0.0
    elif landing_type == "altitude_error":
        landing_conf = 0.1
    elif landing_type == "uncertain":
        # Duration artifact or ambiguous: show as low confidence but non-zero
        landing_conf = 0.15
    else:
        factors = {}

        # Descent signature (baro_rate window)
        factors["descent"] = (descent_score(metrics.recent_points), 2.0)

        # Approach speed (slower = better)
        if metrics.last_airborne_gs is not None:
            factors["approach_spd"] = (_lerp(metrics.last_airborne_gs, 150, 40), 2.0)
        else:
            factors["approach_spd"] = (0.5, 2.0)

        # Final airborne altitude (lower = better; 5000 -> 0, 500 -> 1)
        last_alt = metrics.last_airborne_alt
        if last_alt is not None:
            factors["final_alt"] = (_lerp(last_alt, 5000, 500), 2.0)
        else:
            factors["final_alt"] = (0.5, 2.0)

        # Airport proximity at landing
        if dest_distance_km is not None:
            factors["airport_prox"] = (1.0 - _lerp(dest_distance_km, 0, 10), 2.0)
        else:
            factors["airport_prox"] = (0.3, 2.0)  # no airport match = weak signal

        # Coordinate stability at landing
        spread = metrics.landing_coord_spread()
        factors["coord_stab"] = (1.0 - _lerp(spread, 0.0001, 0.001), 1.0)

        # Post-landing points (we kept the flight open for a few ground points).
        # Even 1 point is meaningful - it confirmed the transition. 4+ points
        # is a clean stop. Map gp=1 -> 0.5 so the floor is soft.
        gp = metrics.ground_points_at_landing
        if gp <= 0:
            trace_tail = 0.0
        elif gp == 1:
            trace_tail = 0.5
        else:
            trace_tail = min(1.0, 0.5 + 0.25 * (gp - 1))
        factors["trace_tail"] = (trace_tail, 1.5)

        # Duration plausibility
        if duration_minutes is not None:
            dur_score = 1.0 if duration_minutes < 1440 else (0.5 if duration_minutes < 2880 else 0.1)
            factors["duration"] = (dur_score, 0.5)

        # Weighted geometric mean (any zero factor drags the whole score down)
        w_total = sum(w for _, w in factors.values())
        log_sum = 0.0
        for f, w in factors.values():
            log_sum += w * math.log(max(0.01, f))
        landing_conf = math.exp(log_sum / w_total) if w_total > 0 else 0.5

    # Penalty for altitude errors
    if landing_type == "altitude_error":
        takeoff_conf *= 0.3

    return round(takeoff_conf, 2), round(landing_conf, 2)
