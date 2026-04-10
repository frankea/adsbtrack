"""Classify flight endings and score confidence.

Determines whether a flight ended with a confirmed landing, signal loss,
altitude encoding error, or is uncertain. Computes confidence scores for
takeoff and landing data quality.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class FlightMetrics:
    """Raw signal metrics accumulated during trace processing."""

    data_points: int = 0
    sources: set[str] = field(default_factory=set)
    max_altitude: int = 0
    last_altitude: int | str | None = None  # feet or "ground"
    last_ground_speed: float | None = None
    ground_points_at_takeoff: int = 0
    ground_points_at_landing: int = 0
    final_altitudes: list[int] = field(default_factory=list)  # last N altitude readings
    ground_speed_while_ground: int = 0  # alt="ground" with gs > threshold
    landing_lats: list[float] = field(default_factory=list)
    landing_lons: list[float] = field(default_factory=list)

    def record_point(
        self,
        alt: int | str | None,
        gs: float | None,
        lat: float,
        lon: float,
        *,
        is_ground: bool,
        state: str | None,
        landing_speed_threshold: float = 80.0,
    ) -> None:
        self.data_points += 1
        self.last_altitude = alt
        self.last_ground_speed = gs

        if not is_ground and isinstance(alt, (int, float)):
            if alt > self.max_altitude:
                self.max_altitude = int(alt)
            # Keep rolling window of last 10 altitudes while airborne
            self.final_altitudes.append(int(alt))
            if len(self.final_altitudes) > 10:
                self.final_altitudes.pop(0)

        if is_ground and gs is not None and gs > landing_speed_threshold:
            self.ground_speed_while_ground += 1

    def record_landing_ground_point(self, lat: float, lon: float) -> None:
        self.ground_points_at_landing += 1
        self.landing_lats.append(lat)
        self.landing_lons.append(lon)

    def record_takeoff_ground_point(self) -> None:
        self.ground_points_at_takeoff += 1

    def landing_coord_spread(self) -> float:
        """Max spread in degrees across landing ground points."""
        if len(self.landing_lats) < 2:
            return 0.0
        lat_spread = max(self.landing_lats) - min(self.landing_lats)
        lon_spread = max(self.landing_lons) - min(self.landing_lons)
        return max(lat_spread, lon_spread)


def _lerp(value: float, low: float, high: float) -> float:
    """Linear interpolation clamped to [0, 1]. Returns 0.0 at low, 1.0 at high."""
    if high <= low:
        return 1.0 if value >= high else 0.0
    return max(0.0, min(1.0, (value - low) / (high - low)))


def _descent_trend(altitudes: list[int]) -> float:
    """Return 0.0 if descending, 1.0 if level/climbing. Uses simple linear slope."""
    if len(altitudes) < 3:
        return 0.5  # not enough data
    n = len(altitudes)
    x_mean = (n - 1) / 2.0
    y_mean = sum(altitudes) / n
    num = sum((i - x_mean) * (a - y_mean) for i, a in enumerate(altitudes))
    den = sum((i - x_mean) ** 2 for i in range(n))
    if den == 0:
        return 0.5
    slope = num / den  # feet per point interval
    # Negative slope = descending (good for landing), positive = climbing (signal loss)
    if slope < -50:
        return 0.0  # strong descent
    if slope < 0:
        return 0.2  # mild descent
    if slope < 50:
        return 0.7  # level
    return 1.0  # climbing


def classify_landing(metrics: FlightMetrics, has_landing: bool) -> str:
    """Classify how a flight ended.

    Returns one of: 'confirmed', 'signal_lost', 'uncertain', 'altitude_error'.
    """
    # Check for altitude encoding errors first (Bell 407 problem)
    if metrics.data_points >= 10:
        error_ratio = metrics.ground_speed_while_ground / metrics.data_points
        if error_ratio > 0.20:
            return "altitude_error"

    if not has_landing:
        # Flight ended without any landing data - check if it looks like signal loss
        last_alt = metrics.last_altitude
        last_gs = metrics.last_ground_speed

        # If last point was at altitude and speed, it's signal loss
        if isinstance(last_alt, (int, float)) and last_alt > 2000:
            return "signal_lost"
        if last_gs is not None and last_gs > 100:
            return "signal_lost"

        return "uncertain"

    # Flight has landing data - score it
    factors = []

    # Factor 1: Last altitude before landing (weight 3.0)
    last_alt_ft = 0
    if metrics.final_altitudes:
        last_alt_ft = metrics.final_altitudes[-1]
    alt_signal = _lerp(last_alt_ft, 500, 5000)
    factors.append((alt_signal, 3.0))

    # Factor 2: Last ground speed (weight 2.5)
    gs_signal = 0.0
    if metrics.last_ground_speed is not None:
        gs_signal = _lerp(metrics.last_ground_speed, 30, 150)
    factors.append((gs_signal, 2.5))

    # Factor 3: Ground points at landing (weight 3.0)
    gp = metrics.ground_points_at_landing
    gp_signal = 1.0 if gp == 0 else (0.5 if gp == 1 else 0.0)
    factors.append((gp_signal, 3.0))

    # Factor 4: Descent trend (weight 2.0)
    descent_signal = _descent_trend(metrics.final_altitudes)
    factors.append((descent_signal, 2.0))

    # Factor 5: Coordinate stability at landing (weight 1.5)
    spread = metrics.landing_coord_spread()
    coord_signal = _lerp(spread, 0.001, 0.01)
    factors.append((coord_signal, 1.5))

    # Weighted average - higher = more like signal loss
    total_weight = sum(w for _, w in factors)
    score = sum(f * w for f, w in factors) / total_weight if total_weight > 0 else 0.5

    if score > 0.6:
        return "signal_lost"
    return "confirmed"


def score_confidence(
    metrics: FlightMetrics,
    has_landing: bool,
    landing_type: str,
    origin_distance_km: float | None = None,
    dest_distance_km: float | None = None,
    duration_minutes: float | None = None,
) -> tuple[float, float]:
    """Compute takeoff and landing confidence scores (0.0-1.0)."""

    # --- Takeoff confidence ---
    takeoff_factors = []

    # Ground points before takeoff
    gp = metrics.ground_points_at_takeoff
    gp_score = 0.2 if gp == 0 else (0.5 if gp == 1 else (0.7 if gp == 2 else 1.0))
    takeoff_factors.append((gp_score, 2.0))

    # Airport proximity at takeoff
    if origin_distance_km is not None:
        prox = 1.0 - _lerp(origin_distance_km, 0, 10)
        takeoff_factors.append((prox, 1.5))

    t_total = sum(w for _, w in takeoff_factors)
    takeoff_conf = sum(f * w for f, w in takeoff_factors) / t_total if t_total > 0 else 0.5

    # --- Landing confidence ---
    if not has_landing or landing_type == "signal_lost":
        landing_conf = 0.0
    elif landing_type == "altitude_error":
        landing_conf = 0.1
    else:
        landing_factors = []

        # Ground points at landing
        gp = metrics.ground_points_at_landing
        gp_score = 0.0 if gp == 0 else (0.3 if gp == 1 else (0.6 if gp == 2 else 1.0))
        landing_factors.append((gp_score, 3.0))

        # Ground speed at landing
        if metrics.last_ground_speed is not None:
            gs_score = 1.0 - _lerp(metrics.last_ground_speed, 10, 80)
            landing_factors.append((gs_score, 2.0))

        # Descent profile
        descent_score = 1.0 - _descent_trend(metrics.final_altitudes)
        landing_factors.append((descent_score, 2.0))

        # Airport proximity at landing
        if dest_distance_km is not None:
            prox = 1.0 - _lerp(dest_distance_km, 0, 10)
            landing_factors.append((prox, 2.0))

        # Coordinate stability
        spread = metrics.landing_coord_spread()
        stab = 1.0 - _lerp(spread, 0.0001, 0.001)
        landing_factors.append((stab, 1.5))

        # Duration plausibility
        if duration_minutes is not None:
            dur_score = 1.0 if duration_minutes < 1440 else (0.5 if duration_minutes < 2880 else 0.1)
            landing_factors.append((dur_score, 0.5))

        l_total = sum(w for _, w in landing_factors)
        landing_conf = sum(f * w for f, w in landing_factors) / l_total if l_total > 0 else 0.5

    # Penalty for altitude errors
    if landing_type == "altitude_error":
        takeoff_conf *= 0.3

    return round(takeoff_conf, 2), round(landing_conf, 2)
