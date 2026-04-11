"""Solar-position math for day/night classification.

Pure NOAA solar-position approximation, no external dependencies.

The core function is ``solar_altitude_deg(dt_utc, lat, lon)`` which returns
the sun's altitude angle in degrees above the horizon at the given UTC time
and observer coordinates. Night is any time the altitude is below the civil
twilight threshold (default -6 degrees, see ``is_night_at``).

Computing solar position per trace point on a 5000-point GLF6 flight would
burn ~50 ms of pure Python math. Since the sun moves at ~15 deg/hour (i.e.
<1 deg per 5 minutes) we cache results by (lat quantized, lon quantized,
ts bucket) so repeated points on the same flight hit the cache.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime
from functools import lru_cache


def _to_utc(dt_utc: datetime) -> datetime:
    """Return a UTC-aware datetime. Naive inputs are assumed already-UTC."""
    return dt_utc.replace(tzinfo=UTC) if dt_utc.tzinfo is None else dt_utc.astimezone(UTC)


def _julian_day(dt_utc: datetime) -> float:
    """Julian day number (with fraction) for a UTC datetime."""
    dt_utc = _to_utc(dt_utc)

    y = dt_utc.year
    m = dt_utc.month
    d = dt_utc.day

    if m <= 2:
        y -= 1
        m += 12

    a = y // 100
    b = 2 - a + a // 4

    jd = int(365.25 * (y + 4716)) + int(30.6001 * (m + 1)) + d + b - 1524.5
    # Add fractional day from the UTC time-of-day
    day_frac = (dt_utc.hour + dt_utc.minute / 60.0 + dt_utc.second / 3600.0) / 24.0
    return jd + day_frac


def solar_altitude_deg(dt_utc: datetime, lat: float, lon: float) -> float:
    """Sun altitude angle in degrees above the horizon.

    Positive = above horizon (day), negative = below horizon (night).
    Civil twilight is ~-6 degrees; nautical twilight ~-12; astronomical -18.

    Uses the NOAA Global Monitoring Laboratory solar-position approximation
    which is accurate to within ~0.5 degrees for years 1800-2100.
    """
    jd = _julian_day(dt_utc)
    # Julian century from J2000.0
    t = (jd - 2451545.0) / 36525.0

    # Mean longitude of the sun (degrees), normalized to [0, 360)
    l0 = (280.46646 + t * (36000.76983 + t * 0.0003032)) % 360.0

    # Mean anomaly of the sun (degrees)
    m = 357.52911 + t * (35999.05029 - 0.0001537 * t)

    # Sun's equation of center (degrees)
    m_rad = math.radians(m)
    c = (
        math.sin(m_rad) * (1.914602 - t * (0.004817 + 0.000014 * t))
        + math.sin(2 * m_rad) * (0.019993 - 0.000101 * t)
        + math.sin(3 * m_rad) * 0.000289
    )

    # True longitude of the sun (degrees)
    true_long = l0 + c

    # Apparent longitude of the sun (degrees), corrected for nutation/aberration
    omega = 125.04 - 1934.136 * t
    app_long = true_long - 0.00569 - 0.00478 * math.sin(math.radians(omega))

    # Mean obliquity of the ecliptic (degrees)
    seconds = 21.448 - t * (46.8150 + t * (0.00059 - t * 0.001813))
    e0 = 23.0 + (26.0 + seconds / 60.0) / 60.0
    e = e0 + 0.00256 * math.cos(math.radians(omega))

    # Sun declination (degrees)
    sin_decl = math.sin(math.radians(e)) * math.sin(math.radians(app_long))
    declination = math.degrees(math.asin(sin_decl))

    # Equation of time (minutes)
    y = math.tan(math.radians(e / 2.0)) ** 2
    eot_rad = (
        y * math.sin(2 * math.radians(l0))
        - 2 * 0.016708634 * math.sin(m_rad)
        + 4 * 0.016708634 * y * math.sin(m_rad) * math.cos(2 * math.radians(l0))
        - 0.5 * y * y * math.sin(4 * math.radians(l0))
        - 1.25 * 0.016708634**2 * math.sin(2 * m_rad)
    )
    eot_min = 4 * math.degrees(eot_rad)  # minutes

    # True solar time (minutes since midnight)
    dt_utc = _to_utc(dt_utc)
    minutes_since_midnight = dt_utc.hour * 60 + dt_utc.minute + dt_utc.second / 60.0
    true_solar_time = (minutes_since_midnight + eot_min + 4.0 * lon) % 1440.0

    # Hour angle (degrees)
    hour_angle = true_solar_time / 4.0 - 180.0
    if hour_angle < -180.0:
        hour_angle += 360.0

    # Solar zenith angle
    lat_rad = math.radians(lat)
    decl_rad = math.radians(declination)
    ha_rad = math.radians(hour_angle)
    cos_zenith = math.sin(lat_rad) * math.sin(decl_rad) + math.cos(lat_rad) * math.cos(decl_rad) * math.cos(ha_rad)
    cos_zenith = max(-1.0, min(1.0, cos_zenith))
    zenith = math.degrees(math.acos(cos_zenith))

    # Altitude = 90 - zenith
    return 90.0 - zenith


@lru_cache(maxsize=8192)
def _is_night_cached(lat_q: float, lon_q: float, ts_bucket: int, threshold_deg: float) -> bool:
    """Cached inner body of is_night_at. Operates on quantized inputs.

    The cache key is (lat quantized to grid, lon quantized, ts bucketed into
    5-min intervals, threshold). Typical hit rate on a 5000-point single
    flight is >99% since all points share a narrow lat/lon range.
    """
    dt_utc = datetime.fromtimestamp(ts_bucket, tz=UTC)
    alt = solar_altitude_deg(dt_utc, lat_q, lon_q)
    return alt < threshold_deg


def is_night_at(
    dt_utc: datetime,
    lat: float,
    lon: float,
    *,
    threshold_deg: float = -6.0,
    lat_lon_quant_deg: float = 0.1,
    ts_bucket_secs: float = 300.0,
) -> bool:
    """True when the sun is below the civil-twilight threshold at (lat, lon).

    Inputs are quantized before consulting the LRU cache, so calls that are
    within ``lat_lon_quant_deg`` of each other and fall in the same
    ``ts_bucket_secs`` window are collapsed to a single solar computation.
    """
    dt_utc = _to_utc(dt_utc)
    ts = dt_utc.timestamp()
    ts_bucket = int(ts // ts_bucket_secs) * int(ts_bucket_secs)
    lat_q = round(lat / lat_lon_quant_deg) * lat_lon_quant_deg
    lon_q = round(lon / lat_lon_quant_deg) * lat_lon_quant_deg
    return _is_night_cached(lat_q, lon_q, ts_bucket, threshold_deg)
