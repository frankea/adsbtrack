"""Tests for solar-position math and day/night classification."""

from datetime import UTC, datetime

from adsbtrack.solar import is_night_at, solar_altitude_deg


def test_solar_noon_summer_equator_positive():
    """At local solar noon on the equator, the sun is nearly overhead."""
    # UTC noon at longitude 0, equator, spring equinox
    dt = datetime(2025, 3, 21, 12, 0, 0, tzinfo=UTC)
    alt = solar_altitude_deg(dt, 0.0, 0.0)
    assert alt > 80.0, f"Expected sun near zenith, got {alt}"


def test_solar_midnight_is_negative():
    """At local solar midnight at the equator, sun is well below the horizon."""
    dt = datetime(2025, 3, 21, 0, 0, 0, tzinfo=UTC)
    alt = solar_altitude_deg(dt, 0.0, 0.0)
    assert alt < -60.0, f"Expected sun well below horizon, got {alt}"


def test_polar_summer_day():
    """High Arctic latitude in June should have sun always above horizon."""
    # 80 N in June, midnight - still polar day
    dt = datetime(2025, 6, 21, 0, 0, 0, tzinfo=UTC)
    alt = solar_altitude_deg(dt, 80.0, 0.0)
    assert alt > 0.0, f"Expected midnight sun at high lat in June, got {alt}"


def test_polar_winter_night():
    """High Arctic latitude in December should have sun always below horizon."""
    # 80 N in December, noon - polar night
    dt = datetime(2025, 12, 21, 12, 0, 0, tzinfo=UTC)
    alt = solar_altitude_deg(dt, 80.0, 0.0)
    assert alt < 0.0, f"Expected polar night at high lat in December, got {alt}"


def test_is_night_at_noon_false():
    dt = datetime(2025, 6, 21, 12, 0, 0, tzinfo=UTC)
    assert is_night_at(dt, 40.0, -74.0) is False


def test_is_night_at_midnight_true():
    # New York at 3 AM UTC in June = 11 PM local = clearly night
    dt = datetime(2025, 6, 21, 5, 0, 0, tzinfo=UTC)
    assert is_night_at(dt, 40.0, -74.0) is True


def test_is_night_at_cache_hit():
    """Two calls within the cache bucket should hit the LRU cache."""
    dt1 = datetime(2025, 6, 21, 12, 0, 0, tzinfo=UTC)
    dt2 = datetime(2025, 6, 21, 12, 3, 0, tzinfo=UTC)  # 3 minutes later
    r1 = is_night_at(dt1, 40.0, -74.0)
    r2 = is_night_at(dt2, 40.0, -74.0)
    assert r1 == r2


def test_naive_datetime_treated_as_utc():
    dt_naive = datetime(2025, 6, 21, 12, 0, 0)
    dt_aware = datetime(2025, 6, 21, 12, 0, 0, tzinfo=UTC)
    assert abs(solar_altitude_deg(dt_naive, 0.0, 0.0) - solar_altitude_deg(dt_aware, 0.0, 0.0)) < 0.01
