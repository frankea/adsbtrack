"""Tests for adsbtrack.config.Config.load -- TOML config file loader."""

import pytest

from adsbtrack.config import Config


def _write_toml(path, text: str) -> None:
    path.write_text(text)


def test_load_overrides_named_fields_keeps_defaults(tmp_path):
    config_path = tmp_path / "config.toml"
    _write_toml(
        config_path,
        """
        airport_match_threshold_km = 5.5
        airports_csv_url = "https://example.com/airports.csv"
        """,
    )

    config = Config.load(config_path)

    assert config.airport_match_threshold_km == 5.5
    assert config.airports_csv_url == "https://example.com/airports.csv"
    # everything else keeps the dataclass default
    defaults = Config()
    assert config.rate_limit == defaults.rate_limit
    assert config.fetch_concurrency == defaults.fetch_concurrency
    assert config.min_flight_minutes == defaults.min_flight_minutes


def test_load_unknown_key_raises_with_key_name(tmp_path):
    config_path = tmp_path / "config.toml"
    _write_toml(config_path, "bogus_field = 1\n")

    with pytest.raises(ValueError, match="bogus_field"):
        Config.load(config_path)


def test_load_honors_adsbtrack_config_env_var(tmp_path, monkeypatch):
    config_path = tmp_path / "from_env.toml"
    _write_toml(config_path, "rate_limit = 1.25\n")
    monkeypatch.setenv("ADSBTRACK_CONFIG", str(config_path))

    config = Config.load()

    assert config.rate_limit == 1.25


def test_load_explicit_path_takes_priority_over_env_var(tmp_path, monkeypatch):
    env_config = tmp_path / "from_env.toml"
    _write_toml(env_config, "rate_limit = 1.25\n")
    explicit_config = tmp_path / "explicit.toml"
    _write_toml(explicit_config, "rate_limit = 9.75\n")
    monkeypatch.setenv("ADSBTRACK_CONFIG", str(env_config))

    config = Config.load(explicit_config)

    assert config.rate_limit == 9.75


def test_load_absent_file_returns_pure_defaults(tmp_path, monkeypatch):
    # Neither an explicit path, nor $ADSBTRACK_CONFIG, nor the default
    # ~/.config/adsbtrack/config.toml (redirected into an empty tmp HOME)
    # exist -- Config.load() must fall back to plain defaults.
    monkeypatch.delenv("ADSBTRACK_CONFIG", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path))

    config = Config.load()

    assert config == Config()


def test_load_missing_explicit_path_returns_pure_defaults(tmp_path):
    missing = tmp_path / "does-not-exist.toml"

    config = Config.load(missing)

    assert config == Config()


def test_load_path_field_accepts_string_and_expanduser(tmp_path, monkeypatch):
    fake_home = tmp_path / "home"
    fake_home.mkdir()
    monkeypatch.setenv("HOME", str(fake_home))

    config_path = tmp_path / "config.toml"
    _write_toml(config_path, 'db_path = "~/flights.db"\n')

    config = Config.load(config_path)

    assert config.db_path == fake_home / "flights.db"


def test_load_type_mismatch_raises(tmp_path):
    config_path = tmp_path / "config.toml"
    # airport_match_threshold_km is a float field; a string is the wrong type.
    _write_toml(config_path, 'airport_match_threshold_km = "not-a-number"\n')

    with pytest.raises(ValueError, match="airport_match_threshold_km"):
        Config.load(config_path)
