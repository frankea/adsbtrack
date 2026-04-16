"""Tests for adsbtrack.registry - FAA aircraft registry import and lookup."""

from adsbtrack.config import Config


def test_config_has_faa_registry_url_default():
    cfg = Config()
    assert cfg.faa_registry_url == "https://registry.faa.gov/database/ReleasableAircraft.zip"
