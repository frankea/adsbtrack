"""Tests for adsbtrack.registry - FAA aircraft registry import and lookup."""

import pytest

from adsbtrack.config import Config
from adsbtrack.registry import octal_mode_s_to_icao_hex


def test_config_has_faa_registry_url_default():
    cfg = Config()
    assert cfg.faa_registry_url == "https://registry.faa.gov/database/ReleasableAircraft.zip"


def test_octal_mode_s_to_icao_hex_known():
    # N512WB: FAA MODE S CODE '51465323' (octal) -> ICAO 'a66ad3'
    assert octal_mode_s_to_icao_hex("51465323") == "a66ad3"


def test_octal_mode_s_to_icao_hex_zero_padding():
    # Short octal strings pad to 6 hex chars.
    assert octal_mode_s_to_icao_hex("1") == "000001"


def test_octal_mode_s_to_icao_hex_strips_whitespace():
    # FAA ships columns space-padded, so strip before parsing.
    assert octal_mode_s_to_icao_hex("  51465323  ") == "a66ad3"


def test_octal_mode_s_to_icao_hex_empty_raises():
    with pytest.raises(ValueError):
        octal_mode_s_to_icao_hex("")


def test_octal_mode_s_to_icao_hex_invalid_raises():
    with pytest.raises(ValueError):
        octal_mode_s_to_icao_hex("8")  # 8 is not a valid octal digit
