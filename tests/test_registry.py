"""Tests for adsbtrack.registry - FAA aircraft registry import and lookup."""

import pytest

from adsbtrack.config import Config
from adsbtrack.registry import octal_mode_s_to_icao_hex, parse_acftref_row, parse_master_row


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


def _make_master_row(**overrides):
    """Build a MASTER.txt dict row with sensible defaults; override per-test."""
    row = {
        "N-NUMBER": "512WB",
        "SERIAL NUMBER": "66-1099",
        "MFR MDL CODE": "1152015",
        "ENG MFR MDL": "41514",
        "YEAR MFR": "1966",
        "TYPE REGISTRANT": "1",
        "NAME": "EXAMPLE OWNER LLC",
        "STREET": "100 MAIN ST",
        "STREET2": "",
        "CITY": "AUSTIN",
        "STATE": "TX",
        "ZIP CODE": "78701",
        "REGION": "2",
        "COUNTY": "453",
        "COUNTRY": "US",
        "LAST ACTION DATE": "20231201",
        "CERT ISSUE DATE": "20201115",
        "CERTIFICATION": "1N",
        "TYPE AIRCRAFT": "4",
        "TYPE ENGINE": "1",
        "STATUS CODE": "V",
        "MODE S CODE": "51465323",
        "FRACT OWNER": "N",
        "AIR WORTH DATE": "19660601",
        "EXPIRATION DATE": "20260101",
        "UNIQUE ID": "00123456",
        "KIT MFR": "",
        "KIT MODEL": "",
        "MODE S CODE HEX": "A66AD3",
    }
    row.update(overrides)
    return row


def test_parse_master_row_produces_expected_tuple():
    row = _make_master_row()
    parsed = parse_master_row(row)
    # Column order must match the INSERT statement in db.insert_faa_registry.
    assert parsed[0] == "512WB"  # n_number, whitespace stripped
    assert parsed[6] == "EXAMPLE OWNER LLC"  # name
    assert parsed[-1] == "a66ad3"  # mode_s_code_hex derived and lowercased


def test_parse_master_row_strips_and_nullifies_empty():
    # Use the real header name 'KIT MFR' (with space) not 'KIT_MFR' - this is dict keyword
    # expansion via **, so spaces must go via an explicit dict update.
    row = _make_master_row(STREET2="   ")
    row["KIT MFR"] = "  "
    parsed = parse_master_row(row)
    # Index 8 is street2, index 26 is kit_mfr
    assert parsed[8] is None
    assert parsed[26] is None


def test_parse_master_row_raises_on_bad_mode_s():
    row = _make_master_row()
    row["MODE S CODE"] = ""
    with pytest.raises(ValueError):
        parse_master_row(row)


def test_parse_acftref_row():
    row = {
        "CODE": "1152015",
        "MFR": "CESSNA",
        "MODEL": "172",
        "TYPE-ACFT": "4",
        "TYPE-ENG": "1",
        "AC-CAT": "1",
        "BUILD-CERT-IND": "",
        "NO-ENG": "1",
        "NO-SEATS": "4",
        "AC-WEIGHT": "CLASS 1",
        "SPEED": "140",
    }
    parsed = parse_acftref_row(row)
    assert parsed == ("1152015", "CESSNA", "172", "4", "1")
