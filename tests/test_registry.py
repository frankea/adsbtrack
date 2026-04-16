"""Tests for adsbtrack.registry - FAA aircraft registry import and lookup."""

import io
import zipfile
from pathlib import Path

import pytest

from adsbtrack.config import Config
from adsbtrack.db import Database
from adsbtrack.registry import (
    import_acftref_from_path,
    import_dereg_from_path,
    import_master_from_path,
    octal_mode_s_to_icao_hex,
    parse_acftref_row,
    parse_master_row,
    refresh_faa_registry,
)


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


# Minimum MASTER header (keeps the fixture short but valid).
_MASTER_HEADER = (
    "N-NUMBER|SERIAL NUMBER|MFR MDL CODE|ENG MFR MDL|YEAR MFR|TYPE REGISTRANT|"
    "NAME|STREET|STREET2|CITY|STATE|ZIP CODE|REGION|COUNTY|COUNTRY|"
    "LAST ACTION DATE|CERT ISSUE DATE|CERTIFICATION|TYPE AIRCRAFT|TYPE ENGINE|"
    "STATUS CODE|MODE S CODE|FRACT OWNER|AIR WORTH DATE|EXPIRATION DATE|"
    "UNIQUE ID|KIT MFR|KIT MODEL|MODE S CODE HEX"
)

# Two rows: N512WB and a minimal second aircraft. Fields padded with spaces
# the way the real FAA file does.
_MASTER_ROWS = [
    "512WB   |66-1099  |1152015|41514|1966|1|EXAMPLE OWNER LLC|100 MAIN ST|   "
    "|AUSTIN|TX|78701|2|453|US|20231201|20201115|1N|4|1|V|51465323|N|19660601|"
    "20260101|00123456|   |   |A66AD3",
    "99SK    |12345    |1234567|54321|2001|1|GHOST HELI LLC|200 OAK AVE|   "
    "|DALLAS|TX|75201|2|113|US|20240101|20210101|1N|6|1|V|00000001|N|20010101|"
    "20270101|00789012|   |   |000001",
]


def _write_pipe_file(path: Path, header: str, rows: list[str]) -> None:
    path.write_text(header + "\n" + "\n".join(rows) + "\n", encoding="latin-1")


def test_import_master_from_path(tmp_path):
    master = tmp_path / "MASTER.txt"
    _write_pipe_file(master, _MASTER_HEADER, _MASTER_ROWS)

    db_path = tmp_path / "t.db"
    with Database(db_path) as db:
        count = import_master_from_path(db, master)
        assert count == 2
        # Lookup by derived hex.
        row = db.get_faa_registry_by_hex("a66ad3")
        assert row["n_number"] == "512WB"
        assert row["name"] == "EXAMPLE OWNER LLC"
        assert row["city"] == "AUSTIN"
        # Empty STREET2 must have become NULL.
        assert row["street2"] is None


def test_import_dereg_from_path(tmp_path):
    dereg = tmp_path / "DEREG.txt"
    _write_pipe_file(dereg, _MASTER_HEADER, _MASTER_ROWS[:1])

    db_path = tmp_path / "t.db"
    with Database(db_path) as db:
        count = import_dereg_from_path(db, dereg)
        assert count == 1
        assert db.get_faa_deregistered_by_hex("a66ad3") is not None


def test_import_acftref_from_path(tmp_path):
    header = "CODE|MFR|MODEL|TYPE-ACFT|TYPE-ENG|AC-CAT|BUILD-CERT-IND|NO-ENG|NO-SEATS|AC-WEIGHT|SPEED"
    rows = [
        "1152015|CESSNA|172|4|1|1||1|4|CLASS 1|140",
        "1234567|BELL|407|6|4|2||1|5|CLASS 1|140",
    ]
    path = tmp_path / "ACFTREF.txt"
    _write_pipe_file(path, header, rows)

    db_path = tmp_path / "t.db"
    with Database(db_path) as db:
        count = import_acftref_from_path(db, path)
        assert count == 2
        ref = db.get_faa_aircraft_ref("1152015")
        assert ref["mfr"] == "CESSNA"
        assert ref["model"] == "172"


def test_import_master_skips_malformed_mode_s(tmp_path):
    """A row with an empty MODE S CODE should be skipped, not crash the import."""
    bad_row = _MASTER_ROWS[0].replace("51465323", "        ")  # 8 spaces = empty
    master = tmp_path / "MASTER.txt"
    _write_pipe_file(master, _MASTER_HEADER, [bad_row, _MASTER_ROWS[1]])

    db_path = tmp_path / "t.db"
    with Database(db_path) as db:
        count = import_master_from_path(db, master)
        # The bad row is skipped, the second row goes in.
        assert count == 1
        assert db.get_faa_registry_by_hex("a66ad3") is None
        assert db.get_faa_registry_by_hex("000001") is not None


def _build_releasable_zip(master_body: str, dereg_body: str, acftref_body: str) -> bytes:
    """Build an in-memory ReleasableAircraft.zip holding the three files."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("MASTER.txt", master_body)
        zf.writestr("DEREG.txt", dereg_body)
        zf.writestr("ACFTREF.txt", acftref_body)
    return buf.getvalue()


def test_refresh_faa_registry_from_local_zip(tmp_path):
    """refresh_faa_registry should accept a pre-downloaded zip path and
    import all three files inside it."""
    master_body = _MASTER_HEADER + "\n" + "\n".join(_MASTER_ROWS) + "\n"
    dereg_body = _MASTER_HEADER + "\n" + _MASTER_ROWS[0] + "\n"
    acftref_body = (
        "CODE|MFR|MODEL|TYPE-ACFT|TYPE-ENG|AC-CAT|BUILD-CERT-IND|NO-ENG|NO-SEATS|AC-WEIGHT|SPEED\n"
        "1152015|CESSNA|172|4|1|1||1|4|CLASS 1|140\n"
    )

    zip_path = tmp_path / "ReleasableAircraft.zip"
    zip_path.write_bytes(_build_releasable_zip(master_body, dereg_body, acftref_body))

    cfg = Config(db_path=tmp_path / "t.db", faa_registry_cache_path=zip_path)
    with Database(cfg.db_path) as db:
        # Pre-seed a stale registry row to prove truncate runs.
        stale = ["X"] * 29
        stale[0] = "OLD"
        stale[6] = "STALE"
        stale[-1] = "deadbe"
        db.insert_faa_registry([tuple(stale)])

        stats = refresh_faa_registry(db, cfg, local_zip=zip_path)

        assert stats["master"] == 2
        assert stats["dereg"] == 1
        assert stats["acftref"] == 1
        # Fresh row must be present.
        assert db.get_faa_registry_by_hex("a66ad3") is not None
        # Stale row must be gone (truncate ran before the fresh load).
        assert db.get_faa_registry_by_hex("deadbe") is None


def test_refresh_faa_registry_from_nested_zip(tmp_path):
    """If the FAA ever re-packages the zip with a nested folder, the
    refresh should still resolve MASTER/DEREG/ACFTREF by basename."""
    from adsbtrack.config import Config
    from adsbtrack.registry import refresh_faa_registry

    master_body = _MASTER_HEADER + "\n" + _MASTER_ROWS[0] + "\n"
    dereg_body = _MASTER_HEADER + "\n" + _MASTER_ROWS[1] + "\n"
    acftref_body = (
        "CODE|MFR|MODEL|TYPE-ACFT|TYPE-ENG|AC-CAT|BUILD-CERT-IND|NO-ENG|NO-SEATS|AC-WEIGHT|SPEED\n"
        "1152015|CESSNA|172|4|1|1||1|4|CLASS 1|140\n"
    )
    zip_path = tmp_path / "ReleasableAircraft.zip"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        # Everything is nested one level deep instead of at the top.
        zf.writestr("ReleasableAircraft/MASTER.txt", master_body)
        zf.writestr("ReleasableAircraft/DEREG.txt", dereg_body)
        zf.writestr("ReleasableAircraft/ACFTREF.txt", acftref_body)
    zip_path.write_bytes(buf.getvalue())

    cfg = Config(db_path=tmp_path / "t.db", faa_registry_cache_path=zip_path)
    with Database(cfg.db_path) as db:
        stats = refresh_faa_registry(db, cfg, local_zip=zip_path)

    assert stats == {"master": 1, "dereg": 1, "acftref": 1}
    with Database(cfg.db_path) as db:
        assert db.get_faa_registry_by_hex("a66ad3") is not None


def test_end_to_end_update_lookup_owner(tmp_path):
    """One-shot: run `registry update` then `registry lookup` + `registry owner`
    against the fake zip. Exercises the full wiring, not just the pieces."""
    from click.testing import CliRunner

    from adsbtrack.cli import cli

    zip_path = tmp_path / "ReleasableAircraft.zip"
    header = _MASTER_HEADER + "\n"
    # Only EXAMPLE OWNER into MASTER; GHOST HELI goes into DEREG so the owner
    # search below has no match in faa_registry.
    master_body = header + _MASTER_ROWS[0] + "\n"
    dereg_body = header + _MASTER_ROWS[1] + "\n"
    acftref_body = (
        "CODE|MFR|MODEL|TYPE-ACFT|TYPE-ENG|AC-CAT|BUILD-CERT-IND|NO-ENG|NO-SEATS|AC-WEIGHT|SPEED\n"
        "1152015|CESSNA|172|4|1|1||1|4|CLASS 1|140\n"
    )
    zip_path.write_bytes(_build_releasable_zip(master_body, dereg_body, acftref_body))

    db_path = tmp_path / "t.db"
    runner = CliRunner()
    r = runner.invoke(cli, ["registry", "update", "--zip", str(zip_path), "--db", str(db_path)])
    assert r.exit_code == 0, r.output

    r = runner.invoke(cli, ["registry", "lookup", "--hex", "a66ad3", "--db", str(db_path)])
    assert r.exit_code == 0, r.output
    assert "EXAMPLE OWNER LLC" in r.output

    r = runner.invoke(cli, ["registry", "owner", "--name", "GHOST", "--db", str(db_path)])
    # GHOST HELI is only in DEREG, not MASTER, so owner search (which hits
    # faa_registry) should return nothing.
    assert r.exit_code == 0, r.output
    assert "no" in r.output.lower() and "match" in r.output.lower()
