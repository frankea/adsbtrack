"""Tests for adsbtrack.cli -- Click command surface."""

import io
import zipfile
from datetime import UTC, datetime
from pathlib import Path

from click.testing import CliRunner

from adsbtrack.cli import cli
from adsbtrack.db import Database
from adsbtrack.models import Flight


def _seed_flights(db_path: Path) -> None:
    """Insert a couple of flights for hex 'ae07b3' so the links command
    has something to print."""
    with Database(db_path) as db:
        db.insert_flight(
            Flight(
                icao="ae07b3",
                takeoff_time=datetime(2022, 6, 16, 12, 43, 27, tzinfo=UTC),
                takeoff_lat=35.035,
                takeoff_lon=-117.932,
                takeoff_date="2022-06-16",
                origin_icao="K9L2",
                origin_name="Edwards Aux",
            )
        )
        db.insert_flight(
            Flight(
                icao="ae07b3",
                takeoff_time=datetime(2022, 6, 15, 17, 6, 45, tzinfo=UTC),
                takeoff_lat=35.021,
                takeoff_lon=-118.002,
                takeoff_date="2022-06-15",
            )
        )


def test_links_default_output_has_prefix(tmp_path):
    """Default `links` output should include the date/origin/destination prefix."""
    db_path = tmp_path / "adsbtrack.db"
    _seed_flights(db_path)

    runner = CliRunner()
    result = runner.invoke(cli, ["links", "--hex", "ae07b3", "--db", str(db_path)])

    assert result.exit_code == 0, result.output
    # Default output is the rich-formatted table-ish line: date, origin -> dest, URL
    assert "2022-06-16" in result.output
    assert "K9L2" in result.output
    assert "https://globe.adsbexchange.com/?icao=ae07b3&showTrace=2022-06-16" in result.output


def test_links_urls_only_emits_one_url_per_line(tmp_path):
    """`links --urls-only` should emit one raw URL per line with no prefix.
    This output format is meant to be piped into shell loops:
        adsbtrack links --hex X --urls-only | while read url; do ...
    """
    db_path = tmp_path / "adsbtrack.db"
    _seed_flights(db_path)

    runner = CliRunner()
    result = runner.invoke(cli, ["links", "--hex", "ae07b3", "--urls-only", "--db", str(db_path)])

    assert result.exit_code == 0, result.output
    lines = [line for line in result.output.splitlines() if line.strip()]
    assert len(lines) == 2, f"Expected 2 URL lines, got: {lines!r}"
    for line in lines:
        assert line.startswith("https://globe.adsbexchange.com/?icao=ae07b3&showTrace="), (
            f"Line is not a bare URL: {line!r}"
        )
        # No date, origin, destination, or rich markup noise.
        assert "->" not in line
        assert "[" not in line
        assert "K9L2" not in line
    # Both flight dates must be represented.
    joined = "\n".join(lines)
    assert "showTrace=2022-06-16" in joined
    assert "showTrace=2022-06-15" in joined


def _build_fake_releasable_zip(path):
    master_header = (
        "N-NUMBER|SERIAL NUMBER|MFR MDL CODE|ENG MFR MDL|YEAR MFR|TYPE REGISTRANT|"
        "NAME|STREET|STREET2|CITY|STATE|ZIP CODE|REGION|COUNTY|COUNTRY|"
        "LAST ACTION DATE|CERT ISSUE DATE|CERTIFICATION|TYPE AIRCRAFT|TYPE ENGINE|"
        "STATUS CODE|MODE S CODE|FRACT OWNER|AIR WORTH DATE|EXPIRATION DATE|"
        "UNIQUE ID|KIT MFR|KIT MODEL|MODE S CODE HEX\n"
    )
    master_body = master_header + (
        "512WB|66-1099|1152015|41514|1966|1|EXAMPLE OWNER LLC|100 MAIN ST||"
        "AUSTIN|TX|78701|2|453|US|20231201|20201115|1N|4|1|V|51465323|N|19660601|"
        "20260101|00123456|||A66AD3\n"
    )
    dereg_body = master_header + (
        "99SK|12345|1234567|54321|2001|1|GHOST HELI LLC|200 OAK AVE||"
        "DALLAS|TX|75201|2|113|US|20240101|20210101|1N|6|1|V|00000001|N|20010101|"
        "20270101|00789012|||000001\n"
    )
    acftref_body = (
        "CODE|MFR|MODEL|TYPE-ACFT|TYPE-ENG|AC-CAT|BUILD-CERT-IND|NO-ENG|NO-SEATS|AC-WEIGHT|SPEED\n"
        "1152015|CESSNA|172|4|1|1||1|4|CLASS 1|140\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("MASTER.txt", master_body)
        zf.writestr("DEREG.txt", dereg_body)
        zf.writestr("ACFTREF.txt", acftref_body)
    path.write_bytes(buf.getvalue())


def test_registry_update_from_local_zip(tmp_path):
    zip_path = tmp_path / "ReleasableAircraft.zip"
    _build_fake_releasable_zip(zip_path)
    db_path = tmp_path / "t.db"

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["registry", "update", "--zip", str(zip_path), "--db", str(db_path)],
    )
    assert result.exit_code == 0, result.output
    # Progress summary mentions at least the MASTER count.
    assert "MASTER" in result.output or "master" in result.output.lower()

    with Database(db_path) as db:
        assert db.get_faa_registry_by_hex("a66ad3") is not None
        assert db.get_faa_deregistered_by_hex("000001") is not None
        assert db.get_faa_aircraft_ref("1152015") is not None


def test_registry_lookup_by_hex(tmp_path):
    zip_path = tmp_path / "ReleasableAircraft.zip"
    _build_fake_releasable_zip(zip_path)
    db_path = tmp_path / "t.db"

    runner = CliRunner()
    # Import first.
    runner.invoke(cli, ["registry", "update", "--zip", str(zip_path), "--db", str(db_path)])
    # Then lookup.
    result = runner.invoke(cli, ["registry", "lookup", "--hex", "a66ad3", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "EXAMPLE OWNER LLC" in result.output
    assert "AUSTIN" in result.output
    assert "N512WB" in result.output


def test_registry_lookup_by_tail(tmp_path):
    zip_path = tmp_path / "ReleasableAircraft.zip"
    _build_fake_releasable_zip(zip_path)
    db_path = tmp_path / "t.db"

    runner = CliRunner()
    runner.invoke(cli, ["registry", "update", "--zip", str(zip_path), "--db", str(db_path)])
    result = runner.invoke(cli, ["registry", "lookup", "--tail", "N512WB", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "EXAMPLE OWNER LLC" in result.output


def test_registry_lookup_shows_deregistered_flag(tmp_path):
    """When the hex is present in faa_deregistered only, the output calls it out."""
    zip_path = tmp_path / "ReleasableAircraft.zip"
    _build_fake_releasable_zip(zip_path)
    db_path = tmp_path / "t.db"

    runner = CliRunner()
    runner.invoke(cli, ["registry", "update", "--zip", str(zip_path), "--db", str(db_path)])
    # 000001 is the hex in faa_deregistered only.
    result = runner.invoke(cli, ["registry", "lookup", "--hex", "000001", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "GHOST HELI LLC" in result.output
    assert "deregistered" in result.output.lower()


def test_registry_lookup_unknown_hex(tmp_path):
    db_path = tmp_path / "t.db"
    with Database(db_path):
        pass
    runner = CliRunner()
    result = runner.invoke(cli, ["registry", "lookup", "--hex", "ffffff", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "no record" in result.output.lower() or "not found" in result.output.lower()
