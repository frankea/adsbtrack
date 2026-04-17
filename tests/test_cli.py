"""Tests for adsbtrack.cli -- Click command surface."""

import io
import re
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
    """Build a releasable zip using the real FAA format:

    - comma-delimited CSV (not pipe)
    - UTF-8 BOM on each file
    - MASTER has OTHER NAMES(1..5) cols between AIR WORTH DATE and EXPIRATION DATE
    - DEREG has a separate dash-separated schema with MAIL / PHYSICAL addresses
    """
    master_header = (
        "N-NUMBER,SERIAL NUMBER,MFR MDL CODE,ENG MFR MDL,YEAR MFR,TYPE REGISTRANT,"
        "NAME,STREET,STREET2,CITY,STATE,ZIP CODE,REGION,COUNTY,COUNTRY,"
        "LAST ACTION DATE,CERT ISSUE DATE,CERTIFICATION,TYPE AIRCRAFT,TYPE ENGINE,"
        "STATUS CODE,MODE S CODE,FRACT OWNER,AIR WORTH DATE,"
        "OTHER NAMES(1),OTHER NAMES(2),OTHER NAMES(3),OTHER NAMES(4),OTHER NAMES(5),"
        "EXPIRATION DATE,UNIQUE ID,KIT MFR, KIT MODEL,MODE S CODE HEX\n"
    )
    master_row = (
        "512WB,66-1099,1152015,41514,1966,1,EXAMPLE OWNER LLC,100 MAIN ST,,"
        "AUSTIN,TX,78701,2,453,US,20231201,20201115,1N,4,1,V,51465323,N,19660601,"
        ",,,,,20260101,00123456,,,A66AD3\n"
    )
    dereg_header = (
        "N-NUMBER,SERIAL-NUMBER,MFR-MDL-CODE,STATUS-CODE,NAME,STREET-MAIL,STREET2-MAIL,"
        "CITY-MAIL,STATE-ABBREV-MAIL,ZIP-CODE-MAIL,ENG-MFR-MDL,YEAR-MFR,CERTIFICATION,"
        "REGION,COUNTY-MAIL,COUNTRY-MAIL,AIR-WORTH-DATE,CANCEL-DATE,MODE-S-CODE,"
        "INDICATOR-GROUP,EXP-COUNTRY,LAST-ACT-DATE,CERT-ISSUE-DATE,STREET-PHYSICAL,"
        "STREET2-PHYSICAL,CITY-PHYSICAL,STATE-ABBREV-PHYSICAL,ZIP-CODE-PHYSICAL,"
        "COUNTY-PHYSICAL,COUNTRY-PHYSICAL,OTHER-NAMES(1),OTHER-NAMES(2),"
        "OTHER-NAMES(3),OTHER-NAMES(4),OTHER-NAMES(5),KIT MFR, KIT MODEL\n"
    )
    dereg_row = (
        "99SK,12345,1234567,A,GHOST HELI LLC,200 OAK AVE,,DALLAS,TX,75201,54321,2001,1N,"
        "2,113,US,20010101,20240101,00000001,,,20240101,20210101,,,,,,,,,,,,,,\n"
    )
    acftref_body = (
        "CODE,MFR,MODEL,TYPE-ACFT,TYPE-ENG,AC-CAT,BUILD-CERT-IND,NO-ENG,NO-SEATS,AC-WEIGHT,SPEED\n"
        "1152015,CESSNA,172,4,1,1,,1,4,CLASS 1,140\n"
    )
    bom = "\ufeff".encode()
    master_bytes = bom + (master_header + master_row).encode("latin-1")
    dereg_bytes = bom + (dereg_header + dereg_row).encode("latin-1")
    acftref_bytes = bom + acftref_body.encode("latin-1")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("MASTER.txt", master_bytes)
        zf.writestr("DEREG.txt", dereg_bytes)
        zf.writestr("ACFTREF.txt", acftref_bytes)
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


def test_registry_update_reports_corrupt_zip(tmp_path):
    """A corrupt local zip should produce a friendly error, not a traceback."""
    bad_zip = tmp_path / "bad.zip"
    bad_zip.write_bytes(b"not actually a zip file")
    db_path = tmp_path / "t.db"

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["registry", "update", "--zip", str(bad_zip), "--db", str(db_path)],
    )
    assert result.exit_code != 0
    assert "corrupt" in result.output.lower()
    # Tracebacks should be suppressed by ClickException.
    assert "Traceback" not in result.output


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


def test_registry_owner_search(tmp_path):
    """Owner search returns all aircraft matching a LIKE pattern on name."""
    zip_path = tmp_path / "ReleasableAircraft.zip"
    _build_fake_releasable_zip(zip_path)
    db_path = tmp_path / "t.db"

    runner = CliRunner()
    runner.invoke(cli, ["registry", "update", "--zip", str(zip_path), "--db", str(db_path)])
    result = runner.invoke(cli, ["registry", "owner", "--name", "EXAMPLE", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "N512WB" in result.output
    assert "EXAMPLE OWNER LLC" in result.output


def test_registry_owner_no_match(tmp_path):
    zip_path = tmp_path / "ReleasableAircraft.zip"
    _build_fake_releasable_zip(zip_path)
    db_path = tmp_path / "t.db"

    runner = CliRunner()
    runner.invoke(cli, ["registry", "update", "--zip", str(zip_path), "--db", str(db_path)])
    result = runner.invoke(cli, ["registry", "owner", "--name", "NONEXISTENT", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "no" in result.output.lower() and "match" in result.output.lower()


def test_registry_address_by_street(tmp_path):
    zip_path = tmp_path / "ReleasableAircraft.zip"
    _build_fake_releasable_zip(zip_path)
    db_path = tmp_path / "t.db"

    runner = CliRunner()
    runner.invoke(cli, ["registry", "update", "--zip", str(zip_path), "--db", str(db_path)])
    result = runner.invoke(
        cli,
        ["registry", "address", "--street", "100 MAIN", "--db", str(db_path)],
    )
    assert result.exit_code == 0, result.output
    assert "N512WB" in result.output


def test_registry_address_by_city_state(tmp_path):
    zip_path = tmp_path / "ReleasableAircraft.zip"
    _build_fake_releasable_zip(zip_path)
    db_path = tmp_path / "t.db"

    runner = CliRunner()
    runner.invoke(cli, ["registry", "update", "--zip", str(zip_path), "--db", str(db_path)])
    result = runner.invoke(
        cli,
        ["registry", "address", "--city", "AUSTIN", "--state", "TX", "--db", str(db_path)],
    )
    assert result.exit_code == 0, result.output
    assert "N512WB" in result.output


def test_registry_address_requires_filter(tmp_path):
    db_path = tmp_path / "t.db"
    with Database(db_path):
        pass
    runner = CliRunner()
    result = runner.invoke(cli, ["registry", "address", "--db", str(db_path)])
    # Missing filters -> UsageError -> non-zero exit.
    assert result.exit_code != 0


def test_status_shows_faa_registry_block(tmp_path):
    """When faa_registry has the hex, status prints registrant/address/cert info."""
    zip_path = tmp_path / "ReleasableAircraft.zip"
    _build_fake_releasable_zip(zip_path)
    db_path = tmp_path / "t.db"

    runner = CliRunner()
    runner.invoke(cli, ["registry", "update", "--zip", str(zip_path), "--db", str(db_path)])

    # Also seed a trace_day for this hex so status has something to report.
    from datetime import UTC, datetime

    with Database(db_path) as db:
        db.insert_trace_day(
            "a66ad3",
            "2024-01-01",
            {
                "r": "N512WB",
                "t": "C172",
                "desc": "Cessna 172",
                "ownOp": "unknown",
                "year": "1966",
                "timestamp": datetime(2024, 1, 1, tzinfo=UTC).timestamp(),
                "trace": [],
            },
        )

    result = runner.invoke(cli, ["status", "--hex", "a66ad3", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    # New FAA block prints registrant and address cues.
    assert "EXAMPLE OWNER LLC" in result.output
    assert "AUSTIN" in result.output
    # Cert issue date surfaces somewhere.
    assert "20201115" in result.output


def test_status_flags_deregistered(tmp_path):
    """Status output notes when the hex appears in faa_deregistered."""
    zip_path = tmp_path / "ReleasableAircraft.zip"
    _build_fake_releasable_zip(zip_path)
    db_path = tmp_path / "t.db"

    runner = CliRunner()
    runner.invoke(cli, ["registry", "update", "--zip", str(zip_path), "--db", str(db_path)])

    # 000001 is the hex in faa_deregistered only.
    result = runner.invoke(cli, ["status", "--hex", "000001", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "GHOST HELI LLC" in result.output
    assert "deregistered" in result.output.lower()


def test_status_shows_position_source_breakdown(tmp_path):
    """`status` should report the ADS-B/MLAT/TIS-B mix when flights have it.

    One flight is all-ADS-B, one is all-MLAT -- weighted by data_points
    the rollup should show 50/50 (they have equal data_points).
    """
    db_path = tmp_path / "adsbtrack.db"
    with Database(db_path) as db:
        db.insert_flight(
            Flight(
                icao="ae07b3",
                takeoff_time=datetime(2022, 6, 15, 12, 0, 0, tzinfo=UTC),
                takeoff_lat=35.0,
                takeoff_lon=-118.0,
                takeoff_date="2022-06-15",
                data_points=100,
                adsb_pct=100.0,
                mlat_pct=0.0,
                tisb_pct=0.0,
            )
        )
        db.insert_flight(
            Flight(
                icao="ae07b3",
                takeoff_time=datetime(2022, 6, 16, 12, 0, 0, tzinfo=UTC),
                takeoff_lat=35.0,
                takeoff_lon=-118.0,
                takeoff_date="2022-06-16",
                data_points=100,
                adsb_pct=0.0,
                mlat_pct=100.0,
                tisb_pct=0.0,
            )
        )

    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--hex", "ae07b3", "--db", str(db_path)])

    assert result.exit_code == 0, result.output
    assert "Position sources" in result.output
    assert "ADS-B" in result.output
    assert "MLAT" in result.output
    assert "TIS-B" in result.output
    # Both 50.0% -- accept either formatting but require the digit.
    assert "50.0" in result.output


def test_acars_cli_fetches_and_stores_messages(tmp_path, monkeypatch):
    """`acars --hex <h> --start <d>` resolves the airframe and stores messages.

    The AirframesClient is monkey-patched to a fake so no network is hit.
    """
    db_path = tmp_path / "a.db"
    # Seed the registry so --tail resolution also works
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO aircraft_registry (icao, registration, last_updated) VALUES (?, ?, ?)",
            ("06a0a5", "A7-BCA", "2026-04-16T00:00:00Z"),
        )

    class FakeClient:
        def __init__(self, *a, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

        def close(self):
            pass

        def get_airframe_by_icao(self, icao):
            return {"id": 14166, "tail": "A7-BCA"}

        def get_airframe_by_id(self, aid):
            return {"flights": [{"id": 1, "createdAt": "2026-04-10T10:00:00Z"}]}

        def get_flight(self, fid):
            return {
                "id": fid,
                "messages": [
                    {
                        "id": 111,
                        "uuid": "u",
                        "timestamp": "2026-04-10T10:30:00Z",
                        "tail": "A7-BCA",
                        "label": "H1",
                        "text": "- #ok",
                        "sourceType": "acars",
                        "linkDirection": "downlink",
                        "fromHex": "06A0A5",
                        "toHex": "00",
                        "blockId": "A",
                        "ack": "!",
                        "mode": "2",
                        "messageNumber": None,
                        "flightNumber": None,
                        "data": None,
                        "latitude": None,
                        "longitude": None,
                        "altitude": None,
                        "departingAirport": None,
                        "destinationAirport": None,
                        "frequency": None,
                        "level": None,
                        "channel": None,
                    }
                ],
            }

    monkeypatch.setenv("AIRFRAMES_API_KEY", "test-key")
    monkeypatch.setattr("adsbtrack.cli.AirframesClient", FakeClient)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["acars", "--hex", "06a0a5", "--start", "2026-04-01", "--end", "2026-04-16", "--db", str(db_path)],
    )
    assert result.exit_code == 0, result.output

    with Database(db_path) as db:
        count = db.conn.execute("SELECT COUNT(*) AS c FROM acars_messages").fetchone()["c"]
        assert count == 1
        flt = db.conn.execute("SELECT message_count FROM acars_flights WHERE flight_id = 1").fetchone()
        assert flt["message_count"] == 1


def test_acars_cli_errors_without_api_key(tmp_path, monkeypatch):
    """With no env var and no credentials file, the CLI should exit non-zero with a clear error."""
    db_path = tmp_path / "a.db"
    Database(db_path).close()
    monkeypatch.delenv("AIRFRAMES_API_KEY", raising=False)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["acars", "--hex", "06a0a5", "--start", "2026-04-01", "--db", str(db_path)],
    )
    assert result.exit_code != 0
    assert "AIRFRAMES_API_KEY" in result.output or "api key" in result.output.lower()


def _seed_flight_with_acars(db_path, msg_count: int, oooi: bool = False):
    """Seed one ADS-B flight and optional ACARS messages overlapping it."""
    with Database(db_path) as db:
        db.insert_flight(
            Flight(
                icao="06a0a5",
                takeoff_time=datetime(2026, 3, 29, 2, 0, tzinfo=UTC),
                takeoff_lat=25.26,
                takeoff_lon=51.61,
                takeoff_date="2026-03-29",
                landing_time=datetime(2026, 3, 29, 15, 0, tzinfo=UTC),
                landing_lat=51.47,
                landing_lon=-0.45,
                landing_date="2026-03-29",
                origin_icao="OTHH",
                destination_icao="EGLL",
                origin_name="Doha",
                destination_name="Heathrow",
                acars_out="2026-03-29T01:33:00+00:00" if oooi else None,
                acars_off="2026-03-29T01:51:00+00:00" if oooi else None,
            )
        )
        for i in range(msg_count):
            db.insert_acars_message(
                {
                    "airframes_id": 10_000 + i,
                    "uuid": f"u{i}",
                    "flight_id": 42,
                    "icao": "06a0a5",
                    "registration": "A7-BCA",
                    "timestamp": "2026-03-29T08:00:00Z",
                    "source_type": "acars",
                    "link_direction": "uplink",
                    "from_hex": None,
                    "to_hex": None,
                    "frequency": None,
                    "level": None,
                    "channel": None,
                    "mode": "2",
                    "label": "H1",
                    "block_id": "A",
                    "message_number": None,
                    "ack": "!",
                    "flight_number": None,
                    "text": "- #ok",
                    "data": None,
                    "latitude": None,
                    "longitude": None,
                    "altitude": None,
                    "departing_airport": None,
                    "destination_airport": None,
                }
            )
        db.commit()


def test_trips_shows_acars_count_when_messages_exist(tmp_path):
    db_path = tmp_path / "t.db"
    _seed_flight_with_acars(db_path, msg_count=3)
    runner = CliRunner()
    result = runner.invoke(cli, ["trips", "--hex", "06a0a5", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    # A count of 3 messages should appear somewhere in the output
    assert "3" in result.output
    # Header or per-row marker identifying the ACARS column
    assert "ACARS" in result.output


def test_trips_shows_oooi_marker(tmp_path):
    db_path = tmp_path / "t.db"
    _seed_flight_with_acars(db_path, msg_count=1, oooi=True)
    runner = CliRunner()
    result = runner.invoke(cli, ["trips", "--hex", "06a0a5", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    # Some visual indicator of OOOI data present
    assert "OOOI" in result.output or "O" in result.output


def test_status_shows_acars_section(tmp_path):
    db_path = tmp_path / "t.db"
    _seed_flight_with_acars(db_path, msg_count=5)
    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--hex", "06a0a5", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "ACARS" in result.output
    assert "5" in result.output  # message count


# ---------------------------------------------------------------------------
# enrich / mil commands
# ---------------------------------------------------------------------------


def _write_mictronics_fixture(cache_dir):
    import json

    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / "aircrafts.json").write_text(
        json.dumps(
            {
                "a66ad3": ["N512WB", "PC12", "00"],
                "c01234": ["C-ABCD", "B737", "00"],
            }
        )
    )
    (cache_dir / "types.json").write_text(
        json.dumps({"PC12": ["PILATUS PC-12", "M", "L"], "B737": ["BOEING 737", "M", "L"]})
    )
    (cache_dir / "operators.json").write_text("{}")
    (cache_dir / "dbversion.json").write_text(json.dumps({"version": "20260101"}))


def test_enrich_hex_uses_mictronics_cache(tmp_path):
    """`enrich hex` with a Mictronics cache fills the row."""
    db_path = tmp_path / "t.db"
    cache_dir = tmp_path / "mictronics"
    _write_mictronics_fixture(cache_dir)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "enrich",
            "hex",
            "--hex",
            "a66ad3",
            "--mictronics-dir",
            str(cache_dir),
            "--no-hexdb",
            "--db",
            str(db_path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "N512WB" in result.output
    assert "PILATUS PC-12" in result.output


def test_enrich_hex_no_data_reports_yellow(tmp_path):
    db_path = tmp_path / "t.db"
    empty_mictronics = tmp_path / "empty-mictronics"
    empty_mictronics.mkdir()
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "enrich",
            "hex",
            "--hex",
            "a66ad3",
            "--no-hexdb",
            "--mictronics-dir",
            str(empty_mictronics),
            "--db",
            str(db_path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "no data" in result.output.lower()


def test_enrich_hex_military_flags_military(tmp_path):
    """A seeded military range should flag is_military regardless of identity sources."""
    db_path = tmp_path / "t.db"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["enrich", "hex", "--hex", "ae1234", "--no-hexdb", "--db", str(db_path)],
    )
    assert result.exit_code == 0, result.output
    assert "Military" in result.output
    assert "United States" in result.output


def test_mil_hex_reports_range(tmp_path):
    db_path = tmp_path / "t.db"
    runner = CliRunner()
    result = runner.invoke(cli, ["mil", "hex", "--hex", "ae1234", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "Military hex" in result.output
    assert "United States" in result.output


def test_mil_hex_civilian_is_clear(tmp_path):
    db_path = tmp_path / "t.db"
    runner = CliRunner()
    result = runner.invoke(cli, ["mil", "hex", "--hex", "a66ad3", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "not in any known military range" in result.output


def test_mil_scan_finds_military_aircraft(tmp_path):
    from datetime import UTC, datetime

    from adsbtrack.db import Database
    from adsbtrack.models import Flight

    db_path = tmp_path / "t.db"
    with Database(db_path) as db:
        db.insert_flight(
            Flight(
                icao="ae1234",
                takeoff_time=datetime(2022, 6, 15, 12, 0, 0, tzinfo=UTC),
                takeoff_lat=35.0,
                takeoff_lon=-118.0,
                takeoff_date="2022-06-15",
            )
        )
        db.insert_flight(
            Flight(
                icao="a66ad3",
                takeoff_time=datetime(2022, 6, 15, 13, 0, 0, tzinfo=UTC),
                takeoff_lat=35.0,
                takeoff_lon=-118.0,
                takeoff_date="2022-06-15",
            )
        )

    runner = CliRunner()
    result = runner.invoke(cli, ["mil", "scan", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "ae1234" in result.output
    assert "United States" in result.output


def test_trips_renders_alignment_column_when_flag_set(tmp_path, monkeypatch):
    """`trips --alignment` must add the RWY column and render a row when
    alignment data exists."""
    db_path = tmp_path / "a.db"
    with Database(db_path) as db:
        f = Flight(
            icao="abc123",
            takeoff_time=datetime(2023, 11, 14, 10, 0),
            takeoff_lat=33.0,
            takeoff_lon=-84.0,
            takeoff_date="2023-11-14",
            landing_time=datetime(2023, 11, 14, 11, 0),
            landing_lat=33.64,
            landing_lon=-84.43,
            landing_date="2023-11-14",
            destination_icao="KFAKE",
            destination_name="Fake Intl",
            destination_distance_km=0.5,
            duration_minutes=60.0,
            landing_type="confirmed",
            landing_confidence=0.85,
            aligned_runway="09",
            aligned_seconds=85.0,
            aligned_min_offset_m=42.3,
        )
        db.insert_flight(f)

    runner = CliRunner()
    monkeypatch.setenv("COLUMNS", "200")
    result = runner.invoke(
        cli,
        ["trips", "--hex", "abc123", "--db", str(db_path), "--alignment"],
    )
    assert result.exit_code == 0, result.output
    assert "Aligned" in result.output
    assert "RWY 09" in result.output and "85s" in result.output


def test_trips_auto_shows_alignment_column_when_any_row_has_data(tmp_path, monkeypatch):
    """If any row has aligned_runway, the column shows up even without the flag."""
    db_path = tmp_path / "a.db"
    with Database(db_path) as db:
        f = Flight(
            icao="abc456",
            takeoff_time=datetime(2023, 11, 14, 10, 0),
            takeoff_lat=33.0,
            takeoff_lon=-84.0,
            takeoff_date="2023-11-14",
            landing_time=datetime(2023, 11, 14, 11, 0),
            landing_lat=33.64,
            landing_lon=-84.43,
            landing_date="2023-11-14",
            destination_icao="KFAKE",
            destination_name="Fake Intl",
            destination_distance_km=0.5,
            duration_minutes=60.0,
            landing_type="confirmed",
            landing_confidence=0.85,
            aligned_runway="27",
            aligned_seconds=62.7,
            aligned_min_offset_m=18.0,
        )
        db.insert_flight(f)

    runner = CliRunner()
    monkeypatch.setenv("COLUMNS", "200")
    result = runner.invoke(
        cli,
        ["trips", "--hex", "abc456", "--db", str(db_path)],
    )
    assert result.exit_code == 0, result.output
    assert "Aligned" in result.output
    assert "RWY 27" in result.output and "63s" in result.output  # 62.7 rounds to 63


def test_trips_from_column_appends_takeoff_runway(tmp_path, monkeypatch) -> None:
    """trips From column shows `KSPG/24` when takeoff_runway is populated."""
    monkeypatch.setenv("COLUMNS", "200")
    db_path = tmp_path / "a.db"
    with Database(db_path) as db:
        f = Flight(
            icao="abc789",
            takeoff_time=datetime(2023, 11, 14, 10, 0),
            takeoff_lat=27.76,
            takeoff_lon=-82.63,
            takeoff_date="2023-11-14",
            landing_time=datetime(2023, 11, 14, 11, 0),
            landing_lat=27.0,
            landing_lon=-82.0,
            landing_date="2023-11-14",
            origin_icao="KSPG",
            origin_name="Albert Whitted",
            origin_distance_km=0.3,
            destination_icao="KPIE",
            destination_name="St Petersburg-Clearwater",
            destination_distance_km=0.5,
            duration_minutes=60.0,
            landing_type="confirmed",
            landing_confidence=0.9,
            takeoff_runway="24",
        )
        db.insert_flight(f)

    runner = CliRunner()
    result = runner.invoke(cli, ["trips", "--hex", "abc789", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "KSPG/24" in result.output


def test_trips_from_column_plain_when_takeoff_runway_null(tmp_path, monkeypatch) -> None:
    """No `/24` suffix when takeoff_runway is NULL."""
    monkeypatch.setenv("COLUMNS", "200")
    db_path = tmp_path / "a.db"
    with Database(db_path) as db:
        f = Flight(
            icao="abc790",
            takeoff_time=datetime(2023, 11, 14, 10, 0),
            takeoff_lat=27.76,
            takeoff_lon=-82.63,
            takeoff_date="2023-11-14",
            landing_time=datetime(2023, 11, 14, 11, 0),
            landing_lat=27.0,
            landing_lon=-82.0,
            landing_date="2023-11-14",
            origin_icao="KSPG",
            origin_name="Albert Whitted",
            origin_distance_km=0.3,
            destination_icao="KPIE",
            destination_name="St Petersburg-Clearwater",
            destination_distance_km=0.5,
            duration_minutes=60.0,
            landing_type="confirmed",
            landing_confidence=0.9,
        )
        db.insert_flight(f)

    runner = CliRunner()
    result = runner.invoke(cli, ["trips", "--hex", "abc790", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "KSPG" in result.output
    assert "KSPG/" not in result.output


def test_status_shows_go_around_and_pattern_counts(tmp_path, monkeypatch) -> None:
    """status output includes go-around count and pattern-work count."""
    monkeypatch.setenv("COLUMNS", "200")
    db_path = tmp_path / "a.db"
    with Database(db_path) as db:
        # Two go-around flights; five pattern-work flights; one normal.
        for i, had_ga, pcycles in [
            (0, 1, 2),
            (1, 1, 3),
            (2, 0, 4),
            (3, 0, 5),
            (4, 0, 2),
            (5, 0, 1),
        ]:
            f = Flight(
                icao="abc999",
                takeoff_time=datetime(2024, 6, 1, 10 + i, 0),
                takeoff_lat=27.76,
                takeoff_lon=-82.63,
                takeoff_date=f"2024-06-{1 + i:02d}",
                landing_time=datetime(2024, 6, 1, 11 + i, 0),
                landing_lat=27.76,
                landing_lon=-82.63,
                landing_date=f"2024-06-{1 + i:02d}",
                origin_icao="KSPG",
                origin_name="Albert Whitted",
                origin_distance_km=0.3,
                destination_icao="KSPG",
                destination_name="Albert Whitted",
                destination_distance_km=0.3,
                duration_minutes=60.0,
                landing_type="confirmed",
                landing_confidence=0.9,
                had_go_around=had_ga,
                pattern_cycles=pcycles,
            )
            db.insert_flight(f)

    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--hex", "abc999", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "Go-arounds:" in result.output
    assert "Pattern work:" in result.output
    go_arounds = re.search(r"Go-arounds:\s+(\d+)", result.output)
    assert go_arounds is not None
    assert go_arounds.group(1) == "2"

    pattern_flights = re.search(r"Pattern work:\s+(\d+)", result.output)
    assert pattern_flights is not None
    assert pattern_flights.group(1) == "5"


def test_status_shows_emergency_breakdown_and_avg_squawk_changes(tmp_path, monkeypatch) -> None:
    """status output includes per-code emergency breakdown + avg squawk changes."""
    monkeypatch.setenv("COLUMNS", "200")
    db_path = tmp_path / "a.db"
    with Database(db_path) as db:
        # Two 7700 flights, one 7600 flight, two normal with changes
        seed = [
            ("7700", 3),
            ("7700", 1),
            ("7600", 0),
            (None, 5),
            (None, 2),
        ]
        for i, (em, changes) in enumerate(seed):
            f = Flight(
                icao="aaaeme",
                takeoff_time=datetime(2024, 6, 1, 10 + i, 0),
                takeoff_lat=27.76,
                takeoff_lon=-82.63,
                takeoff_date=f"2024-06-{1 + i:02d}",
                landing_time=datetime(2024, 6, 1, 11 + i, 0),
                landing_lat=27.76,
                landing_lon=-82.63,
                landing_date=f"2024-06-{1 + i:02d}",
                origin_icao="KSPG",
                destination_icao="KSPG",
                duration_minutes=60.0,
                landing_type="confirmed",
                landing_confidence=0.9,
                emergency_squawk=em,
                had_emergency=1 if em else 0,
                squawk_changes=changes,
            )
            db.insert_flight(f)

    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--hex", "aaaeme", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "Emergencies:" in result.output
    # Use regex: "2 (7700)" for the two 7700 flights
    assert re.search(r"2\s*\(7700\)", result.output) is not None
    assert re.search(r"1\s*\(7600\)", result.output) is not None
    # Avg squawk changes should be (3+1+0+5+2)/5 = 2.2
    assert re.search(r"Squawk changes.*2\.2", result.output) is not None


def test_trips_show_squawk_renders_primary_column(tmp_path, monkeypatch) -> None:
    """trips --show-squawk adds a Squawk column and renders primary_squawk."""
    monkeypatch.setenv("COLUMNS", "200")
    db_path = tmp_path / "a.db"
    with Database(db_path) as db:
        f = Flight(
            icao="sqwk01",
            takeoff_time=datetime(2024, 6, 1, 10, 0),
            takeoff_lat=27.76,
            takeoff_lon=-82.63,
            takeoff_date="2024-06-01",
            landing_time=datetime(2024, 6, 1, 11, 0),
            landing_lat=28.0,
            landing_lon=-82.5,
            landing_date="2024-06-01",
            origin_icao="KSPG",
            destination_icao="KPIE",
            duration_minutes=60.0,
            landing_type="confirmed",
            landing_confidence=0.9,
            primary_squawk="1200",
        )
        db.insert_flight(f)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["trips", "--hex", "sqwk01", "--db", str(db_path), "--show-squawk"],
    )
    assert result.exit_code == 0, result.output
    assert "Squawk" in result.output
    assert "1200" in result.output


def test_trips_no_squawk_column_by_default(tmp_path, monkeypatch) -> None:
    """Without --show-squawk the Squawk column is hidden."""
    monkeypatch.setenv("COLUMNS", "200")
    db_path = tmp_path / "a.db"
    with Database(db_path) as db:
        f = Flight(
            icao="sqwk02",
            takeoff_time=datetime(2024, 6, 1, 10, 0),
            takeoff_lat=27.76,
            takeoff_lon=-82.63,
            takeoff_date="2024-06-01",
            landing_time=datetime(2024, 6, 1, 11, 0),
            landing_lat=28.0,
            landing_lon=-82.5,
            landing_date="2024-06-01",
            origin_icao="KSPG",
            destination_icao="KPIE",
            duration_minutes=60.0,
            landing_type="confirmed",
            landing_confidence=0.9,
            primary_squawk="1200",
        )
        db.insert_flight(f)

    runner = CliRunner()
    result = runner.invoke(cli, ["trips", "--hex", "sqwk02", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "Squawk" not in result.output  # column hidden by default


def test_navaids_refresh_local_csv(tmp_path, monkeypatch):
    from click.testing import CliRunner

    from adsbtrack.cli import cli

    db_path = tmp_path / "nav.db"
    fixture = Path(__file__).parent / "fixtures" / "navaids_sample.csv"

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["navaids", "refresh", "--csv", str(fixture), "--db", str(db_path)],
    )
    assert result.exit_code == 0, result.output
    assert "3 navaids" in result.output


def test_route_cli_prints_chain(tmp_path, monkeypatch):
    import json
    from datetime import datetime

    from click.testing import CliRunner

    from adsbtrack.cli import cli
    from adsbtrack.db import Database
    from adsbtrack.models import Flight

    monkeypatch.setenv("COLUMNS", "200")
    db_path = tmp_path / "r.db"
    track = json.dumps(
        [
            {"navaid_ident": "SHAWZ", "start_ts": 0.0, "end_ts": 900.0, "min_distance_nm": 30.0},
            {"navaid_ident": "KEEMO", "start_ts": 900.0, "end_ts": 1380.0, "min_distance_nm": 20.0},
            {"navaid_ident": "CLT", "start_ts": 1400.0, "end_ts": 1580.0, "min_distance_nm": 1.5},
        ]
    )
    with Database(db_path) as db:
        db.insert_flight(
            Flight(
                icao="abc123",
                takeoff_time=datetime(2026, 3, 27, 14, 0, 0),
                takeoff_lat=35.0,
                takeoff_lon=-80.0,
                takeoff_date="2026-03-27",
                origin_icao="KSPG",
                destination_icao="KHKY",
                navaid_track=track,
            )
        )

    result = CliRunner().invoke(cli, ["route", "--hex", "abc123", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "2026-03-27 KSPG -> KHKY" in result.output
    assert "SHAWZ (15m) -> KEEMO (8m) -> CLT (3m)" in result.output


def test_route_cli_no_data(tmp_path):
    from click.testing import CliRunner

    from adsbtrack.cli import cli
    from adsbtrack.db import Database

    db_path = tmp_path / "r.db"
    with Database(db_path):
        pass  # empty DB, schema only

    result = CliRunner().invoke(cli, ["route", "--hex", "abc123", "--db", str(db_path)])
    assert result.exit_code == 0
    assert "No navaid track" in result.output


def test_route_cli_short_segment_under_a_minute(tmp_path, monkeypatch):
    """A segment that lasts 40 s is rendered as '<1m' (stays visible but
    not misreported as 0m)."""
    import json
    from datetime import datetime

    from click.testing import CliRunner

    from adsbtrack.cli import cli
    from adsbtrack.db import Database
    from adsbtrack.models import Flight

    monkeypatch.setenv("COLUMNS", "200")
    db_path = tmp_path / "r.db"
    track = json.dumps([{"navaid_ident": "NDB1", "start_ts": 0.0, "end_ts": 40.0, "min_distance_nm": 5.0}])
    with Database(db_path) as db:
        db.insert_flight(
            Flight(
                icao="abc123",
                takeoff_time=datetime(2026, 3, 27, 14, 0, 0),
                takeoff_lat=35.0,
                takeoff_lon=-80.0,
                takeoff_date="2026-03-27",
                navaid_track=track,
            )
        )
    result = CliRunner().invoke(cli, ["route", "--hex", "abc123", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "NDB1 (<1m)" in result.output
