"""Tests for adsbtrack.cli -- Click command surface."""

import io
import json
import re
import zipfile
from datetime import UTC, date, datetime, timedelta
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


def test_acars_cli_wires_progress_callback(tmp_path, monkeypatch):
    """`acars` wires a Rich progress bar through fetch_acars's progress_callback,
    and the bar description surfaces the client's rate-limit remaining counts
    when the client has populated them. Pipeline is mocked (no network); this
    is a smoke test that the callback plumbing doesn't crash and reaches
    completion."""
    from adsbtrack import cli as cli_module

    db_path = tmp_path / "a.db"
    Database(db_path).close()

    class FakeClient:
        def __init__(self, *a, **kw):
            self.minute_remaining = 42
            self.daily_remaining = 900

        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

    def fake_fetch_acars(db, client, icao, *, start_date, end_date, progress_callback=None):
        if progress_callback is not None:
            for i in range(1, 4):
                progress_callback(i, 3)
        return {"flights_fetched": 3, "flights_skipped": 0, "messages_inserted": 5, "flights_with_oooi": 1}

    monkeypatch.setenv("AIRFRAMES_API_KEY", "test-key")
    monkeypatch.setattr(cli_module, "AirframesClient", FakeClient)
    monkeypatch.setattr(cli_module, "fetch_acars", fake_fetch_acars)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["acars", "--hex", "06a0a5", "--start", "2026-04-01", "--db", str(db_path)],
    )
    assert result.exit_code == 0, result.output
    assert "Flights fetched: 3" in result.output
    # Bar description surfaced the client's rate-limit counters.
    assert "42" in result.output
    assert "900" in result.output


def test_acars_cli_errors_without_api_key(monkeypatch):
    """With no env var and no credentials file, the CLI should exit non-zero with a clear error.

    Config.credentials_path defaults to a relative "credentials.json", resolved
    against the CWD. Without isolating the CWD, this test would silently pass or
    fail depending on whether the machine running it happens to have a real
    credentials.json sitting in the repo root -- so it runs inside
    runner.isolated_filesystem() to guarantee no such file is visible.
    """
    monkeypatch.delenv("AIRFRAMES_API_KEY", raising=False)
    runner = CliRunner()
    with runner.isolated_filesystem():
        Database(Path("a.db")).close()
        result = runner.invoke(
            cli,
            ["acars", "--hex", "06a0a5", "--start", "2026-04-01", "--db", "a.db"],
        )
    assert result.exit_code != 0
    assert "AIRFRAMES_API_KEY" in result.output or "api key" in result.output.lower()


def test_acars_cli_honors_config_file_via_adsbtrack_config(tmp_path, monkeypatch):
    """A real CLI command builds its Config through the shared config-file
    loader (adsbtrack.cli._load_config), not a bare Config(db_path=...) --
    every command must pick up $ADSBTRACK_CONFIG overrides, not just
    Config.load() when called directly.

    Point $ADSBTRACK_CONFIG at a TOML file overriding credentials_path, then
    invoke `acars` (no API key set, so it fails fast before touching the
    database) and confirm the resulting error message -- which embeds
    config.credentials_path -- names the overridden path rather than the
    hardcoded "credentials.json" default.
    """
    custom_creds = tmp_path / "custom-creds.json"
    config_toml = tmp_path / "adsbtrack.toml"
    config_toml.write_text(f'credentials_path = "{custom_creds}"\n')
    monkeypatch.delenv("AIRFRAMES_API_KEY", raising=False)
    monkeypatch.setenv("ADSBTRACK_CONFIG", str(config_toml))

    runner = CliRunner()
    with runner.isolated_filesystem():
        Database(Path("a.db")).close()
        result = runner.invoke(
            cli,
            ["acars", "--hex", "06a0a5", "--start", "2026-04-01", "--db", "a.db"],
        )
    assert result.exit_code != 0
    assert str(custom_creds) in result.output


def test_load_airframes_api_key_reads_credentials_json(monkeypatch):
    """Inverse of the above: with the env var unset and an isolated CWD that
    *does* have a credentials.json carrying airframesApiKey, the loader
    returns that key. Proves the CWD-relative fallback path actually works,
    not just that it fails safely when absent."""
    import json

    from adsbtrack.cli import _load_airframes_api_key
    from adsbtrack.config import Config

    monkeypatch.delenv("AIRFRAMES_API_KEY", raising=False)
    runner = CliRunner()
    with runner.isolated_filesystem():
        Path("credentials.json").write_text(json.dumps({"airframesApiKey": "k"}))
        config = Config(db_path=Path("a.db"))
        assert _load_airframes_api_key(config) == "k"


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


def test_enrich_all_cli_wires_progress_callback(tmp_path, monkeypatch):
    """`enrich all` wires a Rich progress bar through enrich_all's
    progress_callback (hexes processed / total). Pipeline is mocked; this is
    a smoke test that the callback plumbing doesn't crash and reaches
    completion. enrich_all_cmd imports enrich_all locally from hex_crossref
    on each invocation, so the fake is patched on the source module."""

    wired = {"progress_callback_used": False}

    def fake_enrich_all(db, *, cfg=None, mictronics_cache_dir=None, use_hexdb=True, progress_callback=None):
        assert progress_callback is not None, "enrich all must wire a progress_callback"
        wired["progress_callback_used"] = True
        for i in range(1, 6):
            progress_callback(i, 5)
        return {"processed": 5, "written": 3, "no_data": 2, "conflicts": 0}

    monkeypatch.setattr("adsbtrack.hex_crossref.enrich_all", fake_enrich_all)

    db_path = tmp_path / "t.db"
    Database(db_path).close()

    runner = CliRunner()
    result = runner.invoke(cli, ["enrich", "all", "--no-hexdb", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert wired["progress_callback_used"]
    assert "Processed 5" in result.output
    assert "wrote 3" in result.output


def _v2_sample(sil, nic, flight=""):
    ac = {"version": 2, "nic": nic, "sil": sil, "flight": flight}
    return [0.0, 25.25, 55.38, "ground", 0.5, 30.9, 0, None, ac, "adsb_icao", None, None, None, None]


def test_db_optimize_backfills_legacy_rows_and_is_idempotent(tmp_path):
    """`db optimize` compresses a legacy raw-JSON trace_json row and fills
    the four materialized integrity-stat columns; a second run reports
    nothing left to do."""
    db_path = tmp_path / "legacy.db"
    trace = [_v2_sample(8, 8, "UAL1")] * 3 + [_v2_sample(0, 0, "UAL1")]
    with Database(db_path) as db:
        db.conn.execute(
            """INSERT INTO trace_days
               (icao, date, source, timestamp, trace_json, point_count, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "abc123",
                "2024-01-15",
                "adsbx",
                1700000000.0,
                json.dumps(trace),
                len(trace),
                datetime(2024, 1, 15, tzinfo=UTC).isoformat(),
            ),
        )
        db.commit()

    runner = CliRunner()
    result = runner.invoke(cli, ["db", "optimize", "--db", str(db_path)])
    assert result.exit_code == 0, result.output

    with Database(db_path) as db:
        row = db.conn.execute(
            "SELECT trace_json, v2_samples, v2_sil0, v2_nic0, v2_callsigns FROM trace_days WHERE icao = ?",
            ("abc123",),
        ).fetchone()
    assert isinstance(row["trace_json"], bytes) and row["trace_json"][:1] == b"\x78"
    assert row["v2_samples"] == 4
    assert row["v2_sil0"] == 1
    assert row["v2_nic0"] == 1
    assert row["v2_callsigns"] == 1

    second = runner.invoke(cli, ["db", "optimize", "--db", str(db_path)])
    assert second.exit_code == 0, second.output
    assert "nothing to optimize" in second.output.lower()


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


def test_trips_shows_probable_destination_fallback_for_signal_lost(tmp_path, monkeypatch) -> None:
    """Issue #18: signal_lost flights with an inferred probable_destination_icao
    render `~ICAO` (the existing dropped_on_approach fallback convention) instead
    of just "signal lost" -- _infer_probable_destination computes this field for
    both landing types, but the table only rendered it for one."""
    monkeypatch.setenv("COLUMNS", "200")
    db_path = tmp_path / "a.db"
    with Database(db_path) as db:
        f = Flight(
            icao="ad677e",
            takeoff_time=datetime(2022, 6, 5, 10, 0),
            takeoff_lat=27.76,
            takeoff_lon=-82.63,
            takeoff_date="2022-06-05",
            landing_time=datetime(2022, 6, 5, 11, 0),
            landing_lat=38.05,
            landing_lon=-116.78,
            landing_date="2022-06-05",
            origin_icao="KSPG",
            destination_icao=None,
            duration_minutes=60.0,
            landing_type="signal_lost",
            landing_confidence=0.4,
            probable_destination_icao="KTNX",
            probable_destination_distance_km=4.63,
        )
        db.insert_flight(f)

    runner = CliRunner()
    result = runner.invoke(cli, ["trips", "--hex", "ad677e", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "~KTNX" in result.output
    assert "signal lost" not in result.output


def test_trips_signal_lost_without_probable_destination_still_shows_signal_lost(tmp_path, monkeypatch) -> None:
    """Regression: a signal_lost flight with no inferred destination keeps
    showing the red "signal lost" marker instead of a bogus fallback."""
    monkeypatch.setenv("COLUMNS", "200")
    db_path = tmp_path / "a.db"
    with Database(db_path) as db:
        f = Flight(
            icao="ad677f",
            takeoff_time=datetime(2022, 6, 5, 10, 0),
            takeoff_lat=27.76,
            takeoff_lon=-82.63,
            takeoff_date="2022-06-05",
            landing_time=datetime(2022, 6, 5, 11, 0),
            landing_lat=38.05,
            landing_lon=-116.78,
            landing_date="2022-06-05",
            origin_icao="KSPG",
            destination_icao=None,
            duration_minutes=60.0,
            landing_type="signal_lost",
            landing_confidence=0.4,
        )
        db.insert_flight(f)

    runner = CliRunner()
    result = runner.invoke(cli, ["trips", "--hex", "ad677f", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "signal lost" in result.output


def test_trips_shows_nearest_origin_fallback_when_origin_null(tmp_path, monkeypatch) -> None:
    """Issue #18: origin falls back to `~NEAREST` (yellow) when origin_icao is
    NULL but nearest_origin_icao was resolved (the on-field gate keeps a
    near-match airport out of origin_icao when coverage starts mid-climb)."""
    monkeypatch.setenv("COLUMNS", "200")
    db_path = tmp_path / "a.db"
    with Database(db_path) as db:
        f = Flight(
            icao="ad677e",
            takeoff_time=datetime(2022, 6, 5, 10, 0),
            takeoff_lat=38.06,
            takeoff_lon=-116.77,
            takeoff_date="2022-06-05",
            landing_time=datetime(2022, 6, 5, 11, 0),
            landing_lat=27.76,
            landing_lon=-82.63,
            landing_date="2022-06-05",
            origin_icao=None,
            nearest_origin_icao="KTNX",
            nearest_origin_distance_km=2.67,
            destination_icao="KSPG",
            duration_minutes=60.0,
            landing_type="confirmed",
            landing_confidence=0.9,
        )
        db.insert_flight(f)

    runner = CliRunner()
    result = runner.invoke(cli, ["trips", "--hex", "ad677e", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "~KTNX" in result.output


def test_trips_json_includes_fallback_fields(tmp_path) -> None:
    """--json carries the raw fields backing both fallbacks so downstream
    consumers don't need to re-query the DB to compute them."""
    db_path = tmp_path / "t.db"
    with Database(db_path) as db:
        db.insert_flight(
            Flight(
                icao="ad677e",
                takeoff_time=datetime(2022, 6, 5, 10, 0),
                takeoff_lat=38.06,
                takeoff_lon=-116.77,
                takeoff_date="2022-06-05",
                landing_time=datetime(2022, 6, 5, 11, 0),
                landing_lat=38.05,
                landing_lon=-116.78,
                landing_date="2022-06-05",
                origin_icao=None,
                nearest_origin_icao="KTNX",
                nearest_origin_distance_km=2.67,
                destination_icao=None,
                duration_minutes=60.0,
                landing_type="signal_lost",
                landing_confidence=0.4,
                probable_destination_icao="KTNX",
                probable_destination_distance_km=4.63,
            )
        )

    result = CliRunner().invoke(cli, ["trips", "--hex", "ad677e", "--db", str(db_path), "--json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload[0]["nearest_origin_icao"] == "KTNX"
    assert payload[0]["nearest_origin_distance_km"] == 2.67
    assert payload[0]["probable_destination_icao"] == "KTNX"
    assert payload[0]["probable_destination_distance_km"] == 4.63


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
                icao="aaaeee",
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
    result = runner.invoke(cli, ["status", "--hex", "aaaeee", "--db", str(db_path)])
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
            icao="aab001",
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
        ["trips", "--hex", "aab001", "--db", str(db_path), "--show-squawk"],
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
            icao="aab002",
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
    result = runner.invoke(cli, ["trips", "--hex", "aab002", "--db", str(db_path)])
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


# ---------------------------------------------------------------------------
# _resolve_hex_db: registration-based fetch resolution (shot 4)
# ---------------------------------------------------------------------------


def test_resolve_hex_db_nnumber_uses_algorithm(tmp_path):
    """N-number resolution must not touch the DB. The algorithmic path
    works for any valid N-number whether observed or not."""
    from adsbtrack.cli import _resolve_hex_db

    db_path = tmp_path / "noaircraft.db"
    with Database(db_path) as db:
        # Empty aircraft_registry and hex_crossref -- should still resolve.
        resolved = _resolve_hex_db(db, None, "N512WB")
    assert resolved == "a66ad3"


def test_resolve_hex_db_nonus_reg_via_aircraft_registry(tmp_path):
    from adsbtrack.cli import _resolve_hex_db

    db_path = tmp_path / "reg.db"
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO aircraft_registry (icao, registration, last_updated) VALUES (?, ?, ?)",
            ("abc123", "G-XYZA", "2026-04-10T00:00:00Z"),
        )
        db.conn.commit()
        resolved = _resolve_hex_db(db, None, "G-XYZA")
    assert resolved == "abc123"


def test_resolve_hex_db_nonus_reg_via_hex_crossref(tmp_path):
    """aircraft_registry empty but hex_crossref has the reg: fall through
    to hex_crossref and resolve successfully."""
    from adsbtrack.cli import _resolve_hex_db

    db_path = tmp_path / "xref.db"
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO hex_crossref (icao, registration, source) VALUES (?, ?, ?)",
            ("def456", "D-ABCD", "mictronics"),
        )
        db.conn.commit()
        resolved = _resolve_hex_db(db, None, "D-ABCD")
    assert resolved == "def456"


def test_resolve_hex_db_multiple_matches_picks_newest_and_warns(tmp_path, capsys):
    """If the same reg appears on multiple icao rows in aircraft_registry
    (reg reassigned across aircraft), pick the most-recent last_updated
    and warn on stderr. An analyst with a specific hex in mind can pass
    --hex instead."""

    from adsbtrack.cli import _resolve_hex_db

    db_path = tmp_path / "multi.db"
    with Database(db_path) as db:
        for icao, ts in [("aaa001", "2020-01-01T00:00:00Z"), ("bbb002", "2025-11-20T00:00:00Z")]:
            db.conn.execute(
                "INSERT INTO aircraft_registry (icao, registration, last_updated) VALUES (?, ?, ?)",
                (icao, "N999TEST", ts),
            )
        db.conn.commit()
        # Bypass the algorithmic path: N999TEST is syntactically a valid
        # N-number so nnumber_to_icao will succeed -- that's the correct
        # behavior (algorithm wins over stored data). For the multi-match
        # test we need a non-N-number reg, use a UK-style one:
        for icao, ts in [("ccc003", "2020-01-01T00:00:00Z"), ("ddd004", "2025-11-20T00:00:00Z")]:
            db.conn.execute(
                "INSERT INTO aircraft_registry (icao, registration, last_updated) VALUES (?, ?, ?)",
                (icao, "G-MULTI", ts),
            )
        db.conn.commit()
        resolved = _resolve_hex_db(db, None, "G-MULTI")
    assert resolved == "ddd004"  # newer last_updated wins
    # The warning goes through rich.Console(), but either way it mustn't
    # fail; subsequent behavior is the critical contract.
    _ = capsys.readouterr()  # drain


def test_resolve_hex_db_unknown_reg_errors_with_guidance(tmp_path):
    """Unknown tail raises click.UsageError with a message pointing at
    the manual resolution paths (--hex, registry update, fetch by hex
    first)."""
    import click
    import pytest

    from adsbtrack.cli import _resolve_hex_db

    db_path = tmp_path / "empty.db"
    with Database(db_path) as db, pytest.raises(click.UsageError) as excinfo:
        _resolve_hex_db(db, None, "G-NOPE")
    msg = str(excinfo.value).lower()
    assert "g-nope" in msg or "not" in msg  # surfaces the offending tail
    assert "--hex" in msg or "registry update" in msg  # points at remediation


def test_fetch_cli_accepts_nonus_tail(tmp_path, monkeypatch):
    """End-to-end: `adsbtrack fetch --tail G-XYZA` must resolve the reg
    via aircraft_registry and proceed to (mocked) fetch. Uses monkeypatch
    to stub fetch_traces so the test doesn't do real HTTP."""
    from adsbtrack import cli as cli_module

    calls: list[str] = []

    def fake_fetch_traces(db, config, hex_code, start, end, *, source="adsbx"):
        calls.append(hex_code)
        return {"fetched": 0, "with_data": 0, "skipped": 0, "errors": 0}

    monkeypatch.setattr(cli_module, "fetch_traces", fake_fetch_traces)

    db_path = tmp_path / "fetch.db"
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO aircraft_registry (icao, registration, last_updated) VALUES (?, ?, ?)",
            ("abcdef", "G-XYZA", "2026-04-10T00:00:00Z"),
        )
        db.conn.commit()
        # Seed one airport so ensure_airports() doesn't try to download.
        db.conn.execute(
            "INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type) "
            "VALUES ('EGLL', 'London Heathrow', 51.47, -0.45, 'large_airport')"
        )
        db.conn.commit()

    result = CliRunner().invoke(
        cli,
        [
            "fetch",
            "--tail",
            "G-XYZA",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
            "--db",
            str(db_path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert calls == ["abcdef"]


def test_fetch_cli_prints_failed_days(tmp_path, monkeypatch):
    """When fetch_traces reports retry-exhausted days, the summary lists
    each date with its terminal status so the user knows what still
    needs a retry."""
    from adsbtrack import cli as cli_module

    def fake_fetch_traces(db, config, hex_code, start, end, *, source="adsbx"):
        return {
            "fetched": 2,
            "with_data": 0,
            "skipped": 0,
            "errors": 2,
            "failed_days": [("2026-05-02", 403), ("2026-05-03", 503)],
        }

    monkeypatch.setattr(cli_module, "fetch_traces", fake_fetch_traces)

    db_path = tmp_path / "fetch.db"
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type) "
            "VALUES ('EGLL', 'London Heathrow', 51.47, -0.45, 'large_airport')"
        )
        db.conn.commit()

    result = CliRunner().invoke(
        cli,
        ["fetch", "--hex", "abcdef", "--start", "2026-05-02", "--end", "2026-05-03", "--db", str(db_path)],
    )
    assert result.exit_code == 0, result.output
    assert "Failed days (will retry on next run): 2026-05-02 (403), 2026-05-03 (503)" in result.output


def test_fetch_cli_omits_failed_days_header_when_empty(tmp_path, monkeypatch):
    """No failed_days -> no extra header/line in the summary output."""
    from adsbtrack import cli as cli_module

    def fake_fetch_traces(db, config, hex_code, start, end, *, source="adsbx"):
        return {"fetched": 1, "with_data": 1, "skipped": 0, "errors": 0, "failed_days": []}

    monkeypatch.setattr(cli_module, "fetch_traces", fake_fetch_traces)

    db_path = tmp_path / "fetch.db"
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type) "
            "VALUES ('EGLL', 'London Heathrow', 51.47, -0.45, 'large_airport')"
        )
        db.conn.commit()

    result = CliRunner().invoke(
        cli,
        ["fetch", "--hex", "abcdef", "--start", "2026-05-02", "--end", "2026-05-02", "--db", str(db_path)],
    )
    assert result.exit_code == 0, result.output
    assert "Failed days" not in result.output


def test_fetch_cli_source_all_prints_per_source_stats(tmp_path, monkeypatch):
    """`fetch --source all` fans out to every readsb source in its own thread.
    Each source must get its own stats line in the summary (not just the
    summed total), and fetch_traces must be given a shared Progress so
    concurrent per-source bars don't race for the terminal."""
    from adsbtrack import cli as cli_module

    per_source_returns = {
        "adsbx": {"fetched": 5, "with_data": 4, "skipped": 0, "errors": 1, "failed_days": []},
        "adsbfi": {"fetched": 3, "with_data": 3, "skipped": 0, "errors": 0, "failed_days": []},
        "airplaneslive": {"fetched": 2, "with_data": 2, "skipped": 0, "errors": 0, "failed_days": []},
        "adsblol": {"fetched": 1, "with_data": 1, "skipped": 0, "errors": 0, "failed_days": []},
        "theairtraffic": {"fetched": 0, "with_data": 0, "skipped": 1, "errors": 0, "failed_days": []},
    }
    seen_progress_objects = []

    def fake_fetch_traces(db, config, hex_code, start, end, *, source="adsbx", progress=None):
        seen_progress_objects.append(progress)
        return per_source_returns[source]

    monkeypatch.setattr(cli_module, "fetch_traces", fake_fetch_traces)

    db_path = tmp_path / "fetch.db"
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type) "
            "VALUES ('EGLL', 'London Heathrow', 51.47, -0.45, 'large_airport')"
        )
        db.conn.commit()

    result = CliRunner().invoke(
        cli,
        [
            "fetch",
            "--hex",
            "abcdef",
            "--source",
            "all",
            "--start",
            "2026-05-01",
            "--end",
            "2026-05-01",
            "--db",
            str(db_path),
        ],
    )
    assert result.exit_code == 0, result.output
    for source, stats in per_source_returns.items():
        assert f"{source}: {stats['fetched']} fetched" in result.output, result.output

    # Every call got a progress object, and it's the same shared instance
    # across all sources (one Progress, one task line per source).
    assert len(seen_progress_objects) == len(per_source_returns)
    assert all(p is not None for p in seen_progress_objects)
    assert len({id(p) for p in seen_progress_objects}) == 1


# ---------------------------------------------------------------------------
# fetch resume story: --since-last, implicit resume, runtime default start (U5)
# ---------------------------------------------------------------------------


def test_fetch_cli_resumes_from_last_fetch_by_default(tmp_path, monkeypatch):
    """No --start/--since-last, but fetch_log already has (successful) rows for
    this hex+source: fetch resumes from the day after the last fetched day and
    prints a one-line notice explaining why."""
    from adsbtrack import cli as cli_module

    calls: list[tuple] = []

    def fake_fetch_traces(db, config, hex_code, start, end, *, source="adsbx"):
        calls.append((start, end))
        return {"fetched": 0, "with_data": 0, "skipped": 0, "errors": 0}

    monkeypatch.setattr(cli_module, "fetch_traces", fake_fetch_traces)

    db_path = tmp_path / "fetch.db"
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type) "
            "VALUES ('EGLL', 'London Heathrow', 51.47, -0.45, 'large_airport')"
        )
        for d in ("2026-05-01", "2026-05-02", "2026-05-03"):
            db.insert_fetch_log("aa11bb", d, 200, source="adsbx")
        db.conn.commit()

    result = CliRunner().invoke(
        cli,
        ["fetch", "--hex", "aa11bb", "--end", "2026-05-10", "--db", str(db_path)],
    )
    assert result.exit_code == 0, result.output
    assert calls == [(date(2026, 5, 4), date(2026, 5, 10))]
    assert "Resuming from 2026-05-04 (last fetched day; pass --start to override)" in result.output


def test_fetch_cli_since_last_flag_resumes_explicitly(tmp_path, monkeypatch):
    """--since-last computes MAX(date) + 1 day from fetch_log for this hex+source,
    over the success-filtered dates (retry-exhausted days don't count)."""
    from adsbtrack import cli as cli_module

    calls: list[tuple] = []

    def fake_fetch_traces(db, config, hex_code, start, end, *, source="adsbx"):
        calls.append((start, end))
        return {"fetched": 0, "with_data": 0, "skipped": 0, "errors": 0}

    monkeypatch.setattr(cli_module, "fetch_traces", fake_fetch_traces)

    db_path = tmp_path / "fetch.db"
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type) "
            "VALUES ('EGLL', 'London Heathrow', 51.47, -0.45, 'large_airport')"
        )
        db.insert_fetch_log("bb22cc", "2026-06-01", 200, source="adsbx")
        # Retry-exhausted day: must not count toward MAX(date).
        db.insert_fetch_log("bb22cc", "2026-06-05", 403, source="adsbx")
        db.conn.commit()

    result = CliRunner().invoke(
        cli,
        ["fetch", "--hex", "bb22cc", "--since-last", "--end", "2026-06-10", "--db", str(db_path)],
    )
    assert result.exit_code == 0, result.output
    assert calls == [(date(2026, 6, 2), date(2026, 6, 10))]


def test_fetch_cli_since_last_errors_without_history(tmp_path):
    """--since-last with no prior (success) fetch_log rows for this hex+source
    exits nonzero with a clear message instead of silently picking a default."""
    db_path = tmp_path / "fetch.db"
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type) "
            "VALUES ('EGLL', 'London Heathrow', 51.47, -0.45, 'large_airport')"
        )
        db.conn.commit()

    result = CliRunner().invoke(cli, ["fetch", "--hex", "cc33dd", "--since-last", "--db", str(db_path)])
    assert result.exit_code != 0
    assert "no prior fetches found" in result.output.lower()


def test_fetch_cli_source_all_resumes_each_source_from_its_own_history(tmp_path, monkeypatch):
    """`fetch --source all` (no --start/--since-last) resumes each source
    from the day after ITS OWN last-fetched day, not a single date reduced
    across every source -- a source that's behind no longer drags every
    other source's window back with it. A source with only retry-exhausted
    (403) fetch_log rows counts as having no history, and a source with
    zero fetch_log rows resumes from the earliest peer's start (so it can
    catch up) instead of erroring the whole command."""
    from adsbtrack import cli as cli_module

    calls: list[tuple[str, date, date]] = []

    def fake_fetch_traces(db, config, hex_code, start, end, *, source="adsbx", progress=None):
        calls.append((source, start, end))
        return {"fetched": 0, "with_data": 0, "skipped": 0, "errors": 0, "failed_days": []}

    monkeypatch.setattr(cli_module, "fetch_traces", fake_fetch_traces)

    db_path = tmp_path / "fetch.db"
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type) "
            "VALUES ('EGLL', 'London Heathrow', 51.47, -0.45, 'large_airport')"
        )
        # adsbx is furthest along.
        for d in ("2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04", "2026-05-05"):
            db.insert_fetch_log("dd44ee", d, 200, source="adsbx")
        # adsbfi is furthest behind.
        db.insert_fetch_log("dd44ee", "2026-05-01", 200, source="adsbfi")
        # airplaneslive has only retry-exhausted rows, which don't count as
        # history at all (same success-filtering as the single-source path).
        db.insert_fetch_log("dd44ee", "2026-05-01", 403, source="airplaneslive")
        # adsblol / theairtraffic: no fetch_log rows at all.
        db.conn.commit()

    result = CliRunner().invoke(
        cli,
        ["fetch", "--hex", "dd44ee", "--source", "all", "--end", "2026-05-10", "--db", str(db_path)],
    )
    assert result.exit_code == 0, result.output
    assert len(calls) == 5
    starts_by_source = {c[0]: c[1] for c in calls}
    # adsbx resumes from its own last day + 1, not adsbfi's earlier date.
    assert starts_by_source["adsbx"] == date(2026, 5, 6)
    assert starts_by_source["adsbfi"] == date(2026, 5, 2)
    # Sources with no success-filtered history (including the 403-only
    # airplaneslive) catch up from the earliest peer's start.
    assert starts_by_source["airplaneslive"] == date(2026, 5, 2)
    assert starts_by_source["adsblol"] == date(2026, 5, 2)
    assert starts_by_source["theairtraffic"] == date(2026, 5, 2)
    assert {c[2] for c in calls} == {date(2026, 5, 10)}
    # Per-source starts differ, so the banner prints one line per source
    # instead of a single uniform "Resuming from ..." message.
    assert "adsbx: from 2026-05-06" in result.output
    assert "adsbfi: from 2026-05-02" in result.output


def _capture_fetch_calls(monkeypatch):
    """Shared fetch_traces stub for the per-source resume tests below.
    Records (source, start, end) for every call instead of the summary
    stats -- these tests care about which window each source was given."""
    calls: list[tuple[str, date, date]] = []

    def fake_fetch_traces(db, config, hex_code, start, end, source="adsbx", progress=None):
        calls.append((source, start, end))
        return {"fetched": 0, "with_data": 0, "skipped": 0, "errors": 0, "failed_days": []}

    monkeypatch.setattr("adsbtrack.cli.fetch_traces", fake_fetch_traces)
    return calls


def test_source_all_resumes_each_source_from_its_own_history(tmp_path, monkeypatch):
    """Two sources with different histories resume from their own last
    fetched day, not a single date reduced across every source (#19)."""
    calls = _capture_fetch_calls(monkeypatch)

    today = date.today()
    adsbx_last = today - timedelta(days=10)
    airplaneslive_last = today - timedelta(days=40)

    db_path = tmp_path / "fetch.db"
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type) "
            "VALUES ('EGLL', 'London Heathrow', 51.47, -0.45, 'large_airport')"
        )
        db.insert_fetch_log("aa11aa", adsbx_last.isoformat(), 200, source="adsbx")
        db.insert_fetch_log("aa11aa", airplaneslive_last.isoformat(), 200, source="airplaneslive")
        db.conn.commit()

    result = CliRunner().invoke(
        cli, ["fetch", "--hex", "aa11aa", "--source", "all", "--end", today.isoformat(), "--db", str(db_path)]
    )
    assert result.exit_code == 0, result.output
    starts_by_source = {c[0]: c[1] for c in calls}
    assert starts_by_source["adsbx"] == adsbx_last + timedelta(days=1)
    assert starts_by_source["airplaneslive"] == airplaneslive_last + timedelta(days=1)
    # Not both resuming from airplaneslive's older date.
    assert starts_by_source["adsbx"] != starts_by_source["airplaneslive"]


def test_source_all_clamps_dead_source_to_lookback(tmp_path, monkeypatch):
    """A source with success-filtered history older than
    Config.resume_max_lookback_days has its resume start clamped to that
    lookback floor instead of dragging its whole outage into every run.
    The output warns which source was clamped and how to override it."""
    from adsbtrack.config import Config

    calls = _capture_fetch_calls(monkeypatch)

    today = date.today()
    lookback = Config().resume_max_lookback_days
    adsblol_last = today - timedelta(days=300)
    adsbx_last = today - timedelta(days=5)

    db_path = tmp_path / "fetch.db"
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type) "
            "VALUES ('EGLL', 'London Heathrow', 51.47, -0.45, 'large_airport')"
        )
        db.insert_fetch_log("bb22bb", adsblol_last.isoformat(), 200, source="adsblol")
        db.insert_fetch_log("bb22bb", adsbx_last.isoformat(), 200, source="adsbx")
        db.conn.commit()

    result = CliRunner().invoke(
        cli, ["fetch", "--hex", "bb22bb", "--source", "all", "--end", today.isoformat(), "--db", str(db_path)]
    )
    assert result.exit_code == 0, result.output
    starts_by_source = {c[0]: c[1] for c in calls}

    clamp_floor = today - timedelta(days=lookback)
    assert starts_by_source["adsblol"] == clamp_floor
    # adsbx is well within the lookback window and resumes normally.
    assert starts_by_source["adsbx"] == adsbx_last + timedelta(days=1)
    # A source with no history at all falls back to adsblol's (pre-clamp)
    # date, which is also older than the lookback, so it's clamped too.
    assert starts_by_source["adsbfi"] == clamp_floor

    assert "adsblol" in result.output
    assert "pass --start to backfill" in result.output


def test_single_source_resume_never_clamped(tmp_path, monkeypatch):
    """A single named source (not 'all') is explicit user intent, so its
    resume start is never clamped by resume_max_lookback_days -- even
    across a gap of hundreds of days."""
    calls = _capture_fetch_calls(monkeypatch)

    today = date.today()
    adsblol_last = today - timedelta(days=300)

    db_path = tmp_path / "fetch.db"
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type) "
            "VALUES ('EGLL', 'London Heathrow', 51.47, -0.45, 'large_airport')"
        )
        db.insert_fetch_log("cc33cc", adsblol_last.isoformat(), 200, source="adsblol")
        db.conn.commit()

    result = CliRunner().invoke(
        cli,
        ["fetch", "--hex", "cc33cc", "--source", "adsblol", "--end", today.isoformat(), "--db", str(db_path)],
    )
    assert result.exit_code == 0, result.output
    assert calls == [("adsblol", adsblol_last + timedelta(days=1), today)]
    assert "clamp" not in result.output.lower()


def test_explicit_start_overrides_per_source_resume(tmp_path, monkeypatch):
    """`fetch --source all --start <date>` uses that date for every source
    verbatim, ignoring per-source history and the lookback clamp entirely --
    an explicit --start is user intent."""
    calls = _capture_fetch_calls(monkeypatch)

    today = date.today()
    # Seed history that would otherwise drive a clamp, to prove --start wins.
    old_last = today - timedelta(days=300)

    db_path = tmp_path / "fetch.db"
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type) "
            "VALUES ('EGLL', 'London Heathrow', 51.47, -0.45, 'large_airport')"
        )
        db.insert_fetch_log("dd44dd", old_last.isoformat(), 200, source="adsblol")
        db.conn.commit()

    explicit_start = today - timedelta(days=200)
    result = CliRunner().invoke(
        cli,
        [
            "fetch",
            "--hex",
            "dd44dd",
            "--source",
            "all",
            "--start",
            explicit_start.isoformat(),
            "--end",
            today.isoformat(),
            "--db",
            str(db_path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert len(calls) == 5
    assert {c[1] for c in calls} == {explicit_start}
    assert "clamp" not in result.output.lower()


def test_source_with_no_history_uses_earliest_peer_start(tmp_path, monkeypatch):
    """A readsb source with no fetch history under --source all starts from
    the same date as the earliest peer's resume start, so it can catch up
    instead of defaulting to the Jan-1-last-year fallback."""
    calls = _capture_fetch_calls(monkeypatch)

    today = date.today()
    adsbx_last = today - timedelta(days=10)

    db_path = tmp_path / "fetch.db"
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type) "
            "VALUES ('EGLL', 'London Heathrow', 51.47, -0.45, 'large_airport')"
        )
        db.insert_fetch_log("ee55ee", adsbx_last.isoformat(), 200, source="adsbx")
        db.conn.commit()

    result = CliRunner().invoke(
        cli, ["fetch", "--hex", "ee55ee", "--source", "all", "--end", today.isoformat(), "--db", str(db_path)]
    )
    assert result.exit_code == 0, result.output
    starts_by_source = {c[0]: c[1] for c in calls}
    expected = adsbx_last + timedelta(days=1)
    assert starts_by_source["adsbx"] == expected
    assert starts_by_source["theairtraffic"] == expected


def test_fetch_cli_source_all_since_last_errors_without_any_history(tmp_path):
    """--source all --since-last with no fetch_log rows for ANY readsb source
    errors instead of silently picking a default start, and names the
    per-source situation rather than the literal source 'all'."""
    db_path = tmp_path / "fetch.db"
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type) "
            "VALUES ('EGLL', 'London Heathrow', 51.47, -0.45, 'large_airport')"
        )
        db.conn.commit()

    result = CliRunner().invoke(
        cli, ["fetch", "--hex", "ee55ff", "--source", "all", "--since-last", "--db", str(db_path)]
    )
    assert result.exit_code != 0
    assert "no prior fetches" in result.output.lower()
    assert "adsbx" in result.output  # names the per-source situation, not just "all"


def test_fetch_cli_source_all_single_source_history_unaffected(tmp_path, monkeypatch):
    """Regression: plain --source (not 'all') resume behavior is untouched by
    the multi-source resume logic -- it still resumes off that one source's
    own fetch_log history."""
    from adsbtrack import cli as cli_module

    calls: list[tuple] = []

    def fake_fetch_traces(db, config, hex_code, start, end, *, source="adsbx"):
        calls.append((start, end))
        return {"fetched": 0, "with_data": 0, "skipped": 0, "errors": 0}

    monkeypatch.setattr(cli_module, "fetch_traces", fake_fetch_traces)

    db_path = tmp_path / "fetch.db"
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type) "
            "VALUES ('EGLL', 'London Heathrow', 51.47, -0.45, 'large_airport')"
        )
        db.insert_fetch_log("ff66aa", "2026-05-01", 200, source="adsbx")
        db.conn.commit()

    result = CliRunner().invoke(
        cli, ["fetch", "--hex", "ff66aa", "--source", "adsbx", "--end", "2026-05-10", "--db", str(db_path)]
    )
    assert result.exit_code == 0, result.output
    assert calls == [(date(2026, 5, 2), date(2026, 5, 10))]
    assert "Resuming from 2026-05-02 (last fetched day; pass --start to override)" in result.output


# ---------------------------------------------------------------------------
# source health skip + retention annotations (#20)
# ---------------------------------------------------------------------------


def _seed_sick_source(db, icao: str, source: str, num_days: int = 20, status: int = 502, start_month_day=1) -> None:
    """Seed `num_days` consecutive retryable-failure fetch_log rows for one
    source, starting at 2026-01-<start_month_day>."""
    for i in range(num_days):
        db.insert_fetch_log(icao, f"2026-01-{start_month_day + i:02d}", status, source=source)


def test_source_all_skips_unhealthy_source(tmp_path, monkeypatch):
    """`fetch --source all` skips a readsb source whose last
    source_health_skip_threshold (20) day-requests were all retryable
    failures (403/429/5xx), instead of burning a full backoff ladder on a
    source known to be sick. The other (healthy) readsb sources still run."""
    calls = _capture_fetch_calls(monkeypatch)

    db_path = tmp_path / "fetch.db"
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type) "
            "VALUES ('EGLL', 'London Heathrow', 51.47, -0.45, 'large_airport')"
        )
        _seed_sick_source(db, "ff11ff", "adsblol")
        db.insert_fetch_log("ff11ff", "2026-02-01", 200, source="adsbx")
        db.conn.commit()

    result = CliRunner().invoke(
        cli,
        [
            "fetch",
            "--hex",
            "ff11ff",
            "--source",
            "all",
            "--start",
            "2026-02-01",
            "--end",
            "2026-02-10",
            "--db",
            str(db_path),
        ],
    )
    assert result.exit_code == 0, result.output
    sources_called = {c[0] for c in calls}
    assert "adsblol" not in sources_called
    assert sources_called == {"adsbx", "adsbfi", "airplaneslive", "theairtraffic"}
    assert "Skipping adsblol" in result.output
    assert "--include-unhealthy" in result.output


def test_include_unhealthy_forces_sick_source(tmp_path, monkeypatch):
    """--include-unhealthy overrides the health skip, forcing the sick
    source back into the fetch."""
    calls = _capture_fetch_calls(monkeypatch)

    db_path = tmp_path / "fetch.db"
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type) "
            "VALUES ('EGLL', 'London Heathrow', 51.47, -0.45, 'large_airport')"
        )
        _seed_sick_source(db, "ff12ff", "adsblol")
        db.insert_fetch_log("ff12ff", "2026-02-01", 200, source="adsbx")
        db.conn.commit()

    result = CliRunner().invoke(
        cli,
        [
            "fetch",
            "--hex",
            "ff12ff",
            "--source",
            "all",
            "--include-unhealthy",
            "--start",
            "2026-02-01",
            "--end",
            "2026-02-10",
            "--db",
            str(db_path),
        ],
    )
    assert result.exit_code == 0, result.output
    sources_called = {c[0] for c in calls}
    assert "adsblol" in sources_called
    assert "Skipping adsblol" not in result.output


def test_named_single_source_never_health_skipped(tmp_path, monkeypatch):
    """A named single source (not 'all') is explicit user intent, so it
    always runs regardless of its recent health history."""
    calls = _capture_fetch_calls(monkeypatch)

    db_path = tmp_path / "fetch.db"
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type) "
            "VALUES ('EGLL', 'London Heathrow', 51.47, -0.45, 'large_airport')"
        )
        _seed_sick_source(db, "ff13ff", "adsblol")
        db.conn.commit()

    result = CliRunner().invoke(
        cli,
        [
            "fetch",
            "--hex",
            "ff13ff",
            "--source",
            "adsblol",
            "--start",
            "2026-02-01",
            "--end",
            "2026-02-10",
            "--db",
            str(db_path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert calls and calls[0][0] == "adsblol"
    assert "skipping" not in result.output.lower()


def test_all_sources_unhealthy_falls_back_to_unfiltered(tmp_path, monkeypatch):
    """Health filtering must never brick the fetch entirely: if every readsb
    source looks unhealthy, the filter backs off and fetches all of them
    anyway, with a warning instead of a skip."""
    from adsbtrack.config import SOURCE_URLS

    calls = _capture_fetch_calls(monkeypatch)

    db_path = tmp_path / "fetch.db"
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type) "
            "VALUES ('EGLL', 'London Heathrow', 51.47, -0.45, 'large_airport')"
        )
        for src in SOURCE_URLS:
            _seed_sick_source(db, "aa33aa", src)
        db.conn.commit()

    result = CliRunner().invoke(
        cli,
        [
            "fetch",
            "--hex",
            "aa33aa",
            "--source",
            "all",
            "--start",
            "2026-02-01",
            "--end",
            "2026-02-10",
            "--db",
            str(db_path),
        ],
    )
    assert result.exit_code == 0, result.output
    sources_called = {c[0] for c in calls}
    assert sources_called == set(SOURCE_URLS)
    assert "unhealthy" in result.output.lower()


def test_retention_note_printed_for_old_window(tmp_path, monkeypatch):
    """theairtraffic's observed ~90-day archive retention (source_retention_days)
    gets annotated when a fetch's start predates that window, so a 404 out
    there reads as "probably expired" rather than "aircraft not seen"."""
    calls = _capture_fetch_calls(monkeypatch)

    db_path = tmp_path / "fetch.db"
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type) "
            "VALUES ('EGLL', 'London Heathrow', 51.47, -0.45, 'large_airport')"
        )
        db.conn.commit()

    old_start = date(2026, 1, 1)  # > 90 days before the --end below
    result = CliRunner().invoke(
        cli,
        [
            "fetch",
            "--hex",
            "aa44aa",
            "--source",
            "theairtraffic",
            "--start",
            old_start.isoformat(),
            "--end",
            "2026-08-01",
            "--db",
            str(db_path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert calls == [("theairtraffic", old_start, date(2026, 8, 1))]
    assert "theairtraffic" in result.output
    assert "90" in result.output
    assert "expired" in result.output.lower()


def test_fetch_cli_default_start_is_jan_1_of_last_year(tmp_path, monkeypatch):
    """No prior fetch_log rows and no --start: default start is January 1 of
    the previous calendar year, computed at runtime rather than a frozen date."""
    from adsbtrack import cli as cli_module

    # Use a year where "computed at runtime" and the old frozen "2025-01-01"
    # default would disagree, so this test actually distinguishes the two.
    class FakeDate(date):
        @classmethod
        def today(cls):
            return date(2028, 3, 1)

    monkeypatch.setattr(cli_module, "date", FakeDate)

    calls: list[tuple] = []

    def fake_fetch_traces(db, config, hex_code, start, end, *, source="adsbx"):
        calls.append((start, end))
        return {"fetched": 0, "with_data": 0, "skipped": 0, "errors": 0}

    monkeypatch.setattr(cli_module, "fetch_traces", fake_fetch_traces)

    db_path = tmp_path / "fetch.db"
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type) "
            "VALUES ('EGLL', 'London Heathrow', 51.47, -0.45, 'large_airport')"
        )
        db.conn.commit()

    result = CliRunner().invoke(
        cli,
        ["fetch", "--hex", "dd44ee", "--end", "2028-03-01", "--db", str(db_path)],
    )
    assert result.exit_code == 0, result.output
    assert calls == [(date(2027, 1, 1), date(2028, 3, 1))]


def test_status_cli_honors_adsbtrack_db_envvar(tmp_path, monkeypatch):
    """--db reads $ADSBTRACK_DB when the flag isn't passed, so wrapper scripts
    don't need to repeat --db on every invocation. Runs from an isolated cwd:
    if the envvar isn't wired up, --db falls back to a relative './adsbtrack.db'
    which must not land in (or read from) the real working directory."""
    monkeypatch.chdir(tmp_path)
    db_path = tmp_path / "envvar.db"
    with Database(db_path) as db:
        db.insert_trace_day(
            "ee55ff",
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

    result = CliRunner().invoke(cli, ["status", "--hex", "ee55ff"], env={"ADSBTRACK_DB": str(db_path)})
    assert result.exit_code == 0, result.output
    assert "Status for ee55ff" in result.output
    # Only present if the *seeded* db was actually read, not a fresh fallback one.
    assert "N512WB" in result.output


# ---------------------------------------------------------------------------
# _validate_hex: --hex format validation on every command (A9)
# ---------------------------------------------------------------------------


def test_fetch_cli_rejects_invalid_hex_format(tmp_path):
    """--hex must be 6 hex digits; an invalid value fails fast with a message
    naming the expected format, before any DB/network work happens."""
    db_path = tmp_path / "adsbtrack.db"
    result = CliRunner().invoke(cli, ["fetch", "--hex", "zzz999", "--db", str(db_path)])
    assert result.exit_code != 0
    assert "zzz999" in result.output
    assert "hex" in result.output.lower()


def test_trips_cli_rejects_invalid_hex_format(tmp_path):
    """Same validation applies to every other --hex option, not just fetch."""
    db_path = tmp_path / "adsbtrack.db"
    result = CliRunner().invoke(cli, ["trips", "--hex", "abc12", "--db", str(db_path)])
    assert result.exit_code != 0
    assert "abc12" in result.output
    assert "hex" in result.output.lower()


# ---------------------------------------------------------------------------
# --tail on every command that previously required --hex (U1)
# ---------------------------------------------------------------------------


def test_trips_cli_tail_resolves_nnumber(tmp_path):
    """`trips --tail N512WB` resolves through _resolve_hex_db exactly like fetch."""
    db_path = tmp_path / "adsbtrack.db"
    with Database(db_path) as db:
        db.insert_flight(
            Flight(
                icao="a66ad3",
                takeoff_time=datetime(2022, 6, 16, 12, 43, 27, tzinfo=UTC),
                takeoff_lat=35.035,
                takeoff_lon=-117.932,
                takeoff_date="2022-06-16",
                origin_icao="K9L2",
                origin_name="Edwards Aux",
            )
        )

    result = CliRunner().invoke(cli, ["trips", "--tail", "N512WB", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "a66ad3" in result.output
    assert "K9L2" in result.output


def test_status_cli_tail_resolves_nnumber(tmp_path):
    """`status --tail N512WB` resolves through _resolve_hex_db exactly like fetch."""
    db_path = tmp_path / "adsbtrack.db"
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

    result = CliRunner().invoke(cli, ["status", "--tail", "N512WB", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "a66ad3" in result.output


def test_extract_cli_accepts_tail(tmp_path):
    """`extract` gains --tail like fetch and resolves through _resolve_hex_db."""
    db_path = tmp_path / "adsbtrack.db"
    with Database(db_path) as db:
        # Seed one airport so ensure_airports() doesn't try to download.
        db.conn.execute(
            "INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type) "
            "VALUES ('EGLL', 'London Heathrow', 51.47, -0.45, 'large_airport')"
        )
        db.conn.commit()

    result = CliRunner().invoke(cli, ["extract", "--tail", "N512WB", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "Extracted" in result.output


def test_gaps_cli_accepts_tail(tmp_path):
    """`gaps` gains --tail like fetch and resolves through _resolve_hex_db."""
    db_path = tmp_path / "adsbtrack.db"
    with Database(db_path):
        pass  # empty DB, schema only

    result = CliRunner().invoke(cli, ["gaps", "--tail", "N512WB", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "a66ad3" in result.output


def test_events_cli_accepts_tail(tmp_path):
    """`events` gains --tail like fetch and resolves through _resolve_hex_db."""
    db_path = tmp_path / "adsbtrack.db"
    with Database(db_path):
        pass  # empty DB, schema only

    result = CliRunner().invoke(cli, ["events", "--tail", "N512WB", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "a66ad3" in result.output


def test_links_cli_tail_resolves_via_aircraft_registry(tmp_path):
    """`links` switches from _resolve_hex to _resolve_hex_db, so a non-N-number
    registration now resolves via aircraft_registry instead of only algorithmic
    FAA N-numbers."""
    db_path = tmp_path / "adsbtrack.db"
    _seed_flights(db_path)  # icao='ae07b3'
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO aircraft_registry (icao, registration, last_updated) VALUES (?, ?, ?)",
            ("ae07b3", "G-XYZA", "2026-04-10T00:00:00Z"),
        )
        db.conn.commit()

    result = CliRunner().invoke(cli, ["links", "--tail", "G-XYZA", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "ae07b3" in result.output


def test_route_cli_tail_resolves_via_aircraft_registry(tmp_path, monkeypatch):
    """`route` switches from _resolve_hex to _resolve_hex_db likewise."""
    import json

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
        db.conn.execute(
            "INSERT INTO aircraft_registry (icao, registration, last_updated) VALUES (?, ?, ?)",
            ("abc123", "G-XYZA", "2026-04-10T00:00:00Z"),
        )
        db.conn.commit()

    result = CliRunner().invoke(cli, ["route", "--tail", "G-XYZA", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "NDB1 (<1m)" in result.output


# -----------------------------------------------------------------------------
# --json output (U2)
# -----------------------------------------------------------------------------


def test_trips_json_emits_flight_objects(tmp_path):
    db_path = tmp_path / "t.db"
    _seed_flights(db_path)

    result = CliRunner().invoke(cli, ["trips", "--hex", "ae07b3", "--db", str(db_path), "--json"])
    assert result.exit_code == 0, result.output

    payload = json.loads(result.output)
    assert isinstance(payload, list)
    assert len(payload) == 2
    assert payload[0]["icao"] == "ae07b3"
    assert "id" in payload[0]


def test_trips_default_output_unchanged_with_table_title(tmp_path):
    db_path = tmp_path / "t.db"
    _seed_flights(db_path)

    result = CliRunner().invoke(cli, ["trips", "--hex", "ae07b3", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "Flights for ae07b3" in result.output


def test_status_json_emits_summary_object(tmp_path):
    db_path = tmp_path / "t.db"
    _seed_flights(db_path)

    result = CliRunner().invoke(cli, ["status", "--hex", "ae07b3", "--db", str(db_path), "--json"])
    assert result.exit_code == 0, result.output

    payload = json.loads(result.output)
    assert payload["hex"] == "ae07b3"
    assert payload["total_flights"] == 2


def test_status_default_output_unchanged_with_header(tmp_path):
    db_path = tmp_path / "t.db"
    _seed_flights(db_path)

    result = CliRunner().invoke(cli, ["status", "--hex", "ae07b3", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "Status for ae07b3" in result.output


def _seed_gap_trace(db_path):
    """Single-day trace with a deliberate 600-second gap at FL350.

    Mirrors tests/test_gaps.py's populated_db fixture, trimmed to what
    a CLI-level --json smoke test needs.
    """
    base_ts = 1700000000.0
    trace = []
    for i in range(30):
        alt = min(35000, 1000 + i * 1200)
        lat = 29.5 + i * 0.01
        lon = -98.5 + i * 0.02
        trace.append([i * 30.0, lat, lon, alt, 400.0, 0.0, 0, 0, {}, "adsb_icao"])
    gap_end_offset = 30 * 30.0 + 600.0
    for i in range(20):
        lat = 29.80 + i * 0.01
        lon = -97.86 + i * 0.02
        trace.append([gap_end_offset + i * 30.0, lat, lon, 35000, 400.0, 0.0, 0, 0, {}, "adsb_icao"])

    with Database(db_path) as db:
        db.insert_trace_day(
            "abc123",
            "2023-11-14",
            {"timestamp": base_ts, "trace": trace, "r": "N12345", "t": "C172"},
            source="adsbx",
        )
        db.commit()


def test_gaps_json_emits_row_list(tmp_path):
    db_path = tmp_path / "g.db"
    _seed_gap_trace(db_path)

    result = CliRunner().invoke(cli, ["gaps", "--hex", "abc123", "--db", str(db_path), "--json"])
    assert result.exit_code == 0, result.output

    payload = json.loads(result.output)
    assert isinstance(payload, list)
    assert len(payload) == 1
    assert 550 < payload[0]["duration_secs"] < 650
    assert "classification" in payload[0]


def test_gaps_default_output_unchanged_with_table_title(tmp_path):
    db_path = tmp_path / "g.db"
    _seed_gap_trace(db_path)

    result = CliRunner().invoke(cli, ["gaps", "--hex", "abc123", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "ADS-B gaps for abc123" in result.output


def _seed_event_flight(db_path):
    """One flight with an emergency squawk so collect_events yields exactly one event."""
    with Database(db_path) as db:
        db.insert_flight(
            Flight(
                icao="aaa001",
                takeoff_time=datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
                takeoff_lat=40.0,
                takeoff_lon=-74.0,
                takeoff_date="2024-06-15",
                landing_time=datetime(2024, 6, 15, 13, 30, 0, tzinfo=UTC),
                landing_type="confirmed",
                callsign="UAL100",
                emergency_squawk="7700",
                destination_icao="KBOS",
            )
        )
        db.commit()


def test_events_json_emits_event_list(tmp_path):
    db_path = tmp_path / "e.db"
    _seed_event_flight(db_path)

    result = CliRunner().invoke(cli, ["events", "--hex", "aaa001", "--db", str(db_path), "--json"])
    assert result.exit_code == 0, result.output

    payload = json.loads(result.output)
    assert isinstance(payload, list)
    assert len(payload) == 1
    assert payload[0]["event_type"] == "emergency_squawk"
    assert payload[0]["severity"] == "emergency"


def test_events_default_output_unchanged_with_table_title(tmp_path):
    db_path = tmp_path / "e.db"
    _seed_event_flight(db_path)

    result = CliRunner().invoke(cli, ["events", "--hex", "aaa001", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "Events for aaa001" in result.output


# ---------------------------------------------------------------------------
# fetch -> extract glue (incremental extraction)
# ---------------------------------------------------------------------------


def _seed_airport(db_path: Path) -> None:
    """One airport so ensure_airports() does not try to download the CSV."""
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT INTO airports (ident, name, latitude_deg, longitude_deg, type) "
            "VALUES ('EGLL', 'London Heathrow', 51.47, -0.45, 'large_airport')"
        )
        db.commit()


def _fake_extract(calls: list):
    """Stand-in for parser.extract_flights that records how it was called."""

    def fake_extract_flights(db, config, hex_code, reprocess=False, since_date=None):
        calls.append({"hex": hex_code, "reprocess": reprocess, "since_date": since_date})
        return 0

    return fake_extract_flights


def test_fetch_extracts_incrementally_from_the_earliest_new_day(tmp_path, monkeypatch):
    """A fetch that lands one new trace day re-extracts from that day, not
    from the beginning of the aircraft's history."""
    from adsbtrack import cli as cli_module

    def fake_fetch_traces(db, config, hex_code, start, end, *, source="adsbx"):
        db.insert_trace_day(
            hex_code,
            "2026-05-03",
            {"r": "N1", "t": "C172", "timestamp": 1777766400.0, "trace": []},
            source=source,
        )
        db.commit()
        return {"fetched": 1, "with_data": 1, "skipped": 0, "errors": 0, "failed_days": []}

    calls: list = []
    monkeypatch.setattr(cli_module, "fetch_traces", fake_fetch_traces)
    monkeypatch.setattr(cli_module, "extract_flights", _fake_extract(calls))

    db_path = tmp_path / "fetch.db"
    _seed_airport(db_path)

    result = CliRunner().invoke(
        cli,
        ["fetch", "--hex", "abcdef", "--start", "2026-05-02", "--end", "2026-05-03", "--db", str(db_path)],
    )
    assert result.exit_code == 0, result.output
    assert calls == [{"hex": "abcdef", "reprocess": False, "since_date": date(2026, 5, 3)}]


def test_fetch_skips_extract_when_no_trace_day_landed(tmp_path, monkeypatch):
    """Nothing new on disk means nothing to re-extract."""
    from adsbtrack import cli as cli_module

    def fake_fetch_traces(db, config, hex_code, start, end, *, source="adsbx"):
        return {"fetched": 1, "with_data": 0, "skipped": 0, "errors": 0, "failed_days": []}

    calls: list = []
    monkeypatch.setattr(cli_module, "fetch_traces", fake_fetch_traces)
    monkeypatch.setattr(cli_module, "extract_flights", _fake_extract(calls))

    db_path = tmp_path / "fetch.db"
    _seed_airport(db_path)

    result = CliRunner().invoke(
        cli,
        ["fetch", "--hex", "abcdef", "--start", "2026-05-02", "--end", "2026-05-03", "--db", str(db_path)],
    )
    assert result.exit_code == 0, result.output
    assert calls == []
    assert "No new trace days" in result.output


def test_extract_since_passes_the_date_to_the_parser(tmp_path, monkeypatch):
    from adsbtrack import cli as cli_module

    calls: list = []
    monkeypatch.setattr(cli_module, "extract_flights", _fake_extract(calls))

    db_path = tmp_path / "extract.db"
    _seed_airport(db_path)

    result = CliRunner().invoke(
        cli,
        ["extract", "--hex", "abcdef", "--since", "2026-05-03", "--db", str(db_path)],
    )
    assert result.exit_code == 0, result.output
    assert calls == [{"hex": "abcdef", "reprocess": False, "since_date": date(2026, 5, 3)}]


def test_extract_rejects_since_with_reprocess(tmp_path):
    db_path = tmp_path / "extract.db"
    _seed_airport(db_path)

    result = CliRunner().invoke(
        cli,
        ["extract", "--hex", "abcdef", "--since", "2026-05-03", "--reprocess", "--db", str(db_path)],
    )
    assert result.exit_code != 0
    assert "--reprocess" in result.output and "--since" in result.output


# ---------------------------------------------------------------------------
# inspect: day-level trace forensics (#23)
# ---------------------------------------------------------------------------


def _seed_inspect_trace(db_path: Path) -> None:
    """One trace_days row with a v2 detail dict carrying a callsign/squawk,
    so the fragment table, integrity counts, and timelines all have
    something to show."""
    with Database(db_path) as db:
        db.insert_trace_day(
            "aaaaaa",
            "2024-01-01",
            {
                "r": "N512WB",
                "t": "C172",
                "desc": "Cessna 172",
                "ownOp": "unknown",
                "year": "1966",
                "timestamp": datetime(2024, 1, 1, tzinfo=UTC).timestamp(),
                "trace": [
                    [
                        0,
                        51.47,
                        -0.45,
                        1000,
                        100,
                        None,
                        None,
                        None,
                        {"version": 2, "sil": 0, "nic": 0, "flight": "TEST1", "squawk": "1200"},
                    ],
                    [
                        60,
                        51.48,
                        -0.44,
                        1500,
                        120,
                        None,
                        None,
                        None,
                        {"version": 2, "sil": 0, "nic": 0, "flight": "TEST1", "squawk": "7700"},
                    ],
                ],
            },
        )


def test_inspect_cli_renders_and_json_round_trips(tmp_path):
    db_path = tmp_path / "inspect.db"
    _seed_inspect_trace(db_path)

    json_result = CliRunner().invoke(
        cli, ["inspect", "--hex", "aaaaaa", "--date", "2024-01-01", "--json", "--db", str(db_path)]
    )
    assert json_result.exit_code == 0, json_result.output
    payload = json.loads(json_result.output)
    assert payload["hex"] == "aaaaaa"
    assert payload["date"] == "2024-01-01"
    assert "adsbx" in payload["sources"]
    assert len(payload["sources"]["adsbx"]) >= 1
    assert payload["squawk_timeline"][0][1] == "1200"
    assert payload["closest_approach"] is None

    table_result = CliRunner().invoke(cli, ["inspect", "--hex", "aaaaaa", "--date", "2024-01-01", "--db", str(db_path)])
    assert table_result.exit_code == 0, table_result.output
    assert "FRAG" in table_result.output


def test_inspect_cli_no_trace_data_exits_zero_with_message(tmp_path):
    db_path = tmp_path / "inspect.db"
    with Database(db_path):
        pass  # empty DB, schema only

    result = CliRunner().invoke(cli, ["inspect", "--hex", "aaaaaa", "--date", "2024-01-01", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "no trace data" in result.output.lower()

    json_result = CliRunner().invoke(
        cli, ["inspect", "--hex", "aaaaaa", "--date", "2024-01-01", "--json", "--db", str(db_path)]
    )
    assert json_result.exit_code == 0, json_result.output
    assert json.loads(json_result.output)["sources"] == {}


def test_inspect_cli_unknown_airport_errors(tmp_path):
    db_path = tmp_path / "inspect.db"
    _seed_inspect_trace(db_path)
    # Seed EGLL so ensure_airports() sees a non-empty table and doesn't try to
    # download the OurAirports CSV; query a distinct ident ("ZZZZ") so the
    # unknown-ident path is still exercised.
    _seed_airport(db_path)

    result = CliRunner().invoke(
        cli, ["inspect", "--hex", "aaaaaa", "--date", "2024-01-01", "--airport", "ZZZZ", "--db", str(db_path)]
    )
    assert result.exit_code != 0
    assert "zzzz" in result.output.lower()


def test_inspect_cli_closest_approach_with_airport(tmp_path):
    db_path = tmp_path / "inspect.db"
    _seed_inspect_trace(db_path)
    _seed_airport(db_path)

    result = CliRunner().invoke(
        cli, ["inspect", "--hex", "aaaaaa", "--date", "2024-01-01", "--airport", "EGLL", "--db", str(db_path)]
    )
    assert result.exit_code == 0, result.output
    assert "EGLL" in result.output
