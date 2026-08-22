"""Tests for adsbtrack.export -- per-tail deliverable bundle (issue #25).

Fixture DB carries two aircraft so every scoping assertion can prove the
other aircraft's rows never leak into the bundle, a multi-source day plus a
legacy raw-JSON-TEXT trace row so both stored trace forms go through the
shared decode path, and registry/crossref/FAA rows for the identity stub.
"""

from __future__ import annotations

import csv
import json
import sqlite3
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import pytest
from click.testing import CliRunner

from adsbtrack.cli import cli
from adsbtrack.db import Database
from adsbtrack.export import (
    TRACE_CSV_HEADER,
    export_bundle,
    parse_window,
    parse_windows,
)
from adsbtrack.models import Flight

TARGET = "a54c0c"
OTHER = "bbb222"

DAY1 = "2026-04-20"
DAY2 = "2026-04-21"
DAY3 = "2026-04-22"  # stored as legacy raw JSON TEXT


def _day_ts(day: str) -> float:
    return datetime.fromisoformat(day + "T00:00:00+00:00").timestamp()


def _point(offset: float, lat: float, lon: float, alt, gs=None, detail=None) -> list:
    """Standard readsb point layout (see tests/test_forensics.py)."""
    return [offset, lat, lon, alt, gs, None, None, None, detail or {}]


def _trace_data(day: str, points: list) -> dict:
    return {"timestamp": _day_ts(day), "trace": points, "r": "N9527C", "t": "GLF4"}


DAY2_ADSBX_POINTS = [
    _point(10 * 3600, 40.0, -74.0, "ground", gs=5, detail={"flight": "TST1", "squawk": "1200"}),
    _point(10 * 3600 + 60, 40.01, -74.01, 2000, gs=150),
    _point(11 * 3600, 40.5, -74.5, 30000, gs=420, detail={"squawk": "7700"}),
]
DAY2_ADSBFI_POINTS = [
    _point(10 * 3600 + 30, 40.005, -74.005, 1000, gs=120, detail={"flight": "TST1"}),
]
DAY3_POINTS = [
    _point(9 * 3600, 41.0, -75.0, 15000, gs=300),
    _point(9 * 3600 + 120, 41.1, -75.1, 16000, gs=310),
]


@pytest.fixture
def export_db(tmp_path) -> Path:
    db_path = tmp_path / "adsbtrack.db"
    now_iso = datetime.now(UTC).isoformat()
    with Database(db_path) as db:
        # Trace days: DAY1 single-source, DAY2 two sources, DAY3 legacy TEXT.
        db.insert_trace_day(TARGET, DAY1, _trace_data(DAY1, [_point(3600, 39.0, -73.0, 5000, gs=200)]))
        db.insert_trace_day(TARGET, DAY2, _trace_data(DAY2, DAY2_ADSBX_POINTS), source="adsbx")
        db.insert_trace_day(TARGET, DAY2, _trace_data(DAY2, DAY2_ADSBFI_POINTS), source="adsbfi")
        db.conn.execute(
            """INSERT INTO trace_days (icao, date, source, timestamp, trace_json, point_count, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (TARGET, DAY3, "adsbx", _day_ts(DAY3), json.dumps(DAY3_POINTS), len(DAY3_POINTS), now_iso),
        )
        # Other aircraft's day on the same date must never leak.
        db.insert_trace_day(OTHER, DAY2, {"timestamp": _day_ts(DAY2), "trace": [_point(3600, 50.0, 8.0, 3000)]})

        # Flights: one before the windows, one inside DAY2, one spanning the
        # DAY2/DAY3 boundary (takeoff DAY2 late, landing DAY3), one for OTHER.
        db.insert_flight(
            Flight(
                icao=TARGET,
                takeoff_time=datetime(2026, 3, 1, 12, 0, tzinfo=UTC),
                takeoff_lat=39.0,
                takeoff_lon=-73.0,
                takeoff_date="2026-03-01",
                landing_time=datetime(2026, 3, 1, 13, 0, tzinfo=UTC),
                callsign="EARLY",
            )
        )
        db.insert_flight(
            Flight(
                icao=TARGET,
                takeoff_time=datetime(2026, 4, 21, 10, 0, tzinfo=UTC),
                takeoff_lat=40.0,
                takeoff_lon=-74.0,
                takeoff_date=DAY2,
                landing_time=datetime(2026, 4, 21, 11, 30, tzinfo=UTC),
                callsign="INSIDE",
            )
        )
        db.insert_flight(
            Flight(
                icao=TARGET,
                takeoff_time=datetime(2026, 4, 21, 23, 0, tzinfo=UTC),
                takeoff_lat=40.5,
                takeoff_lon=-74.5,
                takeoff_date=DAY2,
                landing_time=datetime(2026, 4, 22, 1, 0, tzinfo=UTC),
                callsign="SPAN",
            )
        )
        db.insert_flight(
            Flight(
                icao=OTHER,
                takeoff_time=datetime(2026, 4, 21, 10, 0, tzinfo=UTC),
                takeoff_lat=50.0,
                takeoff_lon=8.0,
                takeoff_date=DAY2,
                callsign="LEAK",
            )
        )

        for day in (DAY1, DAY2, DAY3):
            db.insert_fetch_log(TARGET, day, 200)
        db.insert_fetch_log(OTHER, DAY2, 200)

        # Identity rows for README/analysis.
        db.conn.execute(
            "INSERT INTO aircraft_registry (icao, registration, type_code, description, owner_operator) "
            "VALUES (?, ?, ?, ?, ?)",
            (TARGET, "N9527C", "GLF4", "GULFSTREAM IV", "EXAMPLE HOLDINGS LLC"),
        )
        db.upsert_hex_crossref(
            {"icao": TARGET, "registration": "N9527C", "type_code": "GLF4", "operator": "EXAMPLE", "source": "faa"}
        )
        db.conn.execute(
            "INSERT INTO faa_registry (mode_s_code_hex, n_number, serial_number, mfr_mdl_code, year_mfr, name, "
            "street, city, state, zip_code, cert_issue_date, expiration_date) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                TARGET,
                "9527C",
                "1234",
                "GULF0001",
                "1999",
                "EXAMPLE HOLDINGS LLC",
                "1 MAIN ST",
                "DOVER",
                "DE",
                "19901",
                "2020-01-01",
                "2027-01-31",
            ),
        )
        db.conn.execute(
            "INSERT INTO faa_aircraft_ref (code, mfr, model) VALUES (?, ?, ?)",
            ("GULF0001", "GULFSTREAM", "G-IV"),
        )
        db.commit()
    return db_path


# ---------------------------------------------------------------------------
# Window parsing
# ---------------------------------------------------------------------------


def test_parse_window_dates_covers_whole_days():
    window = parse_window("2026-04-20:2026-04-21")
    assert window.label == "2026-04-20_2026-04-21"
    assert window.start == datetime(2026, 4, 20, 0, 0, tzinfo=UTC)
    assert window.end == datetime(2026, 4, 21, 23, 59, 59, 999999, tzinfo=UTC)


def test_parse_window_datetimes_with_colons():
    window = parse_window("2026-04-21T10:00:2026-04-21T11:30:00")
    assert window.start == datetime(2026, 4, 21, 10, 0, tzinfo=UTC)
    assert window.end == datetime(2026, 4, 21, 11, 30, tzinfo=UTC)
    assert window.label == "2026-04-21T100000_2026-04-21T113000"


def test_parse_window_mixed_date_and_datetime():
    window = parse_window("2026-04-21:2026-04-22T06:00")
    assert window.start == datetime(2026, 4, 21, 0, 0, tzinfo=UTC)
    assert window.end == datetime(2026, 4, 22, 6, 0, tzinfo=UTC)


def test_parse_window_single_day():
    window = parse_window("2026-04-21:2026-04-21")
    assert window.start.date().isoformat() == DAY2
    assert window.end.date().isoformat() == DAY2


@pytest.mark.parametrize(
    "spec",
    ["2026-04-21", "2026-04-21:", ":2026-04-21", "20260421:20260422", "2026-04-22:2026-04-21", "yesterday:today"],
)
def test_parse_window_rejects_malformed_and_inverted(spec):
    with pytest.raises(ValueError):
        parse_window(spec)


def test_parse_windows_rejects_duplicates():
    with pytest.raises(ValueError, match="duplicates"):
        parse_windows(["2026-04-21:2026-04-21", "2026-04-21:2026-04-21"])


# ---------------------------------------------------------------------------
# Bundle assembly
# ---------------------------------------------------------------------------


def test_bundle_file_set_without_windows(export_db, tmp_path):
    out = tmp_path / "bundle"
    with Database(export_db) as db:
        result = export_bundle(db, TARGET, out)
    assert {p.name for p in result.files} == {f"{TARGET}.sqlite", "flights.csv", "README.md"}
    assert result.flight_count == 3
    assert result.trace_day_count == 4
    assert result.fetch_log_count == 3


def test_bundle_file_set_with_windows_matches_reference_shape(export_db, tmp_path):
    """Two windows + analysis = the reference 8-file deliverable shape."""
    out = tmp_path / "bundle"
    windows = parse_windows([f"{DAY2}:{DAY2}", f"{DAY3}:{DAY3}"])
    with Database(export_db) as db:
        result = export_bundle(db, TARGET, out, windows, include_analysis=True)
    assert len(result.files) == 8
    assert {p.name for p in result.files} == {
        f"{TARGET}.sqlite",
        "flights.csv",
        f"flights_{DAY2}_{DAY2}.csv",
        f"trace_{DAY2}_{DAY2}.csv",
        f"flights_{DAY3}_{DAY3}.csv",
        f"trace_{DAY3}_{DAY3}.csv",
        "analysis.md",
        "README.md",
    }
    for path in result.files:
        assert path.exists()


def test_bundle_missing_hex_raises(export_db, tmp_path):
    with Database(export_db) as db, pytest.raises(ValueError, match="nothing to export"):
        export_bundle(db, "ffffff", tmp_path / "bundle")


# ---------------------------------------------------------------------------
# SQLite extract
# ---------------------------------------------------------------------------


def test_sqlite_extract_scoped_and_traces_decoded(export_db, tmp_path):
    out = tmp_path / "bundle"
    with Database(export_db) as db:
        export_bundle(db, TARGET, out)

    extract = sqlite3.connect(out / f"{TARGET}.sqlite")
    extract.row_factory = sqlite3.Row
    tables = {r["name"] for r in extract.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert tables == {"flights", "trace_days", "fetch_log"}

    for table in tables:
        icaos = {r["icao"] for r in extract.execute(f"SELECT icao FROM {table}")}
        assert icaos == {TARGET}, f"{table} leaked other aircraft rows"

    assert extract.execute("SELECT COUNT(*) FROM flights").fetchone()[0] == 3
    assert extract.execute("SELECT COUNT(*) FROM trace_days").fetchone()[0] == 4
    assert extract.execute("SELECT COUNT(*) FROM fetch_log").fetchone()[0] == 3

    # Every trace_json in the extract is plain JSON text, including the day
    # stored compressed in the working DB, and decodes to the original trace.
    rows = extract.execute("SELECT date, source, trace_json FROM trace_days").fetchall()
    for row in rows:
        assert isinstance(row["trace_json"], str)
    by_key = {(r["date"], r["source"]): json.loads(r["trace_json"]) for r in rows}
    assert by_key[(DAY2, "adsbx")] == DAY2_ADSBX_POINTS
    assert by_key[(DAY2, "adsbfi")] == DAY2_ADSBFI_POINTS
    assert by_key[(DAY3, "adsbx")] == DAY3_POINTS
    extract.close()


# ---------------------------------------------------------------------------
# Flight CSVs
# ---------------------------------------------------------------------------


def _read_csv(path: Path) -> tuple[list[str], list[dict]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def test_flights_csv_header_and_scoping(export_db, tmp_path):
    out = tmp_path / "bundle"
    with Database(export_db) as db:
        export_bundle(db, TARGET, out)
        flight_columns = [r["name"] for r in db.conn.execute("PRAGMA table_info(flights)")]

    header, rows = _read_csv(out / "flights.csv")
    assert header == flight_columns
    assert [row["callsign"] for row in rows] == ["EARLY", "INSIDE", "SPAN"]  # takeoff_time order
    assert all(row["icao"] == TARGET for row in rows)


def test_window_flights_csv_uses_overlap_semantics(export_db, tmp_path):
    """A DAY3-only window keeps the flight that took off late on DAY2 and
    landed on DAY3, drops everything else."""
    out = tmp_path / "bundle"
    windows = parse_windows([f"{DAY2}:{DAY2}", f"{DAY3}:{DAY3}"])
    with Database(export_db) as db:
        result = export_bundle(db, TARGET, out, windows)

    _, day2_rows = _read_csv(out / f"flights_{DAY2}_{DAY2}.csv")
    assert {row["callsign"] for row in day2_rows} == {"INSIDE", "SPAN"}
    _, day3_rows = _read_csv(out / f"flights_{DAY3}_{DAY3}.csv")
    assert {row["callsign"] for row in day3_rows} == {"SPAN"}
    assert result.window_flight_counts == {f"{DAY2}_{DAY2}": 2, f"{DAY3}_{DAY3}": 1}


# ---------------------------------------------------------------------------
# Trace CSVs
# ---------------------------------------------------------------------------


def test_trace_csv_merges_sources_and_carries_point_fields(export_db, tmp_path):
    out = tmp_path / "bundle"
    windows = parse_windows([f"{DAY2}:{DAY2}"])
    with Database(export_db) as db:
        result = export_bundle(db, TARGET, out, windows)

    header, rows = _read_csv(out / f"trace_{DAY2}_{DAY2}.csv")
    assert header == list(TRACE_CSV_HEADER)
    assert result.window_point_counts[f"{DAY2}_{DAY2}"] == 4
    assert [row["source"] for row in rows] == ["adsbx", "adsbfi", "adsbx", "adsbx"]  # chronological merge
    epochs = [float(row["epoch_s"]) for row in rows]
    assert epochs == sorted(epochs)

    ground = rows[0]
    assert ground["on_ground"] == "1"
    assert ground["alt_ft"] == ""  # "ground" is not a numeric altitude
    assert ground["callsign"] == "TST1"
    assert ground["squawk"] == "1200"
    assert ground["gs_kt"] == "5"
    assert ground["time_utc"].startswith(f"{DAY2}T10:00:00")

    emergency = rows[-1]
    assert emergency["squawk"] == "7700"
    assert emergency["alt_ft"] == "30000"
    assert emergency["callsign"] == ""  # native resolution: no forward-fill


def test_trace_csv_window_subsets_points_and_reads_legacy_text_rows(export_db, tmp_path):
    """A sub-day datetime window drops out-of-window points; a DAY3 window
    exercises the legacy raw-JSON-TEXT decode path."""
    out = tmp_path / "bundle"
    windows = parse_windows([f"{DAY2}T10:00:{DAY2}T10:30", f"{DAY3}:{DAY3}"])
    with Database(export_db) as db:
        result = export_bundle(db, TARGET, out, windows)

    _, sub_day = _read_csv(out / f"trace_{DAY2}T100000_{DAY2}T103000.csv")
    assert len(sub_day) == 3  # 10:00:00 + 10:01:00 adsbx, 10:00:30 adsbfi; 11:00 point excluded
    assert {row["source"] for row in sub_day} == {"adsbx", "adsbfi"}

    _, legacy = _read_csv(out / f"trace_{DAY3}_{DAY3}.csv")
    assert len(legacy) == 2
    assert [row["alt_ft"] for row in legacy] == ["15000", "16000"]
    assert result.window_point_counts[f"{DAY3}_{DAY3}"] == 2


def test_trace_csv_empty_window_still_writes_header(export_db, tmp_path):
    out = tmp_path / "bundle"
    windows = parse_windows(["2027-01-01:2027-01-02"])
    with Database(export_db) as db:
        result = export_bundle(db, TARGET, out, windows)
    header, rows = _read_csv(out / "trace_2027-01-01_2027-01-02.csv")
    assert header == list(TRACE_CSV_HEADER)
    assert rows == []
    assert result.window_point_counts["2027-01-01_2027-01-02"] == 0


# ---------------------------------------------------------------------------
# README and analysis stub
# ---------------------------------------------------------------------------


def test_readme_describes_bundle(export_db, tmp_path):
    out = tmp_path / "bundle"
    windows = parse_windows([f"{DAY2}:{DAY2}"])
    with Database(export_db) as db:
        export_bundle(db, TARGET, out, windows)

    readme = (out / "README.md").read_text(encoding="utf-8")
    assert f"N9527C ({TARGET})" in readme
    assert f"{TARGET}.sqlite" in readme
    assert "flights.csv" in readme
    assert f"trace_{DAY2}_{DAY2}.csv" in readme
    assert "—" not in readme  # no em dashes in deliverable text


def test_analysis_stub_carries_registry_identity(export_db, tmp_path):
    out = tmp_path / "bundle"
    with Database(export_db) as db:
        export_bundle(db, TARGET, out, include_analysis=True)

    analysis = (out / "analysis.md").read_text(encoding="utf-8")
    assert "N9527C" in analysis
    assert "EXAMPLE HOLDINGS LLC" in analysis
    assert "GULFSTREAM G-IV" in analysis
    assert "serial 1234" in analysis
    assert "## Notes" in analysis
    assert "—" not in analysis


# ---------------------------------------------------------------------------
# CLI surface
# ---------------------------------------------------------------------------


def test_cli_export_writes_bundle_and_zip(export_db, tmp_path):
    out = tmp_path / "deliverable"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "export",
            "--hex",
            TARGET,
            "--db",
            str(export_db),
            "--out",
            str(out),
            "--window",
            f"{DAY2}:{DAY2}",
            "--analysis",
            "--zip",
        ],
    )
    assert result.exit_code == 0, result.output
    assert (out / "README.md").exists()
    zip_path = tmp_path / "deliverable.zip"
    assert zip_path.exists()
    with zipfile.ZipFile(zip_path) as archive:
        names = set(archive.namelist())
    assert names == {
        f"{TARGET}.sqlite",
        "flights.csv",
        f"flights_{DAY2}_{DAY2}.csv",
        f"trace_{DAY2}_{DAY2}.csv",
        "analysis.md",
        "README.md",
    }


def test_cli_export_rejects_bad_window(export_db, tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["export", "--hex", TARGET, "--db", str(export_db), "--out", str(tmp_path / "x"), "--window", "nope"],
    )
    assert result.exit_code != 0
    assert "START:END" in result.output


def test_cli_export_unknown_hex_fails_cleanly(export_db, tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["export", "--hex", "ffffff", "--db", str(export_db), "--out", str(tmp_path / "x")],
    )
    assert result.exit_code != 0
    assert "nothing to export" in result.output
