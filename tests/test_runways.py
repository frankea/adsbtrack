"""Tests for adsbtrack.runways -- OurAirports runway ingestion."""

from pathlib import Path

import pytest

from adsbtrack.db import Database
from adsbtrack.runways import import_runways_from_path, parse_runway_row

FIXTURE = Path(__file__).parent / "fixtures" / "runways_sample.csv"


def _row(**overrides) -> dict[str, str]:
    """Build a DictReader-shaped row with sensible defaults.

    Defaults model a well-formed small GA runway pair so individual tests
    only override the fields they care about."""
    row = {
        "id": "1",
        "airport_ref": "1",
        "airport_ident": "KSPG",
        "length_ft": "2864",
        "width_ft": "75",
        "surface": "ASPH",
        "lighted": "1",
        "closed": "0",
        "le_ident": "18",
        "le_latitude_deg": "27.77327",
        "le_longitude_deg": "-82.69509",
        "le_elevation_ft": "7",
        "le_heading_degT": "180.0",
        "le_displaced_threshold_ft": "0",
        "he_ident": "36",
        "he_latitude_deg": "27.76539",
        "he_longitude_deg": "-82.69509",
        "he_elevation_ft": "7",
        "he_heading_degT": "360.0",
        "he_displaced_threshold_ft": "0",
    }
    row.update(overrides)
    return row


def test_parse_runway_row_emits_both_ends():
    """A fully-populated row yields two tuples, one per end."""
    ends = parse_runway_row(_row())
    assert len(ends) == 2
    low, high = ends
    # (airport_ident, runway_name, lat, lon, elev, heading, length_ft,
    #  width_ft, surface, closed, displaced_threshold_ft)
    assert low[0] == "KSPG"
    assert low[1] == "18"
    assert low[2] == pytest.approx(27.77327)
    assert low[3] == pytest.approx(-82.69509)
    assert low[5] == pytest.approx(180.0)
    assert low[6] == 2864
    assert low[8] == "ASPH"
    assert low[9] == 0
    assert high[1] == "36"


def test_parse_runway_row_skips_end_with_missing_latlon():
    """If he_latitude_deg is blank, only the le end is emitted."""
    row = _row(he_latitude_deg="", he_longitude_deg="")
    ends = parse_runway_row(row)
    assert len(ends) == 1
    assert ends[0][1] == "18"


def test_parse_runway_row_skips_both_ends_when_both_blank():
    row = _row(
        le_latitude_deg="",
        le_longitude_deg="",
        he_latitude_deg="",
        he_longitude_deg="",
    )
    assert parse_runway_row(row) == []


def test_parse_runway_row_skips_heliport_H1_pattern():
    """OurAirports represents heliports with le_ident='H1' and blank everything."""
    row = _row(
        le_ident="H1",
        le_latitude_deg="",
        le_longitude_deg="",
        le_elevation_ft="",
        le_heading_degT="",
        le_displaced_threshold_ft="",
        he_ident="",
        he_latitude_deg="",
        he_longitude_deg="",
        he_elevation_ft="",
        he_heading_degT="",
        he_displaced_threshold_ft="",
    )
    assert parse_runway_row(row) == []


def test_parse_runway_row_skips_endpoint_with_blank_ident():
    """An end with a blank ident can't be uniquely keyed - skip it."""
    row = _row(he_ident="")
    ends = parse_runway_row(row)
    assert len(ends) == 1
    assert ends[0][1] == "18"


def test_parse_runway_row_handles_blank_numeric_fields():
    """length_ft / width_ft / displaced_threshold_ft blank -> NULL."""
    row = _row(length_ft="", width_ft="", le_displaced_threshold_ft="")
    ends = parse_runway_row(row)
    low = ends[0]
    assert low[6] is None  # length_ft
    assert low[7] is None  # width_ft
    assert low[10] is None  # displaced_threshold_ft


def test_parse_runway_row_preserves_airport_ident_casing():
    """FAA local codes like '67FL' must survive unchanged."""
    row = _row(airport_ident="67FL")
    ends = parse_runway_row(row)
    assert ends[0][0] == "67FL"


def test_parse_runway_row_closed_flag():
    row = _row(closed="1")
    ends = parse_runway_row(row)
    assert ends[0][9] == 1


def test_import_runways_from_path_counts_ends(tmp_path):
    """The fixture should produce 13 valid runway ends total:
    * KATL:     5 runway pairs * 2 ends    = 10
    * KSPG:     1 runway pair  * 2 ends    =  2
    * SINGLE:   1 valid end (other blank)  =  1
    * BOTHBAD:  0 (no coordinates)         =  0
    * 00A, HELIPORT2: 0 each (heliports)   =  0
    """
    db_path = tmp_path / "t.db"
    with Database(db_path) as db:
        inserted = import_runways_from_path(db, FIXTURE)
        assert inserted == 13
        assert db.runway_count() == 13


def test_import_runways_from_path_katl_has_ten_ends(tmp_path):
    db_path = tmp_path / "t.db"
    with Database(db_path) as db:
        import_runways_from_path(db, FIXTURE)
        rows = db.conn.execute(
            "SELECT runway_name FROM runways WHERE airport_ident = ? ORDER BY runway_name",
            ("KATL",),
        ).fetchall()
        names = [r["runway_name"] for r in rows]
        assert names == sorted(["08L", "26R", "09R", "27L", "08R", "26L", "09L", "27R", "10", "28"])


def test_import_runways_from_path_kspg_has_two_ends(tmp_path):
    db_path = tmp_path / "t.db"
    with Database(db_path) as db:
        import_runways_from_path(db, FIXTURE)
        rows = db.conn.execute(
            "SELECT runway_name, heading_deg_true FROM runways WHERE airport_ident = ? ORDER BY runway_name",
            ("KSPG",),
        ).fetchall()
        assert [r["runway_name"] for r in rows] == ["18", "36"]
        assert rows[0]["heading_deg_true"] == pytest.approx(180.0)


def test_import_runways_from_path_skips_heliport(tmp_path):
    db_path = tmp_path / "t.db"
    with Database(db_path) as db:
        import_runways_from_path(db, FIXTURE)
        count_00a = db.conn.execute(
            "SELECT COUNT(*) AS cnt FROM runways WHERE airport_ident = ?",
            ("00A",),
        ).fetchone()["cnt"]
        count_h2 = db.conn.execute(
            "SELECT COUNT(*) AS cnt FROM runways WHERE airport_ident = ?",
            ("HELIPORT2",),
        ).fetchone()["cnt"]
        assert count_00a == 0
        assert count_h2 == 0


def test_import_runways_from_path_single_end(tmp_path):
    """SINGLE has one well-formed end and one blank end -> 1 row."""
    db_path = tmp_path / "t.db"
    with Database(db_path) as db:
        import_runways_from_path(db, FIXTURE)
        rows = db.conn.execute(
            "SELECT runway_name FROM runways WHERE airport_ident = ?",
            ("SINGLE",),
        ).fetchall()
        assert [r["runway_name"] for r in rows] == ["09"]


def test_import_runways_from_path_is_idempotent(tmp_path):
    """Running twice should leave row count unchanged."""
    db_path = tmp_path / "t.db"
    with Database(db_path) as db:
        import_runways_from_path(db, FIXTURE)
        first = db.runway_count()
        import_runways_from_path(db, FIXTURE)
        assert db.runway_count() == first
