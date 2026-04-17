from pathlib import Path

from adsbtrack.config import Config
from adsbtrack.db import Database
from adsbtrack.navaids import refresh_navaids

FIXTURE = Path(__file__).parent / "fixtures" / "navaids_sample.csv"


def test_refresh_loads_fixture(tmp_path):
    cfg = Config(db_path=tmp_path / "nav.db")
    with Database(cfg.db_path) as db:
        n = refresh_navaids(db, cfg, local_csv=FIXTURE)
        assert n == 3
        rows = db.conn.execute(
            "SELECT ident, name, type, elevation_ft, frequency_khz FROM navaids ORDER BY ident"
        ).fetchall()
        assert rows[0]["ident"] == "CLT"
        assert rows[0]["type"] == "VOR-DME"
        assert rows[0]["elevation_ft"] == 750
        assert rows[0]["frequency_khz"] == 115800
        # Missing elevation tolerated.
        fix_row = [r for r in rows if r["ident"] == "SHAWZ"][0]
        assert fix_row["elevation_ft"] is None


def test_refresh_is_idempotent(tmp_path):
    cfg = Config(db_path=tmp_path / "nav.db")
    with Database(cfg.db_path) as db:
        refresh_navaids(db, cfg, local_csv=FIXTURE)
        refresh_navaids(db, cfg, local_csv=FIXTURE)
        count = db.conn.execute("SELECT COUNT(*) FROM navaids").fetchone()[0]
        assert count == 3


def test_query_navaids_in_bbox(tmp_path):
    from adsbtrack.navaids import query_navaids_in_bbox

    cfg = Config(db_path=tmp_path / "nav.db")
    with Database(cfg.db_path) as db:
        refresh_navaids(db, cfg, local_csv=FIXTURE)
        # All three fixture rows land between lat 34-37, lon -83 to -80.
        rows = query_navaids_in_bbox(db.conn, 34.0, 37.0, -84.0, -80.0)
        idents = {r["ident"] for r in rows}
        assert idents == {"CLT", "SHAWZ", "KEEMO"}

        # Narrow box around SHAWZ only.
        rows = query_navaids_in_bbox(db.conn, 34.5, 34.6, -81.3, -81.2)
        assert {r["ident"] for r in rows} == {"SHAWZ"}
