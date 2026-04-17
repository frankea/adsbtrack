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
