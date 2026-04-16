"""Tests for adsbtrack.hex_crossref - hex enrichment + Mictronics + hexdb.io client."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from adsbtrack.db import Database
from adsbtrack.hex_crossref import (
    HexCrossrefError,
    HexdbClient,
    _hexdb_payload_to_crossref,
    _mictronics_row_to_crossref,
    enrich_all,
    enrich_hex,
    import_mictronics,
)

# ---------------------------------------------------------------------------
# Mictronics parsing + import
# ---------------------------------------------------------------------------


def test_mictronics_row_parse_basic():
    types = {"PC12": ["PILATUS PC-12", "M", "L"]}
    row = _mictronics_row_to_crossref(
        "a66ad3",
        ["N512WB", "PC12", "00"],
        types,
        {},
        source_label="mictronics",
    )
    assert row["icao"] == "a66ad3"
    assert row["registration"] == "N512WB"
    assert row["type_code"] == "PC12"
    assert row["type_description"] == "PILATUS PC-12"
    assert row["source"] == "mictronics"
    assert row["is_military"] is False


def test_mictronics_row_parse_missing_type_code():
    """Entries with unknown type code leave type_description as None
    rather than raising a KeyError."""
    row = _mictronics_row_to_crossref(
        "abcdef",
        ["G-ABCD", "ZZZZ", "00"],
        {"PC12": ["PILATUS PC-12", "M", "L"]},
        {},
        source_label="mictronics",
    )
    assert row["type_code"] == "ZZZZ"
    assert row["type_description"] is None


def _write_mictronics_fixture(cache_dir: Path) -> None:
    """Write small fake Mictronics JSON files into cache_dir."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    aircrafts = {
        "a66ad3": ["N512WB", "PC12", "00"],
        "c01234": ["C-ABCD", "B737", "00"],
    }
    types = {"PC12": ["PILATUS PC-12", "M", "L"], "B737": ["BOEING 737", "M", "L"]}
    operators = {}
    (cache_dir / "aircrafts.json").write_text(json.dumps(aircrafts))
    (cache_dir / "types.json").write_text(json.dumps(types))
    (cache_dir / "operators.json").write_text(json.dumps(operators))
    (cache_dir / "dbversion.json").write_text(json.dumps({"version": "20260101"}))


def test_import_mictronics_writes_all_aircraft(tmp_path):
    cache_dir = tmp_path / "mictronics"
    _write_mictronics_fixture(cache_dir)

    with Database(tmp_path / "t.db") as db:
        count = import_mictronics(db, cache_dir)
        assert count == 2
        row = db.get_hex_crossref("a66ad3")
        assert row is not None
        assert row["registration"] == "N512WB"
        assert row["source"] == "mictronics"
        assert row["type_description"] == "PILATUS PC-12"


# ---------------------------------------------------------------------------
# HexdbClient
# ---------------------------------------------------------------------------


class _FakeTransport(httpx.BaseTransport):
    """Serves hand-crafted responses without going to the network."""

    def __init__(self, responder):
        self._responder = responder

    def handle_request(self, request):
        return self._responder(request)


def _json_response(status, payload):
    return httpx.Response(status, json=payload)


def test_hexdb_returns_payload_on_200():
    def responder(request):
        assert "/api/v1/aircraft/A66AD3" in str(request.url)
        return _json_response(
            200,
            {
                "ModeS": "A66AD3",
                "Registration": "N512WB",
                "Manufacturer": "Pilatus",
                "ICAOTypeCode": "PC12",
                "Type": "PC-12 45",
                "RegisteredOwners": "Air Pilatus LLC",
            },
        )

    client = HexdbClient(
        base_url="https://hexdb.io",
        client=httpx.Client(transport=_FakeTransport(responder)),
        rate_limit_per_min=0,
    )
    result = client.get_aircraft("a66ad3")
    assert result is not None
    assert result["Registration"] == "N512WB"


def test_hexdb_returns_none_on_404():
    """Both status 404 and 200-with-{status:"404"} body must become None."""

    def responder(request):
        return _json_response(404, {"status": "404", "error": "Aircraft not found."})

    client = HexdbClient(
        client=httpx.Client(transport=_FakeTransport(responder)),
        rate_limit_per_min=0,
    )
    assert client.get_aircraft("zzzzzz") is None


def test_hexdb_returns_none_on_200_with_404_body():
    def responder(request):
        return _json_response(200, {"status": "404", "error": "Aircraft not found."})

    client = HexdbClient(
        client=httpx.Client(transport=_FakeTransport(responder)),
        rate_limit_per_min=0,
    )
    assert client.get_aircraft("aaaaaa") is None


def test_hexdb_retries_on_5xx_then_succeeds():
    calls = {"n": 0}

    def responder(request):
        calls["n"] += 1
        if calls["n"] < 2:
            return httpx.Response(503, text="busy")
        return _json_response(200, {"Registration": "N1"})

    client = HexdbClient(
        client=httpx.Client(transport=_FakeTransport(responder)),
        rate_limit_per_min=0,
    )
    # Patch sleep so the test doesn't wait 2^0 = 1 second.
    with patch.object(client, "_sleep"):
        result = client.get_aircraft("abc123")
    assert result == {"Registration": "N1"}
    assert calls["n"] == 2


def test_hexdb_raises_on_other_4xx():
    def responder(request):
        return httpx.Response(400, text="bad request")

    client = HexdbClient(
        client=httpx.Client(transport=_FakeTransport(responder)),
        rate_limit_per_min=0,
    )
    with pytest.raises(HexCrossrefError, match="HTTP 400"):
        client.get_aircraft("abc123")


def test_hexdb_payload_mapping():
    payload = {
        "Registration": "N512WB",
        "ICAOTypeCode": "PC12",
        "Type": "PC-12 45",
        "RegisteredOwners": "Air Pilatus LLC",
    }
    row = _hexdb_payload_to_crossref("a66ad3", payload)
    assert row["icao"] == "a66ad3"
    assert row["registration"] == "N512WB"
    assert row["source"] == "hexdb"
    assert row["type_description"] == "PC-12 45"


# ---------------------------------------------------------------------------
# enrich_hex merge semantics
# ---------------------------------------------------------------------------


def _seed_faa(db, *, hex_code="a66ad3", n_number="512WB", name="EXAMPLE OWNER LLC"):
    """Write a minimal faa_registry row with the columns enrich reads."""
    from adsbtrack.registry import MASTER_COLUMNS

    row = ["X"] * len(MASTER_COLUMNS)
    row[0] = n_number
    row[6] = name
    # mfr_mdl_code at index 2
    row[2] = "1152015"
    row[-1] = hex_code
    db.insert_faa_registry([tuple(row)])
    db.commit()


def test_enrich_hex_prefers_faa_over_mictronics(tmp_path):
    with Database(tmp_path / "t.db") as db:
        _seed_faa(db)
        mictronics = (
            {"a66ad3": ["N000ZZ", "PC12", "00"]},  # different registration
            {"PC12": ["PILATUS PC-12", "M", "L"]},
            {},
        )
        row, conflicts = enrich_hex(db, "a66ad3", mictronics_cache=mictronics)
        assert row is not None
        assert row["source"] == "faa"
        assert row["registration"] == "N512WB"
        # Conflict flagged: FAA N512WB vs Mictronics N000ZZ
        assert any("registration" in c for c in conflicts)


def test_enrich_hex_falls_back_to_mictronics_when_no_faa(tmp_path):
    with Database(tmp_path / "t.db") as db:
        mictronics = (
            {"a66ad3": ["N512WB", "PC12", "00"]},
            {"PC12": ["PILATUS PC-12", "M", "L"]},
            {},
        )
        row, conflicts = enrich_hex(db, "a66ad3", mictronics_cache=mictronics)
        assert row is not None
        assert row["source"] == "mictronics"
        assert row["registration"] == "N512WB"
        assert conflicts == []


def test_enrich_hex_uses_hexdb_when_other_sources_empty(tmp_path):
    """With no FAA / Mictronics match, hexdb.io's response fills the row."""

    def responder(request):
        return _json_response(
            200,
            {
                "ModeS": "AAAAAA",
                "Registration": "G-ABCD",
                "ICAOTypeCode": "B737",
                "Type": "BOEING 737",
                "RegisteredOwners": "Some Airline",
            },
        )

    client = HexdbClient(
        client=httpx.Client(transport=_FakeTransport(responder)),
        rate_limit_per_min=0,
    )
    with Database(tmp_path / "t.db") as db:
        row, conflicts = enrich_hex(db, "aaaaaa", hexdb_client=client)
        assert row is not None
        assert row["source"] == "hexdb"
        assert row["registration"] == "G-ABCD"
        assert conflicts == []
        # And it must be persisted.
        persisted = db.get_hex_crossref("aaaaaa")
        assert persisted["registration"] == "G-ABCD"


def test_enrich_hex_flags_military_on_range_hit(tmp_path):
    """Any hex in a seeded military range gets is_military=1 + country + branch
    stamped, even when no other source returned data."""
    with Database(tmp_path / "t.db") as db:
        row, conflicts = enrich_hex(db, "ae1234")
        assert row is not None
        assert row["is_military"] is True
        assert row["mil_country"] == "United States"
        assert "DoD" in row["mil_branch"]
        assert row["source"] == "mil_range"
        # Persisted correctly (is_military stored as int 1)
        persisted = db.get_hex_crossref("ae1234")
        assert persisted["is_military"] == 1


def test_enrich_hex_military_flag_overlays_civilian_identity(tmp_path):
    """When Mictronics has a record for a hex that ALSO falls in a mil range,
    we keep the civilian identity but add the military flags."""
    with Database(tmp_path / "t.db") as db:
        mictronics = (
            {"ae1234": ["FAKE-REG", "C130", "00"]},
            {"C130": ["LOCKHEED C-130", "H", "L"]},
            {},
        )
        row, conflicts = enrich_hex(db, "ae1234", mictronics_cache=mictronics)
        assert row is not None
        assert row["registration"] == "FAKE-REG"
        assert row["is_military"] is True
        assert row["mil_country"] == "United States"


def test_enrich_hex_returns_none_when_no_source_has_data(tmp_path):
    """Civilian hex, no FAA match, no Mictronics match, no hexdb client -> None."""
    with Database(tmp_path / "t.db") as db:
        row, conflicts = enrich_hex(db, "a66ad3")
        assert row is None
        assert conflicts == []


# ---------------------------------------------------------------------------
# enrich_all backfill
# ---------------------------------------------------------------------------


def test_enrich_all_backfills_missing_icaos(tmp_path):
    from datetime import UTC, datetime

    from adsbtrack.config import Config
    from adsbtrack.models import Flight

    cache_dir = tmp_path / "mictronics"
    _write_mictronics_fixture(cache_dir)

    with Database(tmp_path / "t.db") as db:
        db.insert_flight(
            Flight(
                icao="a66ad3",
                takeoff_time=datetime(2022, 6, 15, 12, 0, 0, tzinfo=UTC),
                takeoff_lat=35.0,
                takeoff_lon=-118.0,
                takeoff_date="2022-06-15",
            )
        )
        db.insert_flight(
            Flight(
                icao="c01234",
                takeoff_time=datetime(2022, 6, 15, 13, 0, 0, tzinfo=UTC),
                takeoff_lat=35.0,
                takeoff_lon=-118.0,
                takeoff_date="2022-06-15",
            )
        )

        cfg = Config(db_path=tmp_path / "t.db")
        stats = enrich_all(
            db,
            cfg=cfg,
            mictronics_cache_dir=cache_dir,
            use_hexdb=False,
        )
        assert stats["processed"] == 2
        assert stats["written"] == 2
        assert db.get_hex_crossref("a66ad3")["registration"] == "N512WB"
        assert db.get_hex_crossref("c01234")["registration"] == "C-ABCD"
