"""Tests for adsbtrack.lookup - universal hex/registration lookup (issue #27)."""

from __future__ import annotations

import json

import httpx
import pytest
from click.testing import CliRunner

from adsbtrack.cli import cli
from adsbtrack.db import Database
from adsbtrack.hex_crossref import HexdbClient
from adsbtrack.lookup import AdsbdbClient, AdsbdbError, _adsbdb_to_crossref, lookup_aircraft


class _FakeTransport(httpx.BaseTransport):
    """Serves hand-crafted responses without going to the network."""

    def __init__(self, responder):
        self._responder = responder

    def handle_request(self, request):
        return self._responder(request)


def _adsbdb_client(responder) -> AdsbdbClient:
    return AdsbdbClient(
        client=httpx.Client(transport=_FakeTransport(responder)),
        rate_limit_per_min=0,
    )


def _hexdb_client(responder) -> HexdbClient:
    return HexdbClient(
        client=httpx.Client(transport=_FakeTransport(responder)),
        rate_limit_per_min=0,
    )


def _adsbdb_envelope(**overrides) -> dict:
    aircraft = {
        "type": "A380 842",
        "icao_type": "A388",
        "manufacturer": "Airbus",
        "mode_s": "896483",
        "registration": "A6-EUY",
        "registered_owner_country_name": "United Arab Emirates",
        "registered_owner": "Emirates Airline",
    }
    aircraft.update(overrides)
    return {"response": {"aircraft": aircraft}}


# ---------------------------------------------------------------------------
# AdsbdbClient
# ---------------------------------------------------------------------------


def test_adsbdb_returns_aircraft_on_200():
    def responder(request):
        assert "/v0/aircraft/896483" in str(request.url)
        return httpx.Response(200, json=_adsbdb_envelope())

    result = _adsbdb_client(responder).get_aircraft("896483")
    assert result is not None
    assert result["registration"] == "A6-EUY"
    assert result["registered_owner"] == "Emirates Airline"


def test_adsbdb_uppercases_query_in_url():
    seen = {}

    def responder(request):
        seen["url"] = str(request.url)
        return httpx.Response(200, json=_adsbdb_envelope())

    _adsbdb_client(responder).get_aircraft("a6-eUy")
    assert seen["url"].endswith("/v0/aircraft/A6-EUY")


def test_adsbdb_returns_none_on_404_unknown_aircraft():
    def responder(request):
        return httpx.Response(404, json={"response": "unknown aircraft"})

    assert _adsbdb_client(responder).get_aircraft("ae9c7c") is None


def test_adsbdb_returns_none_on_200_with_string_response():
    """Defensive: a 200 whose response field isn't the aircraft envelope."""

    def responder(request):
        return httpx.Response(200, json={"response": "unknown aircraft"})

    assert _adsbdb_client(responder).get_aircraft("ae9c7c") is None


def test_adsbdb_retries_on_5xx_then_succeeds():
    calls = {"n": 0}

    def responder(request):
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(503, text="busy")
        return httpx.Response(200, json=_adsbdb_envelope())

    client = _adsbdb_client(responder)
    client._sleep = lambda seconds: None
    result = client.get_aircraft("896483")
    assert result is not None
    assert calls["n"] == 3


def test_adsbdb_raises_on_other_4xx():
    def responder(request):
        return httpx.Response(400, text="bad request")

    with pytest.raises(AdsbdbError):
        _adsbdb_client(responder).get_aircraft("896483")


def test_adsbdb_payload_mapping():
    row = _adsbdb_to_crossref(_adsbdb_envelope()["response"]["aircraft"])
    assert row["icao"] == "896483"
    assert row["registration"] == "A6-EUY"
    assert row["type_code"] == "A388"
    assert row["type_description"] == "Airbus A380 842"
    assert row["operator"] == "Emirates Airline"
    assert row["source"] == "adsbdb"


# ---------------------------------------------------------------------------
# lookup_aircraft - hex queries
# ---------------------------------------------------------------------------


def test_lookup_hex_answers_from_crossref_cache(tmp_path):
    """A cached identity row answers without any client at all."""
    with Database(tmp_path / "t.db") as db:
        db.upsert_hex_crossref(
            {
                "icao": "896483",
                "registration": "A6-EUY",
                "type_code": "A388",
                "operator": "Emirates Airline",
                "source": "adsbdb",
            }
        )
        db.commit()
        result = lookup_aircraft(db, "896483")
    assert result.hex_code == "896483"
    assert result.record["registration"] == "A6-EUY"
    assert result.source == "adsbdb"
    assert result.resolved


def test_lookup_hex_falls_back_to_hexdb_then_caches(tmp_path):
    def responder(request):
        return httpx.Response(
            200,
            json={
                "ModeS": "43C556",
                "Registration": "ZK019",
                "ICAOTypeCode": "HAWK",
                "Type": "Hawk T.2",
                "RegisteredOwners": "Royal Air Force",
            },
        )

    with Database(tmp_path / "t.db") as db:
        result = lookup_aircraft(db, "43c556", hexdb_client=_hexdb_client(responder))
        cached = db.get_hex_crossref("43c556")
    assert result.record["registration"] == "ZK019"
    assert result.source == "hexdb"
    # ZK019 sits inside the seeded RAF range: mil overlay + annotation.
    assert result.record["is_military"]
    assert result.mil_range["country"] == "United Kingdom"
    assert cached["registration"] == "ZK019"


def test_lookup_hex_falls_back_to_adsbdb_when_hexdb_misses(tmp_path):
    """Fallback order: hexdb.io first, adsbdb only after it misses."""

    def hexdb_responder(request):
        return httpx.Response(404, text="not found")

    def adsbdb_responder(request):
        return httpx.Response(200, json=_adsbdb_envelope())

    with Database(tmp_path / "t.db") as db:
        result = lookup_aircraft(
            db,
            "896483",
            hexdb_client=_hexdb_client(hexdb_responder),
            adsbdb_client=_adsbdb_client(adsbdb_responder),
        )
        cached = db.get_hex_crossref("896483")
    assert result.source == "adsbdb"
    assert result.record["registration"] == "A6-EUY"
    assert result.country == "United Arab Emirates"
    # Cached with the adsbdb source tag for later offline lookups.
    assert cached["source"] == "adsbdb"
    assert cached["registration"] == "A6-EUY"


def test_lookup_unresolvable_dod_hex_gets_mil_annotation(tmp_path):
    """AE9C7C casework: no source resolves it, but the DoD-pool range
    annotation still comes back (and exit-code-wise it counts resolved)."""

    def responder_404(request):
        return httpx.Response(404, text="nope")

    with Database(tmp_path / "t.db") as db:
        result = lookup_aircraft(
            db,
            "AE9C7C",
            hexdb_client=_hexdb_client(responder_404),
            adsbdb_client=_adsbdb_client(responder_404),
        )
    assert result.hex_code == "ae9c7c"
    assert result.record["source"] == "mil_range"
    assert result.record["registration"] is None
    assert result.mil_range["country"] == "United States"
    assert "DoD pool" in result.mil_range["notes"]
    assert result.resolved


def test_lookup_unknown_civilian_hex_is_unresolved(tmp_path):
    with Database(tmp_path / "t.db") as db:
        result = lookup_aircraft(db, "39d2f1")
    assert result.hex_code == "39d2f1"
    assert result.record is None
    assert result.mil_range is None
    assert not result.resolved


def test_lookup_us_hex_derives_algorithmic_nnumber_offline(tmp_path):
    """A US-civil-range hex with no local data still yields the algorithmic
    N-number, flagged as derived rather than presented as an identity."""
    with Database(tmp_path / "t.db") as db:
        result = lookup_aircraft(db, "a66ad3")
    assert result.record is None
    assert result.derived_registration == "N512WB"


# ---------------------------------------------------------------------------
# lookup_aircraft - registration queries
# ---------------------------------------------------------------------------


def test_lookup_registration_nnumber_converts_algorithmically(tmp_path):
    with Database(tmp_path / "t.db") as db:
        result = lookup_aircraft(db, "N512WB")
    assert result.hex_code == "a66ad3"


def test_lookup_registration_found_in_local_crossref(tmp_path):
    with Database(tmp_path / "t.db") as db:
        db.upsert_hex_crossref(
            {
                "icao": "896483",
                "registration": "A6-EUY",
                "type_code": "A388",
                "source": "adsbdb",
            }
        )
        db.commit()
        result = lookup_aircraft(db, "a6-euy")  # case-insensitive
    assert result.hex_code == "896483"
    assert result.record["type_code"] == "A388"


def test_lookup_registration_via_hexdb_reg_hex(tmp_path):
    """hexdb.io resolves the registration to a hex, then its aircraft
    endpoint fills the identity."""

    def responder(request):
        url = str(request.url)
        if "/reg-hex" in url:
            return httpx.Response(200, text="43C556")
        return httpx.Response(
            200,
            json={"Registration": "ZK019", "ICAOTypeCode": "HAWK", "RegisteredOwners": "Royal Air Force"},
        )

    with Database(tmp_path / "t.db") as db:
        result = lookup_aircraft(db, "ZK019", hexdb_client=_hexdb_client(responder))
    assert result.hex_code == "43c556"
    assert result.record["registration"] == "ZK019"
    assert result.source == "hexdb"


def test_lookup_registration_via_adsbdb_reuses_payload(tmp_path):
    """When adsbdb resolves the registration, its payload becomes the
    identity without a second aircraft fetch."""
    calls = {"n": 0}

    def adsbdb_responder(request):
        calls["n"] += 1
        return httpx.Response(200, json=_adsbdb_envelope(mode_s="89649D", registration="A6-API"))

    with Database(tmp_path / "t.db") as db:
        result = lookup_aircraft(db, "A6-API", adsbdb_client=_adsbdb_client(adsbdb_responder))
        cached = db.get_hex_crossref("89649d")
    assert result.hex_code == "89649d"
    assert result.record["registration"] == "A6-API"
    assert calls["n"] == 1
    assert cached["source"] == "adsbdb"


def test_lookup_registration_unresolvable(tmp_path):
    def responder_404(request):
        return httpx.Response(404, text="nope")

    with Database(tmp_path / "t.db") as db:
        result = lookup_aircraft(
            db,
            "ZZ-NOPE",
            hexdb_client=_hexdb_client(responder_404),
            adsbdb_client=_adsbdb_client(responder_404),
        )
    assert result.hex_code is None
    assert not result.resolved


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def test_cli_lookup_offline_cached_row(tmp_path):
    db_path = tmp_path / "t.db"
    with Database(db_path) as db:
        db.upsert_hex_crossref(
            {
                "icao": "896483",
                "registration": "A6-EUY",
                "type_code": "A388",
                "operator": "Emirates Airline",
                "source": "adsbdb",
            }
        )
        db.commit()

    runner = CliRunner()
    result = runner.invoke(cli, ["lookup", "896483", "--offline", "--db", str(db_path)])
    assert result.exit_code == 0
    assert "A6-EUY" in result.output
    assert "A388" in result.output


def test_cli_lookup_tail_alias_still_works(tmp_path):
    """Old scripts using `lookup --tail N512WB` still get the hex."""
    db_path = tmp_path / "t.db"
    runner = CliRunner()
    result = runner.invoke(cli, ["lookup", "--tail", "N512WB", "--offline", "--db", str(db_path)])
    assert result.exit_code == 0
    assert "a66ad3" in result.output


def test_cli_lookup_rejects_query_plus_tail(tmp_path):
    runner = CliRunner()
    result = runner.invoke(cli, ["lookup", "896483", "--tail", "N512WB", "--db", str(tmp_path / "t.db")])
    assert result.exit_code != 0
    assert "not both" in result.output


def test_cli_lookup_requires_some_query(tmp_path):
    runner = CliRunner()
    result = runner.invoke(cli, ["lookup", "--db", str(tmp_path / "t.db")])
    assert result.exit_code != 0


def test_cli_lookup_mil_annotation_offline(tmp_path):
    """DoD-pool hex offline: no identity, but the range annotation prints
    and the exit code stays 0 (the annotation IS the answer)."""
    runner = CliRunner()
    result = runner.invoke(cli, ["lookup", "ae9c7c", "--offline", "--db", str(tmp_path / "t.db")])
    assert result.exit_code == 0
    assert "United States" in result.output
    assert "DoD pool" in result.output


def test_cli_lookup_unresolved_exits_nonzero(tmp_path):
    runner = CliRunner()
    result = runner.invoke(cli, ["lookup", "39d2f1", "--offline", "--db", str(tmp_path / "t.db")])
    assert result.exit_code == 1


def test_cli_lookup_json_output(tmp_path):
    db_path = tmp_path / "t.db"
    with Database(db_path) as db:
        db.upsert_hex_crossref(
            {
                "icao": "43c556",
                "registration": "ZK019",
                "type_code": "HAWK",
                "source": "hexdb",
                "is_military": True,
                "mil_country": "United Kingdom",
                "mil_branch": "Military (RAF)",
            }
        )
        db.commit()

    runner = CliRunner()
    result = runner.invoke(cli, ["lookup", "ZK019", "--offline", "--json", "--db", str(db_path)])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["hex_code"] == "43c556"
    assert payload["record"]["registration"] == "ZK019"
    assert payload["mil_range"]["country"] == "United Kingdom"
