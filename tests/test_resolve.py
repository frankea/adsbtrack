"""Tests for adsbtrack.resolve - live callsign -> hex resolution (issue #29)."""

from __future__ import annotations

import json

import httpx
import pytest
from click.testing import CliRunner

from adsbtrack.cli import cli
from adsbtrack.db import Database
from adsbtrack.resolve import LiveApiError, LiveNetworkClient, resolve_callsign


class _FakeTransport(httpx.BaseTransport):
    """Serves hand-crafted responses without going to the network."""

    def __init__(self, responder):
        self._responder = responder

    def handle_request(self, request):
        return self._responder(request)


def _client(name, responder, base_url="https://api.example.test") -> LiveNetworkClient:
    return LiveNetworkClient(
        name,
        base_url,
        client=httpx.Client(transport=_FakeTransport(responder)),
        rate_limit_per_min=0,
    )


def _ac(hex_code="4075af", flight="EXS12DR ", **overrides) -> dict:
    entry = {
        "hex": hex_code,
        "flight": flight,
        "r": "G-DRTE",
        "t": "B738",
        "desc": "BOEING 737-800",
        "alt_baro": 38000,
        "gs": 462.0,
    }
    entry.update(overrides)
    return entry


def _envelope(*acs) -> dict:
    return {"ac": list(acs), "total": len(acs), "msg": "No error"}


# ---------------------------------------------------------------------------
# LiveNetworkClient
# ---------------------------------------------------------------------------


def test_client_returns_ac_list_and_uppercases_callsign():
    seen = {}

    def responder(request):
        seen["url"] = str(request.url)
        return httpx.Response(200, json=_envelope(_ac()))

    result = _client("adsblol", responder).get_callsign("exs12dr")
    assert seen["url"].endswith("/v2/callsign/EXS12DR")
    assert len(result) == 1
    assert result[0]["hex"] == "4075af"


def test_client_returns_empty_list_when_nothing_airborne():
    def responder(request):
        return httpx.Response(200, json=_envelope())

    assert _client("adsblol", responder).get_callsign("UAE201") == []


def test_client_treats_404_as_empty():
    def responder(request):
        return httpx.Response(404, text="not found")

    assert _client("adsbfi", responder).get_callsign("UAE201") == []


def test_client_retries_5xx_then_succeeds():
    calls = {"n": 0}

    def responder(request):
        calls["n"] += 1
        if calls["n"] < 2:
            return httpx.Response(503, text="busy")
        return httpx.Response(200, json=_envelope(_ac()))

    client = _client("adsblol", responder)
    client._sleep = lambda seconds: None
    assert len(client.get_callsign("EXS12DR")) == 1
    assert calls["n"] == 2


def test_client_raises_liveapierror_on_other_4xx():
    def responder(request):
        return httpx.Response(400, text="bad request")

    with pytest.raises(LiveApiError, match="HTTP 400"):
        _client("adsblol", responder).get_callsign("EXS12DR")


# ---------------------------------------------------------------------------
# resolve_callsign
# ---------------------------------------------------------------------------


def test_resolve_strips_padded_callsign_and_matches(tmp_path):
    def responder(request):
        return httpx.Response(200, json=_envelope(_ac(flight="EXS12DR ")))

    with Database(tmp_path / "t.db") as db:
        matches, errors = resolve_callsign(db, " exs12dr ", clients=[_client("adsblol", responder)])
    assert errors == {}
    assert len(matches) == 1
    assert matches[0].hex_code == "4075af"
    assert matches[0].callsign == "EXS12DR"
    assert matches[0].registration == "G-DRTE"
    assert matches[0].networks == ["adsblol"]


def test_resolve_filters_wrong_callsign_and_synthetic_hexes(tmp_path):
    def responder(request):
        return httpx.Response(
            200,
            json=_envelope(
                _ac(flight="OTHER1 "),  # different callsign: drop
                _ac(hex_code="~2e40c9"),  # TIS-B synthetic address: drop
                _ac(hex_code="4075af"),
            ),
        )

    with Database(tmp_path / "t.db") as db:
        matches, _ = resolve_callsign(db, "EXS12DR", clients=[_client("adsblol", responder)])
    assert [m.hex_code for m in matches] == ["4075af"]


def test_resolve_dedupes_across_networks_and_fills_gaps(tmp_path):
    """First network wins per field; the second fills what it left None
    and lands in the networks list."""

    def lol_responder(request):
        return httpx.Response(200, json=_envelope(_ac(desc=None, gs=None)))

    def fi_responder(request):
        return httpx.Response(200, json=_envelope(_ac(desc="BOEING 737-800", gs=462.0)))

    with Database(tmp_path / "t.db") as db:
        matches, errors = resolve_callsign(
            db,
            "EXS12DR",
            clients=[_client("adsblol", lol_responder), _client("adsbfi", fi_responder)],
        )
    assert errors == {}
    assert len(matches) == 1
    assert matches[0].networks == ["adsblol", "adsbfi"]
    assert matches[0].type_description == "BOEING 737-800"
    assert matches[0].ground_speed == 462.0


def test_resolve_survives_one_network_failing(tmp_path):
    def broken(request):
        return httpx.Response(400, text="nope")

    def working(request):
        return httpx.Response(200, json=_envelope(_ac()))

    with Database(tmp_path / "t.db") as db:
        matches, errors = resolve_callsign(
            db,
            "EXS12DR",
            clients=[_client("adsblol", broken), _client("adsbfi", working)],
        )
    assert len(matches) == 1
    assert matches[0].networks == ["adsbfi"]
    assert "adsblol" in errors


def test_resolve_caches_match_into_hex_crossref(tmp_path):
    def responder(request):
        return httpx.Response(200, json=_envelope(_ac()))

    with Database(tmp_path / "t.db") as db:
        resolve_callsign(db, "EXS12DR", clients=[_client("adsblol", responder)])
        row = db.get_hex_crossref("4075af")
    assert row is not None
    assert row["registration"] == "G-DRTE"
    assert row["type_code"] == "B738"
    assert row["source"] == "adsblol_live"


def test_resolve_cache_stamps_mil_flags_for_mil_range_hex(tmp_path):
    """A DoD-pool hex seen live still gets the mil overlay in the cache."""

    def responder(request):
        return httpx.Response(200, json=_envelope(_ac(hex_code="ae1234", r="", t="C17")))

    with Database(tmp_path / "t.db") as db:
        resolve_callsign(db, "EXS12DR", clients=[_client("adsblol", responder)])
        row = db.get_hex_crossref("ae1234")
    assert row["is_military"] == 1
    assert row["mil_country"] == "United States"
    assert row["source"] == "adsblol_live"


def test_resolve_never_clobbers_existing_identity_row(tmp_path):
    """Live-feed identity must not overwrite FAA / hexdb data."""

    def responder(request):
        return httpx.Response(200, json=_envelope(_ac(r="SPOOFED", t="ZZZZ")))

    with Database(tmp_path / "t.db") as db:
        db.upsert_hex_crossref(
            {
                "icao": "4075af",
                "registration": "G-DRTE",
                "type_code": "B738",
                "operator": "Jet2",
                "source": "hexdb",
            }
        )
        db.commit()
        matches, _ = resolve_callsign(db, "EXS12DR", clients=[_client("adsblol", responder)])
        row = db.get_hex_crossref("4075af")
    assert len(matches) == 1  # the match is still reported...
    assert row["source"] == "hexdb"  # ...but the cache keeps the stronger row
    assert row["operator"] == "Jet2"


def test_resolve_cache_false_writes_nothing(tmp_path):
    def responder(request):
        return httpx.Response(200, json=_envelope(_ac()))

    with Database(tmp_path / "t.db") as db:
        resolve_callsign(db, "EXS12DR", clients=[_client("adsblol", responder)], cache=False)
        assert db.get_hex_crossref("4075af") is None


def test_resolve_rejects_empty_callsign(tmp_path):
    with Database(tmp_path / "t.db") as db, pytest.raises(ValueError):
        resolve_callsign(db, "   ", clients=[])


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _patch_clients(monkeypatch, responders):
    """Replace LiveNetworkClient so the CLI builds fake-transport clients.

    responders maps network name -> responder callable; unknown names get
    an empty envelope.
    """
    from adsbtrack import resolve as resolve_mod

    real = resolve_mod.LiveNetworkClient

    def factory(name, base_url, **kwargs):
        responder = responders.get(name, lambda request: httpx.Response(200, json=_envelope()))
        return real(
            name,
            base_url,
            client=httpx.Client(transport=_FakeTransport(responder)),
            rate_limit_per_min=0,
        )

    monkeypatch.setattr(resolve_mod, "LiveNetworkClient", factory)


def test_cli_resolve_prints_match_table(tmp_path, monkeypatch):
    _patch_clients(monkeypatch, {"adsblol": lambda request: httpx.Response(200, json=_envelope(_ac()))})
    runner = CliRunner()
    result = runner.invoke(cli, ["resolve", "exs12dr", "--db", str(tmp_path / "t.db")])
    assert result.exit_code == 0
    assert "4075af" in result.output
    assert "G-DRTE" in result.output
    assert "adsblol" in result.output


def test_cli_resolve_no_match_exits_1(tmp_path, monkeypatch):
    _patch_clients(monkeypatch, {})
    runner = CliRunner()
    result = runner.invoke(cli, ["resolve", "UAE201", "--db", str(tmp_path / "t.db")])
    assert result.exit_code == 1
    assert "No currently-airborne aircraft" in result.output


def test_cli_resolve_all_networks_down_exits_2(tmp_path, monkeypatch):
    def broken(request):
        return httpx.Response(400, text="nope")

    _patch_clients(monkeypatch, {"adsblol": broken, "adsbfi": broken})
    runner = CliRunner()
    result = runner.invoke(cli, ["resolve", "UAE201", "--db", str(tmp_path / "t.db")])
    assert result.exit_code == 2


def test_cli_resolve_json_output(tmp_path, monkeypatch):
    _patch_clients(monkeypatch, {"adsbfi": lambda request: httpx.Response(200, json=_envelope(_ac()))})
    runner = CliRunner()
    result = runner.invoke(cli, ["resolve", "EXS12DR", "--json", "--db", str(tmp_path / "t.db")])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["callsign"] == "EXS12DR"
    assert payload["matches"][0]["hex_code"] == "4075af"
    assert payload["matches"][0]["networks"] == ["adsbfi"]
    assert payload["errors"] == {}


def test_cli_resolve_no_cache_flag(tmp_path, monkeypatch):
    _patch_clients(monkeypatch, {"adsblol": lambda request: httpx.Response(200, json=_envelope(_ac()))})
    db_path = tmp_path / "t.db"
    runner = CliRunner()
    result = runner.invoke(cli, ["resolve", "EXS12DR", "--no-cache", "--db", str(db_path)])
    assert result.exit_code == 0
    with Database(db_path) as db:
        assert db.get_hex_crossref("4075af") is None
