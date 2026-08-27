"""Tests for adsbtrack.metar -- aviationweather.gov METAR history (issue #26).

All HTTP is mocked (MagicMock client per the test_acars.py pattern); the
sample payloads are trimmed copies of live 2026-08 API responses so the
parser is exercised against the real field shapes ("10+" visibility caps,
"VRB" wind, epoch obsTime, SPECI rows).
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from adsbtrack.config import Config
from adsbtrack.db import Database
from adsbtrack.metar import (
    Metar,
    MetarError,
    fetch_flight_endpoint_metars,
    fetch_metars,
    metar_window,
    parse_metar_json,
    stored_metars_near,
)

# Trimmed from a live 2026-08-21 response for KTYS (json format).
SAMPLE_PAYLOAD = [
    {
        "icaoId": "KTYS",
        "receiptTime": "2026-08-21T23:56:26.151Z",
        "obsTime": 1787356380,
        "temp": 26.7,
        "dewp": 22.2,
        "wdir": 340,
        "wspd": 4,
        "visib": "10+",
        "altim": 1014.6,
        "metarType": "METAR",
        "rawOb": "METAR KTYS 212353Z 34004KT 10SM FEW038 27/22 A2996",
        "fltCat": "VFR",
    },
    {
        "icaoId": "KTYS",
        "obsTime": 1787352480,
        "temp": 27,
        "dewp": 22,
        "wdir": "VRB",
        "wspd": 3,
        "wgst": 41,
        "visib": 2.5,
        "altim": 1014.6,
        "metarType": "SPECI",
        "rawOb": "SPECI KTYS 212248Z VRB03G41KT 2 1/2SM +TSRA 27/22 A2996",
        "fltCat": "IFR",
    },
]


def _response(status_code=200, json_body=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json = MagicMock(return_value=json_body)
    return resp


def _client(*responses):
    http = MagicMock()
    http.get = MagicMock(side_effect=list(responses))
    return http


# ---------------------------------------------------------------------------
# parse_metar_json
# ---------------------------------------------------------------------------


def test_parse_metar_json_full_row():
    rows = parse_metar_json(SAMPLE_PAYLOAD)
    assert len(rows) == 2
    m = rows[0]
    assert m.station == "KTYS"
    assert m.obs_time == datetime.fromtimestamp(1787356380, UTC).isoformat()
    assert m.metar_type == "METAR"
    assert m.raw_text.startswith("METAR KTYS 212353Z")
    assert m.temp_c == 26.7
    assert m.wind_dir_deg == 340
    assert m.wind_speed_kt == 4
    assert m.wind_gust_kt is None
    assert m.visibility_mi == 10.0  # "10+" cap parsed to the number
    assert m.altim_hpa == 1014.6
    assert m.flight_category == "VFR"


def test_parse_metar_json_vrb_wind_and_gust():
    m = parse_metar_json(SAMPLE_PAYLOAD)[1]
    assert m.wind_dir_deg is None  # VRB is not a bearing; raw_text keeps it
    assert m.wind_gust_kt == 41
    assert m.visibility_mi == 2.5
    assert m.metar_type == "SPECI"
    assert "VRB03G41KT" in m.raw_text


def test_parse_metar_json_skips_malformed_entries():
    payload = [
        {"icaoId": "KTYS"},  # no obsTime / rawOb
        {"obsTime": 1787356380, "rawOb": "..."},  # no station
        "not a dict",
        SAMPLE_PAYLOAD[0],
    ]
    rows = parse_metar_json(payload)
    assert len(rows) == 1
    assert rows[0].station == "KTYS"


def test_parse_metar_json_non_list_payload():
    assert parse_metar_json({"status": "error"}) == []
    assert parse_metar_json(None) == []


# ---------------------------------------------------------------------------
# fetch_metars
# ---------------------------------------------------------------------------


def test_fetch_metars_builds_request_and_parses():
    http = _client(_response(200, SAMPLE_PAYLOAD))
    rows = fetch_metars(["ktys"], hours=3, config=Config(), client=http)
    assert len(rows) == 2
    url = http.get.call_args.args[0]
    params = http.get.call_args.kwargs["params"]
    assert url == "https://aviationweather.gov/api/data/metar"
    assert params["ids"] == "KTYS"
    assert params["format"] == "json"
    assert params["hours"] == "3"
    assert "date" not in params


def test_fetch_metars_fractional_hours_round_up():
    http = _client(_response(200, []))
    fetch_metars(["OMAA"], hours=2.4, config=Config(), client=http)
    assert http.get.call_args.kwargs["params"]["hours"] == "3"


def test_fetch_metars_date_anchor_iso_z():
    http = _client(_response(200, []))
    fetch_metars(
        ["OMAA", "OMDB"],
        hours=3,
        date=datetime(2026, 8, 10, 12, 0, tzinfo=UTC),
        config=Config(),
        client=http,
    )
    params = http.get.call_args.kwargs["params"]
    assert params["ids"] == "OMAA,OMDB"
    assert params["date"] == "2026-08-10T12:00:00Z"


def test_fetch_metars_error_payload_raises():
    http = _client(_response(200, {"status": "error", "error": "Data is available for up to 30 days for date"}))
    with pytest.raises(MetarError, match="30 days"):
        fetch_metars(["OMAA"], hours=3, config=Config(), client=http)


def test_fetch_metars_http_error_raises():
    http = _client(_response(503))
    with pytest.raises(MetarError, match="503"):
        fetch_metars(["OMAA"], hours=3, config=Config(), client=http)


def test_fetch_metars_no_stations_raises():
    with pytest.raises(MetarError, match="no stations"):
        fetch_metars([" "], hours=3, config=Config(), client=_client())


# ---------------------------------------------------------------------------
# metar_window
# ---------------------------------------------------------------------------


def test_metar_window_centers_on_event():
    now = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
    event = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
    anchor, hours = metar_window(event, 3.0, now=now)
    assert anchor == event + timedelta(hours=1.5)
    assert hours == 3.0


def test_metar_window_clamps_anchor_to_now_and_widens_lookback():
    now = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
    event = now - timedelta(minutes=30)  # half-window would reach the future
    anchor, hours = metar_window(event, 3.0, now=now)
    assert anchor == now
    # window start must still reach event - 1.5h
    assert anchor - timedelta(hours=hours) == event - timedelta(hours=1.5)


def test_metar_window_naive_event_treated_as_utc():
    now = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
    anchor, hours = metar_window(datetime(2026, 8, 10, 12, 0), 3.0, now=now)
    assert anchor == datetime(2026, 8, 10, 13, 30, tzinfo=UTC)
    assert hours == 3.0


# ---------------------------------------------------------------------------
# storage round-trip + stored_metars_near
# ---------------------------------------------------------------------------


def _store(db, metars):
    return db.upsert_metars(dataclasses.asdict(m) for m in metars)


def test_upsert_metars_dedupes_by_station_and_obs_time(tmp_path):
    rows = parse_metar_json(SAMPLE_PAYLOAD)
    with Database(tmp_path / "wx.db") as db:
        assert _store(db, rows) == 2
        assert _store(db, rows) == 2  # re-writing the same observations
        count = db.conn.execute("SELECT COUNT(*) AS n FROM metars").fetchone()["n"]
        assert count == 2


def test_stored_metars_near_window_and_order(tmp_path):
    base = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
    metars = [
        Metar(station="OMAA", obs_time=(base + timedelta(hours=dt)).isoformat(), metar_type="METAR", raw_text=f"M{i}")
        for i, dt in enumerate([-3.0, -1.0, 0.5, 1.0, 4.0])
    ]
    with Database(tmp_path / "wx.db") as db:
        _store(db, metars)
        got = stored_metars_near(db, "omaa", base.isoformat(), window_hours=3.0)
    # only the observations within +/- 1.5 h, oldest first
    assert [r["raw_text"] for r in got] == ["M1", "M2", "M3"]


def test_stored_metars_near_bad_event_time(tmp_path):
    with Database(tmp_path / "wx.db") as db:
        assert stored_metars_near(db, "OMAA", "not-a-time", window_hours=3.0) == []


# ---------------------------------------------------------------------------
# fetch_flight_endpoint_metars
# ---------------------------------------------------------------------------


def _flight(now):
    takeoff = now - timedelta(days=2)
    return {
        "origin_icao": "OMAA",
        "takeoff_time": takeoff.isoformat(),
        "destination_icao": "OMDB",
        "landing_time": (takeoff + timedelta(hours=1)).isoformat(),
    }


def _endpoint_payload(station, event):
    return [
        {
            "icaoId": station,
            "obsTime": int(event.timestamp()),
            "metarType": "METAR",
            "rawOb": f"METAR {station} 101200Z 31015KT CAVOK 44/13 Q0997",
            "fltCat": "VFR",
        }
    ]


def test_fetch_flight_endpoint_metars_stores_both_endpoints(tmp_path):
    now = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
    flight = _flight(now)
    takeoff = datetime.fromisoformat(flight["takeoff_time"])
    landing = datetime.fromisoformat(flight["landing_time"])
    http = _client(
        _response(200, _endpoint_payload("OMAA", takeoff)),
        _response(200, _endpoint_payload("OMDB", landing)),
    )
    config = Config(metar_rate_limit_secs=0.0)
    with Database(tmp_path / "wx.db") as db:
        stored, warnings = fetch_flight_endpoint_metars(db, config, flight, client=http, now=now)
        assert stored == 2
        assert warnings == []
        assert http.get.call_count == 2
        stations = {r["station"] for r in db.conn.execute("SELECT station FROM metars").fetchall()}
    assert stations == {"OMAA", "OMDB"}


def test_fetch_flight_endpoint_metars_skips_cached_windows(tmp_path):
    now = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
    flight = _flight(now)
    takeoff = datetime.fromisoformat(flight["takeoff_time"])
    landing = datetime.fromisoformat(flight["landing_time"])
    config = Config(metar_rate_limit_secs=0.0, metar_min_cached_obs=2)
    with Database(tmp_path / "wx.db") as db:
        # Pre-seed two observations inside each endpoint's window.
        for station, event in (("OMAA", takeoff), ("OMDB", landing)):
            _store(
                db,
                [
                    Metar(
                        station=station,
                        obs_time=(event + timedelta(minutes=m)).isoformat(),
                        metar_type="METAR",
                        raw_text="X",
                    )
                    for m in (-30, 30)
                ],
            )
        http = _client()
        stored, warnings = fetch_flight_endpoint_metars(db, config, flight, client=http, now=now)
    assert stored == 0
    assert warnings == []
    assert http.get.call_count == 0  # METAR history is immutable; cache wins


def test_fetch_flight_endpoint_metars_skips_beyond_api_retention(tmp_path):
    now = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
    old = now - timedelta(days=45)
    flight = {
        "origin_icao": "KTYS",
        "takeoff_time": old.isoformat(),
        "destination_icao": None,
        "landing_time": None,
    }
    http = _client()
    config = Config(metar_rate_limit_secs=0.0)
    with Database(tmp_path / "wx.db") as db:
        stored, warnings = fetch_flight_endpoint_metars(db, config, flight, client=http, now=now)
    assert stored == 0
    assert http.get.call_count == 0
    assert len(warnings) == 1
    assert "30-day history window" in warnings[0]


def test_fetch_flight_endpoint_metars_collects_api_errors(tmp_path):
    now = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
    flight = _flight(now)
    http = _client(
        _response(200, {"status": "error", "error": "boom"}),
        _response(200, _endpoint_payload("OMDB", datetime.fromisoformat(flight["landing_time"]))),
    )
    config = Config(metar_rate_limit_secs=0.0)
    with Database(tmp_path / "wx.db") as db:
        stored, warnings = fetch_flight_endpoint_metars(db, config, flight, client=http, now=now)
    assert stored == 1  # the destination endpoint still landed
    assert len(warnings) == 1
    assert "origin OMAA" in warnings[0]
    assert "boom" in warnings[0]
