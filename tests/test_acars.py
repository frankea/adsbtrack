"""Tests for adsbtrack.acars -- airframes.io ACARS fetcher + OOOI parser."""

from datetime import UTC, date, datetime
from unittest.mock import MagicMock

import httpx
import pytest

from adsbtrack.acars import fetch_acars, parse_oooi
from adsbtrack.airframes import AirframesClient, AirframesError
from adsbtrack.db import Database

# ---------------------------------------------------------------------------
# OOOI parser
# ---------------------------------------------------------------------------


def _ref(hour: int = 8, minute: int = 0) -> datetime:
    """Reference time used to anchor HHMM values from messages. Wall-clock
    UTC. The parser uses this to decide which calendar day an HHMM falls on
    (nearest to ref_time, +/- 12h)."""
    return datetime(2026, 4, 16, hour, minute, 0, tzinfo=UTC)


def test_parse_oooi_unknown_label_returns_empty():
    """Labels outside the OOOI set (14/44/4T/H1 sublabels) return {}."""
    assert parse_oooi("_d", "arbitrary text", _ref()) == {}
    assert parse_oooi("A9", "POS report", _ref()) == {}


def test_parse_oooi_agfsr_out_off_only():
    """Real Air Canada 4T sample, in-flight (CRUISE). Has OUT and OFF,
    ---- placeholders for ON and IN."""
    text = (
        "AGFSR AC0048/15/16/YYZDEL/0824Z/740/5146.7N00934.3E/329/CRUISE"
        "/0678/0629/M56/263046/0301/100/528/0133/0151/----/----"
    )
    out = parse_oooi("4T", text, _ref(hour=8, minute=24))
    assert out.get("out") == datetime(2026, 4, 16, 1, 33, tzinfo=UTC)
    assert out.get("off") == datetime(2026, 4, 16, 1, 51, tzinfo=UTC)
    assert "on" not in out
    assert "in_" not in out


def test_parse_oooi_agfsr_all_four():
    """Synthetic 4T with all OOOI populated - flight has landed."""
    text = "AGFSR AC0870/15/16/YULCDG/1330Z/745/foo/330/ARRIVED/0132/0454/M52/284064/0301/100/551/0230/0246/1314/1329"
    out = parse_oooi("4T", text, _ref(hour=13, minute=30))
    assert out.get("out") == datetime(2026, 4, 16, 2, 30, tzinfo=UTC)
    assert out.get("off") == datetime(2026, 4, 16, 2, 46, tzinfo=UTC)
    assert out.get("on") == datetime(2026, 4, 16, 13, 14, tzinfo=UTC)
    assert out.get("in_") == datetime(2026, 4, 16, 13, 29, tzinfo=UTC)


def test_parse_oooi_agfsr_unrecognized_format_returns_empty():
    """4T without the AGFSR prefix doesn't parse - we don't know the format."""
    assert parse_oooi("4T", "NONSENSE payload", _ref()) == {}


def test_parse_oooi_keyword_space_format():
    """Classic `OUT 0830 OFF 0855 ON 1230 IN 1245` inline format on label 14."""
    text = "ACARS Downlink: OUT 0830 FUEL 100 OFF 0855 ON 1230 IN 1245 END"
    out = parse_oooi("14", text, _ref(hour=13, minute=0))
    assert out.get("out") == datetime(2026, 4, 16, 8, 30, tzinfo=UTC)
    assert out.get("off") == datetime(2026, 4, 16, 8, 55, tzinfo=UTC)
    assert out.get("on") == datetime(2026, 4, 16, 12, 30, tzinfo=UTC)
    assert out.get("in_") == datetime(2026, 4, 16, 12, 45, tzinfo=UTC)


def test_parse_oooi_keyword_slash_format():
    """Slash-delimited /OUT HHMM/OFF HHMM/ON HHMM/IN HHMM/."""
    text = "/OUT 0830/OFF 0855/ON 1230/IN 1245/"
    out = parse_oooi("44", text, _ref(hour=13))
    assert out.get("out") == datetime(2026, 4, 16, 8, 30, tzinfo=UTC)
    assert out.get("in_") == datetime(2026, 4, 16, 12, 45, tzinfo=UTC)


def test_parse_oooi_pos_label_14_returns_empty():
    """Real label-14 POS reports don't contain OOOI - return empty."""
    text = "POS\r\n16APR,0823,283,30005,N 24.038,E120.108,26067,  43,72"
    assert parse_oooi("14", text, _ref(hour=8, minute=23)) == {}


def test_parse_oooi_day_wrap_backwards():
    """If HHMM is >12h in the future of ref_time, assume it was yesterday.

    Example: flight pushed back at 23:45 UTC, ACARS message arrives at 00:15
    UTC the next day. The OUT time 2345 belongs to yesterday.
    """
    text = "OUT 2345"
    ref = datetime(2026, 4, 16, 0, 15, 0, tzinfo=UTC)
    out = parse_oooi("14", text, ref)
    assert out.get("out") == datetime(2026, 4, 15, 23, 45, tzinfo=UTC)


def test_parse_oooi_day_wrap_forwards():
    """If HHMM is >12h in the past of ref_time, assume it's tomorrow.

    Example: scheduled arrival 00:15 UTC the next day, message sent at
    22:00 UTC the previous day.
    """
    text = "IN 0015"
    ref = datetime(2026, 4, 16, 22, 0, 0, tzinfo=UTC)
    out = parse_oooi("14", text, ref)
    assert out.get("in_") == datetime(2026, 4, 17, 0, 15, tzinfo=UTC)


def test_parse_oooi_skips_dashes_for_pending_events():
    """Message with `----` for OUT returns nothing for OUT rather than
    trying to parse the dashes as a time."""
    text = "AGFSR AC0048/15/16/YYZDEL/0824Z/740/pos/329/CRUISE/x/x/x/x/x/x/x/----/----/----/----"
    assert parse_oooi("4T", text, _ref()) == {}


# ---------------------------------------------------------------------------
# AirframesClient HTTP plumbing
# ---------------------------------------------------------------------------


def _mock_response(status_code: int, json_body=None, headers: dict | None = None, text: str = ""):
    """Build a minimal httpx.Response-like mock."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.headers = headers or {}
    resp.text = text
    if json_body is not None:
        resp.json = MagicMock(return_value=json_body)
        resp.headers = {**resp.headers, "content-type": "application/json; charset=utf-8"}
    else:
        resp.json = MagicMock(side_effect=ValueError("no json"))
    return resp


def _make_client(responses):
    """AirframesClient backed by a MagicMock httpx.Client. Each GET pops
    the next response from the provided list. Throttling is disabled so
    tests don't have to sleep."""
    mock_http = MagicMock()
    mock_http.get = MagicMock(side_effect=list(responses))
    # rate_limit_per_min=0 sentinel disables throttling entirely
    return AirframesClient(api_key="test-key", client=mock_http, rate_limit_per_min=0), mock_http


def test_client_sends_api_key_header():
    client, mock_http = _make_client([_mock_response(200, {"id": 1, "tail": "N1"})])
    client.get_airframe_by_icao("ABCDEF")
    call = mock_http.get.call_args
    assert call.args[0] == "https://api.airframes.io/airframes/icao/ABCDEF"
    headers = call.kwargs.get("headers") or {}
    assert headers.get("X-API-KEY") == "test-key"


def test_client_returns_parsed_json_on_200():
    body = {"id": 14166, "tail": "A7-BCA"}
    client, _ = _make_client([_mock_response(200, body)])
    assert client.get_airframe_by_icao("06A0A5") == body


def test_client_returns_none_on_404():
    client, _ = _make_client([_mock_response(404, {"message": "Not Found"})])
    assert client.get_airframe_by_icao("XXXXXX") is None


def test_client_captures_daily_rate_limit_header():
    resp = _mock_response(200, {"id": 1}, headers={"x-ratelimit-remaining": "49999"})
    client, _ = _make_client([resp])
    client.get_airframe_by_icao("ABC")
    assert client.daily_remaining == 49999


def test_client_retries_on_500_then_succeeds():
    responses = [
        _mock_response(500, headers={}),
        _mock_response(500, headers={}),
        _mock_response(200, {"id": 1}),
    ]
    client, mock_http = _make_client(responses)
    client._sleep = MagicMock()  # skip real sleeps
    result = client.get_airframe_by_icao("ABC")
    assert result == {"id": 1}
    assert mock_http.get.call_count == 3


def test_client_retries_on_429_with_retry_after():
    responses = [
        _mock_response(429, headers={"retry-after": "2"}),
        _mock_response(200, {"id": 1}),
    ]
    client, _ = _make_client(responses)
    sleeps = []
    client._sleep = lambda s: sleeps.append(s)
    result = client.get_airframe_by_icao("ABC")
    assert result == {"id": 1}
    assert sleeps and sleeps[0] >= 2


def test_client_raises_after_max_retries():
    responses = [_mock_response(500) for _ in range(6)]
    client, _ = _make_client(responses)
    client._sleep = MagicMock()
    with pytest.raises(AirframesError):
        client.get_airframe_by_icao("ABC")


def test_client_get_flight_path():
    client, mock_http = _make_client([_mock_response(200, {"id": 5538326232, "messages": []})])
    client.get_flight(5538326232)
    assert mock_http.get.call_args.args[0] == "https://api.airframes.io/flights/5538326232"


def test_client_get_airframe_by_id_path():
    client, mock_http = _make_client([_mock_response(200, {"id": 14166, "flights": []})])
    client.get_airframe_by_id(14166)
    assert mock_http.get.call_args.args[0] == "https://api.airframes.io/airframes/14166"


# ---------------------------------------------------------------------------
# fetch_acars pipeline (icao -> airframe -> flights -> messages)
# ---------------------------------------------------------------------------


class FakeClient:
    """Stand-in for AirframesClient with deterministic canned responses.

    Instantiated with:
      icao_to_airframe: dict[icao_hex, airframe_record]
      airframe_to_flights: dict[airframe_id, airframe_record_with_flights]
      flight_to_messages: dict[flight_id, flight_record_with_messages]

    Tracks every call for assertions.
    """

    def __init__(self, icao_to_airframe=None, airframe_to_flights=None, flight_to_messages=None):
        self.icao_to_airframe = icao_to_airframe or {}
        self.airframe_to_flights = airframe_to_flights or {}
        self.flight_to_messages = flight_to_messages or {}
        self.calls: list[tuple[str, object]] = []

    def get_airframe_by_icao(self, icao):
        self.calls.append(("icao", icao.upper()))
        return self.icao_to_airframe.get(icao.upper())

    def get_airframe_by_id(self, airframe_id):
        self.calls.append(("airframe", airframe_id))
        return self.airframe_to_flights.get(airframe_id)

    def get_flight(self, flight_id):
        self.calls.append(("flight", flight_id))
        return self.flight_to_messages.get(flight_id)


def _seed_registry(db, icao, registration):
    db.conn.execute(
        "INSERT OR REPLACE INTO aircraft_registry (icao, registration, last_updated) VALUES (?, ?, ?)",
        (icao, registration, "2026-04-16T00:00:00Z"),
    )
    db.commit()


@pytest.fixture
def dbf(tmp_path):
    db = Database(tmp_path / "t.db")
    yield db
    db.close()


def test_fetch_acars_resolves_and_caches_airframes_id(dbf):
    _seed_registry(dbf, "06a0a5", "A7-BCA")
    fake = FakeClient(
        icao_to_airframe={"06A0A5": {"id": 14166, "tail": "A7-BCA", "icaoType": "A320"}},
        airframe_to_flights={14166: {"flights": []}},
    )
    fetch_acars(dbf, fake, "06A0A5", start_date=date(2026, 1, 1), end_date=date(2026, 4, 16))
    # The icao lookup should have cached the airframes_id on the registry
    row = dbf.conn.execute("SELECT airframes_id FROM aircraft_registry WHERE icao='06a0a5'").fetchone()
    assert row["airframes_id"] == 14166

    # Running again must skip the icao lookup because the id is cached
    fake.calls.clear()
    fetch_acars(dbf, fake, "06A0A5", start_date=date(2026, 1, 1), end_date=date(2026, 4, 16))
    assert ("icao", "06A0A5") not in fake.calls
    # But still fetches the airframe record for flights list
    assert ("airframe", 14166) in fake.calls


def test_fetch_acars_filters_flights_by_date(dbf):
    _seed_registry(dbf, "06a0a5", "A7-BCA")
    flights = [
        {"id": 1, "createdAt": "2026-01-05T00:00:00Z", "flight": "QR1"},
        {"id": 2, "createdAt": "2026-04-01T00:00:00Z", "flight": "QR2"},
        {"id": 3, "createdAt": "2026-04-15T00:00:00Z", "flight": "QR3"},
    ]
    fake = FakeClient(
        icao_to_airframe={"06A0A5": {"id": 14166, "tail": "A7-BCA"}},
        airframe_to_flights={14166: {"flights": flights}},
        flight_to_messages={
            2: {"id": 2, "flight": "QR2", "messages": []},
            3: {"id": 3, "flight": "QR3", "messages": []},
        },
    )
    fetch_acars(dbf, fake, "06A0A5", start_date=date(2026, 3, 1), end_date=date(2026, 4, 20))
    # Only flights 2 and 3 fall in [2026-03-01, 2026-04-20]
    fetched_flight_ids = [fid for kind, fid in fake.calls if kind == "flight"]
    assert fetched_flight_ids == [2, 3]


def test_fetch_acars_skips_already_fetched_flights(dbf):
    _seed_registry(dbf, "06a0a5", "A7-BCA")
    # Pre-seed acars_flights table with flight 1 already fetched
    dbf.upsert_acars_flight(
        {
            "flight_id": 1,
            "airframe_id": 14166,
            "icao": "06a0a5",
            "registration": "A7-BCA",
            "flight_number": None,
            "flight_iata": None,
            "flight_icao": None,
            "status": None,
            "departing_airport": None,
            "destination_airport": None,
            "departure_time_scheduled": None,
            "departure_time_actual": None,
            "arrival_time_scheduled": None,
            "arrival_time_actual": None,
            "first_seen": None,
            "last_seen": None,
            "message_count": 10,
        }
    )
    dbf.commit()

    fake = FakeClient(
        icao_to_airframe={"06A0A5": {"id": 14166, "tail": "A7-BCA"}},
        airframe_to_flights={
            14166: {
                "flights": [
                    {"id": 1, "createdAt": "2026-04-10T00:00:00Z"},
                    {"id": 2, "createdAt": "2026-04-12T00:00:00Z"},
                ]
            }
        },
        flight_to_messages={2: {"id": 2, "messages": []}},
    )
    fetch_acars(dbf, fake, "06A0A5", start_date=date(2026, 4, 1), end_date=date(2026, 4, 16))
    fetched = [fid for kind, fid in fake.calls if kind == "flight"]
    assert fetched == [2]  # flight 1 skipped


def test_fetch_acars_inserts_messages_with_dedup(dbf):
    _seed_registry(dbf, "06a0a5", "A7-BCA")
    msg = {
        "id": 6503832431,
        "uuid": "abc-123",
        "timestamp": "2026-03-29T13:45:35.138Z",
        "tail": "A7-BCA",
        "sourceType": "aero-acars",
        "linkDirection": "uplink",
        "fromHex": "90",
        "toHex": "06A0A5",
        "frequency": None,
        "level": None,
        "channel": None,
        "mode": "2",
        "label": "H1",
        "blockId": "P",
        "messageNumber": None,
        "ack": "!",
        "flightNumber": None,
        "text": "- #EIEM13R0",
        "data": None,
        "latitude": None,
        "longitude": None,
        "altitude": None,
        "departingAirport": None,
        "destinationAirport": None,
    }
    fake = FakeClient(
        icao_to_airframe={"06A0A5": {"id": 14166, "tail": "A7-BCA"}},
        airframe_to_flights={
            14166: {
                "flights": [
                    {"id": 42, "createdAt": "2026-03-29T00:00:00Z"},
                ]
            }
        },
        flight_to_messages={
            42: {
                "id": 42,
                "flight": "QR3255",
                "flightIata": None,
                "flightIcao": None,
                "status": "arrived",
                "departingAirport": "OTHH",
                "destinationAirport": "EGLL",
                "departureTimeScheduled": None,
                "departureTimeActual": "2026-03-29T08:00:00Z",
                "arrivalTimeScheduled": None,
                "arrivalTimeActual": "2026-03-29T14:00:00Z",
                "createdAt": "2026-03-29T00:00:00Z",
                "updatedAt": "2026-03-29T14:10:00Z",
                "messages": [msg, msg],  # duplicate in response - dedup on UNIQUE
            }
        },
    )
    fetch_acars(dbf, fake, "06A0A5", start_date=date(2026, 3, 1), end_date=date(2026, 4, 1))

    # Exactly one row stored despite the duplicate
    count = dbf.conn.execute("SELECT COUNT(*) AS c FROM acars_messages").fetchone()["c"]
    assert count == 1
    stored = dbf.conn.execute("SELECT * FROM acars_messages WHERE airframes_id = ?", (msg["id"],)).fetchone()
    assert stored["icao"] == "06a0a5"
    assert stored["flight_id"] == 42
    assert stored["label"] == "H1"
    assert stored["registration"] == "A7-BCA"

    # acars_flights row populated with flight metadata
    frow = dbf.conn.execute("SELECT * FROM acars_flights WHERE flight_id = 42").fetchone()
    assert frow["airframe_id"] == 14166
    assert frow["flight_number"] == "QR3255"
    assert frow["status"] == "arrived"
    assert frow["message_count"] == 2  # 2 raw messages returned by API, pre-dedup


def test_fetch_acars_returns_none_when_airframe_not_found(dbf):
    _seed_registry(dbf, "AAAAAA", "N-UNKNOWN")
    fake = FakeClient(icao_to_airframe={})  # 404
    # Must not raise, must not insert anything
    fetch_acars(dbf, fake, "AAAAAA", start_date=date(2026, 1, 1), end_date=date(2026, 4, 16))
    assert dbf.conn.execute("SELECT COUNT(*) AS c FROM acars_flights").fetchone()["c"] == 0


def test_fetch_acars_applies_oooi_to_matching_flight(dbf):
    """After fetching messages, any OOOI-bearing message whose timestamp
    falls within an adsbtrack flight's takeoff-landing window should update
    acars_out/off/on/in on that flight row."""
    from adsbtrack.models import Flight

    _seed_registry(dbf, "06a0a5", "A7-BCA")

    # An adsbtrack flight that covers the message window
    flight = Flight(
        icao="06a0a5",
        takeoff_time=datetime(2026, 3, 29, 2, 0, tzinfo=UTC),
        takeoff_lat=25.26,
        takeoff_lon=51.61,
        takeoff_date="2026-03-29",
        landing_time=datetime(2026, 3, 29, 15, 0, tzinfo=UTC),
        landing_lat=51.47,
        landing_lon=-0.45,
        landing_date="2026-03-29",
    )
    dbf.insert_flight(flight)
    dbf.commit()

    # Real 4T AGFSR shape with OUT/OFF set and ON/IN pending
    agfsr_msg = {
        "id": 999,
        "uuid": "u",
        "timestamp": "2026-03-29T09:00:00Z",
        "label": "4T",
        "text": "AGFSR AC0048/15/16/YYZDEL/0900Z/740/pos/329/CRUISE/0/0/M56/0/0/0/0/0133/0151/----/----",
        "tail": "A7-BCA",
        "blockId": "1",
        "ack": "!",
        "mode": "2",
        "sourceType": "acars",
        "linkDirection": "downlink",
        "fromHex": "06A0A5",
        "toHex": "00",
        "frequency": None,
        "level": None,
        "channel": None,
        "messageNumber": None,
        "flightNumber": None,
        "data": None,
        "latitude": None,
        "longitude": None,
        "altitude": None,
        "departingAirport": None,
        "destinationAirport": None,
    }
    fake = FakeClient(
        icao_to_airframe={"06A0A5": {"id": 14166, "tail": "A7-BCA"}},
        airframe_to_flights={
            14166: {
                "flights": [
                    {"id": 100, "createdAt": "2026-03-29T02:00:00Z"},
                ]
            }
        },
        flight_to_messages={100: {"id": 100, "messages": [agfsr_msg]}},
    )
    fetch_acars(dbf, fake, "06A0A5", start_date=date(2026, 3, 1), end_date=date(2026, 4, 1))

    row = dbf.conn.execute(
        "SELECT acars_out, acars_off, acars_on, acars_in FROM flights WHERE icao='06a0a5'"
    ).fetchone()
    assert row["acars_out"] is not None
    assert "01:33" in row["acars_out"]
    assert row["acars_off"] is not None
    assert "01:51" in row["acars_off"]
    # ---- in raw means these stay NULL
    assert row["acars_on"] is None
    assert row["acars_in"] is None
