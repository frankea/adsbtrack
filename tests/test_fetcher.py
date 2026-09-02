"""Tests for adsbtrack.fetcher -- credential loading, OpenSky OAuth, and utility functions."""

import json
import os
from datetime import UTC, date, datetime, timedelta
from unittest.mock import patch

import httpx
import pytest

from adsbtrack.config import Config
from adsbtrack.db import Database
from adsbtrack.fetcher import (
    _build_headers,
    _load_opensky_credentials,
    _opensky_path_to_readsb,
    _OpenSkyAuth,
    build_url,
    date_range,
    fetch_traces_opensky,
    opensky_credentials_available,
)

# ---------------------------------------------------------------------------
# _load_opensky_credentials
# ---------------------------------------------------------------------------


def test_load_credentials_from_env_vars():
    """Should prefer environment variables over credentials file."""
    config = Config()
    env = {
        "OPENSKY_CLIENT_ID": "test_user",
        "OPENSKY_CLIENT_SECRET": "test_pass",
    }
    with patch.dict(os.environ, env, clear=False):
        user, passwd = _load_opensky_credentials(config)
    assert user == "test_user"
    assert passwd == "test_pass"


def test_load_credentials_from_file(tmp_path):
    """Should fall back to credentials.json when env vars are missing."""
    creds_file = tmp_path / "credentials.json"
    creds_file.write_text(
        json.dumps(
            {
                "clientId": "file_user",
                "clientSecret": "file_pass",
            }
        )
    )
    config = Config(credentials_path=creds_file)

    # Ensure env vars are NOT set
    env_cleared = {
        "OPENSKY_CLIENT_ID": "",
        "OPENSKY_CLIENT_SECRET": "",
    }
    with patch.dict(os.environ, env_cleared, clear=False):
        user, passwd = _load_opensky_credentials(config)
    assert user == "file_user"
    assert passwd == "file_pass"


def test_load_credentials_env_takes_priority(tmp_path):
    """Env vars should be preferred even when credentials file exists."""
    creds_file = tmp_path / "credentials.json"
    creds_file.write_text(
        json.dumps(
            {
                "clientId": "file_user",
                "clientSecret": "file_pass",
            }
        )
    )
    config = Config(credentials_path=creds_file)

    env = {
        "OPENSKY_CLIENT_ID": "env_user",
        "OPENSKY_CLIENT_SECRET": "env_pass",
    }
    with patch.dict(os.environ, env, clear=False):
        user, passwd = _load_opensky_credentials(config)
    assert user == "env_user"
    assert passwd == "env_pass"


def test_load_credentials_raises_when_neither_available(tmp_path):
    """Should raise RuntimeError when no credentials are available."""
    config = Config(credentials_path=tmp_path / "nonexistent.json")

    env_cleared = {
        "OPENSKY_CLIENT_ID": "",
        "OPENSKY_CLIENT_SECRET": "",
    }
    with (
        patch.dict(os.environ, env_cleared, clear=False),
        pytest.raises(RuntimeError, match="OpenSky credentials not found"),
    ):
        _load_opensky_credentials(config)


def test_load_credentials_partial_env_falls_to_file(tmp_path):
    """If only one env var is set, should fall back to file."""
    creds_file = tmp_path / "credentials.json"
    creds_file.write_text(
        json.dumps(
            {
                "clientId": "file_user",
                "clientSecret": "file_pass",
            }
        )
    )
    config = Config(credentials_path=creds_file)

    # Only client ID set, no secret
    env = {
        "OPENSKY_CLIENT_ID": "env_user",
        "OPENSKY_CLIENT_SECRET": "",
    }
    with patch.dict(os.environ, env, clear=False):
        user, passwd = _load_opensky_credentials(config)
    assert user == "file_user"
    assert passwd == "file_pass"


# ---------------------------------------------------------------------------
# build_url
# ---------------------------------------------------------------------------


def test_build_url_format():
    url = build_url(
        "https://globe.adsbexchange.com/globe_history",
        "a66ad3",
        date(2024, 6, 15),
    )
    expected = "https://globe.adsbexchange.com/globe_history/2024/06/15/traces/d3/trace_full_a66ad3.json"
    assert url == expected


def test_build_url_last_two_chars():
    """The URL path should use the last 2 characters of the hex code."""
    url = build_url("https://example.com/history", "abc123", date(2024, 1, 5))
    assert "/traces/23/" in url
    assert "trace_full_abc123.json" in url


def test_build_url_zero_padded_date():
    """Month and day should be zero-padded."""
    url = build_url("https://example.com", "aaaaaa", date(2024, 1, 5))
    assert "/2024/01/05/" in url


# ---------------------------------------------------------------------------
# date_range
# ---------------------------------------------------------------------------


def test_date_range_single_day():
    result = date_range(date(2024, 6, 15), date(2024, 6, 15))
    assert result == [date(2024, 6, 15)]


def test_date_range_multiple_days():
    result = date_range(date(2024, 6, 13), date(2024, 6, 16))
    assert result == [
        date(2024, 6, 13),
        date(2024, 6, 14),
        date(2024, 6, 15),
        date(2024, 6, 16),
    ]


def test_date_range_empty():
    """Start after end should produce an empty list."""
    result = date_range(date(2024, 6, 20), date(2024, 6, 15))
    assert result == []


def test_date_range_across_months():
    result = date_range(date(2024, 1, 30), date(2024, 2, 2))
    assert len(result) == 4
    assert result[0] == date(2024, 1, 30)
    assert result[-1] == date(2024, 2, 2)


# ---------------------------------------------------------------------------
# opensky_credentials_available
# ---------------------------------------------------------------------------

_ENV_CLEARED = {"OPENSKY_CLIENT_ID": "", "OPENSKY_CLIENT_SECRET": ""}


def test_credentials_available_from_env():
    config = Config()
    env = {"OPENSKY_CLIENT_ID": "cid", "OPENSKY_CLIENT_SECRET": "sec"}
    with patch.dict(os.environ, env, clear=False):
        assert opensky_credentials_available(config) is True


def test_credentials_available_false_when_missing(tmp_path):
    config = Config(credentials_path=tmp_path / "nonexistent.json")
    with patch.dict(os.environ, _ENV_CLEARED, clear=False):
        assert opensky_credentials_available(config) is False


def test_credentials_available_false_on_malformed_file(tmp_path):
    creds_file = tmp_path / "credentials.json"
    creds_file.write_text("not json{")
    config = Config(credentials_path=creds_file)
    with patch.dict(os.environ, _ENV_CLEARED, clear=False):
        assert opensky_credentials_available(config) is False


def test_credentials_available_false_on_missing_keys(tmp_path):
    creds_file = tmp_path / "credentials.json"
    creds_file.write_text(json.dumps({"airframesApiKey": "x"}))
    config = Config(credentials_path=creds_file)
    with patch.dict(os.environ, _ENV_CLEARED, clear=False):
        assert opensky_credentials_available(config) is False


def test_credentials_available_false_on_empty_values(tmp_path):
    creds_file = tmp_path / "credentials.json"
    creds_file.write_text(json.dumps({"clientId": "", "clientSecret": ""}))
    config = Config(credentials_path=creds_file)
    with patch.dict(os.environ, _ENV_CLEARED, clear=False):
        assert opensky_credentials_available(config) is False


# ---------------------------------------------------------------------------
# _OpenSkyAuth (OAuth2 client-credentials flow, hermetic via MockTransport)
# ---------------------------------------------------------------------------

TOKEN_URL = "https://auth.test/token"
API_URL = "https://api.test/api"


def _token_json(token: str = "tok-1", expires_in: int = 1800) -> dict:
    return {"access_token": token, "expires_in": expires_in, "token_type": "Bearer"}


def test_auth_fetches_token_then_sends_bearer():
    requests_seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request)
        if str(request.url) == TOKEN_URL:
            body = request.read().decode()
            assert "grant_type=client_credentials" in body
            assert "client_id=cid" in body
            assert "client_secret=sec" in body
            return httpx.Response(200, json=_token_json())
        assert request.headers["Authorization"] == "Bearer tok-1"
        return httpx.Response(200, json=[])

    auth = _OpenSkyAuth("cid", "sec", TOKEN_URL, refresh_margin_secs=60.0)
    with httpx.Client(transport=httpx.MockTransport(handler), auth=auth) as client:
        assert client.get(f"{API_URL}/flights/aircraft").status_code == 200
        assert client.get(f"{API_URL}/flights/aircraft").status_code == 200

    token_calls = [r for r in requests_seen if str(r.url) == TOKEN_URL]
    assert len(token_calls) == 1  # token cached across API requests


def test_auth_refreshes_reactively_on_401():
    state = {"tokens_issued": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == TOKEN_URL:
            state["tokens_issued"] += 1
            return httpx.Response(200, json=_token_json(token=f"tok-{state['tokens_issued']}"))
        if request.headers["Authorization"] == "Bearer tok-1":
            return httpx.Response(401)  # token revoked server-side
        return httpx.Response(200, json=[])

    auth = _OpenSkyAuth("cid", "sec", TOKEN_URL, refresh_margin_secs=60.0)
    with httpx.Client(transport=httpx.MockTransport(handler), auth=auth) as client:
        assert client.get(f"{API_URL}/flights/aircraft").status_code == 200
    assert state["tokens_issued"] == 2


def test_auth_refreshes_proactively_before_expiry():
    """A token whose remaining life is inside the refresh margin must be
    re-fetched before the next request, not used until it dies mid-call."""
    state = {"tokens_issued": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == TOKEN_URL:
            state["tokens_issued"] += 1
            # expires_in shorter than the margin -> immediately stale
            return httpx.Response(200, json=_token_json(token=f"tok-{state['tokens_issued']}", expires_in=30))
        return httpx.Response(200, json=[])

    auth = _OpenSkyAuth("cid", "sec", TOKEN_URL, refresh_margin_secs=60.0)
    with httpx.Client(transport=httpx.MockTransport(handler), auth=auth) as client:
        client.get(f"{API_URL}/flights/aircraft")
        client.get(f"{API_URL}/flights/aircraft")
    assert state["tokens_issued"] == 2


def test_auth_bad_credentials_raise_clear_error():
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == TOKEN_URL
        return httpx.Response(401, json={"error": "unauthorized_client"})

    auth = _OpenSkyAuth("cid", "wrong", TOKEN_URL, refresh_margin_secs=60.0)
    with (
        httpx.Client(transport=httpx.MockTransport(handler), auth=auth) as client,
        pytest.raises(RuntimeError, match="OpenSky token request failed"),
    ):
        client.get(f"{API_URL}/flights/aircraft")


# ---------------------------------------------------------------------------
# _opensky_path_to_readsb
# ---------------------------------------------------------------------------


def test_opensky_path_to_readsb_conversion():
    path = [
        [1000, 40.0, -75.0, 304.8, 90.0, False],  # 304.8 m = 1000 ft
        [1010, None, None, 500.0, 90.0, False],  # no position -> dropped
        [1020, 40.1, -75.1, 0.0, 90.0, True],  # on ground
    ]
    data = _opensky_path_to_readsb(path, "TEST123", 1000)
    assert data["timestamp"] == 1000
    assert data["r"] == "TEST123"
    assert len(data["trace"]) == 2
    first, second = data["trace"]
    assert first[0] == 0  # time offset from start_time
    assert first[1] == 40.0 and first[2] == -75.0
    assert first[3] == 1000  # metres converted to feet
    assert second[0] == 20
    assert second[3] == "ground"


# ---------------------------------------------------------------------------
# fetch_traces_opensky (hermetic: MockTransport injected, scratch DB)
# ---------------------------------------------------------------------------

_OPENSKY_ENV = {"OPENSKY_CLIENT_ID": "cid", "OPENSKY_CLIENT_SECRET": "sec"}


@pytest.fixture
def scratch_db(tmp_path):
    with Database(tmp_path / "scratch.db") as db:
        yield db


def _opensky_test_config() -> Config:
    return Config(
        opensky_api_url=API_URL,
        opensky_token_url=TOKEN_URL,
        opensky_rate_limit=0.0,
        opensky_429_default_wait_secs=0.0,
    )


def test_fetch_traces_opensky_inserts_trace_day(scratch_db):
    day = date.today() - timedelta(days=2)
    first_seen = int(datetime(day.year, day.month, day.day, 12, tzinfo=UTC).timestamp())

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url.startswith(TOKEN_URL):
            return httpx.Response(200, json=_token_json())
        assert request.headers["Authorization"] == "Bearer tok-1"
        if "/flights/aircraft" in url:
            return httpx.Response(200, json=[{"icao24": "abc123", "firstSeen": first_seen}])
        if "/tracks/all" in url:
            return httpx.Response(
                200,
                json={
                    "icao24": "abc123",
                    "startTime": first_seen,
                    "callsign": "TEST123",
                    "path": [
                        [first_seen, 40.0, -75.0, 304.8, 90.0, False],
                        [first_seen + 10, 40.1, -75.1, 0.0, 90.0, True],
                    ],
                },
            )
        raise AssertionError(f"unexpected URL {url}")

    with patch.dict(os.environ, _OPENSKY_ENV, clear=False):
        stats = fetch_traces_opensky(
            scratch_db,
            _opensky_test_config(),
            "abc123",
            day,
            day,
            transport=httpx.MockTransport(handler),
        )

    assert stats["with_data"] == 1
    assert stats["errors"] == 0
    assert day.isoformat() in scratch_db.get_fetched_dates("abc123", source="opensky")
    rows = scratch_db.get_trace_day("abc123", day.isoformat())
    assert len(rows) == 1
    assert rows[0]["source"] == "opensky"
    assert rows[0]["point_count"] == 2


def test_fetch_traces_opensky_403_soft_fails_and_keeps_days_retryable(scratch_db):
    """A 403 (insufficient historical access) must not raise -- under
    --source all it runs in a thread. Days are logged 403, which
    get_fetched_dates treats as retryable, so a later run tries again."""
    start = date(2024, 6, 1)
    end = date(2024, 6, 3)

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url).startswith(TOKEN_URL):
            return httpx.Response(200, json=_token_json())
        return httpx.Response(403, text="access denied")

    with patch.dict(os.environ, _OPENSKY_ENV, clear=False):
        stats = fetch_traces_opensky(
            scratch_db, _opensky_test_config(), "abc123", start, end, transport=httpx.MockTransport(handler)
        )

    assert stats["errors"] == 3
    assert stats["fetched"] == 3
    assert scratch_db.get_fetched_dates("abc123", source="opensky") == set()


def test_fetch_traces_opensky_persistent_429_stops_after_one_retry(scratch_db):
    """First 429 waits and retries the window; a second 429 means the daily
    credit quota is gone, so the run stops instead of hammering the API."""
    flights_calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url).startswith(TOKEN_URL):
            return httpx.Response(200, json=_token_json())
        flights_calls["count"] += 1
        return httpx.Response(429, headers={"x-rate-limit-retry-after-seconds": "0"})

    start = date(2024, 6, 1)
    end = date(2024, 6, 4)  # 4 days -> 2 windows

    with patch.dict(os.environ, _OPENSKY_ENV, clear=False):
        stats = fetch_traces_opensky(
            scratch_db, _opensky_test_config(), "abc123", start, end, transport=httpx.MockTransport(handler)
        )

    assert flights_calls["count"] == 2  # first attempt + single retry, then bail
    assert stats["errors"] == 4  # every remaining day logged, none silently dropped
    assert stats["fetched"] == 4
    assert scratch_db.get_fetched_dates("abc123", source="opensky") == set()  # 429 stays retryable


# ---------------------------------------------------------------------------
# _build_headers
# ---------------------------------------------------------------------------


def test_build_headers_only_advertises_encodings_httpx_can_decode():
    """Every Accept-Encoding token must have an httpx decoder behind it.

    httpx silently passes through a Content-Encoding it has no decoder for
    (KeyError -> continue in Response._get_content_decoder), so advertising
    br or zstd without the brotli / zstandard packages would hand
    _parse_trace_body a compressed body it cannot parse the day the CDN
    picks one of them. Guards the httpx[brotli,zstd] extras in pyproject.
    """
    from httpx._decoders import SUPPORTED_DECODERS

    headers = _build_headers("airplaneslive", "a6a2f7")
    advertised = {tok.strip().lower() for tok in headers["accept-encoding"].split(",")}
    undecodable = advertised - set(SUPPORTED_DECODERS)
    assert not undecodable, f"Accept-Encoding advertises encodings httpx cannot decode: {sorted(undecodable)}"
