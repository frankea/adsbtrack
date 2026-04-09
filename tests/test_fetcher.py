"""Tests for adsbtrack.fetcher -- credential loading and utility functions."""

import json
import os
from datetime import date
from unittest.mock import patch

import pytest

from adsbtrack.config import Config
from adsbtrack.fetcher import _load_opensky_credentials, build_url, date_range

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
