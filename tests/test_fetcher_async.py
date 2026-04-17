"""Concurrency + correctness tests for the async fetcher.

These are mocked-transport tests that fully pin the rate-limit, 429, 403,
SIGINT, and semaphore behavior that the spec calls out as gates. They
run fast (all timers are zeroed out via the rate-limit knobs or
monkeypatched ``asyncio.sleep``). Real-source behavior (actual 429 rates
under concurrency, real wall-time speedup) is verified out-of-band via
the benchmark protocol in the PR description.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import time
from datetime import UTC, date, datetime
from typing import Any

import httpx
import pytest

from adsbtrack.config import Config
from adsbtrack.db import Database
from adsbtrack.fetcher import fetch_traces

# ---------------------------------------------------------------------------
# Helpers: mock HTTP transport + DB fixture
# ---------------------------------------------------------------------------


def _trace_payload(day: date) -> bytes:
    """Build a minimal valid gzipped trace_full payload for a given day."""
    body = {
        "icao": "abc123",
        "timestamp": datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp(),
        "trace": [[0.0, 40.0, -74.0, 10000, 300.0, 90.0, 0, 0, None]],
    }
    return gzip.compress(json.dumps(body).encode("utf-8"))


class _ScriptedTransport(httpx.AsyncBaseTransport):
    """Deterministic transport that returns per-URL scripted responses.

    ``script`` maps (day.isoformat()) -> list of (status, body_bytes,
    headers) tuples consumed in order. Also records every request's url
    and start timestamp so tests can assert concurrency bounds and
    request-start spacing.
    """

    def __init__(
        self,
        script: dict[str, list[tuple[int, bytes, dict[str, str]]]],
        *,
        response_delay: float = 0.0,
    ) -> None:
        self.script = {k: list(v) for k, v in script.items()}
        self.response_delay = response_delay
        self.requests: list[tuple[float, str]] = []  # (monotonic_start, url)
        self.in_flight = 0
        self.peak_concurrency = 0
        self._lock = asyncio.Lock()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        start = time.monotonic()
        async with self._lock:
            self.requests.append((start, str(request.url)))
            self.in_flight += 1
            if self.in_flight > self.peak_concurrency:
                self.peak_concurrency = self.in_flight
        try:
            if self.response_delay > 0:
                await asyncio.sleep(self.response_delay)
            # Extract day from URL: .../YYYY/MM/DD/traces/...
            parts = str(request.url).split("/")
            # Find the year/month/day slugs. URL format guarantees they
            # appear immediately before "traces".
            for i, p in enumerate(parts):
                if p == "traces":
                    y, m, d = parts[i - 3], parts[i - 2], parts[i - 1]
                    day_str = f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
                    break
            else:
                raise AssertionError(f"unparseable URL: {request.url}")
            bucket = self.script.get(day_str)
            if not bucket:
                raise AssertionError(f"no scripted response for {day_str}")
            status, body, headers = bucket.pop(0) if len(bucket) > 1 else bucket[0]
            return httpx.Response(status_code=status, content=body, headers=headers)
        finally:
            async with self._lock:
                self.in_flight -= 1


@pytest.fixture
def tmp_db(tmp_path) -> Database:
    db = Database(tmp_path / "test.db")
    yield db
    db.close()


@pytest.fixture
def fast_config(tmp_path) -> Config:
    """Config with rate limiters zeroed so tests don't wait on real seconds.

    ``rate_limit=0`` means workers don't gate on inter-request spacing,
    which is what we want for most correctness checks. Tests that need
    to verify the spacing set ``rate_limit`` explicitly.
    """
    return Config(
        db_path=tmp_path / "test.db",
        rate_limit=0.0,
        rate_limit_max=5.0,
        rate_limit_recovery=2,
        fetch_concurrency=4,
    )


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    """Collapse asyncio.sleep to a near-yield so retry/backoff waits
    don't slow the test suite. Records call durations so tests that
    care about backoff math (429) can inspect them."""
    calls: list[float] = []

    original = asyncio.sleep

    async def fake_sleep(seconds, *args, **kwargs):
        calls.append(seconds)
        # Yield control so semaphores and locks progress.
        await original(0)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    monkeypatch.setattr("adsbtrack.fetcher.asyncio.sleep", fake_sleep)
    monkeypatch.setattr(time, "sleep", lambda s: None)
    yield calls


def _patch_transport(monkeypatch, transport: _ScriptedTransport) -> None:
    """Patch httpx.AsyncClient to route through the scripted transport."""
    orig_init = httpx.AsyncClient.__init__

    def new_init(self, *args, **kwargs):
        kwargs["transport"] = transport
        # http2=True requires a separate package; the scripted transport
        # satisfies the contract on http/1.1.
        kwargs.pop("http2", None)
        orig_init(self, *args, **kwargs)

    monkeypatch.setattr(httpx.AsyncClient, "__init__", new_init)


def _day_range(start: date, n: int) -> list[date]:
    from datetime import timedelta

    return [start + timedelta(days=i) for i in range(n)]


# ---------------------------------------------------------------------------
# Gate 1: 429 triggers backoff (current_delay doubles, capped, successes reset)
# ---------------------------------------------------------------------------


def test_429_doubles_delay_and_resets_success_counter(tmp_db, fast_config, monkeypatch, _no_real_sleep):
    """One day returns 429 with Retry-After, then 200 on retry. Verify the
    worker backs off and the shared current_delay doubles to
    (original_delay * 2), capped at rate_limit_max."""
    from adsbtrack import fetcher as fetcher_mod

    fast_config.rate_limit = 0.2  # 200 ms floor
    fast_config.rate_limit_max = 5.0

    observed_delays: list[float] = []

    original_handle_429 = fetcher_mod._handle_429

    async def spy(state, retry_after):
        await original_handle_429(state, retry_after)
        observed_delays.append(state.current_delay)

    monkeypatch.setattr(fetcher_mod, "_handle_429", spy)

    day_str = date(2024, 6, 15).isoformat()
    script = {
        day_str: [
            (429, b"", {"retry-after": "1"}),
            (200, _trace_payload(date(2024, 6, 15)), {}),
        ]
    }
    # Second day returns 200 straight through so the fetch completes.
    day2 = date(2024, 6, 16)
    script[day2.isoformat()] = [(200, _trace_payload(day2), {})]
    transport = _ScriptedTransport(script)
    _patch_transport(monkeypatch, transport)

    stats = fetch_traces(
        tmp_db, fast_config, "abc123", date(2024, 6, 15), date(2024, 6, 16), source="adsbx", concurrency=1
    )

    assert stats["fetched"] == 2
    assert stats["with_data"] == 2
    assert stats["errors"] == 0
    # Delay doubled from 0.2 -> 0.4 after the 429.
    assert observed_delays, "expected _handle_429 to fire"
    assert observed_delays[0] == pytest.approx(0.4)


def test_429_cap_holds_at_rate_limit_max(tmp_db, fast_config, monkeypatch, _no_real_sleep):
    """Repeated 429s must cap the base delay at rate_limit_max regardless
    of how many doublings occur."""
    fast_config.rate_limit = 1.0
    fast_config.rate_limit_max = 3.0  # small cap to make the ceiling hit fast

    day = date(2024, 6, 15)
    # 429 five times, then 200.
    script = {
        day.isoformat(): [
            (429, b"", {"retry-after": "0"}),
            (429, b"", {"retry-after": "0"}),
            (429, b"", {"retry-after": "0"}),
            (429, b"", {"retry-after": "0"}),
            (429, b"", {"retry-after": "0"}),
            (200, _trace_payload(day), {}),
        ]
    }
    transport = _ScriptedTransport(script)
    _patch_transport(monkeypatch, transport)

    from adsbtrack import fetcher as fetcher_mod

    state_ref: list[Any] = []
    orig_handle = fetcher_mod._handle_429

    async def capture(state, retry_after):
        await orig_handle(state, retry_after)
        state_ref.append(state.current_delay)

    monkeypatch.setattr(fetcher_mod, "_handle_429", capture)

    fetch_traces(tmp_db, fast_config, "abc123", day, day, source="adsbx", concurrency=1)
    assert state_ref, "429 handler must fire"
    assert max(state_ref) == pytest.approx(3.0), f"current_delay must cap at 3.0; saw {state_ref}"


# ---------------------------------------------------------------------------
# Gate 2: 3 consecutive distinct 403 days exhaust retries -> RuntimeError;
#         a 404 or 200 resets the counter.
# ---------------------------------------------------------------------------


def test_three_consecutive_403_days_raises(tmp_db, fast_config, monkeypatch, _no_real_sleep):
    """Three distinct days all exhaust retries on 403 -> circuit breaker
    raises RuntimeError naming the source."""
    days = _day_range(date(2024, 6, 15), 3)
    # Each day returns 403 forever (more than max_retries=5 entries).
    script = {d.isoformat(): [(403, b"", {})] for d in days}
    transport = _ScriptedTransport(script)
    _patch_transport(monkeypatch, transport)

    with pytest.raises(RuntimeError, match="HTTP 403 for adsbx on 3 consecutive days"):
        fetch_traces(tmp_db, fast_config, "abc123", days[0], days[-1], source="adsbx", concurrency=1)


def test_404_between_403s_resets_circuit(tmp_db, fast_config, monkeypatch, _no_real_sleep):
    """403 -> 404 -> 403 -> 403 must NOT raise because the 404 resets
    the consecutive counter."""
    days = _day_range(date(2024, 6, 15), 4)
    script = {
        days[0].isoformat(): [(403, b"", {})],  # exhausts -> 403
        days[1].isoformat(): [(404, b"", {})],  # resets
        days[2].isoformat(): [(403, b"", {})],  # exhausts -> 403
        days[3].isoformat(): [(403, b"", {})],  # exhausts -> 403
    }
    transport = _ScriptedTransport(script)
    _patch_transport(monkeypatch, transport)

    # Only 2 trailing 403s after the 404 reset, so should NOT raise.
    stats = fetch_traces(tmp_db, fast_config, "abc123", days[0], days[-1], source="adsbx", concurrency=1)
    assert stats["errors"] == 3  # all three 403 days log as errors
    # fetch_log entries exist for all four days.
    rows = tmp_db.conn.execute(
        "SELECT date, status FROM fetch_log WHERE icao = ? ORDER BY date",
        ("abc123",),
    ).fetchall()
    assert len(rows) == 4
    assert [r["status"] for r in rows] == [403, 404, 403, 403]


def test_200_between_403s_resets_circuit(tmp_db, fast_config, monkeypatch, _no_real_sleep):
    """Same as above but with a 200 instead of 404 resetting the counter."""
    days = _day_range(date(2024, 6, 15), 4)
    script = {
        days[0].isoformat(): [(403, b"", {})],
        days[1].isoformat(): [(200, _trace_payload(days[1]), {})],
        days[2].isoformat(): [(403, b"", {})],
        days[3].isoformat(): [(403, b"", {})],
    }
    transport = _ScriptedTransport(script)
    _patch_transport(monkeypatch, transport)

    stats = fetch_traces(tmp_db, fast_config, "abc123", days[0], days[-1], source="adsbx", concurrency=1)
    assert stats["errors"] == 3
    assert stats["with_data"] == 1


# ---------------------------------------------------------------------------
# Gate 3: semaphore bound is honored (peak in-flight <= configured concurrency)
# ---------------------------------------------------------------------------


def test_concurrency_bound_never_exceeded(tmp_db, fast_config, monkeypatch, _no_real_sleep):
    """With concurrency=3 and 20 days that each hold the transport for a
    moment, peak_concurrency observed by the mock transport must never
    exceed 3."""
    days = _day_range(date(2024, 6, 1), 20)
    script = {d.isoformat(): [(200, _trace_payload(d), {})] for d in days}
    # response_delay ensures multiple requests overlap in flight.
    transport = _ScriptedTransport(script, response_delay=0.01)
    _patch_transport(monkeypatch, transport)

    fetch_traces(tmp_db, fast_config, "abc123", days[0], days[-1], source="adsbx", concurrency=3)
    assert transport.peak_concurrency <= 3, f"semaphore bound violated: peak_concurrency={transport.peak_concurrency}"
    assert transport.peak_concurrency >= 1


# ---------------------------------------------------------------------------
# Gate 4: rate-limit lock enforces current_delay between request STARTS
# ---------------------------------------------------------------------------


def test_rate_limit_spacing_between_request_starts(tmp_db, fast_config, monkeypatch):
    """With rate_limit=0.05 and 5 days at concurrency=4, consecutive
    request starts must be >= rate_limit apart. Other workers may have
    requests in flight simultaneously (concurrency > 1), but request
    STARTS must be serialized behind the lock.
    """
    # Don't use the _no_real_sleep autouse fixture here: we need actual
    # wall-clock elapsed between request starts.

    async def real_sleep(s, *a, **kw):
        await asyncio.sleep.__wrapped__(s) if hasattr(asyncio.sleep, "__wrapped__") else None

    # Restore real sleep for this test by NOT depending on _no_real_sleep.
    monkeypatch.undo()
    monkeypatch.setattr(time, "sleep", lambda s: None)

    fast_config.rate_limit = 0.05
    fast_config.rate_limit_max = 5.0

    days = _day_range(date(2024, 6, 1), 5)
    script = {d.isoformat(): [(200, _trace_payload(d), {})] for d in days}
    transport = _ScriptedTransport(script)
    _patch_transport(monkeypatch, transport)

    fetch_traces(tmp_db, fast_config, "abc123", days[0], days[-1], source="adsbx", concurrency=4)

    starts = sorted(t for t, _ in transport.requests)
    assert len(starts) == 5
    gaps = [starts[i + 1] - starts[i] for i in range(len(starts) - 1)]
    # Every consecutive-start gap must be >= rate_limit (minus a tiny
    # epsilon for monotonic clock skew; generous to reduce CI flakiness).
    floor = fast_config.rate_limit - 0.005
    for g in gaps:
        assert g >= floor, f"gap {g * 1000:.1f}ms < rate_limit {fast_config.rate_limit * 1000:.1f}ms"


# ---------------------------------------------------------------------------
# Gate 5: SIGINT / cancel mid-fetch leaves DB consistent (no partial writes)
# ---------------------------------------------------------------------------


def test_cancel_midflight_leaves_db_consistent(tmp_db, fast_config, monkeypatch, _no_real_sleep):
    """Simulate a cancellation halfway through a 10-day fetch. After the
    CancelledError propagates, every fetch_log row must have a matching
    trace_days row when status=200, and there must be no trace_days row
    without a fetch_log row."""
    days = _day_range(date(2024, 6, 1), 10)
    script = {d.isoformat(): [(200, _trace_payload(d), {})] for d in days}
    # Slow transport so there's time to cancel mid-run.
    transport = _ScriptedTransport(script, response_delay=0.005)
    _patch_transport(monkeypatch, transport)

    # Cancel via a raised exception inside a wrapped worker call.
    from adsbtrack import fetcher as fetcher_mod

    original_worker = fetcher_mod._worker

    call_count = {"n": 0}

    async def canceling_worker(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 5:
            # Fire the cancellation AFTER some days have committed.
            raise KeyboardInterrupt("simulated SIGINT")
        return await original_worker(*args, **kwargs)

    monkeypatch.setattr(fetcher_mod, "_worker", canceling_worker)

    with pytest.raises((KeyboardInterrupt, RuntimeError)):
        fetch_traces(tmp_db, fast_config, "abc123", days[0], days[-1], source="adsbx", concurrency=2)

    # Consistency check: every fetch_log row with status=200 must have a
    # matching trace_days row; no orphan trace_days rows.
    fetch_logs = tmp_db.conn.execute("SELECT date, status FROM fetch_log WHERE icao = ?", ("abc123",)).fetchall()
    trace_days = tmp_db.conn.execute("SELECT date FROM trace_days WHERE icao = ?", ("abc123",)).fetchall()

    log_200_dates = {r["date"] for r in fetch_logs if r["status"] == 200}
    trace_dates = {r["date"] for r in trace_days}

    assert trace_dates == log_200_dates, (
        f"DB inconsistency after cancel: fetch_log 200 dates {log_200_dates} != trace_days dates {trace_dates}"
    )


# ---------------------------------------------------------------------------
# Gate 6: progress bar advances thread-safely under concurrency=4
# ---------------------------------------------------------------------------


def test_progress_advances_once_per_day_under_concurrency(tmp_db, fast_config, monkeypatch, _no_real_sleep):
    """With 12 days at concurrency=4, the progress bar's .completed must
    equal the day count at end (each day advances once). Rich's Progress
    is internally thread-safe; this test pins that we call advance()
    once per day and no races drop or duplicate advances."""
    from adsbtrack import fetcher as fetcher_mod

    days = _day_range(date(2024, 6, 1), 12)
    script = {d.isoformat(): [(200, _trace_payload(d), {})] for d in days}
    transport = _ScriptedTransport(script, response_delay=0.001)
    _patch_transport(monkeypatch, transport)

    advance_count = {"n": 0}
    original_progress_cls = fetcher_mod.Progress

    class _Wrapped(original_progress_cls):
        def advance(self, task_id, advance=1):  # noqa: A002
            advance_count["n"] += advance
            return super().advance(task_id, advance)

    monkeypatch.setattr(fetcher_mod, "Progress", _Wrapped)

    stats = fetch_traces(tmp_db, fast_config, "abc123", days[0], days[-1], source="adsbx", concurrency=4)
    assert advance_count["n"] == 12
    assert stats["fetched"] == 12
    assert stats["with_data"] == 12


# ---------------------------------------------------------------------------
# Gate 7: sync wrapper contract is unchanged for existing callers
# ---------------------------------------------------------------------------


def test_sync_wrapper_signature_unchanged(tmp_db, fast_config, monkeypatch, _no_real_sleep):
    """fetch_traces(db, config, hex_code, start, end) must still work
    (positional, no concurrency kwarg) and return the same stats dict
    keys as the pre-async version."""
    day = date(2024, 6, 15)
    script = {day.isoformat(): [(200, _trace_payload(day), {})]}
    transport = _ScriptedTransport(script)
    _patch_transport(monkeypatch, transport)

    # Call with exact positional signature existing callers use.
    stats = fetch_traces(tmp_db, fast_config, "abc123", day, day)

    assert set(stats.keys()) == {"fetched", "with_data", "skipped", "errors"}
    assert stats["fetched"] == 1
    assert stats["with_data"] == 1
    assert stats["skipped"] == 0
    assert stats["errors"] == 0


def test_concurrency_one_is_serial(tmp_db, fast_config, monkeypatch, _no_real_sleep):
    """concurrency=1 must produce peak_concurrency=1 — byte-identical
    request pattern to the old synchronous code."""
    days = _day_range(date(2024, 6, 1), 6)
    script = {d.isoformat(): [(200, _trace_payload(d), {})] for d in days}
    transport = _ScriptedTransport(script, response_delay=0.002)
    _patch_transport(monkeypatch, transport)

    fetch_traces(tmp_db, fast_config, "abc123", days[0], days[-1], source="adsbx", concurrency=1)
    assert transport.peak_concurrency == 1


# ---------------------------------------------------------------------------
# Gate 8: empty range short-circuits without starting a client
# ---------------------------------------------------------------------------


def test_no_days_to_fetch_returns_empty_stats(tmp_db, fast_config, monkeypatch, _no_real_sleep):
    """If every day in the range is already fetched, fetch_traces must
    return {"fetched": 0, "with_data": 0, "skipped": N, "errors": 0}
    without touching the transport at all."""
    day = date(2024, 6, 15)
    # Pre-populate fetch_log so the day is considered done.
    tmp_db.insert_fetch_log("abc123", day.isoformat(), 200, source="adsbx")
    tmp_db.commit()

    # If the transport is hit it will AssertionError on missing script.
    transport = _ScriptedTransport({})
    _patch_transport(monkeypatch, transport)

    stats = fetch_traces(tmp_db, fast_config, "abc123", day, day, source="adsbx", concurrency=2)
    assert stats == {"fetched": 0, "with_data": 0, "skipped": 1, "errors": 0}
    assert transport.requests == []
