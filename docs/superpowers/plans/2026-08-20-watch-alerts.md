# Watch Command with Alert Conditions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `adsbtrack watch` fetches a hex watchlist and raises alerts on reactivation after dormancy, emergency squawks, and new spoof quarantines (GitHub issue #24).

**Architecture:** Pure alert-evaluation logic in a new leaf module `adsbtrack/watch.py` (snapshot before fetch, evaluate after - alerts are self-suppressing across runs because the next run's snapshot includes this run's data). A thin CLI command orchestrates: snapshot -> sequential per-source fetch reusing the #19/#20 helpers -> incremental extract -> evaluate -> render/webhook. No schema changes; no threads; readsb sources only (no OpenSky).

**Tech Stack:** Python 3.12+, Click, Rich, sqlite3, urllib.request (no new deps), pytest.

**Spec:** GitHub issue frankea/adsbtrack #24. Controller descope ruling: the issue's fourth condition ("never-before-seen hex in a watched allocation range") requires area-feed discovery the per-hex fetcher cannot do - out of scope for this plan; the controller notes it on the issue.

## Global Constraints

- Line length 120; regular hyphens, never em dashes, in all output and docs.
- New thresholds go in `adsbtrack/config.Config` with a comment (`watch_dormancy_days: int = 30`, `watch_webhook_timeout_secs: float = 10.0`).
- NO schema changes. If you believe you need one, STOP and report BLOCKED.
- Tests use tmp_path databases only, are hermetic (no network - webhook tests monkeypatch `urllib.request.urlopen`), and any test invoking the CLI fetch path must monkeypatch `adsbtrack.cli.fetch_traces` AND guard OpenSky state per the existing `_capture_fetch_calls` pattern in tests/test_cli.py (chdir to tmp_path + delenv both OPENSKY vars) - the watch command does not use OpenSky, but the guard costs one line and prevents environment drift.
- Watch fetches sequentially with `progress=None`; it must reuse `_resume_starts_per_source`, `_source_is_unhealthy`, and `config.resume_max_lookback_days` from the #19/#20 work rather than reimplementing resume/health logic.
- Before each commit: `uv run --no-sync ruff check . && uv run --no-sync ruff format --check . && uv run --no-sync pytest` green.
- Commit messages: concise, why-focused, reference #24, end with the trailer line `Claude-Session: https://claude.ai/code/session_018hjWwnYNy7YC73ayhRDThJ`.

---

### Task 1: watch.py alert core (#24)

**Files:**
- Create: `adsbtrack/watch.py`
- Modify: `adsbtrack/config.py` (one new setting: `watch_dormancy_days`)
- Test: `tests/test_watch.py` (new)

**Interfaces:**
- Consumes: `Database` (tables: trace_days, flights, spoofed_broadcasts - all existing), `Config`.
- Produces (Task 2 consumes verbatim):
  - `@dataclass WatchAlert: kind: str; icao: str; summary: str; detail: dict` with kinds `"reactivation" | "emergency" | "spoof"`.
  - `@dataclass WatchState: has_any_trace: bool; last_data_day: str | None; max_flight_takeoff_time: str | None`
  - `snapshot_state(db, icao) -> WatchState`
  - `evaluate(db, icao, pre: WatchState, run_started_at: str, config) -> list[WatchAlert]`

**Semantics (exact):**
- `snapshot_state`: `has_any_trace` = any trace_days row for the icao; `last_data_day` = `MAX(date)` over that icao's trace_days rows (rows only exist for days with data); `max_flight_takeoff_time` = `MAX(takeoff_time)` over its flights rows (None when no flights).
- `evaluate`, in order:
  1. **Baseline guard:** if `pre.has_any_trace` is False, return `[]` - a first-ever fetch backfills history and must not flood alerts; the run output labels the hex "baselined".
  2. **reactivation:** fires when `pre.last_data_day` is not None AND a post-run trace_days row exists with `date > pre.last_data_day` AND the gap in days between `pre.last_data_day` and the EARLIEST such new day is `>= config.watch_dormancy_days`. detail: `{"dormant_since": pre.last_data_day, "reactivated_on": first_new_day, "gap_days": N}`. One alert max per run per hex.
  3. **emergency:** one alert per flights row with `takeoff_time > pre.max_flight_takeoff_time` (all rows when pre value is None BUT the baseline guard already returned for truly-new hexes) AND (`had_emergency = 1` OR `emergency_squawk IS NOT NULL`). The takeoff_time comparison - not row id - is what suppresses boundary flights that incremental extraction legitimately re-writes with fresh ids. detail: `{"takeoff_time", "callsign", "emergency_squawk", "squawks_observed"}`.
  4. **spoof:** one alert per spoofed_broadcasts row with `detected_at >= run_started_at`. detail: `{"takeoff_time", "callsign", "reason", "reason_detail"}` (reason_detail passed through as the stored JSON string, not re-parsed).
- Summaries are one human line each, e.g. `"ad3f65 active again after 303 days (last seen 2025-10-21)"`, `"aaa111 squawked 7700 on flight 2026-03-01T12:00"`, `"896483 new spoof quarantine (bimodal_integrity)"`.

**Config addition** (next to the spoof block or fetch block, with this comment):

```python
    # watch: minimum days of silence before a reappearance counts as a
    # "reactivation" alert. Grounded/sanctioned airframes waking up is the
    # signal this exists for; routine overnight gaps must not fire it.
    watch_dormancy_days: int = 30
```

- [ ] **Step 1: Write failing tests** in `tests/test_watch.py` (seed tmp DBs with `Database.insert_trace_day`, `insert_flight`, `insert_spoofed_broadcast`):

```python
def test_first_run_baseline_returns_no_alerts(tmp_path):
    # pre snapshot taken on an EMPTY db (has_any_trace False); seed data after;
    # evaluate returns [] even though flights with emergencies now exist.

def test_reactivation_fires_after_dormancy_gap(tmp_path):
    # pre: last_data_day 100 days ago. Post: new trace day today.
    # One reactivation alert, gap_days == 100, kinds/detail fields exact.

def test_reactivation_respects_dormancy_floor(tmp_path):
    # pre: last_data_day 5 days ago; new day today; watch_dormancy_days=30 -> no alert.

def test_emergency_fires_only_for_new_flights(tmp_path):
    # Two emergency flights: one with takeoff_time before pre.max_flight_takeoff_time
    # (boundary re-extract), one after. Exactly one alert, for the later one.

def test_spoof_fires_only_for_rows_detected_after_run_start(tmp_path):
    # Two spoofed rows, detected_at one hour before and one minute after
    # run_started_at. Exactly one alert.

def test_snapshot_state_shapes(tmp_path):
    # Empty db -> (False, None, None); seeded db -> correct MAX values.
```

- [ ] **Step 2: Run them, verify they fail** (`uv run --no-sync pytest tests/test_watch.py -v`).
- [ ] **Step 3: Implement `adsbtrack/watch.py`** (module docstring explains snapshot/evaluate self-suppression; no imports from cli.py - leaf module importing only db/config types) and the Config field.
- [ ] **Step 4: Run new tests + full suite.**
- [ ] **Step 5: Commit** referencing #24, session trailer.

---

### Task 2: watch CLI command, webhook notifier, docs (#24)

**Files:**
- Modify: `adsbtrack/cli.py` (new `watch` command), `adsbtrack/watch.py` (add `_post_webhook`), `adsbtrack/config.py` (`watch_webhook_timeout_secs`), `README.md`. A CLAUDE.md note is NOT needed (controller applies CLAUDE.md at merge time).
- Test: `tests/test_cli.py`

**Interfaces:**
- Consumes: Task 1's `snapshot_state` / `evaluate` / `WatchAlert`; existing cli helpers `_resume_starts_per_source`, `_source_is_unhealthy`, `_validate_hex`, `_db_option`, `_load_config`; `fetch_traces(db, config, hex, start, end, source=src, progress=None)`; `extract_flights(db, config, hex)` (incremental, NOT reprocess); `SOURCE_URLS`.
- Produces: `adsbtrack watch` CLI.

**Command contract:**

```python
@cli.command("watch")
@click.option("--hex", "hex_codes", multiple=True, callback=_validate_hex_multi, help="Hex to watch (repeatable)")
@click.option("--watchlist", "watchlist_path", type=click.Path(exists=True), default=None,
              help="File with one hex per line; '#' comments and blank lines ignored")
@click.option("--webhook", default=None, help="POST alerts as JSON to this URL when any fire")
@click.option("--dormancy-days", type=int, default=None, help="Override Config.watch_dormancy_days")
@click.option("--json", "as_json", is_flag=True, help="Machine-readable output")
@_db_option
def watch_cmd(hex_codes, watchlist_path, webhook, dormancy_days, as_json, db_path):
    """Fetch a hex watchlist and alert on reactivation, emergencies, and new spoof quarantines."""
```

- `_validate_hex_multi`: a thin callback applying the existing `_validate_hex` normalization to each value of the multiple option (Click passes a tuple; validate each element the same way, same error message).
- Union of `--hex` values and watchlist-file entries; error (`click.UsageError`) when the union is empty. Duplicate hexes deduped preserving order.
- `--dormancy-days N` overrides `config.watch_dormancy_days` for the run when provided (set it on the loaded config object before evaluating, mirroring how fetch applies `--rate`/`--concurrency`).
- Per hex, in order: `run_started_at = datetime.now(UTC).isoformat()`; `pre = snapshot_state(db, hex)`; sequential fetch from each readsb source in `SOURCE_URLS` that is not unhealthy (`_source_is_unhealthy`; print one dim skip note per skipped source), each source resuming from its own history via `_resume_starts_per_source` (+1 day; source with no history starts at `end - config.resume_max_lookback_days`; every start clamped to that same lookback), end = today, `progress=None`; then `extract_flights(db, config, hex)` (incremental); then `alerts = evaluate(db, hex, pre, run_started_at, config)`.
- A fetch/extract exception for one hex prints a red warning and continues with the remaining hexes (the failed hex contributes no alerts; exit code logic unaffected by the failure itself).
- Console output: per-hex one status line (`baselined` / `no alerts` / `N alert(s)`), then one Rich table of all alerts (KIND, ICAO, SUMMARY). `--json`: `{"generated_at": ..., "alerts": [asdict(alert), ...], "hexes": {...per-hex status...}}`.
- Webhook: when `--webhook` given AND alerts fired, POST the same JSON document with `urllib.request` (`Content-Type: application/json`, timeout `config.watch_webhook_timeout_secs`). Webhook failure prints a red warning but does not change the exit code. Never sends when zero alerts.
- Exit code: 0 when no alerts, 2 when any alert fired (cron-friendly; document it in README). Use `ctx.exit(2)` / `sys.exit(2)` per the file's existing convention.

**Config addition:**

```python
    # watch --webhook: POST timeout. A hung alert receiver must not wedge a
    # cron-driven watch run.
    watch_webhook_timeout_secs: float = 10.0
```

**README:** new "Watching a hex list" subsection: what it alerts on, the baseline-first-run behavior, exit code 2, a crontab example line (`adsbtrack watch --watchlist ~/.config/adsbtrack/watchlist.txt --webhook https://ntfy.sh/mytopic`), and the watchlist file format.

- [ ] **Step 1: Write failing tests** in `tests/test_cli.py` (follow `_capture_fetch_calls(monkeypatch, tmp_path)` for hermetic fetch stubbing; add a small `_capture_webhook(monkeypatch)` that replaces `urllib.request.urlopen` and records (url, body, timeout)):

```python
def test_watch_requires_some_hex(tmp_path, monkeypatch):
    # no --hex, no --watchlist -> UsageError.

def test_watch_first_run_baselines_without_alerts(tmp_path, monkeypatch):
    # empty db, stubbed fetch adds nothing: exit 0, output contains "baselined", no webhook call.

def test_watch_reactivation_alert_and_exit_code(tmp_path, monkeypatch):
    # seed old trace day 100 days back; stubbed fetch_traces inserts a trace day for
    # today via the real db handle it receives; expect exit 2, table row kind
    # "reactivation", webhook NOT called (no --webhook).

def test_watch_webhook_posts_on_alerts_only(tmp_path, monkeypatch):
    # same alert scenario + --webhook http://example.invalid/hook: captured POST with
    # JSON body containing the alert; and a no-alert run makes zero webhook calls.

def test_watch_watchlist_file_parsing(tmp_path, monkeypatch):
    # file with comments/blank lines/mixed case + one --hex duplicate: fetch stub
    # called once per unique hex, normalized lowercase.

def test_watch_skips_unhealthy_sources(tmp_path, monkeypatch):
    # seed 20 consecutive 502s for one source: stub records no call for it, output
    # mentions the skip.

def test_watch_continues_after_one_hex_fails(tmp_path, monkeypatch):
    # stub raises for the first hex, works for the second: both statuses printed,
    # run completes, exit code reflects only real alerts.
```

- [ ] **Step 2: Run them, verify they fail.**
- [ ] **Step 3: Implement** the command + `_capture_webhook`-compatible webhook sender (module-level `_post_webhook(url, payload, timeout)` in cli.py or watch.py - put it in watch.py so it is unit-testable without Click) + README section.
- [ ] **Step 4: Run new tests + full suite.**
- [ ] **Step 5: Commit** referencing #24 ("closes #24" in the final commit body), session trailer.
