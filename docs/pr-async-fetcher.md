# feat: async fetcher with bounded concurrency [GATE PENDING]

**Status: DO NOT MERGE until the real-source benchmark gate below passes.**
Mocked correctness tests are green; live-source behavior (actual 429/403
rates under concurrency, wall-time speedup) has not been verified from
this branch — that is the reviewer's (your) job before merge.

## What changed

- `adsbtrack/fetcher.py::fetch_traces` rewritten as a sync wrapper around
  a new `_fetch_traces_async` coroutine. Public signature is unchanged;
  existing positional callers work unmodified.
- Bounded concurrency via `asyncio.Semaphore(concurrency)`. Default 4,
  configurable via `Config.fetch_concurrency` and the new `--concurrency`
  CLI flag on `fetch`.
- Rate-limit enforcement is **between request STARTS**, not completions.
  A single `asyncio.Lock` serializes slot reservations so consecutive
  starts are at least `current_delay` seconds apart regardless of how
  many workers are in flight.
- 429 handling: lock-protected `current_delay *= 2` capped at
  `rate_limit_max`, `successes_since_backoff` reset, Retry-After slept
  inside the lock so queued workers pick up the new delay on their next
  slot acquisition.
- 403 circuit breaker: per-day outcome map + a reverse-scan of
  `sorted_days` that trips when the latest 3 completed days (by date
  order, skipping in-flight ones) all exhausted on 403. Any non-403
  terminal outcome resets the streak.
- DB writes serialized via a single `_db_writer` task draining an
  `asyncio.Queue` — avoids WAL write-lock contention. Each day's
  `insert_trace_day` + `insert_fetch_log` + `commit` is atomic, so SIGINT
  mid-run either commits both rows or neither.
- Rich `Progress.advance` called once per day inside the writer task
  (after commit), so the progress bar reflects durable state.

## Mocked tests landed

All 12 new tests in `tests/test_fetcher_async.py` pass. Each pins a
specific gate from the spec:

| Gate | Test |
|------|------|
| 429 doubles `current_delay`, caps at `rate_limit_max`, resets success counter | `test_429_doubles_delay_and_resets_success_counter`, `test_429_cap_holds_at_rate_limit_max` |
| 3 consecutive distinct 403-exhausted days raise `RuntimeError` | `test_three_consecutive_403_days_raises` |
| 404 or 200 resets the 403 circuit counter | `test_404_between_403s_resets_circuit`, `test_200_between_403s_resets_circuit` |
| Semaphore bound never exceeded | `test_concurrency_bound_never_exceeded` |
| Rate-limit lock enforces spacing between request **starts** | `test_rate_limit_spacing_between_request_starts` |
| SIGINT mid-fetch leaves DB consistent (no orphan trace_days vs fetch_log) | `test_cancel_midflight_leaves_db_consistent` |
| Progress bar advances exactly once per day under concurrency=4 | `test_progress_advances_once_per_day_under_concurrency` |
| Sync wrapper contract (positional signature, stats keys) unchanged | `test_sync_wrapper_signature_unchanged` |
| `concurrency=1` is byte-identical to serial | `test_concurrency_one_is_serial` |
| Empty range short-circuits without touching transport | `test_no_days_to_fetch_returns_empty_stats` |

Full suite: **432 passed, 1 skipped, 1 deselected** (pre-existing
mictronics deselect unrelated to this branch).

## Required real-source benchmark (you run this before merge)

### The rate-limit floor is mathematical

Wall time has a hard lower bound from the rate-limit lock alone:

```
  min_wall_time = (N_days - 1) * rate_limit + last_request_latency
```

For the defaults used below (`N_days=50`, `--rate 0.5`), this is:

```
  floor = 49 * 0.5 s + ~0.5 s = ~25.0 s
```

at **any** concurrency. This is not a bug to investigate — it's the
guarantee the lock provides. Parallelism can only help when per-request
latency exceeds `rate_limit` and workers can overlap in flight between
the enforced start gaps. On a CDN with sub-100ms responses and
`rate_limit=0.5s`, concurrency=1 and concurrency=4 will produce
indistinguishable wall times; that's the spec-anticipated "rate limit
is the binding constraint" outcome, not a broken lock.

### Canonical protocol

Canonical path: `adsbtrack/bench.py` (run as `python -m adsbtrack.bench`). It skips the CLI's
`ensure_airports` download and auto-extract, times only `fetch_traces`
itself, and prints the exact config values used so rate_limit can't
silently drift from 0.5s. Pick a test aircraft and a 50-day window with
known data. Example below uses `a66ad3` (a PC-12 with dense trace
history); substitute a hex you have local permission to hammer.

```bash
export HEX=a66ad3
export START=2024-06-01
export END=2024-07-20   # 50 days inclusive
export SRC=adsbx        # or whichever source is currently healthy

# --- 1. One-time: build an airport-populated template DB.
#     Kept separate so per-concurrency runs don't re-download 80k
#     airport rows on every iteration (that's what inflated the
#     pre-script protocol's flat ~25 s result).
rm -f bench-airports.db
uv run python -c "
from adsbtrack.db import Database
from adsbtrack.config import Config
from adsbtrack.airports import download_airports
from pathlib import Path
p = Path('bench-airports.db')
with Database(p) as db:
    cfg = Config(db_path=p)
    n = download_airports(db, cfg)
    db.commit()
    print(f'airports loaded: {n}')
"

# --- 2. Per-concurrency runs: copy the template, bench fetch only.
for C in 1 2 4; do
  echo ""
  echo "========================================="
  echo "  concurrency=${C}"
  echo "========================================="
  cp bench-airports.db bench-c${C}.db
  uv run python -m adsbtrack.bench \
    --hex ${HEX} --start ${START} --end ${END} \
    --source ${SRC} --concurrency ${C} --rate 0.5 \
    --db bench-c${C}.db
done
```

The script prints a `[bench]` header line with `rate_limit=0.5s
rate_limit_max=30.0s airports_in_db=<N>` so every run self-documents
its config. The `elapsed` line is fetch-only wall time. A per-run
status histogram + orphan-200 count follow automatically, so the
consistency invariant is checked on every clean run.

### Optional: stress variant at `--rate 0.1`

The default `--rate 0.5` floors wall time at ~25s regardless of
concurrency (see "rate-limit floor is mathematical" above), so the
default run cannot prove the parallelism infrastructure *does*
anything. It can only prove it doesn't break anything.

To see whether concurrency genuinely overlaps in-flight requests, run
the protocol a second time at `--rate 0.1` against a source that
tolerates it. Floor drops to ~5s (49 × 0.1s).

```bash
# Only if the source is known-cooperative; this is 5x more aggressive
# than the default. Do not run this against a source that has been
# 429ing or 403ing recently.
for C in 1 2 4; do
  cp bench-airports.db bench-c${C}-r01.db
  uv run python -m adsbtrack.bench \
    --hex ${HEX} --start ${START} --end ${END} \
    --source ${SRC} --concurrency ${C} --rate 0.1 \
    --db bench-c${C}-r01.db
done
```

Two possible outcomes at `--rate 0.1`:

| c=1 | c=4 | Interpretation |
|-----|-----|----------------|
| ≈ 5.0 s | ≈ 5.0 s | Per-request latency < 0.1s; rate limit is still binding. Parallelism infrastructure is correct but idle. |
| ≈ 5.0 s | < 5.0 s (meaningfully) | Per-request latency exceeds 0.1s; concurrency overlaps real in-flight time. The parallelism infrastructure actively does something. |

The second outcome validates the async infrastructure on a permissive
source. The first is still a valid merge; it just means adsbx (or
whatever you tested) is fast enough that no amount of concurrency can
beat the rate-limit lock at the chosen `--rate`.

This variant is advisory, not a merge gate. It exists so future-you
(or the next person who touches the fetcher) can confirm the
infrastructure is load-bearing on *some* real-world config, even if
the default config doesn't exercise it.

### Merge decision table

The rate-limit floor is mathematical: 50 request starts at 0.5 s
spacing = **25.0 s minimum** at any concurrency. Any result below
that floor at concurrency=1 means the rate-limit lock is broken.

**Merge is gated on correctness, not speedup.** If all three runs come
in rate-limit-bound at ~25s, that's a legitimate merge outcome: the
async infrastructure is correct, it just isn't needed at the default
`--rate 0.5` on this source. The value you get from merging in that
case is that lowering `--rate` in the future (or switching to a slower
source) starts yielding wall-time gains with zero code changes. Don't
block merge on "must see a speedup at default settings" — block only
on correctness-failure signals (rows below the floor, rising 429/403
rates, orphan rows, inconsistent SIGINT recovery).

| Signal | c=1 | c=2 | c=4 | Merge? |
|--------|-----|-----|-----|--------|
| Wall time | ≈ 25.0 s | ≤ T(c=1) / 1.5 | ≤ T(c=1) / 2 | ✅ merge, concurrency helps |
| Wall time | ≈ 25.0 s | ≈ 25.0 s | ≈ 25.0 s | ⚠️ merge + document: rate-limit bound, effective concurrency=1 on this source |
| Wall time | **< 25.0 s** | any | any | ❌ revert, rate-limit lock is broken (50 starts × 0.5 s = 25.0 s floor) |
| Wall time | ≈ 25.0 s | > 25.0 s | > 25.0 s | ❌ revert, concurrency is hurting (likely lock contention or 429 backoffs) |
| 429 count | baseline | ≤ baseline + 1 | ≤ baseline + 1 | ✅ merge, rate limit is respected |
| 429 count | baseline | > baseline + 3 | any | ❌ revert, rate limit is too loose under concurrency |
| 403 count | baseline | > baseline | any | ❌ revert, CDN is distinguishing bot traffic |
| Orphan 200s (from script output) | 0 | 0 | 0 | prerequisite; any non-zero = revert |
| SIGINT recovery | consistent | consistent | consistent | prerequisite; anything else = revert |

### SIGINT test (manual)

```bash
cp bench-airports.db bench-sigint.db
uv run python -m adsbtrack.bench \
  --hex ${HEX} --start 2024-06-01 --end 2024-06-20 \
  --source ${SRC} --concurrency 4 --rate 0.5 --db bench-sigint.db &
PID=$!
sleep 5                         # let a few days commit
kill -INT ${PID}
wait ${PID} 2>/dev/null

# The bench script already reports orphan 200s on successful exit; on
# cancel we verify it directly:
sqlite3 bench-sigint.db \
  "SELECT COUNT(*) FROM fetch_log f WHERE f.status = 200
   AND NOT EXISTS (SELECT 1 FROM trace_days t
                   WHERE t.icao = f.icao AND t.date = f.date AND t.source = f.source);"

# Resume must pick up where it left off without re-fetching committed days.
uv run python -m adsbtrack.bench \
  --hex ${HEX} --start 2024-06-01 --end 2024-06-20 \
  --source ${SRC} --concurrency 4 --rate 0.5 --db bench-sigint.db
```

## What this branch deliberately does NOT do

- No changes to `fetch_traces_adsblol` or `fetch_traces_opensky`. Those
  remain synchronous.
- No changes to the parser, extractor, or any code downstream of fetch.
- No tuning of `rate_limit` or `rate_limit_max` defaults — the spec is
  explicit that we don't change rate limits to make parallelism "help."

## Why not merged yet

The spec's kill switches all read real-CDN behavior. From a sandbox I
can mock the logic (and have — see the 12 tests above) but cannot
observe real 429/403 rates under live concurrency, and cannot test
SIGINT against a real DB being written concurrently to a real source.
Reviewer runs the benchmark commands above, inspects the decision
table, and either merges or reverts. Flip this PR's "Draft" status only
after that pass.

## Appendix: legacy protocol (superseded by `python -m adsbtrack.bench`)

Earlier drafts of this doc pointed at `/usr/bin/time` wrapped around
the full CLI `fetch` command. That version is preserved here for
anyone who remembers it, but don't use it: it also times the
`ensure_airports` download (adds a few seconds on a fresh DB) and the
auto-extract after fetch (adds seconds proportional to trace size), so
the numbers aren't apples-to-apples with the bench script's fetch-only
timing.

```bash
# LEGACY -- superseded. Use `python -m adsbtrack.bench` above.
for C in 1 2 4; do
  rm -f bench-c${C}.db
  echo "=== concurrency=${C} ==="
  /usr/bin/time -l uv run python -m adsbtrack.cli fetch \
    --hex ${HEX} --start ${START} --end ${END} \
    --source ${SRC} --concurrency ${C} \
    --db bench-c${C}.db 2>&1 | tee bench-c${C}.log
done

for C in 1 2 4; do
  echo "=== c=${C} status histogram ==="
  sqlite3 bench-c${C}.db \
    "SELECT status, COUNT(*) FROM fetch_log GROUP BY status ORDER BY status;"
done
```

Flat-25s-across-all-concurrencies results from this legacy protocol are
ambiguous (could be rate-limit bound OR airport-DL dominated); re-run
with the bench script to disambiguate.
