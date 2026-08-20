"""Single per-trace-day v2/sil0/nic0/callsign counting core.

The raw counting loop over one decoded trace's sample points used to live
duplicated wherever it was needed. `parser.pool_spoof_scores` (the
day-level bimodal-integrity pooling scan backing `events._detect_spoof_events`)
calls `count_v2_integrity` here, and so does `db.insert_trace_day`'s
materialized per-day stat columns (Task 12), so the two can never drift
apart. The flight-scoped reject-in-extract gate (issue #22) does not call
this module directly -- it applies the identical predicate (version == 2,
sil == 0, nic == 0) per point via `classifier.FlightMetrics.record_point`
instead of a decoded-trace scan, so flight-scoped and day-scoped stats
still can never disagree about the same point.

This module has no dependency on `db.py` or `parser.py`. `parser.py`
already imports `db.py`, so `db.py` importing `parser.py` back would
create a cycle; routing the shared counting logic through this leaf
module lets both import it without one importing the other.
"""

from __future__ import annotations


def count_v2_integrity(samples: list) -> tuple[int, int, int, set[str]]:
    """Count DO-260B v2 integrity-field indicators in one decoded trace.

    ``samples`` is a single trace_days row's parsed point list (readsb
    trace format: index 8 holds the aircraft-state dict on samples that
    carry one). Returns ``(v2_samples, sil0_count, nic0_count, callsigns)``:

    - v2_samples: point count with ``ac["version"] == 2``
    - sil0_count: of those, how many carried ``sil == 0``
    - nic0_count: of those, how many carried ``nic == 0``
    - callsigns: distinct non-blank ``ac["flight"]`` values seen on v2 samples
    """
    v2 = 0
    sil0 = 0
    nic0 = 0
    callsigns: set[str] = set()
    for s in samples:
        if not isinstance(s, list) or len(s) <= 8:
            continue
        ac = s[8]
        if not isinstance(ac, dict) or ac.get("version") != 2:
            continue
        v2 += 1
        if ac.get("sil") == 0:
            sil0 += 1
        if ac.get("nic") == 0:
            nic0 += 1
        flight = (ac.get("flight") or "").strip()
        if flight:
            callsigns.add(flight)
    return v2, sil0, nic0, callsigns
