"""Within-flight ADS-B signal gap detection and classification.

Walks the merged trace for an aircraft, finds inter-point gaps longer
than `min_gap_secs`, and tags each one with a conservative classifier:

- ``likely_transponder_off``: high altitude AND good ADS-B coverage on
  both sides AND within 200 nm of a known airport. All four signals
  must agree; the classifier emits this label only when it is the
  simplest explanation.
- ``coverage_hole``: low altitude (line-of-sight horizon), or remote
  location (>300 nm from a known airport), or weak ADS-B on both sides
  (points were already MLAT/mode-S-only before and after the gap).
- ``unknown``: everything else. Mixed signals default here rather than
  guessing. An analyst can read the rich context (altitude, position,
  surrounding source mix) and judge.

A gap-analysis tool that confidently mislabels is worse than none.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from .airports import haversine_km
from .config import Config
from .db import Database

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


@dataclass
class Gap:
    """A single within-flight gap with context for classification."""

    icao: str
    gap_start_ts: float
    gap_end_ts: float
    duration_secs: float
    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float
    start_alt_ft: int | None
    end_alt_ft: int | None
    nearest_airport_nm: float | None
    pre_source_mix: dict[str, int] = field(default_factory=dict)
    post_source_mix: dict[str, int] = field(default_factory=dict)
    classification: str = "unknown"
    classification_reason: str = ""


# ---------------------------------------------------------------------------
# Source-category normalization
# ---------------------------------------------------------------------------


def _source_category(raw: str | None) -> str:
    """Bucket a readsb position-source tag into adsb / mlat / other.

    ADS-B variants (direct broadcast from the aircraft's transponder) all
    map to "adsb". MLAT (multilateration; mode-S reply triangulated by
    >=4 receivers) is its own category because it indicates the
    transponder is active but ADS-B position is not being broadcast.
    Everything else -- TIS-B, mode-S only, ADS-R, unknown tags -- lands
    in "other", which the classifier treats as "not ADS-B".
    """
    if not raw:
        return "other"
    if raw.startswith("adsb_"):
        return "adsb"
    if raw == "mlat":
        return "mlat"
    return "other"


def _source_mix(points: list[dict[str, Any]]) -> dict[str, int]:
    """Count source categories across a list of points."""
    counts = {"adsb": 0, "mlat": 0, "other": 0}
    for p in points:
        counts[_source_category(p.get("position_source"))] += 1
    return counts


def _adsb_fraction(mix: dict[str, int]) -> float:
    total = sum(mix.values())
    return mix.get("adsb", 0) / total if total else 0.0


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------

# Thresholds. Centralized so the rule matrix is auditable.
_LOW_ALT_FT = 3000  # line-of-sight horizon at typical receiver density
_CRUISE_ALT_FT = 15000  # FL150; below this is climb/descent, gap risk legit
_AIRPORT_NEAR_NM = 200  # within this, expect ADS-B coverage
_AIRPORT_FAR_NM = 300  # beyond this, treat as remote
_ADSB_STRONG_FRAC = 0.7  # both sides must exceed to claim transponder-off
_ADSB_WEAK_FRAC = 0.3  # both sides below = coverage-hole


def _classify_gap(
    *,
    alt_ft: int | None,
    pre_adsb_frac: float,
    post_adsb_frac: float,
    nearest_airport_nm: float | None,
    duration_secs: float,
) -> tuple[str, str]:
    """Return (classification, reason) for a gap given its context.

    Pure function. Rules are evaluated in priority order; the first match
    wins. Order matters because the ambiguity-resolution strategy is
    "cheap certain rule beats expensive uncertain rule."
    """
    if alt_ft is None:
        return "unknown", "no altitude at gap start"

    if alt_ft < _LOW_ALT_FT:
        return "coverage_hole", f"low altitude ({alt_ft} ft) near line-of-sight horizon"

    if nearest_airport_nm is None:
        return "coverage_hole", "no airport within search radius (remote area)"

    if nearest_airport_nm > _AIRPORT_FAR_NM:
        return "coverage_hole", f"remote location ({nearest_airport_nm:.0f} nm from nearest airport)"

    if pre_adsb_frac < _ADSB_WEAK_FRAC and post_adsb_frac < _ADSB_WEAK_FRAC:
        return "coverage_hole", "weak ADS-B coverage before and after gap (MLAT/mode-S dominant)"

    if (
        alt_ft >= _CRUISE_ALT_FT
        and pre_adsb_frac >= _ADSB_STRONG_FRAC
        and post_adsb_frac >= _ADSB_STRONG_FRAC
        and nearest_airport_nm <= _AIRPORT_NEAR_NM
    ):
        fl = alt_ft // 100
        return (
            "likely_transponder_off",
            f"FL{fl} with ADS-B coverage on both sides, {nearest_airport_nm:.0f} nm from nearest airport",
        )

    return "unknown", f"{alt_ft} ft, {nearest_airport_nm:.0f} nm from airport, mixed ADS-B coverage"


# ---------------------------------------------------------------------------
# Detection entry point
# ---------------------------------------------------------------------------


def _extract_point_dict(raw_point: list) -> dict[str, Any]:
    """Lift the minimum fields we need for gap analysis out of a raw
    trace point. The adsbx/adsbfi/opensky 9-14 element layout puts
    altitude at index 3, source tag at index 9 (or in detail['type']),
    detail dict at index 8. We only need ts/lat/lon/alt/source here."""
    ts_offset = raw_point[0]
    lat = raw_point[1]
    lon = raw_point[2]
    alt = raw_point[3] if isinstance(raw_point[3], (int, float)) else None

    position_source: str | None = None
    if len(raw_point) > 9 and isinstance(raw_point[9], str):
        position_source = raw_point[9]
    else:
        detail = raw_point[8] if len(raw_point) > 8 else None
        if isinstance(detail, dict):
            det_type = detail.get("type")
            if isinstance(det_type, str):
                position_source = det_type

    return {
        "ts_offset": ts_offset,
        "lat": lat,
        "lon": lon,
        "alt": int(alt) if alt is not None else None,
        "position_source": position_source,
    }


def _find_nearest_airport_nm(db: Database, lat: float, lon: float, config: Config) -> float | None:
    """Return nautical-mile distance to the nearest airport, or None if
    none within search radius. Uses the same widening-search pattern as
    airports.find_nearest_airport but returns raw distance without the
    airport-type gating (any airport counts for coverage purposes -- a
    small airport still has line-of-sight ADS-B receivers nearby)."""
    candidates = db.find_nearby_airports(lat, lon, delta=0.5, types=config.airport_types)
    if not candidates:
        candidates = db.find_nearby_airports(lat, lon, delta=2.0, types=config.airport_types)
    if not candidates:
        return None

    best_km = float("inf")
    for ap in candidates:
        d = haversine_km(lat, lon, ap["latitude_deg"], ap["longitude_deg"])
        if d < best_km:
            best_km = d

    return best_km * 0.539957  # km -> nm


def detect_gaps(
    db: Database,
    icao: str,
    *,
    min_gap_secs: float = 300.0,
    context_points: int = 10,
    config: Config | None = None,
    include_between_flight: bool = False,
) -> list[Gap]:
    """Scan every trace_day row for `icao`, surface inter-point gaps
    longer than `min_gap_secs`, and classify each.

    Points are deduplicated and sorted via the same merge pipeline the
    extractor uses (per-day dedupe inside each trace_days row, then
    cross-day concatenation by absolute timestamp). `context_points`
    controls how many points before and after a gap feed the source-mix
    classifier; default 10 matches typical pre/post-window analytics.

    By default gaps longer than Config.max_point_gap_minutes (30 min)
    are filtered out: the parser's state machine treats such gaps as
    between-flight boundaries (the aircraft was parked / out of
    coverage between flights), not within-flight signal loss. Pass
    ``include_between_flight=True`` to keep them.
    """
    cfg = config or Config()
    max_within_flight_gap_secs = cfg.max_point_gap_minutes * 60.0
    rows = db.get_trace_days(icao)
    if not rows:
        return []

    # Flatten every trace_day row into (abs_ts, point_dict) tuples.
    # Multiple source rows for the same date get merged here by simple
    # concat + sort + dedup on (rounded ts, rounded lat, rounded lon);
    # this is a lightweight version of parser._merge_trace_rows that
    # skips the Config.dedup_* fine-tuning since we only need "same
    # point within a second" detection.
    abs_points: list[tuple[float, dict[str, Any]]] = []
    for row in rows:
        base_ts = row["timestamp"]
        trace = json.loads(row["trace_json"])
        for raw_point in trace:
            pt = _extract_point_dict(raw_point)
            abs_points.append((base_ts + pt["ts_offset"], pt))

    abs_points.sort(key=lambda x: x[0])

    # Dedup near-duplicates (same second + same position to 5 decimal
    # degrees = ~1 m). Multi-source fetches commonly double up on points.
    deduped: list[tuple[float, dict[str, Any]]] = []
    prev_key: tuple[int, float, float] | None = None
    for abs_ts, pt in abs_points:
        key = (int(abs_ts), round(pt["lat"], 5), round(pt["lon"], 5))
        if key == prev_key:
            continue
        deduped.append((abs_ts, pt))
        prev_key = key

    if len(deduped) < 2:
        return []

    gaps: list[Gap] = []
    for i in range(1, len(deduped)):
        prev_ts, prev_pt = deduped[i - 1]
        curr_ts, curr_pt = deduped[i]
        delta = curr_ts - prev_ts
        if delta < min_gap_secs:
            continue
        if not include_between_flight and delta > max_within_flight_gap_secs:
            # Gap exceeds the parser's flight-split threshold; treat as
            # aircraft parked / out of coverage between flights rather
            # than within-flight signal loss.
            continue

        # Context windows: up to context_points before/after the gap.
        pre_window = [p for _, p in deduped[max(0, i - context_points) : i]]
        post_window = [p for _, p in deduped[i : min(len(deduped), i + context_points)]]
        pre_mix = _source_mix(pre_window)
        post_mix = _source_mix(post_window)

        airport_nm = _find_nearest_airport_nm(db, prev_pt["lat"], prev_pt["lon"], cfg)

        classification, reason = _classify_gap(
            alt_ft=prev_pt["alt"],
            pre_adsb_frac=_adsb_fraction(pre_mix),
            post_adsb_frac=_adsb_fraction(post_mix),
            nearest_airport_nm=airport_nm,
            duration_secs=delta,
        )

        gaps.append(
            Gap(
                icao=icao,
                gap_start_ts=prev_ts,
                gap_end_ts=curr_ts,
                duration_secs=delta,
                start_lat=prev_pt["lat"],
                start_lon=prev_pt["lon"],
                end_lat=curr_pt["lat"],
                end_lon=curr_pt["lon"],
                start_alt_ft=prev_pt["alt"],
                end_alt_ft=curr_pt["alt"],
                nearest_airport_nm=airport_nm,
                pre_source_mix=pre_mix,
                post_source_mix=post_mix,
                classification=classification,
                classification_reason=reason,
            )
        )

    return gaps
