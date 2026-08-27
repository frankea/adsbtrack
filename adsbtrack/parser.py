import contextlib
import json
import math
import re
import sqlite3
from bisect import bisect_left
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

from . import features
from .airports import find_nearest_airport, haversine_km
from .classifier import (
    FlightMetrics,
    PointData,
    classify_ground_state,
    classify_landing,
    score_confidence,
)
from .config import TYPE_CEILINGS, TYPE_MAX_GS, Config
from .db import Database, iter_parsed_trace_days
from .ils_alignment import IlsAlignmentResult, detect_all_ils_alignments
from .integrity import count_v2_integrity
from .landing_anchor import LandingAnchor, compute_landing_anchor
from .models import Flight, LandingType
from .takeoff_runway import TakeoffRunwayResult, detect_takeoff_runway

# Stamped on every Flight produced by extract_flights (see the three
# Flight(...) construction sites below). Bump this on any behavior change
# to extraction or derivation -- a new heuristic, a threshold change with
# algorithmic effect, a new derived field -- so rows can be told apart by
# which revision produced them. Legacy rows written before this column
# existed stay NULL.
# v2: integrity/jamming surface columns (issue #30) -- v2_sample_count,
# integrity_degraded_pct, max_implied_speed_kt, integrity_flagged.
EXTRACTOR_VERSION = 2


def _detail_callsign(detail: dict | None) -> str | None:
    """The callsign a trace point's detail object broadcasts, or None for a
    missing or blank one.

    Split out of _extract_point_fields so the incremental extract can
    reconstruct the callsign the state machine carries into its window
    (see _carried_callsign) by the same rule the state machine applies.
    """
    if not detail:
        return None
    flight = detail.get("flight", "")
    if flight:
        flight = flight.strip()
        if flight:
            return flight
    return None


def _extract_point_fields(point: list, ts: float, lat: float, lon: float) -> PointData:
    """Parse a readsb trace point into a PointData dataclass.

    Trace point layout (readsb globe_history format):
      0: time_offset (seconds since day_timestamp)
      1: lat
      2: lon
      3: baro_alt (int feet or the string 'ground')
      4: ground speed (knots)
      5: track
      6: flags
      7: baro vertical rate (ft/min, signed)
      8: detail object (dict) or None
      9: source tag
     10: geom altitude (feet)
     11: geom vertical rate
     12-13: reserved

    Older formats may have 7-9 elements; be defensive.
    """
    baro_alt = point[3]
    gs = point[4] if len(point) > 4 else None
    track = None
    if len(point) > 5 and isinstance(point[5], (int, float)):
        track = float(point[5])

    detail: dict | None = None
    if len(point) > 8 and isinstance(point[8], dict):
        detail = point[8]

    baro_rate = None
    if len(point) > 7 and isinstance(point[7], (int, float)):
        baro_rate = float(point[7])

    geom_alt: int | None = None
    if len(point) > 10 and isinstance(point[10], (int, float)):
        geom_alt = int(point[10])

    geom_rate: float | None = None
    if len(point) > 11 and isinstance(point[11], (int, float)):
        geom_rate = float(point[11])

    # Source type tag: readsb writes this at point[9] in the 14-element
    # layout. In 9-element rows (most real data in this DB), the same
    # value lives inside detail["type"]. Prefer point[9] when both are
    # present (they always match in observed data).
    position_source: str | None = None
    if len(point) > 9 and isinstance(point[9], str):
        position_source = point[9]

    # Rich detail fields (only ~22% of points have the full payload, so guard)
    squawk: str | None = None
    category: str | None = None
    nav_altitude_mcp: int | None = None
    nav_qnh: float | None = None
    emergency_field: str | None = None
    true_heading: float | None = None
    callsign: str | None = None
    adsb_version: int | None = None
    sil: int | None = None
    nic: int | None = None
    if detail:
        v = detail.get("version")
        if isinstance(v, int):
            adsb_version = v
        s = detail.get("sil")
        if isinstance(s, int):
            sil = s
        n = detail.get("nic")
        if isinstance(n, int):
            nic = n
        sq = detail.get("squawk")
        if sq:
            squawk = str(sq)
        cat = detail.get("category")
        if cat:
            category = str(cat)
        mcp = detail.get("nav_altitude_mcp")
        if isinstance(mcp, (int, float)):
            nav_altitude_mcp = int(mcp)
        qnh = detail.get("nav_qnh")
        if isinstance(qnh, (int, float)):
            nav_qnh = float(qnh)
        em = detail.get("emergency")
        if em:
            emergency_field = str(em)
        th = detail.get("true_heading")
        if isinstance(th, (int, float)):
            true_heading = float(th)
        callsign = _detail_callsign(detail)
        # Fall back to detail.alt_geom when the slot index 10 wasn't present
        if geom_alt is None:
            alt_geom = detail.get("alt_geom")
            if isinstance(alt_geom, (int, float)):
                geom_alt = int(alt_geom)
        if geom_rate is None:
            gr = detail.get("geom_rate")
            if isinstance(gr, (int, float)):
                geom_rate = float(gr)
        # detail["type"] is the same source tag as point[9]; use it as the
        # fallback for 9-element rows where point[9] is not present.
        if position_source is None:
            det_type = detail.get("type")
            if isinstance(det_type, str):
                position_source = det_type

    return PointData(
        ts=ts,
        lat=lat,
        lon=lon,
        baro_alt=baro_alt,
        gs=gs,
        track=track,
        geom_alt=geom_alt,
        baro_rate=baro_rate,
        geom_rate=geom_rate,
        squawk=squawk,
        category=category,
        nav_altitude_mcp=nav_altitude_mcp,
        nav_qnh=nav_qnh,
        emergency_field=emergency_field,
        true_heading=true_heading,
        callsign=callsign,
        position_source=position_source,
        adsb_version=adsb_version,
        sil=sil,
        nic=nic,
    )


def _merge_trace_rows(rows: list[tuple[sqlite3.Row, list]], config: Config) -> tuple[str, float, list, set[str]]:
    """Merge trace_day rows for the same date into a single sorted+deduped trace.

    ``rows`` is a list of (row, parsed_trace) pairs -- the trace_json for
    each row is already decoded by the caller (see db.iter_parsed_trace_days)
    so a multi-source day's trace_json is parsed exactly once per extract,
    not once here and once in the spoof-scoring pass.

    Converts relative offsets to absolute timestamps, concatenates (possibly
    across multiple sources), sorts, deduplicates (points within
    dedup_time_secs and dedup_deg are duplicates), then converts back to
    offsets from the earliest base timestamp.

    The single-source fast path is run through the same pipeline so that
    readsb trace files containing "phantom" points (duplicate entries with
    deeply negative offsets from cache glitches or prior-day leakage) get
    both sorted into chronological order AND deduped if they collide with
    an adjacent real point. The state machine assumes chronological order
    and would otherwise corrupt last_point_ts on an out-of-order point.

    Returns (date, base_timestamp, merged_trace, source_names).
    """
    source_names = {row["source"] for row, _trace in rows}

    # Convert all points to absolute timestamps
    abs_points = []
    for row, trace in rows:
        base_ts = row["timestamp"]
        for point in trace:
            abs_ts = base_ts + point[0]
            abs_points.append((abs_ts, point))

    # Sort by absolute timestamp
    abs_points.sort(key=lambda x: x[0])

    # Deduplicate: skip points too close in time and position to the previous kept point
    merged = []
    prev_ts = None
    prev_lat = None
    prev_lon = None
    for abs_ts, point in abs_points:
        lat = point[1]
        lon = point[2]
        if (
            prev_ts is not None
            and abs(abs_ts - prev_ts) < config.dedup_time_secs
            and prev_lat is not None
            and prev_lon is not None
            and abs(lat - prev_lat) < config.dedup_deg
            and abs(lon - prev_lon) < config.dedup_deg
        ):
            continue
        merged.append((abs_ts, point))
        prev_ts = abs_ts
        prev_lat = lat
        prev_lon = lon

    # Convert back to relative offsets from the earliest base timestamp
    base_timestamp = min(row["timestamp"] for row, _trace in rows)
    result_trace = []
    for abs_ts, point in merged:
        new_point = list(point)
        new_point[0] = abs_ts - base_timestamp
        result_trace.append(new_point)

    return rows[0][0]["date"], base_timestamp, result_trace, source_names


def _effective_stitch_gap_minutes(config: Config, type_code: str | None) -> float:
    """The widest fragment gap _stitch_fragments will merge across for this
    type. Shared with the incremental boundary walk, which has to know how
    far a stitch can reach before it can call a coverage gap a safe cut."""
    endurance_minutes = config.type_endurance_minutes.get(type_code or "", config.max_endurance_minutes)
    return max(config.stitch_max_gap_minutes, endurance_minutes * config.stitch_endurance_ratio)


def _day_gap_resets_state(prev_date: str, curr_date: str, config: Config) -> bool:
    """True when the coverage gap between two consecutive trace days is wide
    enough that the state machine drops whatever flight it was holding.

    The single definition of that rule: _scan_state_machine applies it while
    walking the days, and the incremental boundary walk applies it in
    reverse to find a day the machine is guaranteed to start clean on.
    """
    return datetime.fromisoformat(curr_date) - datetime.fromisoformat(prev_date) > timedelta(
        days=config.max_day_gap_days
    )


def _stitch_fragments(
    flights: list[Flight],
    metrics_list: list[FlightMetrics],
    config: Config,
    type_code: str | None = None,
) -> tuple[list[Flight], list[FlightMetrics]]:
    """Merge signal_lost / dropped_on_approach fragments with the next
    found_mid_flight fragment when they are plausibly the same continuous
    flight with a receiver gap in the middle.

    Merge criteria (all must pass):
      1. Previous flight has no landing transition (landing_lat is None) AND
         takeoff_type == "observed" OR has a last_seen position.
      2. Next flight has takeoff_type == "found_mid_flight".
      3. Time gap between prev.last_seen_time and next.takeoff_time is less
         than the per-type effective stitch window (see below).
      4. Great-circle distance between prev.last_seen_* and next.takeoff_*
         is less than time_gap * cruise_speed * slack.
      5. Altitude difference between prev.last_seen_alt_ft and next's first
         airborne altitude is less than config.stitch_max_alt_delta_ft.
      6. Fresh-departure veto (#21) does NOT fire: the gap exceeds
         config.stitch_min_ground_gap_secs AND next's first airborne
         altitude is below config.stitch_fresh_departure_alt_ft AND next's
         raw altitude peak climbs more than config.stitch_fresh_departure_climb_ft
         above that first altitude. That combination means next is a new
         sortie taking off after real ground time, not a coverage hole in
         the same flight.

    Effective stitch window:
      max(config.stitch_max_gap_minutes,
          config.type_endurance_minutes.get(type_code, max_endurance) *
          config.stitch_endurance_ratio)

    This keeps the default 90-min window for light GA while letting long-
    endurance types (KC-135, C-17, etc.) stitch across the multi-hour
    coverage gaps that are normal on their operational missions.

    Merging is destructive: the merged flight inherits prev.takeoff_* (if
    observed) and next.landing_*/last_*, and its metrics are the earlier
    fragment's FlightMetrics with the later fragment folded in via
    FlightMetrics.merge() (see the per-field strategy table declared next to
    each field in classifier.py). The takeoff_type of the merged flight
    becomes "observed" if prev observed its takeoff, otherwise
    "found_mid_flight".
    """
    if len(flights) < 2:
        return flights, metrics_list

    max_gap_secs = _effective_stitch_gap_minutes(config, type_code) * 60.0
    max_alt_delta = config.stitch_max_alt_delta_ft
    cruise_speed_kt = config.stitch_cruise_speed_kts
    slack = config.stitch_distance_slack

    # kt -> km/h factor 1.852, and km/h * hours = km. We just need
    # distance_km = knots * hours * 1.852.
    def _plausible_distance_km(gap_secs: float) -> float:
        return (gap_secs / 3600.0) * cruise_speed_kt * 1.852 * slack

    merged: list[tuple[Flight, FlightMetrics]] = []
    i = 0
    pairs = list(zip(flights, metrics_list, strict=True))
    while i < len(pairs):
        flight, metrics = pairs[i]

        # Only attempt to stitch if this flight ended without a landing
        # transition (signal_lost-ish, may be classified later).
        if i + 1 < len(pairs) and flight.landing_lat is None and metrics.last_seen_ts is not None:
            next_flight, next_metrics = pairs[i + 1]

            if next_metrics.takeoff_type == "found_mid_flight" and next_metrics.first_point_ts is not None:
                gap_secs = next_metrics.first_point_ts - metrics.last_seen_ts
                if 0 <= gap_secs <= max_gap_secs:
                    # Distance check
                    if metrics.last_seen_lat is not None and metrics.last_seen_lon is not None:
                        dist_km = haversine_km(
                            metrics.last_seen_lat,
                            metrics.last_seen_lon,
                            next_flight.takeoff_lat,
                            next_flight.takeoff_lon,
                        )
                        plausible = _plausible_distance_km(max(gap_secs, 60.0))
                    else:
                        dist_km = 0.0
                        plausible = float("inf")

                    # Altitude check
                    alt_ok = True
                    if metrics.last_seen_alt_ft is not None and next_metrics.last_airborne_alt is not None:
                        alt_delta = abs(metrics.last_seen_alt_ft - next_metrics.last_airborne_alt)
                        alt_ok = alt_delta <= max_alt_delta

                    # Fresh-departure veto (#21): a long gap followed by a
                    # reappearance that starts low and then climbs away is a
                    # new sortie, not a coverage hole in the same flight.
                    # raw_peak_altitude_ft, not max_altitude: max_altitude requires
                    # AP corroboration and can fall back to 0 for a short
                    # reappearance fragment, which would silently disable the veto.
                    next_peak = next_metrics.raw_peak_altitude_ft
                    fresh_departure = (
                        gap_secs > config.stitch_min_ground_gap_secs
                        and next_metrics.first_airborne_alt is not None
                        and next_metrics.first_airborne_alt < config.stitch_fresh_departure_alt_ft
                        and next_peak is not None
                        and next_peak - next_metrics.first_airborne_alt > config.stitch_fresh_departure_climb_ft
                    )

                    if dist_km <= plausible and alt_ok and not fresh_departure:
                        # Merge: the next fragment inherits prev's takeoff
                        # position and time (the originally-observed takeoff
                        # if prev was observed, otherwise prev's first point).
                        stitched = next_flight
                        stitched.takeoff_time = flight.takeoff_time
                        stitched.takeoff_lat = flight.takeoff_lat
                        stitched.takeoff_lon = flight.takeoff_lon
                        stitched.takeoff_date = flight.takeoff_date
                        stitched.callsign = flight.callsign or next_flight.callsign

                        # Metrics: fold the later fragment (next_metrics) into
                        # the earlier one (metrics). FlightMetrics.merge
                        # applies the explicit per-field strategy declared
                        # next to each field in classifier.py (sum / max /
                        # min / union / keep-first / keep-last / ...), so
                        # every accumulator - including other_points,
                        # adsc_points, and squawk_durations, which used to
                        # fall through unmerged here - combines correctly,
                        # and takeoff-side fields (takeoff_tracks,
                        # takeoff_points, ...) stay with the fragment that
                        # actually observed the takeoff instead of being
                        # silently overwritten by the later, fictitious
                        # "found mid-flight" fragment's takeoff data.
                        metrics.merge(next_metrics)
                        # +1 for the coverage hole this stitch just bridged,
                        # on top of whatever gaps each fragment already saw.
                        metrics.signal_gap_count += 1

                        # Recompute duration on the merged flight. extract_flights
                        # computes duration_minutes before stitching using the
                        # pre-merge fragment boundaries, so after widening
                        # first_point_ts we need to refresh the Flight field to
                        # cover the whole stitched span (including the coverage
                        # gap between the two fragments).
                        if metrics.first_point_ts is not None and metrics.last_point_ts is not None:
                            span = metrics.last_point_ts - metrics.first_point_ts
                            stitched.duration_minutes = round(span / 60.0, 1)

                        merged.append((stitched, metrics))
                        i += 2
                        continue

        merged.append((flight, metrics))
        i += 1

    stitched_flights = [p[0] for p in merged]
    stitched_metrics = [p[1] for p in merged]
    return stitched_flights, stitched_metrics


_EK_FLIGHTNUM_RE = re.compile(r"^EK\d+$")


def pool_spoof_scores(parsed_rows: Iterable[tuple[sqlite3.Row, list]], config: Config) -> dict[str, dict]:
    """Return a per-date summary of bimodal-integrity spoof indicators.

    ``parsed_rows`` is the output of db.iter_parsed_trace_days -- each
    row's trace_json is already decoded, so pooling never re-parses JSON
    the caller already parsed. This scan now backs only
    events._detect_spoof_events (the opt-in day-level event); issue #22
    moved parser.extract_flights's rejection gate to flight scope (see
    _flight_is_spoofed), which reads FlightMetrics counters accumulated
    per point instead of a day-level pre-scan.

    Pool v2 samples across every aggregator that fetched the same date:
    the denominator is the union of v2 samples from all sources, the
    numerator counts how many carried sil=0. A date is flagged when the
    pooled v2 count is at least config.spoof_min_v2_samples and the
    pooled sil=0 share is >= config.spoof_v2_sil0_pct.

    Pooling (instead of picking the worst single source) dilutes
    aggregator-specific artifacts: a real spoof emitted over the air hits
    every aggregator, so the rate holds up against dilution; a single
    aggregator's transient integrity-field glitch gets averaged out by
    the other sources.

    Each flagged date also carries its earliest row timestamp and the set
    of callsigns seen on v2 samples -- parser.extract_flights ignores
    both, but events._detect_spoof_events needs them to build the
    spoof_bimodal_integrity Event's ts/callsign without a second scan.
    """
    by_date: dict[str, dict] = defaultdict(
        lambda: {
            "v2": 0,
            "sil0": 0,
            "nic0": 0,
            "sources": set(),
            "source_rates": [],
            "timestamp": None,
            "callsigns": set(),
        }
    )
    for row, samples in parsed_rows:
        src_v2, src_sil0, src_nic0, callsigns = count_v2_integrity(samples)
        if src_v2 == 0:
            continue
        agg = by_date[row["date"]]
        agg["v2"] += src_v2
        agg["sil0"] += src_sil0
        agg["nic0"] += src_nic0
        agg["sources"].add(row["source"])
        agg["source_rates"].append((row["source"], round(100.0 * src_sil0 / src_v2, 2)))
        agg["callsigns"] |= callsigns
        if agg["timestamp"] is None:
            agg["timestamp"] = row["timestamp"]

    flagged: dict[str, dict] = {}
    for date_str, agg in by_date.items():
        v2 = agg["v2"]
        if v2 < config.spoof_min_v2_samples:
            continue
        sil_pct = 100.0 * agg["sil0"] / v2
        if sil_pct < config.spoof_v2_sil0_pct:
            continue
        flagged[date_str] = {
            "v2_samples": v2,
            "v2_sil0_pct": sil_pct,
            "v2_nic0_pct": 100.0 * agg["nic0"] / v2,
            "sources": sorted(agg["sources"]),
            "source_rates": sorted(agg["source_rates"]),
            "timestamp": agg["timestamp"],
            "callsigns": sorted(agg["callsigns"]),
        }
    return flagged


def _flight_is_spoofed(flight: Flight, metrics: FlightMetrics, config: Config) -> tuple[str, dict] | None:
    """Return ``(reason, detail)`` when a flight should be rejected.

    Flight-scoped bimodal-integrity gate (two tiers, see Config comment and
    issue #22), then the unchanged crude EK-callsign heuristic. Day-level
    pooling no longer rejects flights - it survives only as the events-layer
    detector (events._detect_spoof_events).
    """
    v2 = metrics.v2_samples
    if v2 >= config.spoof_min_v2_samples:
        sil_pct = 100.0 * metrics.v2_sil0 / v2
        teleport = metrics.max_implied_speed_kt
        hard = sil_pct >= config.spoof_flight_sil0_hard_pct
        corroborated = (
            sil_pct >= config.spoof_v2_sil0_pct and teleport is not None and teleport > config.spoof_teleport_speed_kt
        )
        if hard or corroborated:
            return "bimodal_integrity", {
                "scope": "flight",
                "date": flight.takeoff_date,
                "v2_samples": v2,
                "v2_sil0_pct": round(sil_pct, 2),
                "v2_nic0_pct": round(100.0 * metrics.v2_nic0 / v2, 2),
                "max_implied_speed_kt": round(teleport, 1) if teleport is not None else None,
                "trigger": "hard_sil0" if hard else "sil0_plus_teleport",
            }
    cs = (flight.callsign or "").strip()
    if (
        flight.max_altitude is not None
        and flight.max_altitude < config.spoof_crude_max_altitude_ft
        and flight.origin_icao is None
        and flight.destination_icao is None
        and _EK_FLIGHTNUM_RE.fullmatch(cs)
    ):
        return "crude_heuristic", {
            "max_altitude": flight.max_altitude,
            "callsign": cs,
            "pattern": r"^EK\d+$",
        }
    return None


def _any_climb_between(
    segments: list[IlsAlignmentResult],
    points: Iterable,
    *,
    threshold_ft: float = 500.0,
) -> bool:
    """Return True when any two consecutive segments in ``segments`` are
    separated by a rise of more than ``threshold_ft`` above the earlier
    segment's end altitude. Walks ``points`` for each gap; O(n*m) where n
    is the full per-flight point count (``metrics.all_points``) and
    m<=5 segments, which stays cheap since m is always small."""
    if len(segments) < 2:
        return False
    points = list(points)
    for i in range(len(segments) - 1):
        a, b = segments[i], segments[i + 1]
        if a.end_alt_ft is None:
            continue
        gap_max: int | None = None
        for p in points:
            if a.last_ts < p.ts < b.first_ts:
                alt = p.baro_alt if p.baro_alt is not None else p.geom_alt
                if alt is not None and (gap_max is None or alt > gap_max):
                    gap_max = alt
        if gap_max is not None and gap_max - a.end_alt_ft > threshold_ft:
            return True
    return False


def _compute_navaid_track_json(
    metrics: FlightMetrics,
    *,
    db: Database,
    config: Config,
    navaid_cache: dict[tuple[int, int, int, int], list],
) -> str | None:
    """Emit the navaid_track JSON column value for one flight. Returns None
    when the flight has no qualifying alignment, so flights with no data
    stay uniform with legacy rows where the column is NULL."""
    from .navaid_alignment import detect_navaid_alignments
    from .navaids import flight_bbox_from_points, query_navaids_in_bbox

    bbox = flight_bbox_from_points(metrics.all_points, buffer_nm=config.navaid_bbox_buffer_nm)
    if bbox is None:
        return None

    # Quantize to 0.5 deg so near-duplicate routes share cached navaid rows.
    min_lat, max_lat, min_lon, max_lon = bbox
    key: tuple[int, int, int, int] = (
        int(math.floor(min_lat * 2)),
        int(math.floor(max_lat * 2)),
        int(math.floor(min_lon * 2)),
        int(math.floor(max_lon * 2)),
    )
    if key not in navaid_cache:
        navaid_cache[key] = [dict(r) for r in query_navaids_in_bbox(db.conn, *bbox)]
    navaids = navaid_cache[key]
    if not navaids:
        return None

    segments = detect_navaid_alignments(
        metrics.all_points,
        navaids=navaids,
        tolerance_deg=config.navaid_alignment_tolerance_deg,
        max_distance_nm=config.navaid_max_distance_nm,
        split_gap_secs=config.navaid_split_gap_secs,
        min_duration_secs=config.navaid_min_duration_secs,
        near_pass_max_nm=config.navaid_near_pass_max_nm,
        grid_min_count=config.navaid_grid_min_count,
    )
    if not segments:
        return None
    payload = [
        {
            "navaid_ident": s.navaid_ident,
            "start_ts": s.start_ts,
            "end_ts": s.end_ts,
            "min_distance_nm": round(s.min_distance_km / 1.852, 2),
        }
        for s in segments
    ]
    return json.dumps(payload, ensure_ascii=True)


def _warn_on_type_code_drift(db: Database, hex_code: str, registry_row: dict, type_code: str | None) -> None:
    """Warn when the registry recorded a lot of metadata drift AND at least
    one drift event disagreed on type_code. Pure description drift is noise;
    a type_code conflict indicates the registry entry may be wrong (e.g.
    GLF6 vs GA8C on adf64f)."""
    drift_count = registry_row.get("metadata_drift_count", 0)
    if drift_count <= 20:
        return
    try:
        drift_json = db.conn.execute(
            "SELECT metadata_drift_values FROM aircraft_registry WHERE icao = ?",
            (hex_code,),
        ).fetchone()
        if drift_json and drift_json[0]:
            drift_vals = json.loads(drift_json[0])
            type_conflicts = [d for d in drift_vals if d.get("type_code") and d["type_code"] != type_code]
            if type_conflicts:
                conflict_types = ", ".join(f"{d['type_code']}({d['count']})" for d in type_conflicts)
                print(
                    f"  WARNING: {hex_code} has {drift_count} metadata drift events "
                    f"with type_code conflicts: {type_code} vs {conflict_types}"
                )
    except Exception:
        pass


def _resolve_registry_metadata(db: Database, hex_code: str, trace_days: list) -> tuple[str | None, str | None]:
    """Populate/refresh aircraft_registry and return the authoritative
    ``(type_code, owner_operator)`` for this ICAO.

    type_code drives endurance, hover gating and mission rules, so the
    registry value wins. Falls back to the first trace_days row carrying
    each field when the registry write fails (e.g. tests using a MagicMock
    db) or the registry has no value for it.
    """
    type_code: str | None = None
    owner_operator: str | None = None
    try:
        registry_row = db.upsert_aircraft_registry(hex_code, trace_days)
    except Exception:
        registry_row = None
    if isinstance(registry_row, dict):
        if registry_row.get("type_code"):
            type_code = registry_row["type_code"]
        if registry_row.get("owner_operator"):
            owner_operator = registry_row["owner_operator"]
        _warn_on_type_code_drift(db, hex_code, registry_row, type_code)
    if type_code is None:
        for row in trace_days:
            if row["type_code"]:
                type_code = row["type_code"]
                break
    if owner_operator is None:
        for row in trace_days:
            if row["owner_operator"]:
                owner_operator = row["owner_operator"]
                break
    return type_code, owner_operator


def _load_and_merge(trace_days: list, hex_code: str, config: Config) -> tuple[list[tuple[str, float, list]], set[str]]:
    """Decode the trace rows once and merge each date's (possibly
    multi-source) rows into a single chronological trace.

    Returns ``(merged_days, all_sources)``:
      - merged_days: ``[(date, base_timestamp, trace), ...]`` in date order,
        the input the state machine walks.
      - all_sources: union of every source that contributed a trace day;
        stamped on every flight's FlightMetrics.

    Spoof rejection (issue #22) is flight-scoped: it reads FlightMetrics
    counters the state machine accumulates per point (see
    _flight_is_spoofed), so it no longer needs a day-level pre-scan here.
    Day-level pooling (pool_spoof_scores) still exists, but only for the
    events layer, which scans independently of the extract path.

    Needs no Database handle: the rows are loaded by the caller, which is
    also where any "which days to process" narrowing belongs.
    """
    parsed_days = list(iter_parsed_trace_days(trace_days, hex_code))

    by_date: dict[str, list[tuple[sqlite3.Row, list]]] = defaultdict(list)
    for row, trace in parsed_days:
        by_date[row["date"]].append((row, trace))

    merged_days: list[tuple[str, float, list]] = []
    all_sources: set[str] = set()
    for day_date in sorted(by_date.keys()):
        date_str, base_ts, trace, day_sources = _merge_trace_rows(by_date[day_date], config)
        merged_days.append((date_str, base_ts, trace))
        all_sources |= day_sources
    return merged_days, all_sources


def _scan_state_machine(
    merged_days: list[tuple[str, float, list]],
    config: Config,
    *,
    hex_code: str,
    all_sources: set[str],
    seed_callsign: str | None = None,
) -> tuple[list[Flight], list[FlightMetrics]]:
    """Walk the merged days point by point through the ground/airborne state
    machine, emitting one (Flight, FlightMetrics) pair per detected fragment.

    Fragments come out raw: no duration, no noise filtering and no
    stitching - _run_state_machine layers those on top.

    ``seed_callsign`` is the callsign the walk starts out carrying. It is
    always None for a full extract, which starts before any trace day; an
    incremental extract passes what the full walk would have been holding
    when it reached the window's first day, because a coverage gap resets
    the flight state but not the last callsign heard.
    """
    max_point_gap_secs = config.max_point_gap_minutes * 60.0
    post_landing_window_secs = config.post_landing_window_secs
    post_landing_max_points = config.post_landing_max_points

    flights: list[Flight] = []
    metrics_list: list[FlightMetrics] = []

    # State machine variables
    state: str | None = None  # None / "ground" / "airborne" / "post_landing"
    prev_ground_point = None  # (lat, lon, abs_time, day_date)
    pending_flight: Flight | None = None
    pending_metrics: FlightMetrics | None = None
    current_callsign: str | None = seed_callsign
    prev_day_date: str | None = None
    ground_count_before_takeoff = 0
    prev_point_ts: float | None = None
    post_landing_start_ts: float | None = None
    # OpenSky data lacks ground speed. When gs is None, require two
    # consecutive ground points before landing: the first sets this flag
    # and is otherwise ignored; the second confirms the transition.
    prev_was_ground_no_gs = False

    def _close_pending() -> None:
        """Finalize the current pending flight (if any) and reset state variables."""
        nonlocal pending_flight, pending_metrics, state, prev_ground_point
        nonlocal ground_count_before_takeoff, post_landing_start_ts, prev_was_ground_no_gs
        if pending_flight is not None:
            flights.append(pending_flight)
            metrics_list.append(pending_metrics or FlightMetrics())
        pending_flight = None
        pending_metrics = None
        state = None
        prev_ground_point = None
        ground_count_before_takeoff = 0
        post_landing_start_ts = None
        prev_was_ground_no_gs = False

    for day_date, day_timestamp, trace in merged_days:
        # Reset state on large cross-day gap
        if prev_day_date is not None and _day_gap_resets_state(prev_day_date, day_date, config):
            _close_pending()
            prev_point_ts = None

        prev_day_date = day_date

        for point in trace:
            time_offset = point[0]
            lat = point[1]
            lon = point[2]
            abs_ts = day_timestamp + time_offset
            abs_time = datetime.fromtimestamp(abs_ts, tz=UTC)
            point_data = _extract_point_fields(point, abs_ts, lat, lon)
            baro_alt = point_data.baro_alt
            gs = point_data.gs
            geom_alt = point_data.geom_alt

            # Update callsign from PointData
            if point_data.callsign:
                current_callsign = point_data.callsign

            # Intra-trace gap check: any gap longer than max_point_gap_minutes
            # forces a flight close. Real operations rarely have more than a
            # few minutes between trace points; multi-hour gaps are coverage
            # holes that the state machine should not stitch across. Uses
            # abs() so a backwards-in-time jump (phantom point with a stale
            # timestamp that survives sorting via a duplicate offset) also
            # triggers a close instead of silently corrupting state.
            if prev_point_ts is not None and abs(abs_ts - prev_point_ts) > max_point_gap_secs:
                _close_pending()
            prev_point_ts = abs_ts

            # Classify the point using baro + geom altitude fusion
            point_state, point_reason = classify_ground_state(
                baro_alt,
                geom_alt,
                gs,
                landing_speed_threshold=config.landing_speed_threshold_kts,
                baro_error_geom_threshold=config.baro_error_geom_threshold_ft,
            )

            # Record metrics for pending flight (all points, including ground)
            if pending_metrics is not None:
                pending_metrics.record_point(
                    point_data,
                    ground_state=point_state,
                    ground_reason=point_reason,
                    config=config,
                    landing_speed_threshold=config.landing_speed_threshold_kts,
                )

            is_ground = point_state == "ground"
            is_airborne = point_state == "airborne"

            # ---- STATE TRANSITIONS ----

            if state is None:
                if is_ground:
                    state = "ground"
                    prev_ground_point = (lat, lon, abs_time, day_date)
                    ground_count_before_takeoff += 1
                elif is_airborne:
                    # First observed point is already airborne: this is a
                    # "found_mid_flight" situation. Open a pending flight
                    # so we can at least track signal loss.
                    state = "airborne"
                    pending_flight = Flight(
                        icao=hex_code,
                        takeoff_time=abs_time,
                        takeoff_lat=lat,
                        takeoff_lon=lon,
                        takeoff_date=day_date,
                        callsign=current_callsign,
                        extractor_version=EXTRACTOR_VERSION,
                    )
                    pending_metrics = FlightMetrics(sources=set(all_sources))
                    pending_metrics.takeoff_type = "found_mid_flight"
                    pending_metrics.ground_points_at_takeoff = 0
                    pending_metrics.record_point(
                        point_data,
                        ground_state=point_state,
                        ground_reason=point_reason,
                        config=config,
                        landing_speed_threshold=config.landing_speed_threshold_kts,
                    )
                    ground_count_before_takeoff = 0
                # else: unknown - leave state as None
                continue

            if state == "ground":
                if is_airborne:
                    # TAKEOFF observed: use the previous ground point for the airport fix
                    if prev_ground_point:
                        to_lat, to_lon, to_time, to_date = prev_ground_point
                    else:
                        to_lat, to_lon, to_time, to_date = lat, lon, abs_time, day_date

                    state = "airborne"
                    pending_flight = Flight(
                        icao=hex_code,
                        takeoff_time=to_time,
                        takeoff_lat=to_lat,
                        takeoff_lon=to_lon,
                        takeoff_date=to_date,
                        callsign=current_callsign,
                        extractor_version=EXTRACTOR_VERSION,
                    )
                    pending_metrics = FlightMetrics(sources=set(all_sources))
                    pending_metrics.takeoff_type = "observed"
                    pending_metrics.ground_points_at_takeoff = ground_count_before_takeoff
                    pending_metrics.record_point(
                        point_data,
                        ground_state=point_state,
                        ground_reason=point_reason,
                        config=config,
                        landing_speed_threshold=config.landing_speed_threshold_kts,
                    )
                    ground_count_before_takeoff = 0
                elif is_ground:
                    ground_count_before_takeoff += 1
                    prev_ground_point = (lat, lon, abs_time, day_date)
                # unknown point: ignore
                continue

            if state == "airborne":
                if is_ground:
                    # OpenSky hysteresis: when gs is None we require two
                    # consecutive ground points to confirm a landing, because
                    # single-point altitude glitches without a speed signal
                    # are too risky to trust.
                    if gs is None and not prev_was_ground_no_gs:
                        prev_was_ground_no_gs = True
                        continue
                    prev_was_ground_no_gs = False

                    # LANDING transition. Record the landing info and enter
                    # post-landing mode to collect a few more ground points.
                    if pending_metrics is not None:
                        pending_metrics.record_landing_ground_point(lat, lon)
                        pending_metrics.landing_transition_ts = abs_ts
                    if pending_flight is not None:
                        pending_flight.landing_time = abs_time
                        pending_flight.landing_lat = lat
                        pending_flight.landing_lon = lon
                        pending_flight.landing_date = day_date
                    state = "post_landing"
                    post_landing_start_ts = abs_ts
                    prev_ground_point = (lat, lon, abs_time, day_date)
                else:
                    # Still airborne or unknown - reset the OpenSky hysteresis
                    prev_was_ground_no_gs = False
                continue

            if state == "post_landing":
                window_expired = (
                    post_landing_start_ts is not None and (abs_ts - post_landing_start_ts) > post_landing_window_secs
                )
                # count_expired is re-evaluated after recording the current
                # ground point below so the cap lands exactly on
                # post_landing_max_points, not max+1.

                if is_airborne:
                    # Aircraft took off again right after landing (touch and
                    # go or quick stop). Close the current flight and start a
                    # new pending flight immediately.
                    finalized_flight = pending_flight
                    finalized_metrics = pending_metrics
                    pending_flight = None
                    pending_metrics = None
                    if finalized_flight is not None:
                        flights.append(finalized_flight)
                        metrics_list.append(finalized_metrics or FlightMetrics())

                    state = "airborne"
                    if prev_ground_point:
                        to_lat, to_lon, to_time, to_date = prev_ground_point
                    else:
                        to_lat, to_lon, to_time, to_date = lat, lon, abs_time, day_date
                    pending_flight = Flight(
                        icao=hex_code,
                        takeoff_time=to_time,
                        takeoff_lat=to_lat,
                        takeoff_lon=to_lon,
                        takeoff_date=to_date,
                        callsign=current_callsign,
                        extractor_version=EXTRACTOR_VERSION,
                    )
                    pending_metrics = FlightMetrics(sources=set(all_sources))
                    pending_metrics.takeoff_type = "observed"
                    pending_metrics.ground_points_at_takeoff = 1
                    pending_metrics.record_point(
                        point_data,
                        ground_state=point_state,
                        ground_reason=point_reason,
                        config=config,
                        landing_speed_threshold=config.landing_speed_threshold_kts,
                    )
                    ground_count_before_takeoff = 0
                    post_landing_start_ts = None
                    continue

                if is_ground:
                    # Collect another post-landing ground point
                    if pending_metrics is not None:
                        pending_metrics.record_landing_ground_point(lat, lon)
                    prev_ground_point = (lat, lon, abs_time, day_date)

                # Check count expiry *after* recording the point so the cap
                # value is respected exactly (previously off-by-one).
                count_expired = (
                    pending_metrics is not None and pending_metrics.ground_points_at_landing >= post_landing_max_points
                )

                if window_expired or count_expired:
                    # Finalize the landing and fall back to ground state.
                    # Remember the ground-point count before clearing metrics
                    # so the next takeoff's ground_count_before_takeoff reflects
                    # the points we collected during the post-landing window.
                    pre_clear_gp = pending_metrics.ground_points_at_landing if pending_metrics else 1
                    if pending_flight is not None:
                        flights.append(pending_flight)
                        metrics_list.append(pending_metrics or FlightMetrics())
                    pending_flight = None
                    pending_metrics = None
                    state = "ground"
                    ground_count_before_takeoff = max(1, pre_clear_gp)
                    post_landing_start_ts = None
                    continue

    # End of all trace days: flush any pending flight
    if pending_flight is not None:
        flights.append(pending_flight)
        metrics_list.append(pending_metrics or FlightMetrics())

    return flights, metrics_list


def _run_state_machine(
    merged_days: list[tuple[str, float, list]],
    config: Config,
    *,
    hex_code: str,
    all_sources: set[str],
    type_code: str | None,
    seed_callsign: str | None = None,
) -> list[tuple[Flight, FlightMetrics]]:
    """Turn merged trace days into the flight fragments enrichment will see.

    The sequence is load-bearing: fragments get a duration first because the
    noise filter reads it, the filter runs before last_seen backfill and
    stitching so dropped slivers can never be stitched onto a real flight,
    and _stitch_fragments refreshes duration on any pair it merges.
    """
    flights, metrics_list = _scan_state_machine(
        merged_days,
        config,
        hex_code=hex_code,
        all_sources=all_sources,
        seed_callsign=seed_callsign,
    )

    # Compute durations for every flight from first/last trace point.
    # Previously duration was only set on flights with a landing transition;
    # signal_lost / dropped_on_approach flights got NULL. Now every flight
    # with any data has a duration (time airborne or time observed).
    for flight, metrics in zip(flights, metrics_list, strict=True):
        if metrics.first_point_ts is not None and metrics.last_point_ts is not None:
            span = metrics.last_point_ts - metrics.first_point_ts
            flight.duration_minutes = round(span / 60.0, 1)

    # Filter: drop bogus single-point "flights" (e.g. leftover phantom points
    # from readsb cache glitches that survived dedup because their nearest
    # real neighbor was outside the dedup window) and taxi-length flights
    # that barely moved.
    valid_flights = []
    valid_metrics = []
    for flight, metrics in zip(flights, metrics_list, strict=True):
        # A one-point "flight" has no trajectory and no usable metrics.
        if metrics.data_points <= 1:
            continue
        if (
            flight.duration_minutes is not None
            and flight.duration_minutes < config.min_flight_minutes
            and flight.landing_lat is not None
        ):
            dist = haversine_km(flight.takeoff_lat, flight.takeoff_lon, flight.landing_lat, flight.landing_lon)
            if dist < config.min_flight_distance_km:
                continue
        valid_flights.append(flight)
        valid_metrics.append(metrics)

    # Populate last_seen_* from metrics regardless of landing outcome
    for flight, metrics in zip(valid_flights, valid_metrics, strict=True):
        if metrics.last_seen_ts is not None:
            flight.last_seen_lat = metrics.last_seen_lat
            flight.last_seen_lon = metrics.last_seen_lon
            flight.last_seen_alt_ft = metrics.last_seen_alt_ft
            flight.last_seen_time = datetime.fromtimestamp(metrics.last_seen_ts, tz=UTC)

    # Run the stitch_fragments pass: merge signal_lost / dropped_on_approach
    # followed by a found_mid_flight fragment when they are plausibly the
    # same continuous flight with a coverage hole in the middle. The type_code
    # lets long-endurance aircraft stitch across wider gaps than the default
    # 90-min window.
    valid_flights, valid_metrics = _stitch_fragments(valid_flights, valid_metrics, config, type_code=type_code)

    return list(zip(valid_flights, valid_metrics, strict=True))


@dataclass
class _EnrichContext:
    """Handles and caches shared by every per-flight enrichment stage.

    The caches live for one extract run. An aircraft typically has 2-5 home
    airports that get hit over and over, so without them a fleet-sized
    extract would issue 2N elevation/runway queries against the same ICAOs,
    and near-duplicate routes would re-query the same navaid boxes.
    """

    db: Database
    config: Config
    hex_code: str
    type_code: str | None
    owner_operator: str | None
    # True when this run rebuilds only part of the aircraft's history, so
    # the ICAO-wide post-passes have to account for rows they never saw.
    incremental: bool = False
    airport_elev_cache: dict[str, int | None] = field(default_factory=dict)
    runway_cache: dict[str, list] = field(default_factory=dict)
    navaid_cache: dict[tuple[int, int, int, int], list] = field(default_factory=dict)

    def airport_elevation(self, icao: str) -> int | None:
        """Field elevation in feet, or None when the airport is not in our DB.
        A None answer is cached too, so a miss costs one query per run."""
        if icao not in self.airport_elev_cache:
            self.airport_elev_cache[icao] = self.db.get_airport_elevation(icao)
        return self.airport_elev_cache[icao]

    def runways(self, icao: str) -> list:
        """Runway-end rows for an airport; empty when we have no geometry."""
        if icao not in self.runway_cache:
            self.runway_cache[icao] = self.db.get_runways_for_airport(icao)
        return self.runway_cache[icao]


def _flight_is_noise(flight: Flight, metrics: FlightMetrics, config: Config) -> bool:
    """True for fragments that should be dropped instead of persisted.

    Reads landing_type, so it can only run after classify_landing.
    """
    # Drop signal_lost, uncertain, and dropped_on_approach slivers that are
    # BOTH short AND sparse. Confirmed landings (legitimate quick helicopter
    # hops) are kept regardless of size.
    if flight.landing_type in (
        LandingType.SIGNAL_LOST,
        LandingType.UNCERTAIN,
        LandingType.DROPPED_ON_APPROACH,
    ) and (
        flight.duration_minutes is not None
        and flight.duration_minutes < config.min_viable_flight_minutes
        and metrics.data_points < config.min_viable_flight_points
    ):
        return True

    # Stationary broadcaster (transponder left on at the ramp).
    return (
        metrics.path_length_km < config.stationary_path_km
        and metrics.max_distance_from_origin_km < config.stationary_path_km
        and metrics.max_altitude < config.stationary_max_alt_ft
        and metrics.max_gs_kt < config.stationary_max_gs_kt
    )


def _match_airports(
    flight: Flight,
    metrics: FlightMetrics,
    ctx: _EnrichContext,
    *,
    anchor: LandingAnchor | None,
    has_landing: bool,
) -> None:
    """D1: match both ends of the flight to airports and identify the
    departure runway. Each end splits on-field (within
    airport_on_field_threshold_km, written to origin/destination_icao) from
    merely nearest (written to the nearest_* columns).

    The destination match is skipped for signal_lost / dropped_on_approach
    flights; those get a probable destination inferred later instead.
    """
    config = ctx.config
    db = ctx.db

    origin = find_nearest_airport(db, flight.takeoff_lat, flight.takeoff_lon, config)
    if origin:
        if origin.distance_km <= config.airport_on_field_threshold_km:
            flight.origin_icao = origin.ident
            flight.origin_name = origin.name
            flight.origin_distance_km = origin.distance_km
        else:
            flight.nearest_origin_icao = origin.ident
            flight.nearest_origin_distance_km = origin.distance_km

    # --- Takeoff runway identification (adsbtrack/takeoff_runway.py) ---
    # Scales the GS floor down to takeoff_runway_min_gs_kt_low (60 kt) when
    # the effective type is a rotorcraft (H-prefix or in
    # config.helicopter_types) or a light piston single listed in
    # config.takeoff_low_gs_types.
    takeoff_origin_icao = flight.origin_icao or flight.nearest_origin_icao
    if takeoff_origin_icao:
        origin_elev = ctx.airport_elevation(takeoff_origin_icao)
        origin_runways = ctx.runways(takeoff_origin_icao)
        if origin_runways:
            effective_type = ctx.type_code or ""
            is_low_gs = (
                effective_type.startswith("H")
                or effective_type in config.takeoff_low_gs_types
                or effective_type in config.helicopter_types
            )
            min_gs = config.takeoff_runway_min_gs_kt_low if is_low_gs else config.takeoff_runway_min_gs_kt_default
            to_result: TakeoffRunwayResult | None = detect_takeoff_runway(
                metrics,
                # Fallback 0.0 is safe because the `if origin_runways:`
                # guard above only fires when the airport is in our DB
                # (runways and elevations come from the same OurAirports
                # load, so both are present or both are absent).
                airport_elev_ft=float(origin_elev) if origin_elev is not None else 0.0,
                runway_ends=[dict(r) for r in origin_runways],
                config=config,
                min_gs_kt=min_gs,
            )
            if to_result is not None:
                flight.takeoff_runway = to_result.runway_name

    if has_landing and flight.landing_type not in (LandingType.SIGNAL_LOST, LandingType.DROPPED_ON_APPROACH):
        # Use anchor (alt-min in final window) when available; fall back
        # to landing_lat/lon only if compute_landing_anchor returned None
        # (shouldn't happen on a has_landing flight but guards against
        # empty recent_points).
        dest_lat = anchor.lat if anchor is not None else flight.landing_lat
        dest_lon = anchor.lon if anchor is not None else flight.landing_lon
        dest = find_nearest_airport(db, dest_lat, dest_lon, config)
        if dest:
            if dest.distance_km <= config.airport_on_field_threshold_km:
                flight.destination_icao = dest.ident
                flight.destination_name = dest.name
                flight.destination_distance_km = dest.distance_km
            else:
                flight.nearest_destination_icao = dest.ident
                flight.nearest_destination_distance_km = dest.distance_km


def _copy_metrics_to_flight(flight: Flight, metrics: FlightMetrics, config: Config) -> None:
    """Copy the accumulator's counters onto the Flight row."""
    flight.data_points = metrics.data_points
    flight.sources = ",".join(sorted(metrics.sources)) if metrics.sources else None
    # Store raw persistence-filtered altitude; the ceiling cap is applied
    # later, once derive_all has set type_override.
    flight.max_altitude = metrics.max_altitude if metrics.max_altitude > 0 else None
    flight.ground_points_at_landing = metrics.ground_points_at_landing
    flight.ground_points_at_takeoff = metrics.ground_points_at_takeoff
    flight.baro_error_points = metrics.baro_error_points

    # Position source mix. When data_points is zero or the trace carried
    # no source tags, leave every bucket at 0.0 so downstream queries
    # can rely on non-null values. other_pct covers unknown/Mode-S-only
    # rebroadcasts; adsc_pct breaks out CPDLC/ADS-C oceanic reports.
    total = metrics.data_points
    if total > 0:
        flight.mlat_pct = round(metrics.mlat_points * 100.0 / total, 2)
        flight.tisb_pct = round(metrics.tisb_points * 100.0 / total, 2)
        flight.adsb_pct = round(metrics.adsb_points * 100.0 / total, 2)
        flight.other_pct = round(metrics.other_points * 100.0 / total, 2)
        flight.adsc_pct = round(metrics.adsc_points * 100.0 / total, 2)
    else:
        flight.mlat_pct = 0.0
        flight.tisb_pct = 0.0
        flight.adsb_pct = 0.0
        flight.other_pct = 0.0
        flight.adsc_pct = 0.0

    # Integrity/jamming surface columns (issue #30): the same FlightMetrics
    # counters the spoof gate reads (_flight_is_spoofed), persisted on the
    # flights that survive it. The gate quarantines outright fabrications;
    # the lower Config integrity_flag_* thresholds mark kept-but-degraded
    # flights -- real traffic that transited a GPS-jamming corridor.
    v2 = metrics.v2_samples
    flight.v2_sample_count = v2
    degraded_pct = 100.0 * metrics.v2_sil0 / v2 if v2 > 0 else None
    flight.integrity_degraded_pct = round(degraded_pct, 2) if degraded_pct is not None else None
    teleport = metrics.max_implied_speed_kt
    flight.max_implied_speed_kt = round(teleport, 1) if teleport is not None else None
    has_v2_signal = degraded_pct is not None and v2 >= config.spoof_min_v2_samples
    degraded = has_v2_signal and degraded_pct >= config.integrity_flag_degraded_pct
    # The teleport trigger requires corroborating degradation (see the
    # Config comment): standalone implied-speed spikes are dominated by
    # position-decode garbage in historical traces, not GPS interference.
    teleported = (
        teleport is not None
        and teleport > config.integrity_flag_teleport_kt
        and has_v2_signal
        and degraded_pct >= config.integrity_flag_teleport_min_degraded_pct
    )
    flight.integrity_flagged = 1 if (degraded or teleported) else 0


def _apply_type_caps(flight: Flight, ctx: _EnrichContext) -> None:
    """Clamp altitude and ground speed to the effective type's envelope.

    Must run after features.derive_all: type_override is what derive_all
    sets, and it wins over the registry type_code here. Applying the
    ceiling earlier (off a preliminary ae69xx altitude check) missed
    flights that only derive_all classifies via cruise_gs.
    """
    effective_type = flight.type_override or ctx.type_code
    if flight.max_altitude is not None:
        ceiling = TYPE_CEILINGS.get(effective_type or "", 60_000)
        # only give 10% tolerance when the flight has
        # coherent AP data. Without AP, or when the AP target
        # wildly disagrees with max_altitude (>5,000 ft delta --
        # e.g. S92 a7a622 AP=3,008 vs alt=16,500), cap at exactly
        # the book ceiling so corrupt spikes don't exceed physical
        # limits.
        ap = flight.autopilot_target_alt_ft
        ap_coherent = ap is not None and abs(flight.max_altitude - ap) <= 5000
        alt_cap = int(ceiling * 1.1) if ap_coherent else ceiling
        if flight.max_altitude > alt_cap:
            flight.max_altitude = alt_cap
        # Also re-cap cruise_alt_ft after ceiling adjustment
        if flight.cruise_alt_ft is not None and flight.cruise_alt_ft > flight.max_altitude:
            flight.cruise_alt_ft = flight.max_altitude

    if flight.max_gs_kt is not None:
        gs_ceiling = TYPE_MAX_GS.get(effective_type or "", 800)
        gs_cap = int(gs_ceiling * 1.1)
        if flight.max_gs_kt > gs_cap:
            flight.max_gs_kt = gs_cap
        # Both cruise_gs and max_gs must share the same cap so
        # cruise <= max always holds. The v15 removal of this line
        # caused 3,134 flights to violate the invariant.
        if flight.cruise_gs_kt is not None and flight.cruise_gs_kt > gs_cap:
            flight.cruise_gs_kt = gs_cap


def _infer_probable_destination(
    flight: Flight, metrics: FlightMetrics, ctx: _EnrichContext, *, anchor: LandingAnchor | None
) -> None:
    """v3 destination inference for dropped / signal_lost flights.

    Queries candidates around the alt-min anchor (falling back to
    last_seen), i.e. "where the aircraft was trying to land" rather than
    where it was last observed, which may be at altitude.
    """
    if flight.landing_type not in (LandingType.SIGNAL_LOST, LandingType.DROPPED_ON_APPROACH):
        return
    ref_lat = anchor.lat if anchor is not None else flight.last_seen_lat
    ref_lon = anchor.lon if anchor is not None else flight.last_seen_lon
    if ref_lat is None or ref_lon is None:
        return
    try:
        candidates = ctx.db.find_nearby_airports(
            ref_lat,
            ref_lon,
            delta=ctx.config.prob_dest_search_delta,
            types=ctx.config.airport_types,
        )
    except Exception:
        candidates = []
    infer = features.infer_destination(
        flight=flight,
        metrics=metrics,
        candidates=list(candidates),
        config=ctx.config,
        anchor_lat=ref_lat,
        anchor_lon=ref_lon,
    )
    flight.probable_destination_icao = infer["probable_destination_icao"]
    flight.probable_destination_distance_km = infer["probable_destination_distance_km"]
    flight.probable_destination_confidence = infer["probable_destination_confidence"]


def _apply_runway_alignment(flight: Flight, metrics: FlightMetrics, ctx: _EnrichContext) -> None:
    """ILS alignment plus the pattern_cycles / go-around signals derived
    from the same segment scan.

    Runs after destination inference: the candidate airport is resolved in
    priority order on-field match -> nearest hit -> probable destination,
    and that last one only exists once _infer_probable_destination has run.
    """
    config = ctx.config
    alignment_icao = flight.destination_icao or flight.nearest_destination_icao or flight.probable_destination_icao
    alignment: IlsAlignmentResult | None = None
    # Fallback 0.0 is safe because the `if runway_rows:` guard below only
    # fires when the airport is in our DB (runways and elevations come from
    # the same OurAirports load).
    airport_elev_ft = 0.0
    all_segments: list[IlsAlignmentResult] = []
    if alignment_icao:
        elev = ctx.airport_elevation(alignment_icao)
        if elev is not None:
            airport_elev_ft = float(elev)
        runway_rows = ctx.runways(alignment_icao)
        if runway_rows:
            # Single pass: all-segments feeds both the longest-wins
            # ILS signal and the pattern_cycles/go-around block below.
            all_segments = detect_all_ils_alignments(
                metrics.all_points,
                airport_elev_ft=airport_elev_ft,
                runway_ends=[dict(r) for r in runway_rows],
                max_offset_m=config.ils_alignment_max_offset_m,
                max_ft_above_airport=config.ils_alignment_max_ft_above_airport,
                split_gap_secs=config.ils_alignment_split_gap_secs,
                min_duration_secs=config.ils_alignment_min_duration_secs,
            )
            if all_segments:
                # Longest wins; tie-break on earliest first_ts for determinism.
                alignment = max(all_segments, key=lambda s: (s.duration_secs, -s.first_ts))

    if alignment is not None:
        flight.aligned_runway = alignment.runway_name
        flight.aligned_seconds = alignment.duration_secs
        flight.aligned_min_offset_m = alignment.min_offset_m

        # Additive confidence bonus (clamped to 1.0). Applied only when
        # `landing_confidence` was already set to a non-None value; don't
        # revive a NULL landing_confidence on types that deliberately
        # have none.
        if flight.landing_confidence is not None:
            if alignment.duration_secs >= config.ils_alignment_bonus_long_secs:
                bonus = config.ils_alignment_bonus_long
            elif alignment.duration_secs >= config.ils_alignment_bonus_short_secs:
                bonus = config.ils_alignment_bonus_short
            else:
                bonus = 0.0
            if bonus > 0.0:
                flight.landing_confidence = round(min(1.0, flight.landing_confidence + bonus), 2)

        # Classification upgrade: a signal_lost flight with a 60s+
        # alignment segment at low altitude is indistinguishable from
        # dropped_on_approach. Promote so downstream sees the stronger
        # type. Altitude gate uses last_airborne_alt vs airport_elev +
        # max_ft_above_airport to match the detector's AGL cap.
        if (
            flight.landing_type == LandingType.SIGNAL_LOST
            and alignment.duration_secs >= config.ils_alignment_bonus_long_secs
            and metrics.last_airborne_alt is not None
            and metrics.last_airborne_alt < airport_elev_ft + config.ils_alignment_max_ft_above_airport
        ):
            flight.landing_type = LandingType.DROPPED_ON_APPROACH

    # --- Go-around + pattern_cycles (adsbtrack/ils_alignment.py) ---
    # An empty all_segments means no candidate airport, no runway data, or
    # no qualifying alignment.
    flight.pattern_cycles = len(all_segments)
    flight.had_go_around = 1 if _any_climb_between(all_segments, metrics.all_points, threshold_ft=500.0) else 0


def _apply_pattern_mission_override(flight: Flight) -> None:
    """Upgrade same-airport flights with 2+ aligned segments to mission_type
    "pattern". Only applies when the classifier already produced a generic
    bucket (unknown / transport) or the existing pattern rule already fired
    - more specific buckets (training, ems_hems, survey, offshore,
    exec_charter) are preserved."""
    if (
        flight.origin_icao is not None
        and flight.destination_icao is not None
        and flight.origin_icao == flight.destination_icao
        and flight.pattern_cycles is not None
        and flight.pattern_cycles >= 2
        and flight.mission_type in ("unknown", "transport", "pattern")
    ):
        flight.mission_type = "pattern"


def _enrich_flight(flight: Flight, metrics: FlightMetrics, ctx: _EnrichContext) -> bool:
    """Derive every column of one flight, and report whether it survives.

    Returns False when the flight is dropped as noise; the caller must then
    neither persist it nor let it advance the turnaround chain.

    The call order below is the whole ordering contract, which used to live
    in comments scattered through a 340-line loop body: classify_landing
    feeds the noise guards and the airport match; airport matching feeds
    the confidence scores and derive_all; derive_all sets type_override,
    which the caps need; destination inference supplies the last-resort
    airport the alignment scan falls back to; and the pattern override
    reads the pattern_cycles that scan produces.
    """
    config = ctx.config
    has_landing = flight.landing_lat is not None

    flight.takeoff_type = metrics.takeoff_type
    flight.landing_type = classify_landing(
        metrics,
        has_landing,
        config=config,
        duration_minutes=flight.duration_minutes,
        type_code=ctx.type_code,
    )

    # Landing airport-matching anchor. Altitude-minimum point within the
    # final N minutes is a stronger estimator than the last observed
    # point for flights that drifted laterally or lost signal at
    # altitude. Falls back to last_point when tail altitudes are missing.
    anchor = compute_landing_anchor(metrics, window_minutes=config.landing_anchor_window_minutes)
    flight.landing_anchor_method = anchor.method if anchor is not None else None

    if _flight_is_noise(flight, metrics, config):
        return False

    _match_airports(flight, metrics, ctx, anchor=anchor, has_landing=has_landing)

    # Single source of truth for duration_minutes. Compute from wall-clock
    # (landing_time or last_seen_time) - takeoff_time, not from the metric
    # span (last_point_ts - first_point_ts): the metric span misses
    # signal-gap time on stitched flights. Must precede score_confidence
    # and derive_all, which both read duration_minutes.
    end_time = flight.landing_time or flight.last_seen_time
    if end_time is not None:
        wall_secs = (end_time - flight.takeoff_time).total_seconds()
        if wall_secs > 0:
            flight.duration_minutes = round(wall_secs / 60.0, 1)

    takeoff_conf, landing_conf = score_confidence(
        metrics,
        has_landing,
        flight.landing_type,
        origin_distance_km=flight.origin_distance_km,
        dest_distance_km=flight.destination_distance_km,
        duration_minutes=flight.duration_minutes,
    )
    flight.takeoff_confidence = takeoff_conf
    flight.landing_confidence = landing_conf

    _copy_metrics_to_flight(flight, metrics, config)

    features.derive_all(
        flight,
        metrics,
        config=config,
        type_code=ctx.type_code,
        owner_operator=ctx.owner_operator,
    )
    _apply_type_caps(flight, ctx)

    _infer_probable_destination(flight, metrics, ctx, anchor=anchor)
    _apply_runway_alignment(flight, metrics, ctx)
    _apply_pattern_mission_override(flight)

    flight.navaid_track = _compute_navaid_track_json(
        metrics,
        db=ctx.db,
        config=config,
        navaid_cache=ctx.navaid_cache,
    )
    # Drop all_points after the navaid pass so per-flight buffers don't
    # stay pinned until extract returns on multi-hundred-flight hexes.
    metrics.all_points.clear()
    return True


def _reject_if_spoofed(flight: Flight, metrics: FlightMetrics, ctx: _EnrichContext) -> bool:
    """Divert a spoofed broadcast into spoofed_broadcasts and report that it
    must not be persisted as a flight.

    Runs after every derivation so the gate sees final values for
    max_altitude / origin_icao / destination_icao / callsign, and the fully
    merged (post-stitch) FlightMetrics for its v2 integrity and implied-
    speed counters. A rejected flight is also kept out of the turnaround
    chain, so the next real flight's gap is not measured against a
    fabricated one.
    """
    if not ctx.config.reject_spoofed_flights:
        return False
    verdict = _flight_is_spoofed(flight, metrics, ctx.config)
    if verdict is None:
        return False
    reason, detail = verdict
    with contextlib.suppress(Exception):
        ctx.db.insert_spoofed_broadcast(
            icao=flight.icao,
            takeoff_time=flight.takeoff_time.isoformat(),
            landing_time=flight.landing_time.isoformat() if flight.landing_time else None,
            takeoff_date=flight.takeoff_date,
            callsign=flight.callsign,
            takeoff_lat=flight.takeoff_lat,
            takeoff_lon=flight.takeoff_lon,
            landing_lat=flight.landing_lat,
            landing_lon=flight.landing_lon,
            max_altitude=flight.max_altitude,
            data_points=flight.data_points,
            sources=flight.sources,
            origin_icao=flight.origin_icao,
            destination_icao=flight.destination_icao,
            reason=reason,
            reason_detail=json.dumps(detail),
        )
    return True


def _apply_sequence_fields(flight: Flight, prev_end_time: datetime | None) -> None:
    """Set the fields that describe this flight's place in the ICAO's
    sequence of persisted flights: turnaround from the previous one, its
    category, and the first/last observed flags.

    turnaround_minutes is capped at 72 hours (4320 min); anything longer is
    a collection gap or a parked aircraft, not a real turnaround, and would
    pollute fleet utilisation averages. Every flight gets a non-null
    category: the ones whose turnaround is NULL (over the cap, or a
    negative gap) fall into 'extended_gap'.
    """
    if prev_end_time is not None:
        turn_secs = (flight.takeoff_time - prev_end_time).total_seconds()
        if turn_secs >= 0:
            turn_min = round(turn_secs / 60.0, 1)
            flight.turnaround_minutes = turn_min if turn_min <= 4320.0 else None
        if flight.turnaround_minutes is not None:
            tm = flight.turnaround_minutes
            if tm < 30:
                flight.turnaround_category = "quick"
            elif tm < 240:
                flight.turnaround_category = "medium"
            elif tm < 1080:
                flight.turnaround_category = "overnight"
            else:
                flight.turnaround_category = "multi_day"
        else:
            flight.turnaround_category = "extended_gap"
        flight.is_first_observed_flight = 0
    else:
        # first observed flight for this ICAO
        flight.is_first_observed_flight = 1
        flight.turnaround_category = "first_observed"
    # default to 0; _run_post_passes sets the last flight to 1.
    flight.is_last_observed_flight = 0


def _run_post_passes(ctx: _EnrichContext, final_flights: list[Flight]) -> None:
    """ICAO-wide passes that can only run once every flight is written."""
    db = ctx.db
    hex_code = ctx.hex_code

    # mark the last flight for this ICAO and assign 'last_observed'
    # turnaround category when the category is still NULL (turnaround_minutes
    # was NULL or exceeded the 72-hour cap). This is the mirror of
    # is_first_observed_flight. Every flight now has a non-null category.
    if final_flights:
        last = final_flights[-1]
        last.is_last_observed_flight = 1
        if last.turnaround_category is None:
            last.turnaround_category = "last_observed"
        db.update_last_observed_flag(last)

    # An incremental run only sees its own window, so the flag may still sit
    # on a surviving row from before the boundary - and a window that
    # produced nothing at all still has to hand the flag to whatever row is
    # now newest. Settle it against the table rather than the window.
    if ctx.incremental:
        db.refresh_last_observed_flag(hex_code)

    # registry-level MIL_FW promotion. If an ae69xx ICAO has >= 3
    # flights classified MIL_FW, the registry type is wrong -- update it
    # and back-fill the remaining flights so ceiling/GS caps use the
    # correct envelope for the entire fleet history.
    if hex_code.startswith("ae69"):
        mil_fw_count = sum(1 for f in final_flights if f.type_override == "MIL_FW")
        if mil_fw_count >= 3:
            with contextlib.suppress(Exception):
                db.promote_registry_type(hex_code, "MIL_FW")
            # Back-fill: set type_override on flights that weren't classified
            # MIL_FW by the per-flight gate (low/slow flights on an ICAO that
            # is demonstrably fixed-wing). Re-apply ceiling/GS caps too.
            for f in final_flights:
                if f.type_override is None:
                    f.type_override = "MIL_FW"
                    # Re-cap with MIL_FW envelope
                    if f.max_altitude is not None:
                        ceiling = TYPE_CEILINGS.get("MIL_FW", 60_000)
                        alt_cap = int(ceiling * 1.1)
                        if f.max_altitude > alt_cap:
                            f.max_altitude = alt_cap
                    if f.max_gs_kt is not None:
                        gs_ceiling = TYPE_MAX_GS.get("MIL_FW", 800)
                        gs_cap = int(gs_ceiling * 1.1)
                        if f.max_gs_kt > gs_cap:
                            f.max_gs_kt = gs_cap
                    db.update_flight_type_override(f)

    # back-fill origin_helipad_id / destination_helipad_id from the
    # helipads table. Runs after all flights are inserted so the helipad
    # foreign keys survive INSERT OR REPLACE. Uses the same eps as DBSCAN
    # clustering (0.2 km).
    with contextlib.suppress(Exception):
        db.backfill_helipad_ids(hex_code, eps_km=0.2)

    # refresh materialized aircraft_stats for this ICAO
    with contextlib.suppress(Exception):
        db.refresh_aircraft_stats(hex_code)

    # purge registry entry if this ICAO ended up with zero flights
    # after extraction (e.g. all fragments were filtered out as noise).
    if not final_flights:
        with contextlib.suppress(Exception):
            db.purge_zero_flight_registry(hex_code)


def _persist(
    pairs: list[tuple[Flight, FlightMetrics]],
    ctx: _EnrichContext,
    *,
    prev_end_time: datetime | None = None,
) -> list[Flight]:
    """Enrich, gate and write each flight in trace order, then run the
    ICAO-wide post-passes. Returns the flights that reached the DB.

    The chain is sequential on purpose: only a flight that survives both
    _enrich_flight's noise guards and the spoof gate gets sequence fields,
    a row, and the right to become the previous flight the next turnaround
    is measured from.

    ``prev_end_time`` starts that chain. It is None for a full extract - the
    first flight written is the first one ever observed - and, for an
    incremental extract, the end of the last flight before the boundary, so
    the chain continues across the window edge instead of restarting.
    """
    final_flights: list[Flight] = []
    for flight, metrics in pairs:
        if not _enrich_flight(flight, metrics, ctx):
            continue
        if _reject_if_spoofed(flight, metrics, ctx):
            continue

        _apply_sequence_fields(flight, prev_end_time)
        prev_end_time = flight.landing_time or flight.last_seen_time

        ctx.db.insert_flight(flight)
        final_flights.append(flight)

    _run_post_passes(ctx, final_flights)
    return final_flights


def _is_safe_extract_boundary(prev_date: str, curr_date: str, config: Config, *, type_code: str | None) -> bool:
    """True when nothing the extractor computes can tie a flight ending on or
    before ``prev_date`` to one starting on or after ``curr_date``.

    Two independent linkages have to be ruled out:

    1. The state machine carries a pending flight from day to day, and only
       a gap of more than max_day_gap_days makes it drop one.
    2. _stitch_fragments then merges an unlanded fragment into the next
       found-mid-flight one purely on the wall-clock gap between them, with
       no regard for day boundaries. The narrowest gap two days this far
       apart can produce is (gap_days - 1) full days -- the last point of
       one day right before midnight, the first point of the other right
       after it -- so that has to exceed the stitch window as well.

    Turnaround chaining (_apply_sequence_fields) is a third link between
    flights, but no gap width breaks it: it reaches back to the previous
    flight however long ago that was. An incremental extract restores it
    from the database instead (see db.get_last_flight_end_before).
    """
    if not _day_gap_resets_state(prev_date, curr_date, config):
        return False
    gap_days = (date.fromisoformat(curr_date) - date.fromisoformat(prev_date)).days
    minutes_between = (gap_days - 1) * 24 * 60
    return minutes_between > _effective_stitch_gap_minutes(config, type_code)


def _find_extract_boundary(
    dates: list[str], since_date: date, config: Config, *, type_code: str | None = None
) -> str | None:
    """The earliest trace day an incremental extract has to re-process for
    new data landing on ``since_date``.

    ``dates`` is the ICAO's distinct trace-day dates, ascending. The walk
    starts at the first day at or after ``since_date`` and steps backwards
    while the day it is standing on could still be linked to the one before
    it, stopping at the first safe cut (or at the start of data, which makes
    the "incremental" extract a full one). Returns None when the aircraft
    has no trace day at or after ``since_date`` -- nothing landed inside the
    history, so there is nothing to rebuild.
    """
    index = bisect_left(dates, since_date.isoformat())
    if index >= len(dates):
        return None
    while index > 0 and not _is_safe_extract_boundary(dates[index - 1], dates[index], config, type_code=type_code):
        index -= 1
    return dates[index]


@dataclass
class _CarriedState:
    """What a full extract is already holding by the time it reaches an
    incremental window's first day, and therefore what that window has to be
    handed to produce the same rows.

    Empty for a full extract, which starts before the aircraft's first trace
    day and so carries nothing.
    """

    # Every source that ever contributed a trace day. Stamped on each
    # flight's metrics, so a window that only saw one source would otherwise
    # write a narrower `sources` column than a full run.
    sources: set[str] = field(default_factory=set)
    # Last callsign broadcast before the window. The state machine keeps
    # carrying one across a coverage gap even though it drops the flight.
    callsign: str | None = None
    # End of the last flight before the window, which the turnaround chain
    # measures the window's first flight against.
    prev_end_time: datetime | None = None


def _last_callsign_of_day(rows: list[sqlite3.Row], hex_code: str, config: Config) -> str | None:
    """The last callsign broadcast on one trace day, or None if it had none.

    Merges the day's sources exactly as the extract pipeline does, so the
    answer is the same point the state machine would have seen last.
    """
    parsed = list(iter_parsed_trace_days(rows, hex_code))
    if not parsed:
        return None
    _date_str, _base_ts, trace, _sources = _merge_trace_rows(parsed, config)
    for point in reversed(trace):
        detail = point[8] if len(point) > 8 and isinstance(point[8], dict) else None
        callsign = _detail_callsign(detail)
        if callsign is not None:
            return callsign
    return None


def _carried_callsign(db: Database, config: Config, hex_code: str, days_before: list[str]) -> str | None:
    """The callsign a full extract would still be carrying when it reaches
    the boundary: the last one broadcast on any earlier trace day.

    ``days_before`` is the aircraft's trace days before the boundary, in
    ascending order. Reads them newest first and stops at the first one with
    a callsign, which is nearly always the day right before the boundary.
    Aircraft that never broadcast a callsign are the worst case and cost one
    pass over their (by definition callsign-free) history.
    """
    for day in reversed(days_before):
        callsign = _last_callsign_of_day(db.get_trace_day(hex_code, day), hex_code, config)
        if callsign is not None:
            return callsign
    return None


def _incremental_refusal(db: Database, hex_code: str) -> str | None:
    """Why this ICAO cannot be extended one window at a time, or None when it
    can. Every answer here is a case where a partial rebuild would leave the
    flights table holding something no full reprocess would ever produce.
    """
    if hex_code.startswith("ae69"):
        # _run_post_passes promotes an ae69xx registry to MIL_FW on a vote of
        # 3+ flights and back-fills type_override across the whole history.
        # A window can neither count those votes nor re-cap the rows it
        # cannot see.
        return "ae69xx MIL_FW promotion votes across the whole history"
    if db.get_flight_count(hex_code) == 0:
        return "no flights extracted yet"
    stale = db.count_stale_extractor_flights(hex_code, EXTRACTOR_VERSION)
    if stale:
        return f"{stale} existing flight(s) from another extractor version"
    return None


def _extract_window(
    db: Database,
    config: Config,
    hex_code: str,
    trace_days: list,
    *,
    type_code: str | None,
    owner_operator: str | None,
    carried: _CarriedState,
    incremental: bool = False,
) -> int:
    """Load, merge, walk and persist one set of trace days. The whole history
    for a full extract, the days from the boundary onward for an incremental
    one. Returns the number of flights written."""
    merged_days, window_sources = _load_and_merge(trace_days, hex_code, config)
    pairs = _run_state_machine(
        merged_days,
        config,
        hex_code=hex_code,
        all_sources=window_sources | carried.sources,
        type_code=type_code,
        seed_callsign=carried.callsign,
    )
    ctx = _EnrichContext(
        db=db,
        config=config,
        hex_code=hex_code,
        type_code=type_code,
        owner_operator=owner_operator,
        incremental=incremental,
    )
    return len(_persist(pairs, ctx, prev_end_time=carried.prev_end_time))


def _extract_incremental(db: Database, config: Config, hex_code: str, since_date: date) -> int:
    """Rebuild only the trace days that new data on ``since_date`` can
    affect, leaving earlier flights in place. Returns the number of flights
    written, which is the window's count and not the aircraft's total.
    """
    refusal = _incremental_refusal(db, hex_code)
    if refusal is not None:
        print(f"  incremental extract of {hex_code} not possible ({refusal}); doing a full reprocess")
        return extract_flights(db, config, hex_code, reprocess=True)

    # Registry resolution votes over every trace day the aircraft has, not
    # just the window: type_code drives endurance, ceilings and the stitch
    # window, so narrowing the vote could hand the window a different type
    # than the full extract used. These rows carry no trace_json, so the
    # full-history pass stays cheap.
    metadata_rows = db.get_trace_day_metadata(hex_code)
    if not metadata_rows:
        return 0
    type_code, owner_operator = _resolve_registry_metadata(db, hex_code, metadata_rows)

    dates = sorted({row["date"] for row in metadata_rows})
    boundary = _find_extract_boundary(dates, since_date, config, type_code=type_code)
    if boundary is None:
        return 0
    trace_days = list(db.get_trace_days(hex_code, since=boundary))
    if not trace_days:
        return 0

    prev_end = db.get_last_flight_end_before(hex_code, boundary)
    carried = _CarriedState(
        sources={row["source"] for row in metadata_rows},
        callsign=_carried_callsign(db, config, hex_code, [day for day in dates if day < boundary]),
        prev_end_time=datetime.fromisoformat(prev_end) if prev_end else None,
    )
    db.clear_flights_since(hex_code, boundary)
    return _extract_window(
        db,
        config,
        hex_code,
        trace_days,
        type_code=type_code,
        owner_operator=owner_operator,
        carried=carried,
        incremental=True,
    )


def extract_flights(
    db: Database, config: Config, hex_code: str, reprocess: bool = False, since_date: date | None = None
):
    """Extract this ICAO's flight history from its stored trace days and
    write it to the flights table. Returns the number of flights written.

    Four stages, each a module-private function below: resolve the aircraft
    metadata, load and merge the trace days, run the ground/airborne state
    machine over them, then enrich and persist the fragments it emitted.

    ``since_date`` switches to an incremental extract: only the trace days
    that new data on that date can affect are re-processed, and only the
    flights those days produced are deleted first. The rows it writes are
    identical to the ones a full reprocess would write; where they could not
    be, _incremental_refusal falls back to a full reprocess instead. Without
    it, behavior is unchanged: ``reprocess`` clears the history and rebuilds
    it, and a plain call re-writes every flight over the existing rows.
    """
    if since_date is not None:
        return _extract_incremental(db, config, hex_code, since_date)

    if reprocess:
        db.clear_flights(hex_code)

    # get_trace_days is a generator (P7); materialize it here since the
    # stages below make multiple passes over trace_days (registry upsert,
    # type_code/owner_operator fallback scans, iter_parsed_trace_days).
    trace_days = list(db.get_trace_days(hex_code))
    if not trace_days:
        return 0

    type_code, owner_operator = _resolve_registry_metadata(db, hex_code, trace_days)
    return _extract_window(
        db,
        config,
        hex_code,
        trace_days,
        type_code=type_code,
        owner_operator=owner_operator,
        carried=_CarriedState(),
    )
