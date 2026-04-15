import contextlib
import json
from collections import defaultdict
from datetime import UTC, datetime, timedelta

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
from .db import Database
from .models import Flight


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

    # Rich detail fields (only ~22% of points have the full payload, so guard)
    squawk: str | None = None
    category: str | None = None
    nav_altitude_mcp: int | None = None
    nav_qnh: float | None = None
    emergency_field: str | None = None
    true_heading: float | None = None
    callsign: str | None = None
    if detail:
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
        fl = detail.get("flight", "")
        if fl:
            fl = fl.strip()
            if fl:
                callsign = fl
        # Fall back to detail.alt_geom when the slot index 10 wasn't present
        if geom_alt is None:
            alt_geom = detail.get("alt_geom")
            if isinstance(alt_geom, (int, float)):
                geom_alt = int(alt_geom)
        if geom_rate is None:
            gr = detail.get("geom_rate")
            if isinstance(gr, (int, float)):
                geom_rate = float(gr)

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
    )


def _merge_trace_rows(rows: list, config: Config) -> tuple[str, float, list, set[str]]:
    """Merge trace_day rows for the same date into a single sorted+deduped trace.

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
    source_names = {row["source"] for row in rows}

    # Convert all points to absolute timestamps
    abs_points = []
    for row in rows:
        base_ts = row["timestamp"]
        trace = json.loads(row["trace_json"])
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
    base_timestamp = min(row["timestamp"] for row in rows)
    result_trace = []
    for abs_ts, point in merged:
        new_point = list(point)
        new_point[0] = abs_ts - base_timestamp
        result_trace.append(new_point)

    return rows[0]["date"], base_timestamp, result_trace, source_names


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

    Effective stitch window:
      max(config.stitch_max_gap_minutes,
          config.type_endurance_minutes.get(type_code, max_endurance) *
          config.stitch_endurance_ratio)

    This keeps the default 90-min window for light GA while letting long-
    endurance types (KC-135, C-17, etc.) stitch across the multi-hour
    coverage gaps that are normal on their operational missions.

    Merging is destructive: the merged flight inherits prev.takeoff_* (if
    observed) and next.last_*, and its metrics are taken from the next
    fragment (since the classifier needs the tail of the trace). The
    takeoff_type of the merged flight becomes "observed" if prev observed
    its takeoff, otherwise "found_mid_flight".
    """
    if len(flights) < 2:
        return flights, metrics_list

    endurance_minutes = config.type_endurance_minutes.get(type_code or "", config.max_endurance_minutes)
    effective_gap_minutes = max(
        config.stitch_max_gap_minutes,
        endurance_minutes * config.stitch_endurance_ratio,
    )
    max_gap_secs = effective_gap_minutes * 60.0
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

                    if dist_km <= plausible and alt_ok:
                        # Merge: the next fragment inherits prev's takeoff
                        # position and time (the originally-observed takeoff
                        # if prev was observed, otherwise prev's first point).
                        stitched = next_flight
                        stitched.takeoff_time = flight.takeoff_time
                        stitched.takeoff_lat = flight.takeoff_lat
                        stitched.takeoff_lon = flight.takeoff_lon
                        stitched.takeoff_date = flight.takeoff_date
                        stitched.callsign = flight.callsign or next_flight.callsign

                        # Metrics: carry forward sources and takeoff type
                        next_metrics.sources |= metrics.sources
                        if metrics.takeoff_type == "observed":
                            next_metrics.takeoff_type = "observed"
                            next_metrics.ground_points_at_takeoff = metrics.ground_points_at_takeoff
                        # Use the earliest first_point_ts so duration covers
                        # the full span including the coverage gap.
                        if (
                            metrics.first_point_ts is not None
                            and next_metrics.first_point_ts is not None
                            and metrics.first_point_ts < next_metrics.first_point_ts
                        ):
                            next_metrics.first_point_ts = metrics.first_point_ts

                        # v3: merge every accumulator so stitched flights
                        # don't silently undercount. Path length sums,
                        # phase counters sum, peak rates take the extremum,
                        # squawk/callsign histories union.
                        next_metrics.data_points += metrics.data_points
                        next_metrics.path_length_km += metrics.path_length_km
                        next_metrics.max_distance_from_origin_km = max(
                            next_metrics.max_distance_from_origin_km,
                            metrics.max_distance_from_origin_km,
                        )
                        next_metrics.climb_secs += metrics.climb_secs
                        next_metrics.descent_secs += metrics.descent_secs
                        next_metrics.level_secs += metrics.level_secs
                        next_metrics.level_buf = metrics.level_buf + next_metrics.level_buf
                        if metrics.peak_climb_fpm > next_metrics.peak_climb_fpm:
                            next_metrics.peak_climb_fpm = metrics.peak_climb_fpm
                        if metrics.peak_descent_fpm < next_metrics.peak_descent_fpm:
                            next_metrics.peak_descent_fpm = metrics.peak_descent_fpm
                        if metrics.max_hover_secs > next_metrics.max_hover_secs:
                            next_metrics.max_hover_secs = metrics.max_hover_secs
                        next_metrics.hover_episodes += metrics.hover_episodes
                        next_metrics.squawk_1200_count += metrics.squawk_1200_count
                        next_metrics.squawk_total_count += metrics.squawk_total_count
                        if next_metrics.squawk_first is None:
                            next_metrics.squawk_first = metrics.squawk_first
                        next_metrics.squawk_changes += metrics.squawk_changes
                        next_metrics.emergency_squawks_seen |= metrics.emergency_squawks_seen
                        for cs in metrics.callsigns_seen:
                            if cs not in next_metrics.callsigns_seen:
                                next_metrics.callsigns_seen.insert(0, cs)
                        next_metrics.callsign_changes += metrics.callsign_changes
                        for cat, cnt in metrics.category_counts.items():
                            next_metrics.category_counts[cat] = next_metrics.category_counts.get(cat, 0) + cnt
                        if next_metrics.autopilot_target_alt_ft is None:
                            next_metrics.autopilot_target_alt_ft = metrics.autopilot_target_alt_ft
                        if next_metrics.emergency_flag is None and metrics.emergency_flag is not None:
                            next_metrics.emergency_flag = metrics.emergency_flag
                        # Max altitude of the merged flight is the max of both
                        if metrics.max_altitude > next_metrics.max_altitude:
                            next_metrics.max_altitude = metrics.max_altitude
                        next_metrics.baro_error_points += metrics.baro_error_points
                        next_metrics.total_ground_points += metrics.total_ground_points
                        next_metrics.ground_speed_while_ground += metrics.ground_speed_while_ground
                        # v5 F2: accumulate fragment count across stitches
                        next_metrics.fragments_stitched += metrics.fragments_stitched
                        # v5 F1: carry signal_gap_count through stitching.
                        # Also add 1 for the coverage hole we just bridged.
                        next_metrics.signal_gap_count += metrics.signal_gap_count + 1

                        # Recompute duration on the merged flight. extract_flights
                        # computes duration_minutes before stitching using the
                        # pre-merge fragment boundaries, so after widening
                        # first_point_ts we need to refresh the Flight field to
                        # cover the whole stitched span (including the coverage
                        # gap between the two fragments).
                        if next_metrics.first_point_ts is not None and next_metrics.last_point_ts is not None:
                            span = next_metrics.last_point_ts - next_metrics.first_point_ts
                            stitched.duration_minutes = round(span / 60.0, 1)

                        merged.append((stitched, next_metrics))
                        i += 2
                        continue

        merged.append((flight, metrics))
        i += 1

    stitched_flights = [p[0] for p in merged]
    stitched_metrics = [p[1] for p in merged]
    return stitched_flights, stitched_metrics


def extract_flights(db: Database, config: Config, hex_code: str, reprocess: bool = False):
    if reprocess:
        db.clear_flights(hex_code)

    trace_days = db.get_trace_days(hex_code)
    if not trace_days:
        return 0

    # v3: populate/refresh aircraft_registry and use the authoritative
    # type_code for endurance, hover gating and mission rules. Fall back
    # to the first row's type_code if the registry write fails (e.g. in
    # tests using a MagicMock db).
    type_code: str | None = None
    owner_operator: str | None = None
    try:
        registry_row = db.upsert_aircraft_registry(hex_code, list(trace_days))
    except Exception:
        registry_row = None
    if isinstance(registry_row, dict):
        if registry_row.get("type_code"):
            type_code = registry_row["type_code"]
        if registry_row.get("owner_operator"):
            owner_operator = registry_row["owner_operator"]
        # v12 N24: warn when type_code drift exceeds threshold. Pure
        # description drift is noise; type_code conflicts indicate the
        # registry entry may be wrong (e.g. GLF6 vs GA8C on adf64f).
        drift_count = registry_row.get("metadata_drift_count", 0)
        if drift_count > 20:
            try:
                drift_json = db.conn.execute(
                    "SELECT metadata_drift_values FROM aircraft_registry WHERE icao = ?",
                    (hex_code,),
                ).fetchone()
                if drift_json and drift_json[0]:
                    import json as _json
                    drift_vals = _json.loads(drift_json[0])
                    type_conflicts = [
                        d for d in drift_vals
                        if d.get("type_code") and d["type_code"] != type_code
                    ]
                    if type_conflicts:
                        conflict_types = ", ".join(
                            f"{d['type_code']}({d['count']})" for d in type_conflicts
                        )
                        print(
                            f"  WARNING: {hex_code} has {drift_count} metadata drift events "
                            f"with type_code conflicts: {type_code} vs {conflict_types}"
                        )
            except Exception:
                pass
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

    # Group by date and merge multi-source rows
    by_date: dict[str, list] = defaultdict(list)
    for row in trace_days:
        by_date[row["date"]].append(row)

    merged_days = []
    all_sources: set[str] = set()
    for day_date in sorted(by_date.keys()):
        date_str, base_ts, trace, day_sources = _merge_trace_rows(by_date[day_date], config)
        merged_days.append((date_str, base_ts, trace))
        all_sources |= day_sources

    max_day_gap = timedelta(days=config.max_day_gap_days)
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
    current_callsign: str | None = None
    prev_day_date: str | None = None
    ground_count_before_takeoff = 0
    prev_point_ts: float | None = None
    post_landing_start_ts: float | None = None
    # OpenSky data lacks ground speed. When gs is None, require two
    # consecutive ground points before landing: the first sets this flag
    # and is otherwise ignored; the second confirms the transition.
    prev_was_ground_no_gs = False

    def _close_pending(reason: str) -> None:
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
        if prev_day_date is not None:
            prev = datetime.fromisoformat(prev_day_date)
            curr = datetime.fromisoformat(day_date)
            if curr - prev > max_day_gap:
                _close_pending("day_gap")
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
                _close_pending("intra_trace_gap")
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

    # Classify, score confidence, match airports, and save.
    # Order of operations (v5 plan):
    #   classify_landing -> B7/B8 filter -> airport (D1) -> B1 duration
    #   recompute -> derive_all (uses signal budget for B2) -> insert (B3 guard)
    final_flights: list[Flight] = []
    # v7 F3: track previous flight's end time for turnaround computation.
    prev_end_time: datetime | None = None
    for flight, metrics in zip(valid_flights, valid_metrics, strict=True):
        has_landing = flight.landing_lat is not None

        flight.takeoff_type = metrics.takeoff_type

        flight.landing_type = classify_landing(
            metrics,
            has_landing,
            duration_minutes=flight.duration_minutes,
            type_code=type_code,
            type_endurance_minutes=config.type_endurance_minutes,
            default_endurance_minutes=config.max_endurance_minutes,
            dropped_tail_window=config.dropped_tail_window,
            dropped_tail_descent_min_count=config.dropped_tail_descent_min_count,
            dropped_tail_descent_rate_fpm=config.dropped_tail_descent_rate_fpm,
            dropped_max_alt_ft=config.dropped_max_alt_ft,
        )

        # v8 R5: tighten tiny-flight guard. Drop signal_lost, uncertain, and
        # dropped_on_approach slivers that are BOTH short AND sparse. Keep
        # confirmed landings (legitimate quick helicopter hops) regardless
        # of size. Threshold raised to 3 min and dropped_on_approach added
        # since a sub-3-min dropped fragment is noise, not a real approach.
        if flight.landing_type in ("signal_lost", "uncertain", "dropped_on_approach") and (
            flight.duration_minutes is not None
            and flight.duration_minutes < config.min_viable_flight_minutes
            and metrics.data_points < config.min_viable_flight_points
        ):
            continue

        # v5 B8: drop stationary broadcasters (transponder on the ramp).
        if (
            metrics.path_length_km < config.stationary_path_km
            and metrics.max_distance_from_origin_km < config.stationary_path_km
            and metrics.max_altitude < config.stationary_max_alt_ft
            and metrics.max_gs_kt < config.stationary_max_gs_kt
        ):
            continue

        # D1: match airports with on-field vs nearest split.
        origin = find_nearest_airport(db, flight.takeoff_lat, flight.takeoff_lon, config)
        if origin:
            if origin.distance_km <= config.airport_on_field_threshold_km:
                flight.origin_icao = origin.ident
                flight.origin_name = origin.name
                flight.origin_distance_km = origin.distance_km
            else:
                flight.nearest_origin_icao = origin.ident
                flight.nearest_origin_distance_km = origin.distance_km

        if has_landing and flight.landing_type not in ("signal_lost", "dropped_on_approach"):
            dest = find_nearest_airport(db, flight.landing_lat, flight.landing_lon, config)
            if dest:
                if dest.distance_km <= config.airport_on_field_threshold_km:
                    flight.destination_icao = dest.ident
                    flight.destination_name = dest.name
                    flight.destination_distance_km = dest.distance_km
                else:
                    flight.nearest_destination_icao = dest.ident
                    flight.nearest_destination_distance_km = dest.distance_km

        # v5 B1: single source of truth for duration_minutes. Compute from
        # wall-clock (landing_time or last_seen_time) - takeoff_time, not
        # from the metric-span (last_point_ts - first_point_ts). The metric
        # span misses signal-gap time on stitched flights.
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

        flight.data_points = metrics.data_points
        flight.sources = ",".join(sorted(metrics.sources)) if metrics.sources else None
        # Store raw persistence-filtered altitude; ceiling cap is applied
        # after derive_all so type_override is available for the lookup.
        flight.max_altitude = metrics.max_altitude if metrics.max_altitude > 0 else None
        flight.ground_points_at_landing = metrics.ground_points_at_landing
        flight.ground_points_at_takeoff = metrics.ground_points_at_takeoff
        flight.baro_error_points = metrics.baro_error_points

        # v3 derived features - must run AFTER classify_landing + airport
        # matching so mission/loiter/cruise/day-night all see final values.
        features.derive_all(
            flight,
            metrics,
            config=config,
            type_code=type_code,
            owner_operator=owner_operator,
        )

        # v9 R4: ceiling + GS cap using effective type (type_override wins).
        # Runs after derive_all so MIL_FW override is set. The ceiling
        # was previously applied before derive_all using only a preliminary
        # ae69xx altitude check, which missed flights classified by cruise_gs.
        effective_type = flight.type_override or type_code
        if flight.max_altitude is not None:
            ceiling = TYPE_CEILINGS.get(effective_type or "", 60_000)
            # v14 R4a: only give 10% tolerance when the flight has
            # coherent AP data. Without AP, or when the AP target
            # wildly disagrees with max_altitude (>5,000 ft delta --
            # e.g. S92 a7a622 AP=3,008 vs alt=16,500), cap at exactly
            # the book ceiling so corrupt spikes don't exceed physical
            # limits.
            ap = flight.autopilot_target_alt_ft
            ap_coherent = (
                ap is not None
                and abs(flight.max_altitude - ap) <= 5000
            )
            alt_cap = int(ceiling * 1.1) if ap_coherent else ceiling
            if flight.max_altitude > alt_cap:
                flight.max_altitude = alt_cap
            # Also re-cap cruise_alt_ft after ceiling adjustment
            if flight.cruise_alt_ft is not None and flight.cruise_alt_ft > flight.max_altitude:
                flight.cruise_alt_ft = flight.max_altitude

        # v9 R3: type-based GS cap for max_gs_kt. Same pattern as altitude
        # ceiling -- if the persistence-filtered value still exceeds the
        # type's physical max by >10%, clamp it.
        if flight.max_gs_kt is not None:
            gs_ceiling = TYPE_MAX_GS.get(effective_type or "", 800)
            gs_cap = int(gs_ceiling * 1.1)
            if flight.max_gs_kt > gs_cap:
                flight.max_gs_kt = gs_cap
            # v17 R5: re-add type cap on cruise_gs_kt. Both cruise_gs
            # and max_gs must share the same caps so cruise <= max
            # always holds. The v15 removal caused 3,134 flights to
            # violate this invariant.
            if flight.cruise_gs_kt is not None and flight.cruise_gs_kt > gs_cap:
                flight.cruise_gs_kt = gs_cap

        # v3 destination inference for dropped / signal_lost flights
        if flight.landing_type in ("signal_lost", "dropped_on_approach") and flight.last_seen_lat is not None:
            try:
                candidates = db.find_nearby_airports(
                    flight.last_seen_lat,
                    flight.last_seen_lon,
                    delta=config.prob_dest_search_delta,
                    types=config.airport_types,
                )
            except Exception:
                candidates = []
            infer = features.infer_destination(
                flight=flight,
                metrics=metrics,
                candidates=list(candidates),
                config=config,
            )
            flight.probable_destination_icao = infer["probable_destination_icao"]
            flight.probable_destination_distance_km = infer["probable_destination_distance_km"]
            flight.probable_destination_confidence = infer["probable_destination_confidence"]

        # v7 F3: turnaround_minutes from previous flight's end to this takeoff.
        # v8 N8: cap at 72 hours (4320 min). Anything longer reflects a
        # collection gap or parked aircraft, not a real turnaround. NULL
        # these out so they don't pollute fleet utilisation averages.
        if prev_end_time is not None:
            turn_secs = (flight.takeoff_time - prev_end_time).total_seconds()
            if turn_secs >= 0:
                turn_min = round(turn_secs / 60.0, 1)
                flight.turnaround_minutes = turn_min if turn_min <= 4320.0 else None
            # v10 N16: turnaround category for distribution analysis.
            # v12 N23: every flight must get a non-null category. Flights
            # where turnaround_minutes is NULL (>72 h cap or negative
            # turn_secs) get 'extended_gap' so the NULL bucket is empty.
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
            # v10 F9: first observed flight for this ICAO
            flight.is_first_observed_flight = 1
            flight.turnaround_category = "first_observed"
        # v11 N20: default to 0; the post-loop pass sets the last flight to 1.
        flight.is_last_observed_flight = 0
        prev_end_time = flight.landing_time or flight.last_seen_time

        db.insert_flight(flight)
        final_flights.append(flight)

    # v11 N20: mark the last flight for this ICAO and assign 'last_observed'
    # turnaround category when the category is still NULL (turnaround_minutes
    # was NULL or exceeded the 72-hour cap). This is the mirror of
    # is_first_observed_flight. Every flight now has a non-null category.
    if final_flights:
        last = final_flights[-1]
        last.is_last_observed_flight = 1
        if last.turnaround_category is None:
            last.turnaround_category = "last_observed"
        db.update_last_observed_flag(last)

    # v9 H1a: registry-level MIL_FW promotion. If an ae69xx ICAO has >= 3
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

    # v8 H5: back-fill origin_helipad_id / destination_helipad_id from the
    # helipads table. Runs after all flights are inserted so the helipad
    # foreign keys survive INSERT OR REPLACE. Uses the same eps as DBSCAN
    # clustering (0.2 km).
    with contextlib.suppress(Exception):
        db.backfill_helipad_ids(hex_code, eps_km=0.2)

    # v3: refresh materialized aircraft_stats for this ICAO
    with contextlib.suppress(Exception):
        db.refresh_aircraft_stats(hex_code)

    # v8 N9: purge registry entry if this ICAO ended up with zero flights
    # after extraction (e.g. all fragments were filtered out as noise).
    if not final_flights:
        with contextlib.suppress(Exception):
            db.purge_zero_flight_registry(hex_code)

    return len(final_flights)
