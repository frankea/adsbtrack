import json
from collections import defaultdict
from datetime import UTC, datetime, timedelta

from .airports import find_nearest_airport
from .classifier import (
    FlightMetrics,
    classify_ground_state,
    classify_landing,
    score_confidence,
)
from .config import Config
from .db import Database
from .models import Flight


def _extract_point_fields(point: list) -> tuple[int | str | None, float | None, dict | None, int | None, float | None]:
    """Return (baro_alt, gs, detail_dict, geom_alt, baro_rate) from a trace point.

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

    detail = None
    if len(point) > 8 and isinstance(point[8], dict):
        detail = point[8]

    baro_rate = None
    if len(point) > 7 and isinstance(point[7], (int, float)):
        baro_rate = float(point[7])

    geom_alt = None
    if len(point) > 10 and isinstance(point[10], (int, float)):
        geom_alt = int(point[10])

    return baro_alt, gs, detail, geom_alt, baro_rate


def _merge_trace_rows(rows: list, config: Config) -> tuple[str, float, list, set[str]]:
    """Merge multiple trace_day rows for the same date from different sources.

    Converts relative offsets to absolute timestamps, concatenates, sorts,
    deduplicates (points within dedup_time_secs and dedup_deg are duplicates),
    then converts back to offsets from the earliest timestamp.

    Returns (date, base_timestamp, merged_trace, source_names).
    """
    source_names = {row["source"] for row in rows}

    if len(rows) == 1:
        return rows[0]["date"], rows[0]["timestamp"], json.loads(rows[0]["trace_json"]), source_names

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


def extract_flights(db: Database, config: Config, hex_code: str, reprocess: bool = False):
    if reprocess:
        db.clear_flights(hex_code)

    trace_days = db.get_trace_days(hex_code)
    if not trace_days:
        return 0

    # Figure out the aircraft type for endurance lookup
    type_code = None
    for row in trace_days:
        if row["type_code"]:
            type_code = row["type_code"]
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
            baro_alt, gs, detail, geom_alt, baro_rate = _extract_point_fields(point)
            abs_ts = day_timestamp + time_offset
            abs_time = datetime.fromtimestamp(abs_ts, tz=UTC)

            # Update callsign from detail object when present
            if detail:
                flight_id = detail.get("flight", "").strip()
                if flight_id:
                    current_callsign = flight_id

            # Intra-trace gap check: any gap longer than max_point_gap_minutes
            # forces a flight close. Real operations rarely have more than a
            # few minutes between trace points; multi-hour gaps are coverage
            # holes that the state machine should not stitch across.
            if prev_point_ts is not None and (abs_ts - prev_point_ts) > max_point_gap_secs:
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
                    baro_alt=baro_alt,
                    geom_alt=geom_alt,
                    gs=gs,
                    baro_rate=baro_rate,
                    lat=lat,
                    lon=lon,
                    ts=abs_ts,
                    ground_state=point_state,
                    ground_reason=point_reason,
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
                        baro_alt=baro_alt,
                        geom_alt=geom_alt,
                        gs=gs,
                        baro_rate=baro_rate,
                        lat=lat,
                        lon=lon,
                        ts=abs_ts,
                        ground_state=point_state,
                        ground_reason=point_reason,
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
                        baro_alt=baro_alt,
                        geom_alt=geom_alt,
                        gs=gs,
                        baro_rate=baro_rate,
                        lat=lat,
                        lon=lon,
                        ts=abs_ts,
                        ground_state=point_state,
                        ground_reason=point_reason,
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
                    if pending_flight is not None:
                        pending_flight.landing_time = abs_time
                        pending_flight.landing_lat = lat
                        pending_flight.landing_lon = lon
                        pending_flight.landing_date = day_date
                        if pending_flight.landing_time and pending_flight.takeoff_time:
                            delta = (pending_flight.landing_time - pending_flight.takeoff_time).total_seconds()
                            pending_flight.duration_minutes = round(delta / 60, 1)
                    state = "post_landing"
                    post_landing_start_ts = abs_ts
                    prev_ground_point = (lat, lon, abs_time, day_date)
                else:
                    # Still airborne or unknown - reset the OpenSky hysteresis
                    prev_was_ground_no_gs = False
                continue

            if state == "post_landing":
                window_expired = (
                    post_landing_start_ts is not None
                    and (abs_ts - post_landing_start_ts) > post_landing_window_secs
                )
                count_expired = (
                    pending_metrics is not None
                    and pending_metrics.ground_points_at_landing >= post_landing_max_points
                )

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
                        baro_alt=baro_alt,
                        geom_alt=geom_alt,
                        gs=gs,
                        baro_rate=baro_rate,
                        lat=lat,
                        lon=lon,
                        ts=abs_ts,
                        ground_state=point_state,
                        ground_reason=point_reason,
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

                if window_expired or count_expired:
                    # Finalize the landing and fall back to ground state.
                    # Remember the ground-point count before clearing metrics
                    # so the next takeoff's ground_count_before_takeoff reflects
                    # the points we collected during the post-landing window.
                    pre_clear_gp = (
                        pending_metrics.ground_points_at_landing if pending_metrics else 1
                    )
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

    # Filter and classify
    valid_flights = []
    valid_metrics = []
    for flight, metrics in zip(flights, metrics_list, strict=True):
        if (
            flight.duration_minutes is not None
            and flight.duration_minutes < config.min_flight_minutes
            and flight.landing_lat is not None
        ):
            from .airports import haversine_km

            dist = haversine_km(flight.takeoff_lat, flight.takeoff_lon, flight.landing_lat, flight.landing_lon)
            if dist < config.min_flight_distance_km:
                continue
        valid_flights.append(flight)
        valid_metrics.append(metrics)

    # Classify, score confidence, match airports, and save
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
        )

        # Match airports (skip destination for signal_lost / dropped_on_approach)
        origin = find_nearest_airport(db, flight.takeoff_lat, flight.takeoff_lon, config)
        if origin:
            flight.origin_icao = origin.ident
            flight.origin_name = origin.name
            flight.origin_distance_km = origin.distance_km

        if has_landing and flight.landing_type not in ("signal_lost", "dropped_on_approach"):
            dest = find_nearest_airport(db, flight.landing_lat, flight.landing_lon, config)
            if dest:
                flight.destination_icao = dest.ident
                flight.destination_name = dest.name
                flight.destination_distance_km = dest.distance_km

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
        flight.max_altitude = metrics.max_altitude if metrics.max_altitude > 0 else None
        flight.ground_points_at_landing = metrics.ground_points_at_landing
        flight.ground_points_at_takeoff = metrics.ground_points_at_takeoff
        flight.baro_error_points = metrics.baro_error_points

        db.insert_flight(flight)

    return len(valid_flights)
