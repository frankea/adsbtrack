import json
from collections import defaultdict
from datetime import UTC, datetime, timedelta

from .airports import find_nearest_airport
from .classifier import FlightMetrics, classify_landing, score_confidence
from .config import Config
from .db import Database
from .models import Flight

# Max gap between trace days before we reset state.
# If data is sparse (monthly samples), a flight "spanning" months is an artifact.
MAX_DAY_GAP = timedelta(days=2)

# Minimum duration to consider a valid flight (filters taxi movements)
MIN_FLIGHT_MINUTES = 5.0

# Deduplication thresholds for merging multi-source trace points
_DEDUP_TIME_SECS = 2.0
_DEDUP_DEG = 0.001


def _merge_trace_rows(rows: list) -> tuple[str, float, list, set[str]]:
    """Merge multiple trace_day rows for the same date from different sources.

    Converts relative offsets to absolute timestamps, concatenates, sorts,
    deduplicates (points within 2 seconds and 0.001 degrees are duplicates),
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
            and abs(abs_ts - prev_ts) < _DEDUP_TIME_SECS
            and prev_lat is not None
            and prev_lon is not None
            and abs(lat - prev_lat) < _DEDUP_DEG
            and abs(lon - prev_lon) < _DEDUP_DEG
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

    # Group by date and merge multi-source rows
    by_date: dict[str, list] = defaultdict(list)
    for row in trace_days:
        by_date[row["date"]].append(row)

    merged_days = []
    all_sources: set[str] = set()
    for day_date in sorted(by_date.keys()):
        date_str, base_ts, trace, day_sources = _merge_trace_rows(by_date[day_date])
        merged_days.append((date_str, base_ts, trace))
        all_sources |= day_sources

    flights: list[Flight] = []
    metrics_list: list[FlightMetrics] = []
    state = None  # None = unknown, "ground" or "airborne"
    prev_ground_point = None
    pending_flight: Flight | None = None
    pending_metrics: FlightMetrics | None = None
    current_callsign = None
    prev_day_date = None
    _prev_was_ground = False  # for gs=None hysteresis (require 2 consecutive ground points)
    ground_count_before_takeoff = 0  # track ground points in current ground state

    for day_date, day_timestamp, trace in merged_days:
        # Reset state if there's a gap between trace days
        if prev_day_date is not None:
            prev = datetime.fromisoformat(prev_day_date)
            curr = datetime.fromisoformat(day_date)
            if curr - prev > MAX_DAY_GAP:
                # Save incomplete flight if any
                if pending_flight:
                    flights.append(pending_flight)
                    metrics_list.append(pending_metrics or FlightMetrics())
                    pending_flight = None
                    pending_metrics = None
                state = None
                prev_ground_point = None
                _prev_was_ground = False
                ground_count_before_takeoff = 0

        prev_day_date = day_date

        for point in trace:
            time_offset = point[0]
            lat = point[1]
            lon = point[2]
            alt = point[3]
            gs = point[4]  # ground speed in knots

            # Detail object location varies by format version:
            #   len 14 (current): index 8
            #   len 9 (old):      index 8
            #   len 7-8 (oldest): no detail object
            detail = None
            for idx in (8,):
                if len(point) > idx and isinstance(point[idx], dict):
                    detail = point[idx]
                    break

            # Update callsign from detail object when present
            if detail:
                flight_id = detail.get("flight", "").strip()
                if flight_id:
                    current_callsign = flight_id

            is_ground = alt == "ground"
            abs_time = datetime.fromtimestamp(day_timestamp + time_offset, tz=UTC)

            # Record metrics for pending flight
            if pending_metrics:
                pending_metrics.record_point(
                    alt, gs, lat, lon,
                    is_ground=is_ground,
                    state=state,
                    landing_speed_threshold=config.landing_speed_threshold_kts,
                )

            if state is None:
                if is_ground:
                    state = "ground"
                    prev_ground_point = (lat, lon, abs_time, day_date)
                    ground_count_before_takeoff += 1
                else:
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
                    pending_metrics.ground_points_at_takeoff = ground_count_before_takeoff
                    pending_metrics.record_point(
                        alt, gs, lat, lon, is_ground=is_ground, state=state,
                        landing_speed_threshold=config.landing_speed_threshold_kts,
                    )
                    ground_count_before_takeoff = 0
                continue

            if state == "ground" and not is_ground:
                # TAKEOFF - use previous ground point for airport location
                state = "airborne"
                _prev_was_ground = False
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
                pending_metrics.ground_points_at_takeoff = ground_count_before_takeoff
                pending_metrics.record_point(
                    alt, gs, lat, lon, is_ground=is_ground, state=state,
                    landing_speed_threshold=config.landing_speed_threshold_kts,
                )
                ground_count_before_takeoff = 0

            elif state == "airborne" and is_ground:
                # Possible LANDING - check ground speed for hysteresis
                if gs is not None and gs > config.landing_speed_threshold_kts:
                    continue
                # When ground speed is unavailable (e.g. OpenSky data), require
                # consecutive ground points to avoid false landings from momentary
                # altitude glitches. We peek ahead by deferring the state change
                # until we see a second ground point.
                if gs is None and not _prev_was_ground:
                    _prev_was_ground = True
                    continue
                _prev_was_ground = False

                # Record landing ground point in metrics
                if pending_metrics:
                    pending_metrics.record_landing_ground_point(lat, lon)

                state = "ground"
                ground_count_before_takeoff = 1  # this ground point counts for next takeoff
                if pending_flight:
                    pending_flight.landing_time = abs_time
                    pending_flight.landing_lat = lat
                    pending_flight.landing_lon = lon
                    pending_flight.landing_date = day_date
                    if pending_flight.landing_time and pending_flight.takeoff_time:
                        delta = (pending_flight.landing_time - pending_flight.takeoff_time).total_seconds()
                        pending_flight.duration_minutes = round(delta / 60, 1)
                    flights.append(pending_flight)
                    metrics_list.append(pending_metrics or FlightMetrics())
                    pending_flight = None
                    pending_metrics = None

            elif state == "ground" and is_ground:
                ground_count_before_takeoff += 1

            if is_ground:
                prev_ground_point = (lat, lon, abs_time, day_date)

    # Handle flight still in progress at end of data
    if pending_flight:
        flights.append(pending_flight)
        metrics_list.append(pending_metrics or FlightMetrics())

    # Filter and classify
    valid_flights = []
    valid_metrics = []
    for flight, metrics in zip(flights, metrics_list):
        # Skip very short movements at the same location (taxi, ground tests)
        if (
            flight.duration_minutes is not None
            and flight.duration_minutes < MIN_FLIGHT_MINUTES
            and flight.landing_lat is not None
        ):
            from .airports import haversine_km

            dist = haversine_km(flight.takeoff_lat, flight.takeoff_lon, flight.landing_lat, flight.landing_lon)
            if dist < 5:  # Less than 5km traveled - not a real flight
                continue
        valid_flights.append(flight)
        valid_metrics.append(metrics)

    # Classify, score confidence, match airports, and save
    for flight, metrics in zip(valid_flights, valid_metrics):
        has_landing = flight.landing_lat is not None

        # Classify landing type
        flight.landing_type = classify_landing(metrics, has_landing)

        # Match airports (skip destination for signal_lost flights)
        origin = find_nearest_airport(db, flight.takeoff_lat, flight.takeoff_lon, config)
        if origin:
            flight.origin_icao = origin.ident
            flight.origin_name = origin.name
            flight.origin_distance_km = origin.distance_km

        if has_landing and flight.landing_type != "signal_lost":
            dest = find_nearest_airport(db, flight.landing_lat, flight.landing_lon, config)
            if dest:
                flight.destination_icao = dest.ident
                flight.destination_name = dest.name
                flight.destination_distance_km = dest.distance_km

        # Score confidence
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

        # Store metrics summary
        flight.data_points = metrics.data_points
        flight.sources = ",".join(sorted(metrics.sources)) if metrics.sources else None
        flight.max_altitude = metrics.max_altitude if metrics.max_altitude > 0 else None
        flight.ground_points_at_landing = metrics.ground_points_at_landing

        db.insert_flight(flight)

    return len(valid_flights)
