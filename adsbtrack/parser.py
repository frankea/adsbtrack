import json
from datetime import datetime, timedelta, timezone

from .airports import find_nearest_airport
from .config import Config
from .db import Database
from .models import Flight

# Max gap between trace days before we reset state.
# If data is sparse (monthly samples), a flight "spanning" months is an artifact.
MAX_DAY_GAP = timedelta(days=2)

# Minimum duration to consider a valid flight (filters taxi movements)
MIN_FLIGHT_MINUTES = 5.0


def extract_flights(db: Database, config: Config, hex_code: str, reprocess: bool = False):
    if reprocess:
        db.clear_flights(hex_code)

    trace_days = db.get_trace_days(hex_code)
    if not trace_days:
        return 0

    flights: list[Flight] = []
    state = None  # None = unknown, "ground" or "airborne"
    prev_ground_point = None
    pending_flight: Flight | None = None
    current_callsign = None
    prev_day_date = None

    for day_row in trace_days:
        day_date = day_row["date"]
        day_timestamp = day_row["timestamp"]
        trace = json.loads(day_row["trace_json"])

        # Reset state if there's a gap between trace days
        if prev_day_date is not None:
            prev = datetime.fromisoformat(prev_day_date)
            curr = datetime.fromisoformat(day_date)
            if curr - prev > MAX_DAY_GAP:
                # Save incomplete flight if any
                if pending_flight:
                    flights.append(pending_flight)
                    pending_flight = None
                state = None
                prev_ground_point = None

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
            abs_time = datetime.fromtimestamp(day_timestamp + time_offset, tz=timezone.utc)

            if state is None:
                if is_ground:
                    state = "ground"
                    prev_ground_point = (lat, lon, abs_time, day_date)
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
                continue

            if state == "ground" and not is_ground:
                # TAKEOFF - use previous ground point for airport location
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

            elif state == "airborne" and is_ground:
                # Possible LANDING - check ground speed for hysteresis
                if gs is not None and gs > 80:
                    continue

                state = "ground"
                if pending_flight:
                    pending_flight.landing_time = abs_time
                    pending_flight.landing_lat = lat
                    pending_flight.landing_lon = lon
                    pending_flight.landing_date = day_date
                    if pending_flight.landing_time and pending_flight.takeoff_time:
                        delta = (pending_flight.landing_time - pending_flight.takeoff_time).total_seconds()
                        pending_flight.duration_minutes = round(delta / 60, 1)
                    flights.append(pending_flight)
                    pending_flight = None

            if is_ground:
                prev_ground_point = (lat, lon, abs_time, day_date)

    # Handle flight still in progress at end of data
    if pending_flight:
        flights.append(pending_flight)

    # Filter and save
    valid_flights = []
    for flight in flights:
        # Skip very short movements at the same location (taxi, ground tests)
        if (flight.duration_minutes is not None
                and flight.duration_minutes < MIN_FLIGHT_MINUTES
                and flight.landing_lat is not None):
            from .airports import haversine_km
            dist = haversine_km(flight.takeoff_lat, flight.takeoff_lon,
                                flight.landing_lat, flight.landing_lon)
            if dist < 5:  # Less than 5km traveled — not a real flight
                continue
        valid_flights.append(flight)

    # Match airports and save
    for flight in valid_flights:
        origin = find_nearest_airport(db, flight.takeoff_lat, flight.takeoff_lon, config)
        if origin:
            flight.origin_icao = origin.ident
            flight.origin_name = origin.name
            flight.origin_distance_km = origin.distance_km

        if flight.landing_lat is not None and flight.landing_lon is not None:
            dest = find_nearest_airport(db, flight.landing_lat, flight.landing_lon, config)
            if dest:
                flight.destination_icao = dest.ident
                flight.destination_name = dest.name
                flight.destination_distance_km = dest.distance_km

        db.insert_flight(flight)

    return len(valid_flights)
