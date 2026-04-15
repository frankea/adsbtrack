# How it works

ADS-B Exchange and sibling trackers store daily trace files for every aircraft they've seen. Each trace is a series of timestamped position reports with lat/lon, barometric altitude, ground speed, vertical rate, and geometric altitude. The fetcher downloads these day by day and stores them in SQLite.

## Trace merging

When multiple data sources are fetched for the same aircraft, traces are merged by absolute timestamp and deduplicated (points within 1 second and 0.001 degrees of each other are collapsed). Different receiver networks catch different points for the same flight, so combining them improves coverage.

The single-source path runs through the same sort + dedupe pipeline, so trace files that contain out-of-order or "phantom" points (cache glitches in readsb's `trace_full` output that occasionally write prior-day leakage with deeply negative offsets) are reordered into chronological order before the state machine sees them. Without this, a phantom point can overwrite the pending flight's `last_point_ts` with a timestamp earlier than its `first_point_ts` and produce a negative `duration_minutes`.

## Flight extraction

The extractor walks through the merged trace points in chronological order and runs a state machine (`None` -> `ground` -> `airborne` -> `post_landing` -> `ground`...) that detects takeoff and landing transitions.

Ground-vs-airborne for each point is decided by fusing:

- **Barometric altitude** (the `'ground'` sentinel or an int in feet)
- **Geometric altitude** (GPS-derived, in feet)
- **Ground speed** in knots
- **Barometric vertical rate** in ft/min

This catches tricky cases the old "baro says ground" heuristic missed:

- **Baro encoder errors** on helicopters like the Bell 407: the barometric encoder frequently reports `'ground'` while the aircraft is hovering at 300-500 ft AGL. Geometric altitude disagrees, so the classifier treats the point as airborne and flags the flight as `altitude_error` when the ratio gets high enough.
- **Speed override**: a `'ground'` altitude at flight speed (>80 kt) is always a glitch, not a landing.
- **OpenSky data with no ground speed**: requires two consecutive ground points before confirming a landing, to avoid false touchdowns from altitude glitches.

Additional state machine behaviors:

- **Intra-trace gap splitting**: any gap longer than 30 minutes between consecutive points (absolute value - a backwards-in-time jump also triggers a close) finalizes the pending flight.
- **Post-landing window**: after a ground transition, the flight stays "open" for up to 60 seconds or 5 more ground points to collect landing-quality metrics.
- **Touch-and-go detection**: an airborne point inside the post-landing window finalizes the current flight and immediately opens a new one.
- **Short-movement filter**: flights shorter than 5 minutes that travel less than 5 km are filtered out as taxi movements. Single-point "flights" left over from phantom trace points are also dropped.

## Fragment stitching

After extraction, a post-processing pass walks each aircraft's flights chronologically and merges pairs where a previous flight ended without a landing and the next flight starts with `takeoff_type = found_mid_flight`, within:

- **Type-endurance-aware time gap**: `max(stitch_max_gap_minutes, endurance_for(type_code) * stitch_endurance_ratio)`. The default `stitch_max_gap_minutes` is 90, which is the right window for light GA. For long-endurance types that regularly have multi-hour coverage gaps during one operational mission (KC-135R at 720 min, KC-46 at 780 min, C-5M at 900 min, GLF6 at 900 min, etc.), the effective window scales up automatically. With the default `stitch_endurance_ratio = 0.4`, a KC-135R gets a 288-minute stitch window while a Cessna 172 stays at 96 minutes. Without this scaling, a tanker orbit over restricted airspace shows up as two signal-lost fragments instead of one continuous flight.
- **Great-circle distance** less than `cruise_speed * time_gap * 1.2` (with 300 kt as the upper bound)
- **Altitude delta** under 3000 ft

The stitched flight inherits the original takeoff position and time, which recovers the actual origin airport for flights that would otherwise be classified as mid-flight fragments. Duration is recomputed after merging so the wall-clock span covers the coverage gap.

## Altitude cross-validation

`max_altitude` uses a three-layer defence against corrupt altitude spikes from pressure-datum swaps and geometric-altitude errors:

1. **AP-validated persistence filter.** Only samples where `nav_altitude_mcp` (autopilot target) is present AND agrees with the candidate altitude (within 5,000 ft) enter the persisted peak tracker. Squawk is not used -- it's always present on operating transponders and does not correlate with altitude validity. On B748 06a1e4, the correlation was perfect: 4/4 ceiling violations had squawk set but AP NULL; 198/198 normal flights had AP set.

2. **Raw fallback.** Flights without AP data fall back to the raw max (all samples tracked unconditionally). This ensures every flight gets a max_altitude even with sparse mode-S data.

3. **Per-type ceiling cap.** `TYPE_CEILINGS` maps each type code to its book service ceiling. Flights with coherent AP data (AP present and agreeing with max_altitude within 5,000 ft) get 10% tolerance; flights without coherent AP cap at exactly the book ceiling. This prevents uncorroborated spikes from exceeding physical limits while preserving legitimate high-altitude operations.

The same persistence filter pattern applies to `max_gs_kt`, with per-type caps from `TYPE_MAX_GS`.

## Helipad clustering and naming

After flight extraction, landing coordinates that don't match any airport are clustered using DBSCAN (200 m epsilon) to identify recurring helipad sites. Each cluster becomes a row in the `helipads` table with a centroid and landing count.

Helipad names are enriched in two passes: (1) an OurAirports CSV join that matches helipad centroids against `type='heliport'` entries within 500 m, and (2) a manual override dictionary for known facilities not in external databases (hospitals, HEMS bases, international helipads). The enrichment runs once per `extract` call and only updates helipads that still have generic names.

Flight-helipad linkage (`origin_helipad_id`, `destination_helipad_id`) is back-filled after all flights are inserted, matching takeoff/landing coordinates against helipad centroids within 200 m.

## Airport matching

Origin and destination are matched via a bounding-box query against the OurAirports database (~47k airports) followed by haversine distance calculation. Origin/destination ICAO codes are only populated when the match is within 2 km (on-field). Farther hits (up to 10 km) populate diagnostic `nearest_origin_icao` / `nearest_destination_icao` columns so the information isn't lost but helicopters and offshore ops don't get false-attributed to nearby civil airports. Matches are skipped entirely for `signal_lost` and `dropped_on_approach` flights.
