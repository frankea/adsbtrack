# Flight quality and derived features

## Landing types and confidence scoring

Every extracted flight carries two independent confidence scores (`takeoff_confidence`, `landing_confidence`) in [0.0, 1.0] plus a landing type:

| Landing type | Meaning |
|--------------|---------|
| `confirmed` | Clean landing: clear descent, low final speed, low final altitude, ground points collected, and stable coordinates |
| `signal_lost` | Aircraft was airborne at last contact - coverage dropped mid-flight |
| `dropped_on_approach` | Signal lost but the last few samples show sustained descent below 5000 ft. The landing probably happened at a nearby airport but we never saw it |
| `uncertain` | Ambiguous - duration exceeds max endurance for the type (likely a data gap artifact), or low/slow but no landing transition |
| `altitude_error` | The barometric encoder is clearly broken for this flight (Bell 407 hover pathology or similar) |

Takeoff type similarly distinguishes `observed` (we saw the ground-to-airborne transition) from `found_mid_flight` (first trace point was already airborne). `found_mid_flight` flights cap their takeoff confidence at 0.30 because we never observed the actual origin.

**Landing confidence** is a weighted geometric mean across seven factors:

- **Descent signature** over a 30-180 sec pre-flare window (skips the flare itself so clean ILS approaches score high)
- **Approach speed** (120-150 kt for jets, 25-50 kt for helicopters, both rewarded)
- **Final altitude** (lower = better)
- **Airport proximity** at the landing point
- **Per-sample coordinate stability** (normal taxi motion is fine; only sudden >500 m jumps count as receiver noise)
- **Post-landing ground points** (how many samples confirmed the touchdown)
- **Duration plausibility** (penalized for flights approaching multi-day data gaps)

The geometric mean lets any single failing factor drag the whole score down, which catches "this looks like a landing by 5 metrics but the descent trace is missing" cases that a simple average would gloss over.

**Max endurance is per aircraft type**: a 240 min global cap would reject legitimate Gulfstream transcons, so the classifier consults a type_code lookup (B407=180, S92=300, PC12=420, GLF6=900, KC-135R=720, C-17=720, KC-46=780, C-5M=900, etc.). Flights longer than the type's endurance become `uncertain` rather than `confirmed`. The same lookup feeds the type-endurance-aware fragment stitcher, so long-endurance types can merge across the wider coverage gaps that are normal on their operational missions.

## Derived per-flight features

Beyond classification and confidence, every extracted flight is tagged with a set of derived features that turn "a flight happened" into something you can query against in detail. These come from a single pass that accumulates raw counters during trace processing and a post-classification pass that turns those counters into per-flight columns.

**Mission classification.** `mission_type` is one of `ems_hems`, `offshore`, `exec_charter`, `training`, `survey`, `pattern`, `transport`, or `unknown`. Resolved by a callsign prefix lookup table (TWY/GLF -> exec_charter, PHM/PHI/ERA -> offshore, N911 / *MT suffix -> ems_hems, etc.) followed by physics rules: high loiter ratio + low cruise speed -> survey, same-airport low-altitude -> pattern, distinct origin/destination -> transport.

**Path metrics.** `path_length_km` is the haversine sum of all in-flight segments (skipping coverage holes > 60 s). `max_distance_km` is the max distance ever reached from the takeoff point. `loiter_ratio = path_length / (2 * max_distance)` - a value of 1.0 is a straight there-and-back, 3+ is a survey or holding pattern, 5+ is dedicated orbiting. `path_efficiency = great_circle / path_length` is populated only when origin and destination are different airports.

**Phase of flight.** `climb_secs`, `cruise_secs`, `descent_secs`, `level_secs` partition the flight into climb (rate > +250 fpm), descent (rate < -250 fpm), and level. Cruise is the level subset above 70% of `max_altitude`, with `cruise_alt_ft` as a time-weighted mean and `cruise_gs_kt` as a time-weighted median with 2-sigma outlier rejection. The four bins are rescaled proportionally so their sum equals `active_minutes * 60` exactly. `cruise_detected` is 1 when a stable cruise segment was found, 0 otherwise (never NULL).

**Signal budget.** `active_minutes` is the on-signal wall-clock time (sum of phase seconds / 60). `signal_gap_secs` is `duration_minutes * 60 - active_minutes * 60`. `signal_gap_count` is the number of inter-point gaps larger than 60 s observed while airborne. `fragments_stitched` counts how many raw trace fragments were merged into this flight (1 = not stitched).

**Peak rates.** `peak_climb_fpm` and `peak_descent_fpm` are the best mean rate observed over a 60-second rolling window with outlier filtering (not point-to-point). Hard-capped at 10,000 fpm in either direction.

**Altitude cross-validation.** `max_altitude` uses a layered defence: (1) an AP-validated persistence filter -- only samples where `nav_altitude_mcp` is present AND agrees with the altitude (within 5,000 ft) enter the persisted peak tracker; (2) a raw fallback for flights without AP data; (3) per-type ceiling caps from `TYPE_CEILINGS` -- flights with coherent AP get 10% tolerance, flights without AP cap at exactly the book ceiling. This eliminates pressure-datum-swap spikes that previously pushed B748 to 49,500 ft and S92 to 16,500 ft. `max_gs_kt` uses a similar dual-track persistence filter with per-type caps from `TYPE_MAX_GS`.

**Signal quality.** `heavy_signal_gap` is 1 when `active_minutes / duration_minutes < 0.5`, flagging flights where more than half the duration was unobserved. These flights should be excluded from speed analyses since GS samples reflect only the observable (often low-speed) segments.

**Hover detection.** Helicopters only. `max_hover_secs` and `hover_episodes` count contiguous windows >= 20 s where the aircraft was airborne with `gs < 5 kt` and `|baro_rate| < 100 fpm`.

**Go-around detection.** `go_around_count` is the number of "approach -> climb -> approach" sequences in the final 600 s before touchdown. Only runs on confirmed landings.

**Headings.** `takeoff_heading_deg` and `landing_heading_deg` are circular means of ground-track samples in the first/last 60 s of the flight, filtered to `gs > 40 kt`. Helicopters use a widening fallback window with `gs > 10 kt`.

**Day / night.** `takeoff_is_night`, `landing_is_night`, and `night_flight`. `night_flight = 1` when either endpoint is night (FAR 91.205(c) standard). Computed inline using a NOAA solar-position approximation.

**Squawks.** `squawk_first` / `squawk_last`, `squawk_changes` (transition count), `emergency_squawk` (most severe of any 7500/7600/7700), `vfr_flight` (1 when >= 80% of squawks were 1200).

**Callsigns.** `callsigns` is a JSON array of distinct callsigns seen. `callsign_changes` is capped at `max(0, distinct - 1)` so ping-pong flicker doesn't inflate the count. `callsign_count` is the distinct count.

**Probable destination.** For `signal_lost` and `dropped_on_approach` flights, `probable_destination_icao` is inferred from the last-seen position with a separate confidence score based on altitude, distance, and descent rate.

**Takeoff runway.** `takeoff_runway`. Runway name the aircraft used to depart, inferred by testing which of the origin airport's runway trapezoid polygons the first 600 seconds of trace data passed through longest. For each runway end a trapezoid is built at the runway threshold, extending 6 km along the departure heading (`Config.takeoff_runway_zone_length_m`) with a 50 m narrow base (`Config.takeoff_runway_little_base_m`) and a 5 degree symmetric opening (`Config.takeoff_runway_opening_deg`). Points are filtered to those below `airport_elevation + 2,000 ft` (`Config.takeoff_runway_max_ft_above_airport`) that are either climbing faster than 256 fpm or rolling above the minimum ground speed. The runway whose polygon was occupied longest wins, subject to reaching the GS floor inside the polygon.

The minimum-GS threshold scales by aircraft type. Commercial jets use 140 kt (`Config.takeoff_runway_min_gs_kt_default`). Helicopters (any type_code starting with `H` or present in `Config.helicopter_types`) and light piston singles (type_codes listed in `Config.takeoff_low_gs_types`: C150, C152, C172, DA20, PA28, SR22, etc.) drop to 60 kt (`Config.takeoff_runway_min_gs_kt_low`) so their slower rotation speeds don't disqualify an otherwise clean takeoff segment. Reimplementation of the `PolygonBasedRunwayDetection` class from `xoolive/traffic` (MIT-licensed); attribution in `adsbtrack/takeoff_runway.py`. NULL when the airport has no runway rows, no polygon matched, or the GS floor wasn't reached.

**ILS alignment.** `aligned_runway`, `aligned_seconds`, `aligned_min_offset_m`. A geometric signal that says "the aircraft was established on final for runway X for N seconds." For each runway end at the candidate landing airport (from `destination_icao`, else `nearest_destination_icao`, else `probable_destination_icao`), the detector keeps trace points where (a) perpendicular offset from the extended centerline is under 100 m (`Config.ils_alignment_max_offset_m`), (b) the bearing to the threshold has a positive track-component (aircraft moving toward it), and (c) the altitude is under `airport_elevation + 5,000 ft` (`Config.ils_alignment_max_ft_above_airport`). Kept points are split on gaps longer than 20 s; any segment at least 30 s long (`Config.ils_alignment_min_duration_secs`) becomes a candidate. The longest candidate across all runway ends wins. Reimplementation of the algorithm in `xoolive/traffic`'s `LandingAlignedOnILS` (MIT-licensed); attribution in `adsbtrack/ils_alignment.py`. NULL when the airport has no runway rows or no segment qualified.

The alignment result feeds two downstream signals:

1. **Landing confidence bump.** `landing_confidence` gets an additive bonus (clamped to 1.0): `+0.15` when `aligned_seconds >= 30` (`Config.ils_alignment_bonus_short_secs`), `+0.25` when `aligned_seconds >= 60` (`Config.ils_alignment_bonus_long_secs`). This is independent of the geometric-mean factors inside `score_confidence` so a missing or noisy factor cannot cancel the alignment evidence.

2. **Classification upgrade.** A `signal_lost` flight with `aligned_seconds >= 60` at an altitude below `airport_elevation + 5,000 ft` is promoted to `dropped_on_approach`. The alignment proves the aircraft was geometrically committed to a specific runway at low altitude even though we never observed touchdown, which is precisely what `dropped_on_approach` is meant to capture. Other types (`confirmed`, `dropped_on_approach`, `uncertain`, `altitude_error`) are never re-classified by alignment; they record the alignment columns as metadata only.

**Go-around and pattern work.** `had_go_around`, `pattern_cycles`. After computing the longest ILS-aligned segment for landing confidence (previous subsection), the extractor also collects ALL qualifying segments at the candidate landing airport via `adsbtrack.ils_alignment.detect_all_ils_alignments`. `pattern_cycles` is the count of qualifying segments for the flight (1 for a normal approach, 2+ for go-around / touch-and-go / pattern work). `had_go_around = 1` when any two consecutive segments are separated by a climb exceeding 500 ft above the earlier segment's end altitude (`adsbtrack.parser._any_climb_between`).

**Additive pattern trigger.** The mission classifier's existing `pattern` rule (same-airport flight with `max_altitude < 3000 ft`) is complemented by a second trigger in the parser: when `origin_icao == destination_icao` and `pattern_cycles >= 2`, the flight is promoted to `mission_type = "pattern"` regardless of its peak altitude. The upgrade only fires when the prior classification was `unknown`, `transport`, or already `pattern`; more specific buckets (`training`, `ems_hems`, `survey`, etc.) are not overridden. This catches pattern practice that climbs above the 3000 ft cutoff or that originally got classified as a transport flight between the same two ICAO codes on paper.

**Landing airport anchor.** `landing_anchor_method` records whether the destination / probable-destination airport match used the altitude-minimum point within the final 10 minutes of the flight (`"alt_min"`) or fell back to the last observed position (`"last_point"`). The altitude minimum is a stronger "where the aircraft was trying to land" estimator than the last point, which can be at altitude or laterally drifted on `signal_lost` / `dropped_on_approach` flights. The window length is configurable via `Config.landing_anchor_window_minutes` (default 10). The anchor is used both to pick candidate airports via the on-field bounding-box query and to score the final match; the landing confidence factors and weights are unchanged.

**Turnaround.** `turnaround_minutes` is the gap from the previous flight's landing (or last_seen) to this flight's takeoff, same ICAO. Capped at 72 hours; longer gaps are NULL. `turnaround_category` bins this into `quick` (<30 min), `medium` (30-240 min), `overnight` (4-18 h), `multi_day` (>18 h), `extended_gap` (>72 h), `first_observed`, or `last_observed`. Every flight has a non-null category. `is_first_observed_flight` and `is_last_observed_flight` are symmetric boolean flags (exactly 1 per aircraft each).

**Helipad linkage.** `origin_helipad_id` and `destination_helipad_id` link flights to DBSCAN-clustered helipad sites (within 200 m of the cluster centroid). Helipad names are enriched from OurAirports heliport entries (500 m join tolerance) plus manual overrides for known facilities not in external databases. 85 of 185 clusters carry real facility names, covering 87% of helipad-origin flights.

**Type override.** `type_override` is set when a flight's cruise envelope indicates it's not the registered type. Used for ae69xx ICAOs registered as H60 (Black Hawk) that sometimes fly fixed-wing profiles (C-17, KC-135) -- these get `type_override = 'MIL_FW'` so ceiling and GS caps use the correct envelope.

## Aircraft registry and stats

**`aircraft_registry`** is the authoritative metadata for each ICAO. The registry is populated at the start of every `extract` call by picking the most recently fetched `trace_days` row as the source of truth, then flagging metadata drift when other rows disagree on type_code, description, or registration.

**`aircraft_stats`** is a rollup table refreshed at the end of every extract: `total_flights`, `confirmed_flights`, `total_hours`, `total_cycles`, `distinct_airports`, `distinct_callsigns`, `avg_flight_minutes`, `busiest_day_date`, `busiest_day_count`, `home_base_icao`, `home_base_share`, `home_base_uncertain`, `second_base_icao`, `second_base_share`. `home_base_uncertain = 1` when `home_base_share < 0.40` (nomadic aircraft operating from multiple bases). Populated via SQL aggregation over the `flights` table.

Both tables are surfaced in the `status` command and queryable directly.

## Position source breakdown

Every point in a readsb trace carries a source type (`adsb_icao`, `mlat`, `tisb_icao`, `other`, `adsc`, ...). `classifier.FlightMetrics` tallies three buckets per flight -- `adsb_points`, `mlat_points`, `tisb_points` -- and the parser writes `adsb_pct`, `mlat_pct`, `tisb_pct` on the flight row at close time. Points outside those three buckets (e.g. `other`, `mode_s`, `adsc`, NULL) are not counted, so the three percentages do not necessarily sum to 100.

The point source is read from trace element `point[9]` in 14-element rows and from `detail["type"]` in 9-element rows (they match in every observed sample). OpenSky-synthesized traces under 10 elements with no `detail` object get NULL sources and contribute no percentage.

`status` renders a "Position sources" block showing the three percentages, weighted by flight `data_points`, whenever any flight in the dataset has at least one tagged point.

## ACARS OOOI on flights

When `acars --hex <icao> --start <date>` runs, the fetcher pulls ACARS / VDL2 / HFDL messages for the aircraft from airframes.io and the OOOI parser scans each message against the flight timeline. Supported formats:

* **Air Canada AGFSR 4T** -- trailing 4 slash-delimited fields on label `4T` are OUT / OFF / ON / IN as `HHMM` (or `----` for events that haven't happened yet)
* **Keyword scan** -- labels `14`, `44`, `4T`, `H1` with free-form text containing `OUT 0830` / `OFF 0855` / `ON 1230` / `IN 1245` (case-insensitive, word-boundaried)

Matched timestamps are anchored to the flight's calendar day (+/-1 day) via closest-time-to-reference heuristic to handle UTC day rollover. Four columns get populated on the `flights` table: `acars_out`, `acars_off`, `acars_on`, `acars_in` (ISO 8601 UTC, NULL when no OOOI match fell inside the flight window).

`trips` renders an ACARS column when the aircraft has any stored messages: message count alone, or `N OOOI` in green when any of the four OOOI columns are non-null. `status` shows a per-aircraft rollup (total messages, total flights covered, count with OOOI).

## Hex cross-reference

`enrich all` and `enrich hex` populate `hex_crossref` by merging three external sources in preference order: FAA registry -> Mictronics DB -> hexdb.io live lookup. Conflicts between sources (differing registrations or type codes) are reported to the caller for manual review but don't prevent the row from being written.

Every hex is also checked against `mil_hex_ranges` independently of the civilian identity sources: a hex can carry a Mictronics registration AND be flagged `is_military=1` with country / branch attribution, which surfaces government-operated aircraft sitting in known military allocation blocks (e.g. Bell 407s in the US DoD AE-prefix range).
