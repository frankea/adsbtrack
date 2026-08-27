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

**Squawk signals.** `squawks_observed`, `had_emergency`, `primary_squawk`, plus the pre-existing `squawk_first`, `squawk_last`, `squawk_changes`, `emergency_squawk`, `vfr_flight`. Every trace point carries a transponder squawk code; the extractor credits each point's inter-point interval to the then-held code and emits three new aggregate columns at end of flight.

- `squawks_observed` is a JSON-encoded sorted list of every unique squawk code seen, e.g. `'["1200","5201","7700"]'`. NULL when the flight carried no squawk data.
- `had_emergency = 1` when any of the three emergency codes (7500 hijack, 7600 radio failure, 7700 emergency) appeared at any point. Independent of `emergency_squawk`, which records the single most-severe code observed.
- `primary_squawk` is the squawk held for the greatest cumulative duration. On exact duration ties the alphabetically earliest code wins (deterministic). For steady-state VFR flights this is typically "1200" (US) or "7000" (EU); for flights with ATC handoffs it is the code held for the longest single segment.

These columns are diagnostic only and do not feed into mission classification or confidence scoring. Military-allocation squawks (US 4000-4777 block, MODE 3/A) are persisted in `squawks_observed` like any other code; the extractor deliberately does not tag or visually highlight them.

**Callsigns.** `callsigns` is a JSON array of distinct callsigns seen. The distinct count is derivable as `len(json.loads(callsigns))` when `callsigns` is non-null. `callsign_changes` is capped at `max(0, distinct - 1)` so ping-pong flicker doesn't inflate the count.

**Probable destination.** For `signal_lost` and `dropped_on_approach` flights, `probable_destination_icao` is inferred from the last-seen position with a separate confidence score based on altitude, distance, and descent rate. The `trips` CLI table, TUI flights view, and GUI flights table all display this as a yellow `~ICAO` marker in the destination column for both landing types (issue #18); a null `origin_icao` gets the same `~ICAO` treatment from `nearest_origin_icao` when a near-match airport (2-10 km) exists.

**Takeoff runway.** `takeoff_runway`. Runway name the aircraft used to depart, inferred by testing which of the origin airport's runway trapezoid polygons the first 600 seconds of trace data passed through longest. For each runway end a trapezoid is built at the runway threshold, extending 6 km along the departure heading (`Config.takeoff_runway_zone_length_m`) with a 50 m narrow base (`Config.takeoff_runway_little_base_m`) and a 5 degree symmetric opening (`Config.takeoff_runway_opening_deg`). Points are filtered to those below `airport_elevation + 2,000 ft` (`Config.takeoff_runway_max_ft_above_airport`) that are either climbing faster than 256 fpm or rolling above the minimum ground speed. The runway whose polygon was occupied longest wins, subject to reaching the GS floor inside the polygon.

The minimum-GS threshold scales by aircraft type. Commercial jets use 140 kt (`Config.takeoff_runway_min_gs_kt_default`). Helicopters (any type_code starting with `H` or present in `Config.helicopter_types`) and light piston singles (type_codes listed in `Config.takeoff_low_gs_types`: C150, C152, C172, DA20, PA28, SR22, etc.) drop to 60 kt (`Config.takeoff_runway_min_gs_kt_low`) so their slower rotation speeds don't disqualify an otherwise clean takeoff segment. Reimplementation of the `PolygonBasedRunwayDetection` class from `xoolive/traffic` (MIT-licensed); attribution in `adsbtrack/takeoff_runway.py`. NULL when the airport has no runway rows, no polygon matched, or the GS floor wasn't reached.

**ILS alignment.** `aligned_runway`, `aligned_seconds`, `aligned_min_offset_m`. A geometric signal that says "the aircraft was established on final for runway X for N seconds." For each runway end at the candidate landing airport (from `destination_icao`, else `nearest_destination_icao`, else `probable_destination_icao`), the detector keeps trace points where (a) perpendicular offset from the extended centerline is under 100 m (`Config.ils_alignment_max_offset_m`), (b) the bearing to the threshold has a positive track-component (aircraft moving toward it), and (c) the altitude is under `airport_elevation + 5,000 ft` (`Config.ils_alignment_max_ft_above_airport`). Kept points are split on gaps longer than 20 s; any segment at least 30 s long (`Config.ils_alignment_min_duration_secs`) becomes a candidate. The longest candidate across all runway ends wins. Reimplementation of the algorithm in `xoolive/traffic`'s `LandingAlignedOnILS` (MIT-licensed); attribution in `adsbtrack/ils_alignment.py`. NULL when the airport has no runway rows or no segment qualified.

The alignment result feeds two downstream signals:

1. **Landing confidence bump.** `landing_confidence` gets an additive bonus (clamped to 1.0): `+0.15` when `aligned_seconds >= 30` (`Config.ils_alignment_bonus_short_secs`), `+0.25` when `aligned_seconds >= 60` (`Config.ils_alignment_bonus_long_secs`). This is independent of the geometric-mean factors inside `score_confidence` so a missing or noisy factor cannot cancel the alignment evidence.

2. **Classification upgrade.** A `signal_lost` flight with `aligned_seconds >= 60` at an altitude below `airport_elevation + 5,000 ft` is promoted to `dropped_on_approach`. The alignment proves the aircraft was geometrically committed to a specific runway at low altitude even though we never observed touchdown, which is precisely what `dropped_on_approach` is meant to capture. Other types (`confirmed`, `dropped_on_approach`, `uncertain`, `altitude_error`) are never re-classified by alignment; they record the alignment columns as metadata only.

**Go-around and pattern work.** `had_go_around`, `pattern_cycles`. After computing the longest ILS-aligned segment for landing confidence (previous subsection), the extractor also collects ALL qualifying segments at the candidate landing airport via `adsbtrack.ils_alignment.detect_all_ils_alignments`. `pattern_cycles` is the count of qualifying segments for the flight (1 for a normal approach, 2+ for go-around / touch-and-go / pattern work). `had_go_around = 1` when any two consecutive segments are separated by a climb exceeding 500 ft above the earlier segment's end altitude (`adsbtrack.parser._any_climb_between`).

**Additive pattern trigger.** The mission classifier's existing `pattern` rule (same-airport flight with `max_altitude < 3000 ft`) is complemented by a second trigger in the parser: when `origin_icao == destination_icao` and `pattern_cycles >= 2`, the flight is promoted to `mission_type = "pattern"` regardless of its peak altitude. The upgrade only fires when the prior classification was `unknown`, `transport`, or already `pattern`; more specific buckets (`training`, `ems_hems`, `survey`, etc.) are not overridden. This catches pattern practice that climbs above the 3000 ft cutoff or that originally got classified as a transport flight between the same two ICAO codes on paper.

**Full-flight coverage and tie-break caveats.** `aligned_seconds`, `pattern_cycles`, and `had_go_around` are computed from `FlightMetrics.all_points`, the full per-flight point stream, not `recent_points` (a 240-sample tail deque covering roughly 4-8 minutes at 1-2 s trace spacing -- see the Navaid alignment section below for the same distinction). This means long pattern sessions (30+ minutes) count every lap, not just whichever ones fit in the tail window. `all_points` is cleared once these derivations and the navaid pass complete, so per-flight buffers don't stay pinned in memory across a multi-hundred-flight hex. When two runway ends produce alignment segments of exactly equal `duration_secs`, the earlier-starting segment wins (deterministic tie-break on `first_ts`); this is a behavior change from the initial ILS alignment milestone, where iteration order of runway rows decided the tie.

**Landing airport anchor.** `landing_anchor_method` records whether the destination / probable-destination airport match used the altitude-minimum point within the final 10 minutes of the flight (`"alt_min"`) or fell back to the last observed position (`"last_point"`). The altitude minimum is a stronger "where the aircraft was trying to land" estimator than the last point, which can be at altitude or laterally drifted on `signal_lost` / `dropped_on_approach` flights. The window length is configurable via `Config.landing_anchor_window_minutes` (default 10). The anchor is used both to pick candidate airports via the on-field bounding-box query and to score the final match; the landing confidence factors and weights are unchanged.

**Turnaround.** `turnaround_minutes` is the gap from the previous flight's landing (or last_seen) to this flight's takeoff, same ICAO. Capped at 72 hours; longer gaps are NULL. `turnaround_category` bins this into `quick` (<30 min), `medium` (30-240 min), `overnight` (4-18 h), `multi_day` (>18 h), `extended_gap` (>72 h), `first_observed`, or `last_observed`. Every flight has a non-null category. `is_first_observed_flight` and `is_last_observed_flight` are symmetric boolean flags (exactly 1 per aircraft each).

**Helipad linkage.** `origin_helipad_id` and `destination_helipad_id` link flights to DBSCAN-clustered helipad sites (within 200 m of the cluster centroid). Helipad names are enriched from OurAirports heliport entries (500 m join tolerance) plus manual overrides for known facilities not in external databases. 85 of 185 clusters carry real facility names, covering 87% of helipad-origin flights.

**Type override.** `type_override` is set when a flight's cruise envelope indicates it's not the registered type. Used for ae69xx ICAOs registered as H60 (Black Hawk) that sometimes fly fixed-wing profiles (C-17, KC-135) -- these get `type_override = 'MIL_FW'` so ceiling and GS caps use the correct envelope.

## Aircraft registry and stats

**`aircraft_registry`** is the authoritative metadata for each ICAO. The registry is populated at the start of every `extract` call by picking the most recently fetched `trace_days` row as the source of truth, then flagging metadata drift when other rows disagree on type_code, description, or registration.

**`aircraft_stats`** is a rollup table refreshed at the end of every extract: `total_flights`, `confirmed_flights`, `total_hours`, `total_cycles`, `distinct_airports`, `distinct_callsigns`, `avg_flight_minutes`, `busiest_day_date`, `busiest_day_count`, `home_base_icao`, `home_base_share`, `home_base_uncertain`, `second_base_icao`, `second_base_share`. `home_base_uncertain = 1` when `home_base_share < 0.40` (nomadic aircraft operating from multiple bases). Populated via SQL aggregation over the `flights` table.

Both tables are surfaced in the `status` command and queryable directly.

## Position source breakdown

Every point in a readsb trace carries a source type (`adsb_icao`, `mlat`, `tisb_icao`, `other`, `adsc`, ...). `classifier.FlightMetrics` tallies five buckets per flight -- `adsb_points`, `mlat_points`, `tisb_points`, `adsc_points`, `other_points` -- and the parser writes `adsb_pct`, `mlat_pct`, `tisb_pct`, `adsc_pct`, `other_pct` on the flight row at close time. `adsc_pct` covers CPDLC/ADS-C oceanic reports; `other_pct` is the catch-all for anything the named buckets don't claim (readsb's own `other` and `mode_s` tags land here). Points with no source tag contribute to none of the buckets, so the five percentages need not sum to 100.

The point source is read from trace element `point[9]` in 14-element rows and from `detail["type"]` in 9-element rows (they match in every observed sample). OpenSky-synthesized traces under 10 elements with no `detail` object get NULL sources and contribute no percentage.

A flight whose `adsb_pct` is high (>90) while `mlat_pct` and `other_pct` are both 0 across hours of claimed cruise is a suspicious signature: real en-route traffic picks up MLAT samples from multi-receiver overlap and `other` samples from ADS-R / Mode-S rebroadcasts. Pure-ADS-B-only flights far from any feeder are either out of coverage or (combined with other tells) spoofed.

`status` renders a "Position sources" block showing the five percentages, weighted by flight `data_points`, whenever any flight in the dataset has at least one tagged point.

## Spoofed-broadcast handling

**Detector (`events.py`).** `collect_events(db, icao, include_spoof_checks=True)` runs a bimodal-integrity scan over stored trace_days and emits a `spoof_bimodal_integrity` event for any date whose pooled ADS-B version-2 samples show `sil=0` on >= 10% of samples (`Config.spoof_v2_sil0_pct`), provided the pooled v2 count on the date is >= 25 (`Config.spoof_min_v2_samples`). Pooling runs across every aggregator that fetched the same date so a single source's transient integrity-field glitch does not by itself produce an event; real spoofs emitted over the air hit every receiver that can hear them. Event context includes the per-source `sil=0` rate list so the evidence is auditable. The flag defaults to False so historical queries do not retroactively tag trace_days.

**Why `sil=0` under v2 is diagnostic.** DO-260B transponders on production aircraft report a Source Integrity Level >= 2; a populated broadcast with a significant fraction of `sil=0` samples implies either two independent emitters on the same ICAO (one realistic, one garbage) or a single spoofer that hardcoded the integrity fields. Empirical calibration from the 2026-04 Strait-of-Hormuz Emirates A380 spoofs had the spoofed days at 14-42% pooled v2_sil0, while the same airframes' legitimate Dec-2025 flights sat at 0-1.4%.

**Reject-in-extract gate (`parser.py`, flight-scoped, issue #22).** Day-level pooling used to back this gate too: any flight whose `takeoff_date` had a flagged pooled v2_sil0 rate was quarantined, including real flights that only transited a GPS-jamming corridor on a day that separately carried a ghost broadcast (A6-EUY 896483, 2026-05-19: the day's pooled 817 v2 samples were 21.5% sil0 and the day-scoped gate rejected all five extracted candidates, two of which were real). The gate is now flight-scoped: it reads `FlightMetrics.v2_samples` / `v2_sil0` / `v2_nic0` (the same per-point predicate as `integrity.count_v2_integrity` -- `ac["version"] == 2`, `sil == 0`, `nic == 0` -- applied to that flight's own points) and `FlightMetrics.max_implied_speed_kt` (peak inter-fix ground speed, computed from consecutive-point great-circle distance / elapsed time, skipping sub-10s spacing as jitter-dominated). Before inserting each derived flight into `flights`, the extractor applies two tiers plus the unchanged crude heuristic:

1. `bimodal_integrity`, trigger `hard_sil0` -- the flight's own v2_sil0 share is >= `config.spoof_flight_sil0_hard_pct` (60%). Catches saturated ghost fragments outright (the A6-EUY ghosts ran 87-100% sil0).
2. `bimodal_integrity`, trigger `sil0_plus_teleport` -- the flight's v2_sil0 share is >= `config.spoof_v2_sil0_pct` (10%) AND `max_implied_speed_kt` exceeds `config.spoof_teleport_speed_kt` (900 kt; no aircraft in this DB's scope sustains that over ground). Catches Hormuz-style 25-50% sil0 spoofs, which teleport between fixes, while a jammed-but-real flight whose broadcast positions stay physically plausible passes. A6-EUY's 2026-05-19 outbound DXB-DUS leg is the counter-example, not a pass case: moderate sil0 (21.27%) AND a 1,684 kt implied position jump, so it quarantines under this trigger -- its jammed GPS genuinely teleported. The same-day return legs (near-zero sil0, physical ground speeds) extract normally.
3. `crude_heuristic` -- unchanged: `max_altitude < config.spoof_crude_max_altitude_ft` (default 500 ft) AND `origin_icao` / `destination_icao` both NULL AND callsign matching `^EK\d+$` (IATA flight-number format, not an ATC callsign).

The bimodal tiers win if either fires; a flight below `config.spoof_min_v2_samples` (25) v2 samples skips the bimodal check entirely (too little signal to trust the ratio) and falls through to the crude heuristic. A rejected flight is inserted into `spoofed_broadcasts` (see [schema.md](schema.md)) with reason + detail, skipped from `flights`, and does not advance `prev_end_time` so turnaround math for the next real flight is not polluted by a fabricated gap. `reason_detail.scope == "flight"` marks rows produced by this flight-scoped gate; day-scoped rows written before issue #22 remain in existing databases and are still valid history, just without that key. Turn off by setting `config.reject_spoofed_flights = False`.

Day-level pooling (`pool_spoof_scores`) still exists, but only backs the events-layer detector above -- it is no longer consulted by the reject-in-extract gate.

## Integrity/jamming surface columns

GPS jamming/spoofing corridor cases keep recurring (Levant, Gulf/Iran), and the reject-in-extract gate above deliberately keeps real flights that merely transited a jammed corridor. Issue #30 surfaces the gate's already-computed per-flight stats as first-class `flights` columns so those kept-but-degraded flights are queryable without decoding raw traces:

* `v2_sample_count` -- the flight's DO-260B version-2 sample count (`FlightMetrics.v2_samples`).
* `integrity_degraded_pct` -- share of those samples reporting `sil=0`, the exact number `parser._flight_is_spoofed` computes for its tiers. NULL when the flight carried no v2 samples.
* `max_implied_speed_kt` -- the issue #22 teleport detector: peak consecutive-fix great-circle distance / elapsed time, sub-10s spacing skipped as jitter-dominated.
* `integrity_flagged` -- 1 when `integrity_degraded_pct >= Config.integrity_flag_degraded_pct` (default 5%) with at least `Config.spoof_min_v2_samples` (25) v2 samples, or when `max_implied_speed_kt > Config.integrity_flag_teleport_kt` (default 800 kt) AND `integrity_degraded_pct >= Config.integrity_flag_teleport_min_degraded_pct` (default 2%, same v2 floor).

The flag thresholds are deliberately lower than the spoof gate's (5% vs the 10% tier-2 floor / 60% hard tier; 800 kt vs the 900 kt quarantine teleport): the gate quarantines fabrications into `spoofed_broadcasts`, while the flag marks degraded-but-kept flights. The teleport trigger requires corroborating sil0 degradation, mirroring the gate's own corroboration structure: the 2026-08 three-aircraft calibration (per the CLAUDE.md rule) showed a standalone >800 kt trigger flagging 18.5% (152/820) of the clean-corridor US GA baseline (ad3f65) -- historical ADS-B traces carry position-decode garbage with implied jumps up to ~468,000 kt on flights whose integrity fields are pristine, and those spikes are decode noise, not GPS interference. With corroboration required, the calibration spread is: ad3f65 (clean corridor) 0/820 flagged, degraded pct 0.0 throughout (one 5.00% outlier sat below the 25-sample floor); A6-EUY 896483 (Gulf/Levant corridor) 1/14 kept legs flagged (2026-05-21, 4.08% sil0 + 3,118 kt implied jump) on top of 2 quarantined jammed legs; ZK019 43c556 0/9 (no v2 data at all, so the metric is correctly silent rather than spuriously firing on its 100%-MLAT position jitter). A transparency-spectrum panel run at the same thresholds (open-operator PC-12 N512WB a66ad3 1/695 flagged, manufacturer-demo N999YY adf64f 1/723, trust-anonymized N9527C ad3f65 0/820) stayed flat at 0-0.14% while the corridor-exposed A6-EUY sat at 1/14 kept + 2 quarantined: the contrast tracks route exposure, not operator opacity, which is the expected ground truth for a GNSS-integrity flag.

All four columns are populated by `_copy_metrics_to_flight` at extract time and are NULL on rows written before the columns shipped -- run `extract --reprocess` to backfill. The TUI flights view and the GUI flight table render an `INTEG` pill for flagged flights, and the TUI/GUI spoof views surface `max_implied_speed_kt` and the firing trigger as proper columns.

## ACARS OOOI on flights

When `acars --hex <icao> --start <date>` runs, the fetcher pulls ACARS / VDL2 / HFDL messages for the aircraft from airframes.io and the OOOI parser scans each message against the flight timeline. Supported formats:

* **Air Canada AGFSR 4T** -- trailing 4 slash-delimited fields on label `4T` are OUT / OFF / ON / IN as `HHMM` (or `----` for events that haven't happened yet)
* **Keyword scan** -- labels `14`, `44`, `4T`, `H1` with free-form text containing `OUT 0830` / `OFF 0855` / `ON 1230` / `IN 1245` (case-insensitive, word-boundaried)

Matched timestamps are anchored to the flight's calendar day (+/-1 day) via closest-time-to-reference heuristic to handle UTC day rollover. Four columns get populated on the `flights` table: `acars_out`, `acars_off`, `acars_on`, `acars_in` (ISO 8601 UTC, NULL when no OOOI match fell inside the flight window).

`trips` renders an ACARS column when the aircraft has any stored messages: message count alone, or `N OOOI` in green when any of the four OOOI columns are non-null. `status` shows a per-aircraft rollup (total messages, total flights covered, count with OOOI).

## Hex cross-reference

`enrich all` and `enrich hex` populate `hex_crossref` by merging three external sources in preference order: FAA registry -> Mictronics DB -> hexdb.io live lookup. Conflicts between sources (differing registrations or type codes) are reported to the caller for manual review but don't prevent the row from being written.

Every hex is also checked against `mil_hex_ranges` independently of the civilian identity sources: a hex can carry a Mictronics registration AND be flagged `is_military=1` with country / branch attribution, which surfaces government-operated aircraft sitting in known military allocation blocks (e.g. Bell 407s in the US DoD AE-prefix range).

`lookup <hex|registration>` runs the same merge for a single ad-hoc query - any country's registration format, not just FAA N-numbers - and adds a final adsbdb fallback for foreign airframes hexdb.io misses, caching whatever it finds into `hex_crossref`. An unresolvable hex inside a military allocation block still gets the range annotation (country, branch, notes) as its answer.

`resolve <callsign>` covers the remaining entry point: casework that starts from a flight number. It asks the open live-traffic APIs (adsb.lol, then adsb.fi) which airframes are broadcasting that callsign right now and returns hex + registration + type per match, caching identities into `hex_crossref` under a `<network>_live` tag (never overwriting an existing identity row). Live-only: historical callsign search is out of scope.

## Navaid alignment

**Column:** `navaid_track` (JSON string or NULL).

**What it captures.** For each flight, the ordered list of VORs / NDBs / fixes whose bearing the ground track pointed directly toward for at least `navaid_min_duration_secs` seconds, excluding segments whose closest approach to the navaid exceeded `navaid_near_pass_max_nm` nm. This is a compact fingerprint of the enroute routing: a helicopter that always flies `SHAWZ -> KEEMO -> direct destination` will show that chain on most flights, while a point-to-point shuttle will typically show zero or one navaid.

**Algorithm (adsbtrack/navaid_alignment.py).** For each flight and each pre-filtered navaid within the flight's bounding box (plus `navaid_bbox_buffer_nm` buffer), the per-point bearing-to-navaid is compared with the ground track. Points with delta under `navaid_alignment_tolerance_deg` and distance under `navaid_max_distance_nm` are kept. Kept points are split into segments on any gap longer than `navaid_split_gap_secs`, then segments are filtered by `navaid_min_duration_secs` duration and `navaid_near_pass_max_nm` closest-approach distance. Defaults: 1 degree tolerance, 500 nm cutoff, 120 s gap split, 30 s duration floor, 80 nm closest-approach cap.

**Input source.** The detector is fed from `FlightMetrics.all_points`, which is a full per-flight list (unlike `recent_points`, which is a 240-sample tail deque insufficient for enroute alignment).

**Output shape.** Each qualifying segment serializes as `{"navaid_ident": "<IDENT>", "start_ts": <unix>, "end_ts": <unix>, "min_distance_nm": <number>}`. Flights with no qualifying segments emit NULL rather than `[]` so the column is informative.

**CLI.** `adsbtrack.cli route --hex <icao>` prints one line per flight with a non-empty navaid_track:

    2026-03-27 KSPG -> KHKY  SHAWZ (15m) -> KEEMO (8m) -> CLT (3m)

**Limitations.**

- A 1-degree tolerance and 500 nm range mean that on any long straight leg, a passing sector can coincidentally align with a distant navaid. The 80 nm closest-approach filter rejects most such spurious matches but not all: navaids the aircraft actually flew past by 30-80 nm will register even when the pilot had no intent to track them. Alignment is not intent.
- The algorithm treats each navaid independently. An aircraft that alternates between two parallel airways a few nm apart will produce both navaids in its fingerprint.
- Antimeridian-crossing flights are skipped (`flight_bbox_from_points` returns None when the longitude span exceeds 180 deg). This is negligible for US / Europe / single-operator workloads; revisit if it becomes relevant for a future region.
- The `navaids` table must be refreshed via `adsbtrack.cli navaids refresh` for this column to ever populate. With an empty navaids table, `navaid_track` is always NULL.

**Performance.** Measured on a local 1.8 GB dev DB over two aircraft, `extract --reprocess` median wall-clock across three runs each:

| Hex      | Flights | Avg points / flight | Empty navaids | 11,010 navaids | Delta |
|----------|---------|---------------------|---------------|----------------|-------|
| a7a622   | 1,609   | 119                 | 3.69 s        | 4.75 s         | +29%  |
| a4aaa0   | 515     | 502                 | 5.33 s        | 12.71 s        | +138% |

The light-trace rotorcraft hex stays under the <50% target; the heavy-trace fixed-wing hex with wide geographic spread (49 distinct origins across the eastern US) exceeds it because per-flight cost scales with `points_per_flight * navaids_in_bbox` and the bbox cache hits are diluted by operator diversity. Overhead is bounded by `O(flights * navaids_in_cached_bbox * points_per_flight)`; the per-extract-run bbox cache (quantized to 0.5 degrees) coalesces same-region flights so the SQL cost is paid once per ~0.5-degree grid cell rather than per flight. The 1-degree bearing tolerance rejects most points before any trig, which keeps the constant factor small but not small enough to hide at ~500 points / flight over a US-wide bbox. Users ingesting wide-ranging fleets should expect the navaid column to roughly double `extract --reprocess` time; narrow-operation fleets (helicopters, regional commuters) will see closer to the 30% range. Numbers are from a single developer machine and are indicative rather than authoritative.
