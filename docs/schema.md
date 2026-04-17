# Database schema

All data is stored in a local SQLite database (`adsbtrack.db`). WAL mode is enabled so multiple fetch/extract sessions can run concurrently from different terminals. Schema migrations run automatically on open.

## trace_days

Raw daily trace data per aircraft per source.

| Column | Type | Description |
|--------|------|-------------|
| icao | TEXT | ICAO hex code |
| date | TEXT | Date (YYYY-MM-DD) |
| source | TEXT | Data source (adsbx, adsbfi, airplaneslive, etc.) |
| registration | TEXT | Tail number from trace metadata |
| type_code | TEXT | Aircraft type code (B407, S92, GLF6, etc.) |
| description | TEXT | Aircraft type description |
| owner_operator | TEXT | Owner from trace metadata |
| timestamp | REAL | Base Unix timestamp for the day |
| trace_json | TEXT | Raw trace points as JSON array |
| point_count | INTEGER | Number of trace points |

## flights

Extracted flights with airport matching, quality classification, confidence scoring, and derived features. See [features.md](features.md) for what each derived column means.

| Column | Type | Description |
|--------|------|-------------|
| icao | TEXT | ICAO hex code |
| takeoff_time | TEXT | ISO timestamp |
| takeoff_lat/lon | REAL | Takeoff coordinates |
| takeoff_date | TEXT | Source trace date (YYYY-MM-DD) |
| landing_time | TEXT | ISO timestamp (null if signal lost) |
| landing_lat/lon | REAL | Landing coordinates (null if signal lost) |
| landing_date | TEXT | Source trace date of landing |
| origin_icao | TEXT | Airport ICAO code (only when takeoff is within 2 km of the airport) |
| origin_name | TEXT | Airport name |
| origin_distance_km | REAL | Haversine distance from takeoff fix |
| nearest_origin_icao | TEXT | Nearest airport when takeoff is 2-10 km away (diagnostic) |
| nearest_origin_distance_km | REAL | Distance to nearest airport |
| destination_icao | TEXT | Airport ICAO code (only when landing is within 2 km) |
| destination_name | TEXT | Airport name |
| destination_distance_km | REAL | Haversine distance from landing fix |
| nearest_destination_icao | TEXT | Nearest airport when landing is 2-10 km away |
| nearest_destination_distance_km | REAL | Distance to nearest airport |
| duration_minutes | REAL | Wall-clock flight duration: (landing_time or last_seen_time) - takeoff_time |
| active_minutes | REAL | On-signal time (sum of phase seconds / 60) |
| signal_gap_secs | INTEGER | duration - active (coverage hole time) |
| signal_gap_count | INTEGER | Number of inter-point gaps > 60 s while airborne |
| fragments_stitched | INTEGER | Number of raw trace fragments merged (1 = not stitched) |
| callsign | TEXT | Callsign from ADS-B |
| landing_type | TEXT | `confirmed` / `signal_lost` / `dropped_on_approach` / `uncertain` / `altitude_error` |
| takeoff_type | TEXT | `observed` / `found_mid_flight` |
| takeoff_confidence | REAL | [0.0, 1.0] |
| landing_confidence | REAL | [0.0, 1.0] - weighted geometric mean of seven factors |
| data_points | INTEGER | Trace points recorded for this flight |
| sources | TEXT | Comma-separated data sources that contributed |
| max_altitude | INTEGER | Peak baro altitude in feet (AP-validated persistence filter; type-specific ceiling cap without tolerance when AP is absent or disagrees) |
| max_gs_kt | INTEGER | Peak ground speed in knots (persistence-filtered, type-capped at TYPE_MAX_GS * 1.1) |
| ground_points_at_takeoff | INTEGER | Ground points collected before takeoff transition |
| ground_points_at_landing | INTEGER | Ground points collected after landing transition |
| baro_error_points | INTEGER | Points where baro encoder disagreed with geometric altitude or ground speed |
| last_seen_lat/lon | REAL | Last observed position |
| last_seen_alt_ft | INTEGER | Last observed altitude |
| last_seen_time | TEXT | Last observed timestamp (ISO) |
| squawk_first / squawk_last | TEXT | First and last transponder code observed |
| squawk_changes | INTEGER | Number of transitions between distinct squawks |
| emergency_squawk | TEXT | Most severe of any 7500/7600/7700 observed |
| vfr_flight | INTEGER | 1 when >= 80% of squawks were 1200 |
| mission_type | TEXT | `ems_hems` / `offshore` / `exec_charter` / `training` / `survey` / `pattern` / `transport` / `unknown` |
| category_do260 | TEXT | DO-260B category (A0-B7) |
| autopilot_target_alt_ft | INTEGER | Last `nav_altitude_mcp` before first sustained descent |
| emergency_flag | TEXT | Latest non-"none" `detail.emergency` value |
| path_length_km | REAL | Sum of haversine between consecutive points |
| max_distance_km | REAL | Max distance reached from the takeoff point |
| loiter_ratio | REAL | path_length / (2 * max_distance) |
| path_efficiency | REAL | great_circle / path_length (only when origin != destination) |
| max_hover_secs | INTEGER | Longest contiguous hover (rotorcraft only) |
| hover_episodes | INTEGER | Count of hover episodes >= 20 s (rotorcraft only) |
| go_around_count | INTEGER | Approach-climb-approach sequences in the final 600 s |
| takeoff_heading_deg | REAL | Circular mean of track in first 60 s of takeoff |
| landing_heading_deg | REAL | Circular mean of track in last 60 s before landing |
| climb_secs / descent_secs / level_secs / cruise_secs | INTEGER | Phase-of-flight time budget |
| cruise_alt_ft | INTEGER | Time-weighted mean altitude during cruise phase |
| cruise_gs_kt | INTEGER | Time-weighted median ground speed during cruise phase (2-sigma outlier rejection, capped at max_gs_kt and TYPE_MAX_GS * 1.1) |
| cruise_detected | INTEGER | 1 if a stable cruise segment was found by the phase budget, 0 otherwise (never NULL) |
| heavy_signal_gap | INTEGER | 1 if active_minutes / duration_minutes < 0.5 (advisory: exclude from speed analyses) |
| peak_climb_fpm / peak_descent_fpm | INTEGER | Best 60-s rolling-window mean climb / descent rate |
| takeoff_is_night / landing_is_night | INTEGER | 1 if sun was below -6 degrees |
| night_flight | INTEGER | 1 if >= 50% of in-flight points were at night |
| callsigns | TEXT | JSON array of distinct callsigns seen |
| callsign_changes | INTEGER | Transitions between distinct callsigns (capped at distinct - 1) |
| callsign_count | INTEGER | Number of distinct callsigns |
| probable_destination_icao | TEXT | Inferred destination for dropped/signal-lost flights |
| probable_destination_distance_km | REAL | Distance from last_seen to inferred destination |
| probable_destination_confidence | REAL | [0.0, 1.0] confidence in the inference |
| aligned_runway | TEXT | Runway end the aircraft was geometrically aligned with on short final, e.g. `"09"` / `"26L"`. NULL when no segment qualified. |
| aligned_seconds | REAL | Duration in seconds of the longest qualifying alignment segment. NULL when no segment qualified. |
| aligned_min_offset_m | REAL | Minimum perpendicular offset in meters from the extended centerline over the winning segment. NULL when no segment qualified. |
| takeoff_runway | TEXT | Runway name (e.g. "24", "08R") the aircraft departed from. NULL when detection failed or runway data is unavailable. |
| turnaround_minutes | REAL | Minutes from the previous flight's landing to this takeoff (same ICAO; NULL if > 72 h) |
| turnaround_category | TEXT | `quick` (<30 min) / `medium` (30-240 min) / `overnight` (4-18 h) / `multi_day` (>18 h) / `extended_gap` (>72 h) / `first_observed` / `last_observed` (never NULL) |
| is_first_observed_flight | INTEGER | 1 if no prior flight exists for this ICAO |
| is_last_observed_flight | INTEGER | 1 if no following flight exists for this ICAO (symmetric with first) |
| origin_helipad_id | INTEGER | FK to helipads table (takeoff within 200 m of a helipad centroid) |
| destination_helipad_id | INTEGER | FK to helipads table (landing within 200 m of a helipad centroid) |
| type_override | TEXT | Per-flight type override when cruise envelope indicates a different type (e.g. MIL_FW for ae69xx H60 ICAOs flying fixed-wing profiles) |

## aircraft_registry

Authoritative metadata per ICAO (resolves drift across daily fetches).

| Column | Type | Description |
|--------|------|-------------|
| icao | TEXT | ICAO hex code (primary key) |
| registration | TEXT | Authoritative tail number |
| type_code | TEXT | Authoritative type code |
| description | TEXT | Type description |
| owner_operator | TEXT | Owner string |
| year | TEXT | Year of manufacture |
| last_updated | TEXT | When the registry row was last refreshed |
| metadata_drift_count | INTEGER | Number of trace_days rows that disagreed |
| metadata_drift_values | TEXT | JSON list of conflicting (type_code, description, count) entries |
| confirmation_rate | REAL | Fraction of flights with confirmed landings |
| signal_quality_tier | TEXT | excellent / good / poor / very_poor |
| airframes_id | INTEGER | Cached airframes.io numeric airframe id (filled by `acars`) |

## aircraft_stats

Materialized rollup of utilization per aircraft (refreshed on every extract).

| Column | Type | Description |
|--------|------|-------------|
| icao | TEXT | ICAO hex code (primary key) |
| registration | TEXT | From aircraft_registry |
| type_code | TEXT | From aircraft_registry |
| first_seen / last_seen | TEXT | Earliest / latest takeoff_date |
| total_flights | INTEGER | Count of all flights |
| confirmed_flights | INTEGER | Count where landing_type = 'confirmed' |
| total_hours | REAL | Sum of duration_minutes / 60 (excludes negative durations) |
| total_cycles | INTEGER | Cycles (= confirmed_flights) |
| distinct_airports | INTEGER | Distinct origin or destination ICAOs |
| distinct_callsigns | INTEGER | Distinct callsigns observed |
| avg_flight_minutes | REAL | Mean flight duration (excludes non-positive) |
| busiest_day_date | TEXT | Date with the most flights |
| busiest_day_count | INTEGER | Number of flights on busiest_day_date |
| home_base_icao | TEXT | Airport with the most takeoffs |
| home_base_share | REAL | Fraction of takeoffs from home base |
| home_base_uncertain | INTEGER | 1 if home_base_share < 0.40 (nomadic aircraft without a clear single base) |
| second_base_icao | TEXT | Second most common takeoff airport |
| second_base_share | REAL | Fraction of takeoffs from second base |
| updated_at | TEXT | When the rollup was last refreshed |

## fetch_log

Tracks which dates have been fetched per source.

| Column | Type | Description |
|--------|------|-------------|
| icao | TEXT | ICAO hex code |
| date | TEXT | Date checked |
| source | TEXT | Data source |
| status | INTEGER | HTTP status (200, 404, etc.) |

## airports

OurAirports database (~47k airports).

| Column | Type | Description |
|--------|------|-------------|
| ident | TEXT | ICAO code (primary key) |
| type | TEXT | large_airport / medium_airport / small_airport / heliport / closed |
| name | TEXT | Airport name |
| latitude_deg/longitude_deg | REAL | Coordinates |
| elevation_ft | INTEGER | Elevation |
| iso_country | TEXT | ISO country code |
| iso_region | TEXT | ISO region code |
| municipality | TEXT | City |
| iata_code | TEXT | IATA code |

## runways

Per-runway-end geometry from OurAirports `runways.csv`. One row per runway END (so runway 09/27 is two rows, not one). Loaded by `runways refresh`.

Rows with missing endpoint lat/lon are skipped (heliports, some unsurveyed fields). The `airport_ident` is preserved exactly as published by OurAirports - may be an ICAO code ("KSPG") or an FAA local code ("67FL"); airport matching code already tolerates both.

| Column | Type | Description |
|--------|------|-------------|
| airport_ident | TEXT | OurAirports ident (ICAO or FAA local code); primary key with runway_name |
| runway_name | TEXT | Runway end designator (e.g. "09", "27L", "18") |
| latitude_deg / longitude_deg | REAL | Endpoint coordinates (not null; rows without coordinates are skipped at load) |
| elevation_ft | INTEGER | Threshold elevation (MSL) |
| heading_deg_true | REAL | Runway heading in degrees true (from `*_heading_degT`) |
| length_ft | INTEGER | Overall runway length |
| width_ft | INTEGER | Runway width |
| surface | TEXT | Surface code ("ASPH", "CONC", "GRVL", ...) |
| closed | INTEGER | 1 if the runway is marked closed in OurAirports |
| displaced_threshold_ft | INTEGER | Displaced threshold distance at this end |

## helipads

DBSCAN-clustered landing sites that don't match any airport. Enriched with names from OurAirports heliport entries and manual overrides.

| Column | Type | Description |
|--------|------|-------------|
| helipad_id | INTEGER | Primary key |
| centroid_lat / centroid_lon | REAL | Cluster centroid coordinates |
| landing_count | INTEGER | Number of landings in this cluster |
| first_seen / last_seen | TEXT | Earliest / latest landing date |
| name_hint | TEXT | Facility name (from OurAirports heliport join or manual override; `helipad_N` / `offshore_platform_N` if unresolved) |

## flights_with_type (view)

Convenience view joining flights with their effective type code (coalesces `type_override` with `aircraft_registry.type_code`). Use this instead of manually joining and coalescing in downstream queries.

## faa_registry / faa_deregistered

Loaded from `ReleasableAircraft.zip` by `registry update`. Both tables share the same 29-column schema; `faa_registry` holds active registrations (MASTER.txt), `faa_deregistered` holds historical records (DEREG.txt projected onto the same shape with MAIL / PHYSICAL addresses preferring PHYSICAL and CANCEL-DATE mapped to `expiration_date`).

| Column | Type | Description |
|--------|------|-------------|
| mode_s_code_hex | TEXT | 6-char ICAO hex (primary key; derived from octal MODE S CODE or read from MASTER's MODE S CODE HEX column) |
| n_number | TEXT | N-number without the leading 'N' |
| serial_number | TEXT | Manufacturer serial |
| mfr_mdl_code | TEXT | FK to `faa_aircraft_ref.code` |
| eng_mfr_mdl | TEXT | Engine manufacturer / model code |
| year_mfr | TEXT | Year of manufacture |
| type_registrant | TEXT | 1=Individual, 2=Partnership, 3=Corp, ... (faa_registry only, NULL in faa_deregistered) |
| name | TEXT | Registrant name |
| street / street2 / city / state / zip_code / region / county / country | TEXT | Address (DEREG prefers PHYSICAL with MAIL fallback) |
| last_action_date | TEXT | Date of last FAA action (YYYYMMDD) |
| cert_issue_date | TEXT | Date certificate was issued |
| certification | TEXT | Certification class (e.g. 1N) |
| type_aircraft | TEXT | Aircraft type code (faa_registry only) |
| type_engine | TEXT | Engine type (faa_registry only) |
| status_code | TEXT | Registration status |
| mode_s_code | TEXT | Original octal MODE S code |
| fract_owner | TEXT | Fractional ownership indicator |
| air_worth_date | TEXT | Airworthiness cert date |
| expiration_date | TEXT | Registration expiration; in DEREG this holds CANCEL-DATE |
| unique_id | TEXT | FAA unique id (faa_registry only) |
| kit_mfr / kit_model | TEXT | Experimental kit info |

## faa_aircraft_ref

Manufacturer / model lookup table loaded from ACFTREF.txt.

| Column | Type | Description |
|--------|------|-------------|
| code | TEXT | MFR MDL CODE (primary key, join key for `*.mfr_mdl_code`) |
| mfr | TEXT | Manufacturer name |
| model | TEXT | Model name |
| type_acft | TEXT | Aircraft type code |
| type_eng | TEXT | Engine type code |

## acars_flights

One row per airframes.io flight we've fetched messages for. Used as a skip-list so repeated `acars` runs don't refetch the same flights.

| Column | Type | Description |
|--------|------|-------------|
| flight_id | INTEGER | airframes.io flight id (primary key) |
| airframe_id | INTEGER | airframes.io numeric airframe id |
| icao | TEXT | ICAO hex |
| registration | TEXT | Tail number at fetch time |
| flight_number / flight_iata / flight_icao | TEXT | Flight designators when known |
| status | TEXT | airframes.io flight status |
| departing_airport / destination_airport | TEXT | ICAO codes from airframes.io |
| departure_time_scheduled / departure_time_actual | TEXT | ISO timestamps |
| arrival_time_scheduled / arrival_time_actual | TEXT | ISO timestamps |
| first_seen / last_seen | TEXT | Earliest / latest message timestamp |
| message_count | INTEGER | Number of messages in this flight |
| fetched_at | TEXT | When we pulled it |

## acars_messages

Raw ACARS / VDL2 / HFDL messages. `UNIQUE(airframes_id)` makes re-fetches idempotent.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Local row id |
| airframes_id | INTEGER | airframes.io message id (UNIQUE) |
| uuid | TEXT | airframes.io UUID |
| flight_id | INTEGER | FK to `acars_flights.flight_id` |
| icao | TEXT | ICAO hex |
| registration | TEXT | Tail |
| timestamp | TEXT | Message timestamp (ISO) |
| source_type | TEXT | ACARS / VDL2 / HFDL / SATCOM |
| link_direction | TEXT | uplink / downlink |
| from_hex / to_hex | TEXT | Hex addresses in the ACARS header |
| frequency | REAL | Receiver frequency (MHz) |
| level | REAL | Signal level |
| channel | TEXT | Channel identifier |
| mode | TEXT | Transmission mode |
| label | TEXT | ACARS label (14, 44, 4T, H1, SA, ...) |
| block_id / message_number / ack | TEXT | ACARS header fields |
| flight_number | TEXT | Flight identifier from the message |
| text | TEXT | Human-readable body |
| data | TEXT | Raw binary / encoded payload |
| latitude / longitude / altitude | REAL | Position report values when present |
| departing_airport / destination_airport | TEXT | Route hints parsed from the message |
| fetched_at | TEXT | When we pulled it |

The `flights` table also gains four ACARS-derived columns populated by the OOOI parser: `acars_out`, `acars_off`, `acars_on`, `acars_in` (ISO 8601 UTC, NULL when the event was not observed or the message format wasn't parseable).

## hex_crossref

Merged hex -> identity lookup from FAA + Mictronics + hexdb.io. `enrich all` / `enrich hex` populates.

| Column | Type | Description |
|--------|------|-------------|
| icao | TEXT | ICAO hex (primary key) |
| registration | TEXT | Registration / tail |
| type_code | TEXT | ICAO type designator |
| type_description | TEXT | Type description |
| operator | TEXT | Registered owner / operator |
| source | TEXT | Which provider supplied the row (faa / mictronics / hexdb / mil_range) |
| is_military | INTEGER | 1 when the hex falls in a `mil_hex_ranges` block |
| mil_country | TEXT | Country attribution from `mil_hex_ranges` |
| mil_branch | TEXT | Branch attribution from `mil_hex_ranges` |
| last_updated | TEXT | When the row was last enriched |

## mil_hex_ranges

Static military ICAO allocation ranges. Seeded with 25 well-documented blocks (US DoD, UK RAF, Luftwaffe, French AF, JASDF, RAAF, RCAF, VKS, ...) on DB init; users can extend by inserting their own rows (`INSERT OR REPLACE` composes cleanly with the seeder).

| Column | Type | Description |
|--------|------|-------------|
| range_start | TEXT | First hex in the allocation (lowercase, 6 chars) |
| range_end | TEXT | Last hex in the allocation (primary key is the pair) |
| country | TEXT | Attributing country |
| branch | TEXT | Service branch (Military (DoD), RAF, Luftwaffe, ...) |
| notes | TEXT | Source / caveats |
