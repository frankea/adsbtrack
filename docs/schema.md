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
| max_altitude | INTEGER | Peak baro altitude in feet (dual-track persistence-filtered, 3+ samples over 30 s, hard-capped at 60,000 ft) |
| max_gs_kt | INTEGER | Peak ground speed in knots (dual-track persistence-filtered, hard-capped at 600 kt) |
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
| cruise_alt_ft / cruise_gs_kt | INTEGER | Mean altitude / gs during cruise |
| peak_climb_fpm / peak_descent_fpm | INTEGER | Best 60-s rolling-window mean climb / descent rate |
| takeoff_is_night / landing_is_night | INTEGER | 1 if sun was below -6 degrees |
| night_flight | INTEGER | 1 if >= 50% of in-flight points were at night |
| callsigns | TEXT | JSON array of distinct callsigns seen |
| callsign_changes | INTEGER | Transitions between distinct callsigns (capped at distinct - 1) |
| callsign_count | INTEGER | Number of distinct callsigns |
| probable_destination_icao | TEXT | Inferred destination for dropped/signal-lost flights |
| probable_destination_distance_km | REAL | Distance from last_seen to inferred destination |
| probable_destination_confidence | REAL | [0.0, 1.0] confidence in the inference |

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
