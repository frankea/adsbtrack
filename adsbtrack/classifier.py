"""Classify flight endings and score confidence.

Determines whether a flight ended with a confirmed landing, signal loss,
dropped on approach, altitude encoding error, or is uncertain. Computes
confidence scores for takeoff and landing data quality.

v2 changes (Apr 2026):
- Uses geometric altitude (trace index 10) in addition to barometric (index 3)
  to detect the Bell 407 hover-at-altitude-with-baro=ground pathology.
- Uses barometric vertical rate (trace index 7) for descent detection with
  a wall-clock time window instead of a point-count window.
- Landing confidence uses a weighted geometric mean so any single failing
  factor drags the whole score down instead of being averaged away.
- Per-type endurance cap (Config.type_endurance_minutes) replaces the
  global max_endurance_minutes for flights where the type is known.
- Adds dropped_on_approach landing type for signal-lost flights that show
  a clear descent trajectory at the last observed point.
- takeoff_type distinguishes "observed" (saw a ground-to-airborne transition)
  from "found_mid_flight" (first trace point was already airborne).
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field


@dataclass(slots=True, frozen=True)
class PointData:
    """All fields extracted from a single readsb trace point.

    Produced by `parser._extract_point_fields`, consumed by
    `FlightMetrics.record_point`. A frozen slotted dataclass keeps the
    hot-loop memory footprint small and catches accidental mutation.

    Fields that are not present in a given trace point are None.
    """

    ts: float  # absolute unix timestamp
    lat: float
    lon: float
    baro_alt: int | str | None  # int ft or the literal string 'ground'
    gs: float | None
    track: float | None  # ground track, degrees true
    geom_alt: int | None
    baro_rate: float | None
    geom_rate: float | None
    squawk: str | None
    category: str | None  # DO-260B category code, e.g. "A7"
    nav_altitude_mcp: int | None  # autopilot selected altitude
    nav_qnh: float | None
    emergency_field: str | None  # detail.emergency ("none", "general", "lifeguard", ...)
    true_heading: float | None
    callsign: str | None  # detail.flight (stripped)
    # readsb globe_history source type, e.g. "adsb_icao", "mlat", "tisb_icao".
    # Present at point[9] in 14-element rows and inside detail["type"] (same
    # value) in both 9- and 14-element rows. None when the trace lacks a
    # detail object and is under 10 elements long (e.g. OpenSky synthesized
    # traces).
    position_source: str | None = None


@dataclass
class _PointSample:
    """A lightweight snapshot of a trace point kept for descent scoring and
    landing airport anchor selection."""

    ts: float  # absolute unix timestamp
    baro_alt: int | None  # None when trace reports 'ground'
    geom_alt: int | None
    gs: float | None
    baro_rate: float | None
    # Position is used by adsbtrack.landing_anchor to pick the alt-min anchor
    # for destination airport matching. Defaults to None so any future
    # constructor that omits them still compiles (existing consumers of
    # recent_points only read ts / alt / gs / baro_rate).
    lat: float | None = None
    lon: float | None = None
    # Ground track in degrees true. Used by adsbtrack.ils_alignment. None
    # when the point's trace didn't carry track (older readsb builds, or
    # ground samples). Placed last so every existing construction call site
    # continues to compile unchanged.
    track: float | None = None


@dataclass
class FlightMetrics:
    """Raw signal metrics accumulated during trace processing."""

    data_points: int = 0
    total_ground_points: int = 0  # any point where classify_ground_state said "ground"
    baro_error_points: int = 0  # baro=ground but geom or gs disagreed (see record_point)
    sources: set[str] = field(default_factory=set)
    # Position source tallies (readsb type/src field). Every recorded point
    # is bucketed by its position_source so per-flight percentages line up
    # with data_points. Values outside ADS-B / MLAT / TIS-B (e.g. "other",
    # "mode_s", "adsc", None) fall through and are not counted here.
    adsb_points: int = 0
    mlat_points: int = 0
    tisb_points: int = 0
    max_altitude: int = 0
    max_gs_kt: int = 0
    # v6 fix: dual-track raw and persisted peaks. The raw value is updated
    # every point (warmup fallback). The persisted value is updated only
    # when the persistence filter has >= min_samples points in its window.
    # max_altitude / max_gs_kt use persisted when > 0, else raw. This
    # corrects the v5 bug where a spike during warmup (< min_samples)
    # pegged the raw max permanently.
    _raw_max_altitude: int = 0
    _persisted_max_altitude: int = 0
    _raw_max_gs_kt: int = 0
    _persisted_max_gs_kt: int = 0
    _alt_persist_window: deque = field(default_factory=deque)
    _gs_persist_window: deque = field(default_factory=deque)
    # v5 F1: count of inter-point gaps longer than path_max_segment_secs
    # observed while airborne. Used to produce signal_gap_count on the flight.
    signal_gap_count: int = 0
    # v5 F2: number of raw fragments this metric represents. Non-stitched
    # flights are 1 by definition; _stitch_fragments bumps this on merge.
    fragments_stitched: int = 1
    # v5 B4: last observed raw callsign (distinct from callsigns_seen which
    # is the distinct-in-order list). Needed to tell a real transition from
    # a flicker where [-1] of the distinct list is not the most recent.
    _last_callsign: str | None = None
    last_airborne_alt: int | None = None  # last airborne baro altitude
    last_airborne_geom: int | None = None
    last_airborne_gs: float | None = None
    last_airborne_baro_rate: float | None = None
    ground_points_at_takeoff: int = 0
    ground_points_at_landing: int = 0
    ground_speed_while_ground: int = 0  # baro=ground with gs > landing threshold
    landing_lats: list[float] = field(default_factory=list)
    landing_lons: list[float] = field(default_factory=list)
    # Rolling wall-clock window of recent points for descent analysis.
    # Bumped from 40 to 240 in v3 so go-around detection can walk back 600s.
    recent_points: deque = field(default_factory=lambda: deque(maxlen=240))
    # Dedicated (ts, alt) deque for go-around detection. Separate from
    # recent_points so the two use-cases don't constrain each other.
    approach_alts: deque = field(default_factory=lambda: deque(maxlen=240))
    # Takeoff category: "observed" (saw ground -> airborne) or "found_mid_flight"
    takeoff_type: str = "unknown"
    # First/last observed point timestamps - used to compute duration for
    # signal_lost / dropped_on_approach flights that never transitioned to ground.
    first_point_ts: float | None = None
    last_point_ts: float | None = None
    # Last-seen snapshot (may be airborne or ground). For confirmed landings
    # this ends up equal to the landing point; for signal_lost it is where
    # coverage dropped.
    last_seen_lat: float | None = None
    last_seen_lon: float | None = None
    last_seen_alt_ft: int | None = None
    last_seen_ts: float | None = None
    # Transition timestamp of the airborne -> ground landing event. Used by
    # the classifier to pick a pre-flare descent window.
    landing_transition_ts: float | None = None

    # --- v3 accumulators ---

    # Squawk tracking
    squawk_first: str | None = None
    squawk_last: str | None = None
    squawk_changes: int = 0
    squawk_1200_count: int = 0
    squawk_total_count: int = 0
    emergency_squawks_seen: set[str] = field(default_factory=set)

    # Callsign history (observed callsigns across the flight in order of
    # first appearance; distinct only)
    callsigns_seen: list[str] = field(default_factory=list)
    callsign_changes: int = 0

    # Path length and position tracking
    origin_lat: float | None = None
    origin_lon: float | None = None
    path_length_km: float = 0.0
    max_distance_from_origin_km: float = 0.0
    _prev_path_lat: float | None = None
    _prev_path_lon: float | None = None
    _prev_path_ts: float | None = None

    # Phase of flight: integer-second counters for climb/descent/level;
    # cruise is computed in a post-pass from level_buf once max_altitude is known
    climb_secs: float = 0.0
    descent_secs: float = 0.0
    level_secs: float = 0.0
    # Each entry: (dt_secs, alt_ft, gs_kt). Only level-phase samples.
    level_buf: list[tuple[float, int, float | None]] = field(default_factory=list)

    # Peak climb/descent over a rolling 30s window. The window is maintained
    # during record_point as (ts, rate_fpm) tuples; rate is chosen as
    # geom_rate > baro_rate > derived. Peaks are best 30s-mean observed.
    _rate_window: deque = field(default_factory=deque)
    peak_climb_fpm: float = 0.0
    peak_descent_fpm: float = 0.0

    # Hover state machine
    _hover_start_ts: float | None = None
    max_hover_secs: float = 0.0
    hover_episodes: int = 0

    # DO-260 category histogram
    category_counts: dict[str, int] = field(default_factory=dict)

    # Autopilot target altitude: last nav_altitude_mcp seen before the first
    # sustained descent (heuristic for "intended cruise alt").
    _sustained_descent_hit: bool = False
    autopilot_target_alt_ft: int | None = None

    # Emergency field (distinct from squawk). Holds the latest non-"none" value.
    emergency_flag: str | None = None

    # Night-at-point counter (incremented externally via record_solar if used)
    night_point_count: int = 0
    day_point_count: int = 0

    # Tracks near takeoff / landing, for heading computation. Stored as
    # (ts, track_deg, gs) tuples for later filtering in features.compute_headings.
    takeoff_tracks: list[tuple[float, float, float | None]] = field(default_factory=list)
    landing_tracks: list[tuple[float, float, float | None]] = field(default_factory=list)
    # v9 N7: recent airborne positions for bearing-based heading fallback
    # when track data is unavailable (helicopter hover approaches).
    _recent_positions: deque = field(default_factory=lambda: deque(maxlen=30))

    def record_point(
        self,
        point: PointData,
        *,
        ground_state: str,
        ground_reason: str,
        config: _ConfigLike | None = None,
        landing_speed_threshold: float = 80.0,
    ) -> None:
        """Record a single trace point into the running metrics.

        Takes a PointData object plus the ground classification produced by
        classify_ground_state. The optional ``config`` object supplies
        thresholds for phase, peak-rate, and hover accumulators; if None
        sensible defaults are used.

        ``landing_speed_threshold`` is kept as a keyword for backwards
        compatibility with older call sites that only supplied the legacy
        scalar knob.
        """
        ts = point.ts
        lat = point.lat
        lon = point.lon
        baro_alt = point.baro_alt
        geom_alt = point.geom_alt
        gs = point.gs
        baro_rate = point.baro_rate
        geom_rate = point.geom_rate

        self.data_points += 1
        # Position source bucket. Matches the three percentage columns on
        # flights. Counts every point (ground or airborne) so the three
        # percentages are comparable fractions of data_points.
        src = point.position_source
        if src is not None:
            if src.startswith("adsb_"):
                self.adsb_points += 1
            elif src == "mlat":
                self.mlat_points += 1
            elif src.startswith("tisb_"):
                self.tisb_points += 1
        prev_ts = self.last_point_ts
        if self.first_point_ts is None:
            self.first_point_ts = ts
        self.last_point_ts = ts

        # Last-seen snapshot (overwritten every point - this ends up holding
        # the final observed position regardless of whether the flight lands).
        self.last_seen_lat = lat
        self.last_seen_lon = lon
        self.last_seen_ts = ts
        if isinstance(baro_alt, (int, float)):
            self.last_seen_alt_ft = int(baro_alt)
        elif isinstance(geom_alt, (int, float)):
            self.last_seen_alt_ft = int(geom_alt)
        # else: leave last_seen_alt_ft as-is so we keep the last known value

        # Rolling wall-clock window sample for descent analysis
        sample_baro_alt = None
        if isinstance(baro_alt, (int, float)):
            sample_baro_alt = int(baro_alt)
        self.recent_points.append(
            _PointSample(
                ts=ts,
                baro_alt=sample_baro_alt,
                geom_alt=int(geom_alt) if isinstance(geom_alt, (int, float)) else None,
                gs=gs,
                baro_rate=baro_rate,
                lat=lat,
                lon=lon,
                track=float(point.track) if point.track is not None else None,
            )
        )

        # Approach altitudes deque for go-around detection, phase budget
        # level_buf, and cruise altitude. v6 N1 fix: prefer baro_alt to
        # match max_altitude's source. Previously this preferred geom_alt,
        # causing cruise_alt_ft (from level_buf) to exceed max_altitude
        # (from baro) on 50% of flights due to the QNH correction offset
        # between baro and GPS altitude.
        airborne_alt: int | None = None
        if isinstance(baro_alt, (int, float)):
            airborne_alt = int(baro_alt)
        elif isinstance(geom_alt, (int, float)):
            airborne_alt = int(geom_alt)
        if airborne_alt is not None:
            self.approach_alts.append((ts, airborne_alt))

        if ground_state == "ground":
            self.total_ground_points += 1

        # Count broken-encoder points under baro_error_points.
        if ground_reason in ("baro_error", "speed_override"):
            self.baro_error_points += 1

        # v9 N7: buffer recent airborne positions for bearing fallback
        if ground_state == "airborne":
            self._recent_positions.append((ts, lat, lon))

        # Track last airborne signals for confidence scoring
        if ground_state == "airborne":
            # v6 B5 fix: dual-track raw and persisted max altitude.
            # Raw is updated every point (warmup / fallback for short
            # flights). Persisted is updated only when the rolling window
            # has >= min_samples. The public max_altitude uses persisted
            # when available, correcting any spike the raw path recorded
            # during warmup. min() over the window naturally suppresses
            # single-sample spikes because the non-spike values are lower.
            alt_win_secs = (
                config.alt_persistence_window_secs
                if config is not None and hasattr(config, "alt_persistence_window_secs")
                else 30.0
            )
            alt_min_samples = (
                config.alt_persistence_min_samples
                if config is not None and hasattr(config, "alt_persistence_min_samples")
                else 3
            )
            candidate_alt: int | None = None
            if isinstance(baro_alt, (int, float)):
                self.last_airborne_alt = int(baro_alt)
                candidate_alt = int(baro_alt)
            elif isinstance(geom_alt, (int, float)):
                self.last_airborne_alt = int(geom_alt)
                candidate_alt = int(geom_alt)
            if candidate_alt is not None:
                # Always track raw max (fallback for flights without
                # mode-S extended data). Type ceiling catches extremes.
                if candidate_alt > self._raw_max_altitude:
                    self._raw_max_altitude = candidate_alt
                # v14 R4a: AP-validated persistence filter. Only samples
                # with nav_altitude_mcp (autopilot target) present enter
                # the persistence window. Squawk is NOT sufficient --
                # it's always present on operating transponders and does
                # not correlate with altitude validity.
                # v15 R4c: AP must also AGREE with the altitude sample.
                # S92 a7a622 had AP=3,008 ft while altitude=16,500 ft --
                # a 13,000 ft disagreement from mixed trace segments.
                # AP only validates the altitude when the two are within
                # 5,000 ft of each other.
                has_ap = point.nav_altitude_mcp is not None and abs(candidate_alt - int(point.nav_altitude_mcp)) <= 5000
                if has_ap:
                    self._alt_persist_window.append((ts, candidate_alt))
                alt_cutoff = ts - alt_win_secs
                while self._alt_persist_window and self._alt_persist_window[0][0] < alt_cutoff:
                    self._alt_persist_window.popleft()
                if len(self._alt_persist_window) >= alt_min_samples:
                    sustained = min(a for _, a in self._alt_persist_window)
                    if sustained > self._persisted_max_altitude:
                        self._persisted_max_altitude = sustained
                # Public value: persisted wins when available, else raw
                self.max_altitude = (
                    self._persisted_max_altitude if self._persisted_max_altitude > 0 else self._raw_max_altitude
                )
            if isinstance(geom_alt, (int, float)):
                self.last_airborne_geom = int(geom_alt)
            if gs is not None:
                self.last_airborne_gs = gs
                # v6 B6 fix: same dual-track for ground speed.
                gs_win_secs = (
                    config.gs_persistence_window_secs
                    if config is not None and hasattr(config, "gs_persistence_window_secs")
                    else 30.0
                )
                gs_min_samples = (
                    config.gs_persistence_min_samples
                    if config is not None and hasattr(config, "gs_persistence_min_samples")
                    else 3
                )
                gs_val = int(gs)
                if gs_val > self._raw_max_gs_kt:
                    self._raw_max_gs_kt = gs_val
                self._gs_persist_window.append((ts, float(gs)))
                gs_cutoff = ts - gs_win_secs
                while self._gs_persist_window and self._gs_persist_window[0][0] < gs_cutoff:
                    self._gs_persist_window.popleft()
                if len(self._gs_persist_window) >= gs_min_samples:
                    sustained_gs = int(min(g for _, g in self._gs_persist_window))
                    if sustained_gs > self._persisted_max_gs_kt:
                        self._persisted_max_gs_kt = sustained_gs
                self.max_gs_kt = self._persisted_max_gs_kt if self._persisted_max_gs_kt > 0 else self._raw_max_gs_kt
            if baro_rate is not None:
                self.last_airborne_baro_rate = baro_rate

        # Legacy altitude_error heuristic: baro=ground + high gs.
        if baro_alt == "ground" and gs is not None and gs > landing_speed_threshold:
            self.ground_speed_while_ground += 1

        # --- v3 accumulators ---

        # Phase / peak rate gap threshold: anything above this is treated
        # as a coverage hole and gets special-cased downstream.
        max_seg_secs = (
            config.path_max_segment_secs if config is not None and hasattr(config, "path_max_segment_secs") else 60.0
        )

        # Origin (first observed point, used for max_distance_from_origin)
        if self.origin_lat is None:
            self.origin_lat = lat
            self.origin_lon = lon

        # Path length and max-distance-from-origin: walk the same iterator.
        # v4 fix: drop the dt-based gap filter on path length so the two
        # quantities can never get out of sync. The state machine has already
        # split flights at multi-hour gaps via max_point_gap_minutes; within
        # a single flight every consecutive segment is real motion. Coverage
        # holes get bridged by the haversine of (prev, curr), which is the
        # great-circle minimum the aircraft could have travelled - a safe
        # lower bound. Round-3 spec was wrong here.
        if self._prev_path_lat is not None and self._prev_path_lon is not None:
            seg_m = _haversine_m(self._prev_path_lat, self._prev_path_lon, lat, lon)
            self.path_length_km += seg_m / 1000.0
        self._prev_path_lat = lat
        self._prev_path_lon = lon
        self._prev_path_ts = ts

        if self.origin_lat is not None and self.origin_lon is not None:
            dist_from_origin_m = _haversine_m(self.origin_lat, self.origin_lon, lat, lon)
            dist_km = dist_from_origin_m / 1000.0
            if dist_km > self.max_distance_from_origin_km:
                self.max_distance_from_origin_km = dist_km

        # Squawk tracking
        sq = point.squawk
        if sq:
            self.squawk_total_count += 1
            if self.squawk_first is None:
                self.squawk_first = sq
            if self.squawk_last is not None and sq != self.squawk_last:
                self.squawk_changes += 1
            self.squawk_last = sq
            if sq == "1200":
                self.squawk_1200_count += 1
            if sq in ("7500", "7600", "7700"):
                self.emergency_squawks_seen.add(sq)

        # Callsign history
        # v5 B4 fix: track _last_callsign as the most-recently-observed
        # value (separate from callsigns_seen which is the dedup-in-order
        # list). Only bump callsign_changes when the new observation is a
        # real transition from the last-observed value. The old code used
        # callsigns_seen[-1] which is the last-appended-unique, not the
        # last-observed, so it missed A->B->A->B flicker and over-counted.
        cs = point.callsign
        if cs:
            cs = cs.strip()
            if cs:
                if cs not in self.callsigns_seen:
                    self.callsigns_seen.append(cs)
                if self._last_callsign is not None and cs != self._last_callsign:
                    self.callsign_changes += 1
                self._last_callsign = cs

        # DO-260B category
        if point.category:
            self.category_counts[point.category] = self.category_counts.get(point.category, 0) + 1

        # Emergency (detail field, not squawk)
        if point.emergency_field and point.emergency_field.lower() not in ("none", ""):
            self.emergency_flag = point.emergency_field

        # Choose a rate signal for phase and peak-rate work: geom_rate > baro_rate.
        # If neither is available for this point we fall through to "level" in
        # phase attribution, which is the safer default.
        chosen_rate: float | None = None
        if geom_rate is not None:
            chosen_rate = float(geom_rate)
        elif baro_rate is not None:
            chosen_rate = float(baro_rate)

        # Phase of flight attribution (airborne samples only).
        # v4 fix (§1.2): when there's a coverage gap between airborne points,
        # attribute the gap to cruise (if both bracketing alts are above 70%
        # of running max_altitude) or level. Stops the phase sum from
        # silently undercounting on long flights.
        phase_climb_fpm = config.phase_climb_fpm if config is not None and hasattr(config, "phase_climb_fpm") else 250.0
        cruise_ratio = (
            config.phase_cruise_alt_ratio if config is not None and hasattr(config, "phase_cruise_alt_ratio") else 0.70
        )
        if ground_state == "airborne" and prev_ts is not None:
            dt = ts - prev_ts
            if dt > 0:
                if dt <= max_seg_secs:
                    # Normal point: attribute to climb/descent/level by rate
                    if chosen_rate is not None and chosen_rate > phase_climb_fpm:
                        self.climb_secs += dt
                    elif chosen_rate is not None and chosen_rate < -phase_climb_fpm:
                        self.descent_secs += dt
                    else:
                        self.level_secs += dt
                        if airborne_alt is not None:
                            self.level_buf.append((dt, airborne_alt, gs))
                else:
                    # v5 F1: count this inter-point gap so
                    # signal_gap_count tracks how fragmented the coverage was.
                    self.signal_gap_count += 1
                    # Coverage gap: attribute to cruise (level_buf with the
                    # bracketing altitude) if both endpoints are at cruise
                    # altitude, otherwise level. Cap the per-gap attribution
                    # at the configured intra-trace splitter so we never
                    # silently swallow multi-day gaps.
                    cap_secs = min(dt, max(max_seg_secs * 30, 1800.0))
                    if (
                        airborne_alt is not None
                        and self.last_airborne_alt is not None
                        and self.max_altitude > 0
                        and airborne_alt >= cruise_ratio * self.max_altitude
                        and self.last_airborne_alt >= cruise_ratio * self.max_altitude
                    ):
                        self.level_secs += cap_secs
                        self.level_buf.append((cap_secs, airborne_alt, gs))
                    else:
                        self.level_secs += cap_secs

        # Peak rate: rolling window mean.
        # v4 fix (§1.7): bumped window from 30s to 60s and min samples from
        # 3 to 4 so that 1-2 point baro spikes can't peg the peak. Also
        # filters obvious outliers (>3x median absolute) before computing
        # the mean to suppress single-sample contamination.
        peak_win_secs = (
            config.peak_rate_window_secs if config is not None and hasattr(config, "peak_rate_window_secs") else 60.0
        )
        peak_min_samples = (
            config.peak_rate_min_samples if config is not None and hasattr(config, "peak_rate_min_samples") else 4
        )
        peak_min_span = (
            config.peak_rate_min_span_secs
            if config is not None and hasattr(config, "peak_rate_min_span_secs")
            else 30.0
        )
        if chosen_rate is not None and ground_state == "airborne":
            self._rate_window.append((ts, chosen_rate))
            cutoff = ts - peak_win_secs
            while self._rate_window and self._rate_window[0][0] < cutoff:
                self._rate_window.popleft()
            if len(self._rate_window) >= peak_min_samples:
                span = self._rate_window[-1][0] - self._rate_window[0][0]
                if span >= peak_min_span:
                    rates = sorted(r for _, r in self._rate_window)
                    n = len(rates)
                    median = rates[n // 2] if n % 2 else (rates[n // 2 - 1] + rates[n // 2]) / 2
                    abs_median = max(1.0, abs(median))
                    filtered = [r for r in rates if abs(r - median) <= 3.0 * abs_median + 500.0]
                    if len(filtered) >= peak_min_samples:
                        mean_rate = sum(filtered) / len(filtered)
                        if mean_rate > self.peak_climb_fpm:
                            self.peak_climb_fpm = mean_rate
                        if mean_rate < self.peak_descent_fpm:
                            self.peak_descent_fpm = mean_rate

        # Hover state machine (any rotorcraft gating happens in features.py;
        # here we just collect the raw stats so features.compute_hover can
        # decide whether to emit).
        hover_gs = (
            config.hover_gs_threshold_kts if config is not None and hasattr(config, "hover_gs_threshold_kts") else 5.0
        )
        hover_baro_max = (
            config.hover_baro_rate_max_fpm
            if config is not None and hasattr(config, "hover_baro_rate_max_fpm")
            else 100.0
        )
        hover_min_dur = (
            config.hover_min_duration_secs
            if config is not None and hasattr(config, "hover_min_duration_secs")
            else 20.0
        )
        is_hover_now = (
            ground_state == "airborne"
            and gs is not None
            and gs < hover_gs
            and (baro_rate is None or abs(baro_rate) < hover_baro_max)
        )
        if is_hover_now:
            if self._hover_start_ts is None:
                self._hover_start_ts = ts
        else:
            if self._hover_start_ts is not None:
                hover_dur = (prev_ts if prev_ts is not None else ts) - self._hover_start_ts
                if hover_dur >= hover_min_dur:
                    self.hover_episodes += 1
                    if hover_dur > self.max_hover_secs:
                        self.max_hover_secs = hover_dur
                self._hover_start_ts = None

        # Autopilot target altitude (v4 fix §1.6).
        # The intended-cruise altitude is the MCP setting at top of climb /
        # while in cruise. The original v3 implementation captured the
        # descent target because pilots set the descent FL on the MCP before
        # top-of-descent while still nominally "level". Fix: only capture
        # when MCP is within ±500 ft of the current altitude AND aircraft
        # is at >= 85% of running max AND not descending fast. Once a
        # sustained descent has begun we stop updating - that locks in the
        # latest cruise-altitude selection before top of descent.
        if (
            not self._sustained_descent_hit
            and point.nav_altitude_mcp is not None
            and ground_state == "airborne"
            and self.max_altitude > 0
            and isinstance(baro_alt, (int, float))
            and baro_alt >= 0.85 * self.max_altitude
            and abs(int(point.nav_altitude_mcp) - int(baro_alt)) < 500
            and (chosen_rate is None or chosen_rate > -250.0)
        ):
            self.autopilot_target_alt_ft = int(point.nav_altitude_mcp)
        if chosen_rate is not None and chosen_rate < -500.0 and ground_state == "airborne":
            self._sustained_descent_hit = True

        # Tracks for heading computation: record (ts, track, gs) during
        # the first and last wall-clock windows around takeoff and landing.
        # Takeoff window is "the first N seconds after takeoff was observed"
        # which we approximate here by capturing the first N seconds of any
        # airborne point stream. Landing window is "the last N seconds"
        # which we capture unconditionally - features.compute_headings will
        # filter by the landing transition timestamp.
        #
        # v8 N7: takeoff tracks still require airborne state (ground tracks
        # at takeoff are taxi noise). Landing tracks now also accept ground-
        # state points with a valid track, because helicopters transition to
        # baro=ground while still moving at low GS during final approach.
        # Without this, 358 B407 confirmed landings had no landing_heading
        # because the last airborne track was too far from touchdown.
        if point.track is not None:
            heading_window = (
                config.heading_window_secs if config is not None and hasattr(config, "heading_window_secs") else 60.0
            )
            if ground_state == "airborne":
                # First-60-seconds buffer (takeoff): only accept if within the
                # heading window from the first observed airborne point.
                if self.first_point_ts is not None and (ts - self.first_point_ts) <= heading_window:
                    self.takeoff_tracks.append((ts, float(point.track), gs))
                # Landing buffer: always append airborne tracks
                self.landing_tracks.append((ts, float(point.track), gs))
            elif ground_state == "ground" and gs is not None and gs > 0:
                # Ground-state points with movement: include in landing buffer
                # so helicopter hover-approach tracks reach compute_headings.
                self.landing_tracks.append((ts, float(point.track), gs))
            if len(self.landing_tracks) > 240:
                # Keep the most recent 240 points only
                self.landing_tracks = self.landing_tracks[-240:]

    def record_landing_ground_point(self, lat: float, lon: float) -> None:
        self.ground_points_at_landing += 1
        self.landing_lats.append(lat)
        self.landing_lons.append(lon)

    def landing_coord_spread(self) -> float:
        """Max spread in degrees across landing ground points (legacy)."""
        if len(self.landing_lats) < 2:
            return 0.0
        lat_spread = max(self.landing_lats) - min(self.landing_lats)
        lon_spread = max(self.landing_lons) - min(self.landing_lons)
        return max(lat_spread, lon_spread)

    def landing_max_jump_m(self) -> float:
        """Max distance in meters between adjacent landing ground points.

        This is a better signal than total spread because it catches receiver
        noise (sudden jumps) without penalizing normal taxi motion. Aircraft
        that taxi 1km after touchdown will have small per-sample jumps (a few
        tens of meters each) even though their total spread is huge.
        """
        if len(self.landing_lats) < 2:
            return 0.0
        max_jump = 0.0
        for i in range(1, len(self.landing_lats)):
            d = _haversine_m(
                self.landing_lats[i - 1],
                self.landing_lons[i - 1],
                self.landing_lats[i],
                self.landing_lons[i],
            )
            if d > max_jump:
                max_jump = d
        return max_jump


# Minimal protocol so FlightMetrics.record_point can accept a Config-like
# object without importing the real Config (keeps classifier.py standalone).
class _ConfigLike:  # pragma: no cover - structural
    path_max_segment_secs: float
    phase_climb_fpm: float
    peak_rate_window_secs: float
    peak_rate_min_samples: int
    peak_rate_min_span_secs: float
    hover_gs_threshold_kts: float
    hover_baro_rate_max_fpm: float
    hover_min_duration_secs: float
    heading_window_secs: float


# ----------------------------------------------------------------------
# Geographic helpers
# ----------------------------------------------------------------------


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in meters between two lat/lon points."""
    r = 6_371_000.0  # earth radius in meters
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


# ----------------------------------------------------------------------
# Point classification (baro + geom fusion)
# ----------------------------------------------------------------------


def classify_ground_state(
    baro_alt: int | str | None,
    geom_alt: int | None,
    gs: float | None,
    *,
    landing_speed_threshold: float = 80.0,
    baro_error_geom_threshold: float = 300.0,
) -> tuple[str, str]:
    """Classify a single trace point as ground / airborne / unknown.

    Returns (state, reason) where state is one of:
      - "ground"   - aircraft is on the surface
      - "airborne" - aircraft is flying (including hover)
      - "unknown"  - insufficient data to decide

    And reason is:
      - "ok"              - agreement between available signals
      - "baro_error"      - baro reports ground but geom altitude disagrees
      - "speed_override"  - baro reports ground but ground speed is high
      - "insufficient"    - no usable altitude data
    """
    baro_is_ground = baro_alt == "ground"
    baro_low = isinstance(baro_alt, (int, float)) and baro_alt < 50
    geom_low = isinstance(geom_alt, (int, float)) and geom_alt < 200
    geom_high = isinstance(geom_alt, (int, float)) and geom_alt > baro_error_geom_threshold

    # Bell 407 pathology: baro reports ground but geometric altitude is well
    # above ground level. The aircraft is actually hovering or in the pattern.
    if baro_is_ground and geom_high:
        return ("airborne", "baro_error")

    # Speed override: baro says ground but ground speed is clearly above
    # landing threshold. Strict greater-than so gs exactly at the threshold
    # is still treated as a valid landing (matches historical behavior).
    if baro_is_ground and gs is not None and gs > landing_speed_threshold:
        return ("airborne", "speed_override")

    # Strong ground: baro says ground (and no overriding signals)
    if baro_is_ground:
        return ("ground", "ok")

    # Strong ground: both altitudes low
    if baro_low and (geom_low or geom_alt is None):
        return ("ground", "ok")

    # Strong airborne: baro clearly above ground
    if isinstance(baro_alt, (int, float)) and baro_alt >= 50:
        return ("airborne", "ok")

    # Fallback to geom
    if isinstance(geom_alt, (int, float)) and geom_alt >= 200:
        return ("airborne", "ok")
    if isinstance(geom_alt, (int, float)) and geom_alt < 200:
        return ("ground", "ok")

    return ("unknown", "insufficient")


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _lerp(value: float, low: float, high: float) -> float:
    """Linear interpolation clamped to [0, 1]. Returns 0.0 at low, 1.0 at high.
    If high < low, reverses the mapping (useful for "lower is better")."""
    if high == low:
        return 1.0 if value >= high else 0.0
    if high > low:
        return max(0.0, min(1.0, (value - low) / (high - low)))
    # Reversed: high < low means lower input = higher output
    return max(0.0, min(1.0, (low - value) / (low - high)))


def _descent_score_from_window(window: list[_PointSample]) -> float:
    """Core descent scoring used by descent_score and descent_score_preflare."""
    if not window:
        return 0.5

    rates = [p.baro_rate for p in window if p.baro_rate is not None]
    if rates:
        total_w = 0.0
        acc = 0.0
        for i, p in enumerate(window):
            if p.baro_rate is None:
                continue
            w = i + 1.0
            total_w += w
            acc += p.baro_rate * w
        if total_w == 0:
            return 0.5
        avg_rate = acc / total_w  # ft/min
        # -800 ft/min or better = strong descent (1.0). 0 = level (0.0). Climbing = 0.0.
        return max(0.0, min(1.0, -avg_rate / 800.0))

    alts = [p.baro_alt for p in window if p.baro_alt is not None]
    if len(alts) < 2:
        return 0.5
    delta = alts[-1] - alts[0]  # negative means descending
    span_secs = max(1.0, window[-1].ts - window[0].ts)
    rate = (delta / span_secs) * 60.0  # ft/min
    return max(0.0, min(1.0, -rate / 800.0))


def descent_score(
    recent_points: deque,
    *,
    window_secs: float = 120.0,
) -> float:
    """Return a score in [0, 1] where 1.0 = strong descent, 0.0 = climbing/level.

    Uses barometric vertical rate (ft/min) when available, averaged over a
    wall-clock window anchored at the last recorded point. Falls back to a
    simple first/last altitude difference if no vertical rate data is present.

    This is the "signal_lost" descent window: it looks at the very last
    seconds of trace data to ask "was the aircraft descending when coverage
    dropped?" For confirmed landings use descent_score_preflare() instead,
    which skips the flare.
    """
    if not recent_points:
        return 0.5

    points = list(recent_points)
    cutoff = points[-1].ts - window_secs
    window = [p for p in points if p.ts >= cutoff]
    return _descent_score_from_window(window)


def descent_score_preflare(
    recent_points: deque,
    transition_ts: float,
    *,
    lookback_start_secs: float = 180.0,
    lookback_end_secs: float = 30.0,
) -> float:
    """Descent score computed over a pre-flare window for confirmed landings.

    For a touched-down flight, the 30 s immediately before the ground
    transition is the flare (level-off, baro_rate near zero). Including it
    in the descent score pushes the signal toward "level", which is wrong:
    we want to confirm the approach, and the approach descent is the
    minute or two before the flare. The default window is [30s, 180s]
    before the transition.
    """
    if not recent_points:
        return 0.5

    cutoff_start = transition_ts - lookback_start_secs
    cutoff_end = transition_ts - lookback_end_secs
    points = list(recent_points)
    window = [p for p in points if cutoff_start <= p.ts <= cutoff_end]
    if not window:
        # Not enough trace data before touchdown (short flight or sparse
        # coverage). Fall back to the regular last-window score.
        return descent_score(recent_points)
    return _descent_score_from_window(window)


def sustained_descent(
    recent_points: deque,
    *,
    tail_window: int = 5,
    min_count: int = 3,
    descent_rate_fpm: float = -200.0,
) -> bool:
    """True if the last `tail_window` samples show sustained descent.

    At least `min_count` of the final `tail_window` baro_rate readings must
    be at or below `descent_rate_fpm` (ft/min; negative = descending).
    Gates the dropped_on_approach classification so a window-averaged score
    on an otherwise-climbing flight does not leak into dropped_on_approach.
    """
    if not recent_points:
        return False
    tail = [p.baro_rate for p in list(recent_points)[-tail_window:] if p.baro_rate is not None]
    if len(tail) < min_count:
        return False
    return sum(1 for br in tail if br <= descent_rate_fpm) >= min_count


def endurance_for(
    type_code: str | None,
    type_endurance_minutes: dict[str, float],
    default: float = 240.0,
) -> float:
    """Look up max endurance for a Mode S type code."""
    if not type_code:
        return default
    return type_endurance_minutes.get(type_code, default)


# ----------------------------------------------------------------------
# Classification
# ----------------------------------------------------------------------


def classify_landing(
    metrics: FlightMetrics,
    has_landing: bool,
    *,
    duration_minutes: float | None = None,
    type_code: str | None = None,
    type_endurance_minutes: dict[str, float] | None = None,
    default_endurance_minutes: float = 240.0,
    dropped_tail_window: int = 5,
    dropped_tail_descent_min_count: int = 3,
    dropped_tail_descent_rate_fpm: float = -200.0,
    dropped_max_alt_ft: float = 5000.0,
) -> str:
    """Classify how a flight ended.

    Returns one of:
      - 'confirmed'           - clean landing with good supporting signals
      - 'signal_lost'         - aircraft was airborne at last contact (dropout)
      - 'dropped_on_approach' - signal lost with sustained descent at last contact
      - 'uncertain'           - ambiguous, duration artifact, or no data
      - 'altitude_error'      - baro altimeter clearly broken (Bell 407 pathology)
    """
    # Altitude error detection. speed_override points are now counted under
    # baro_error_points (see record_point), so the unified baro_error_ratio
    # check catches both the hover pathology and the mid-cruise encoder
    # glitch. Keep the legacy gs_ground_ratio as a safety net for flights
    # whose speed_override points never landed in total_ground_points.
    if metrics.data_points >= 10:
        gs_ground_ratio = metrics.ground_speed_while_ground / max(1, metrics.total_ground_points)
        baro_error_ratio = metrics.baro_error_points / max(1, metrics.data_points)
        if baro_error_ratio > 0.20 or gs_ground_ratio > 0.20:
            return "altitude_error"

    # Flight with no landing transition: signal loss or taxi-like
    if not has_landing:
        last_alt = metrics.last_airborne_alt
        last_gs = metrics.last_airborne_gs

        looks_airborne = (
            (last_alt is not None and last_alt > 2000)
            or (last_gs is not None and last_gs > 100)
            or metrics.max_altitude > 3000
        )
        if looks_airborne:
            # dropped_on_approach requires *sustained* descent in the final
            # samples, not a window-averaged descent that might reflect an
            # earlier descent phase.
            if (
                last_alt is not None
                and last_alt < dropped_max_alt_ft
                and sustained_descent(
                    metrics.recent_points,
                    tail_window=dropped_tail_window,
                    min_count=dropped_tail_descent_min_count,
                    descent_rate_fpm=dropped_tail_descent_rate_fpm,
                )
            ):
                return "dropped_on_approach"
            return "signal_lost"
        return "uncertain"

    # Duration sanity check. Per-type cap beats the global default.
    endurance_cap = default_endurance_minutes
    if type_endurance_minutes is not None:
        endurance_cap = endurance_for(type_code, type_endurance_minutes, default_endurance_minutes)
    if duration_minutes is not None and duration_minutes > endurance_cap:
        return "uncertain"

    # Flight has a landing transition. Score it on multiple factors.
    factors = []

    # Factor 1: last airborne altitude (lower = better landing)
    last_alt_ft = metrics.last_airborne_alt or 0
    alt_signal = _lerp(last_alt_ft, 500, 5000)  # 0.0 at 500, 1.0 at 5000
    factors.append((alt_signal, 3.0))

    # Factor 2: last airborne ground speed (slower = better).
    # Stretched range (180, 30) so jet approach speeds (120-150 kt) do not
    # crush the signal. Light helicopters still max out at their natural
    # 30-50 kt approach speeds.
    gs_signal = 0.0
    if metrics.last_airborne_gs is not None:
        gs_signal = _lerp(metrics.last_airborne_gs, 30, 180)
    factors.append((gs_signal, 2.5))

    # Factor 3: ground points collected at landing
    gp = metrics.ground_points_at_landing
    gp_signal = 1.0 if gp == 0 else (0.3 if gp == 1 else (0.1 if gp == 2 else 0.0))
    factors.append((gp_signal, 3.0))

    # Factor 4: descent trend. For a confirmed landing use the pre-flare
    # window so we score the approach descent, not the flare-level final
    # seconds. Semantics: 0 = descending (good), 1 = climbing (bad).
    if metrics.landing_transition_ts is not None:
        d = descent_score_preflare(metrics.recent_points, metrics.landing_transition_ts)
    else:
        d = descent_score(metrics.recent_points)
    descent_signal = 1.0 - d
    factors.append((descent_signal, 2.0))

    # Factor 5: coordinate stability. Use the per-sample max jump instead
    # of the total spread so normal taxi motion does not score as noise.
    max_jump_m = metrics.landing_max_jump_m()
    coord_signal = _lerp(max_jump_m, 100.0, 500.0)  # 100m jump = 0.0, 500m+ = 1.0
    factors.append((coord_signal, 1.5))

    total_weight = sum(w for _, w in factors)
    score = sum(f * w for f, w in factors) / total_weight if total_weight > 0 else 0.5

    if score > 0.6:
        return "signal_lost"
    return "confirmed"


def score_confidence(
    metrics: FlightMetrics,
    has_landing: bool,
    landing_type: str,
    *,
    origin_distance_km: float | None = None,
    dest_distance_km: float | None = None,
    duration_minutes: float | None = None,
) -> tuple[float, float]:
    """Compute takeoff and landing confidence scores in [0.0, 1.0].

    Landing confidence uses a weighted geometric mean across independent
    factors. Any single factor near zero drags the whole score down, which
    is the desired behavior: "one bad signal means we do not trust it."
    """

    # ---- Takeoff confidence ----
    if metrics.takeoff_type == "found_mid_flight":
        # We never observed an actual takeoff transition - conservatively
        # score low regardless of where we first saw the aircraft.
        takeoff_conf = 0.3
    else:
        takeoff_factors = []
        gp = metrics.ground_points_at_takeoff
        gp_score = 0.2 if gp == 0 else (0.5 if gp == 1 else (0.7 if gp == 2 else 1.0))
        takeoff_factors.append((gp_score, 2.0))

        if origin_distance_km is not None:
            prox = 1.0 - _lerp(origin_distance_km, 0, 10)
            takeoff_factors.append((prox, 1.5))

        t_total = sum(w for _, w in takeoff_factors)
        takeoff_conf = sum(f * w for f, w in takeoff_factors) / t_total if t_total > 0 else 0.5

    # ---- Landing confidence ----
    if not has_landing or landing_type in ("signal_lost", "dropped_on_approach"):
        landing_conf = 0.0
    elif landing_type == "altitude_error":
        landing_conf = 0.1
    elif landing_type == "uncertain":
        # Duration artifact or ambiguous: show as low confidence but non-zero
        landing_conf = 0.15
    else:
        factors = {}

        # Descent signature: pre-flare window for confirmed landings so we
        # score the approach descent, not the flare-level final seconds.
        if metrics.landing_transition_ts is not None:
            factors["descent"] = (
                descent_score_preflare(metrics.recent_points, metrics.landing_transition_ts),
                2.0,
            )
        else:
            factors["descent"] = (descent_score(metrics.recent_points), 2.0)

        # Approach speed. Stretched range: 180 kt -> 0.0, 30 kt -> 1.0.
        # Jets land at 120-150 kt so they now land in the 0.17-0.40 band
        # instead of near-zero; helicopters at 25-50 kt still max out.
        if metrics.last_airborne_gs is not None:
            factors["approach_spd"] = (_lerp(metrics.last_airborne_gs, 180, 30), 2.0)
        else:
            factors["approach_spd"] = (0.5, 2.0)

        # Final airborne altitude (lower = better; 5000 -> 0, 500 -> 1)
        last_alt = metrics.last_airborne_alt
        if last_alt is not None:
            factors["final_alt"] = (_lerp(last_alt, 5000, 500), 2.0)
        else:
            factors["final_alt"] = (0.5, 2.0)

        # Airport proximity at landing
        if dest_distance_km is not None:
            factors["airport_prox"] = (1.0 - _lerp(dest_distance_km, 0, 10), 2.0)
        else:
            factors["airport_prox"] = (0.3, 2.0)  # no airport match = weak signal

        # Coordinate stability: per-sample max jump (not total spread) so
        # normal taxi motion does not register as receiver noise. Jumps
        # below 200m are fine; 500m+ is noise; the lerp handles between.
        max_jump_m = metrics.landing_max_jump_m()
        factors["coord_stab"] = (1.0 - _lerp(max_jump_m, 200.0, 500.0), 1.0)

        # Post-landing points (we kept the flight open for a few ground points).
        # Even 1 point is meaningful - it confirmed the transition. 4+ points
        # is a clean stop. Map gp=1 -> 0.5 so the floor is soft.
        gp = metrics.ground_points_at_landing
        if gp <= 0:
            trace_tail = 0.0
        elif gp == 1:
            trace_tail = 0.5
        else:
            trace_tail = min(1.0, 0.5 + 0.25 * (gp - 1))
        factors["trace_tail"] = (trace_tail, 1.5)

        # Duration plausibility
        if duration_minutes is not None:
            dur_score = 1.0 if duration_minutes < 1440 else (0.5 if duration_minutes < 2880 else 0.1)
            factors["duration"] = (dur_score, 0.5)

        # Weighted geometric mean (any zero factor drags the whole score down)
        w_total = sum(w for _, w in factors.values())
        log_sum = 0.0
        for f, w in factors.values():
            log_sum += w * math.log(max(0.01, f))
        landing_conf = math.exp(log_sum / w_total) if w_total > 0 else 0.5

    # Penalty for altitude errors
    if landing_type == "altitude_error":
        takeoff_conf *= 0.3

    return round(takeoff_conf, 2), round(landing_conf, 2)
