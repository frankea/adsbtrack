"""Day-level trace forensics behind the `inspect` CLI command.

Pure functions over decoded trace rows (list-of-points readsb format), so
tests drive them with synthetic data and the CLI stays a thin renderer.
The "what happened here" loop for one aircraft-day: fragment table,
integrity stats, squawk/callsign timeline, closest approach to a fix.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field

from .geo import haversine_km

DEFAULT_FRAGMENT_GAP_SECS = 300.0


@dataclass
class FragmentSummary:
    source: str
    start_ts: float
    end_ts: float
    n_points: int
    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float
    alt_min: float | None
    alt_max: float | None
    gs_min: float | None
    gs_max: float | None
    v2_samples: int
    v2_sil0: int
    v2_nic0: int
    callsigns: list[str]
    squawks: list[str]
    position_sources: dict[str, int] = field(default_factory=dict)


def _point_detail(point: list) -> dict | None:
    """The point's detail dict (index 8), or None when absent/malformed.
    Shared by every function below so the "is this a rich-detail point"
    check can't drift between them."""
    if len(point) > 8 and isinstance(point[8], dict):
        return point[8]
    return None


def _point_position_source(point: list, detail: dict | None) -> str | None:
    """Position source tag: prefer point[9] (14-element readsb layout),
    falling back to detail["type"] for the 9-element layout most rows in
    this DB actually use. Mirrors parser._extract_point_fields."""
    if len(point) > 9 and isinstance(point[9], str):
        return point[9]
    if detail:
        det_type = detail.get("type")
        if isinstance(det_type, str):
            return det_type
    return None


def split_fragments(trace: list, base_ts: float, gap_secs: float) -> list[list[tuple[float, list]]]:
    """Split a decoded trace into fragments, starting a new one whenever
    the gap to the previous point exceeds ``gap_secs``.

    Non-list rows (malformed points) are skipped rather than breaking a
    fragment. Returns a list of fragments, each a chronological list of
    (abs_ts, point) pairs.
    """
    fragments: list[list[tuple[float, list]]] = []
    prev_ts: float | None = None
    for point in trace:
        if not isinstance(point, list):
            continue
        abs_ts = base_ts + point[0]
        if prev_ts is None or abs_ts - prev_ts > gap_secs:
            fragments.append([])
        fragments[-1].append((abs_ts, point))
        prev_ts = abs_ts
    return fragments


def summarize_fragments(source: str, base_ts: float, trace: list, gap_secs: float) -> list[FragmentSummary]:
    """Build one FragmentSummary per fragment of ``trace``.

    Altitude min/max come from numeric point[3] values only: the string
    "ground" counts as 0 for the minimum but is excluded from the maximum
    (a ground point is the lowest possible altitude, not a real reading to
    average against cruise). gs min/max come from numeric point[4].
    Integrity uses the same predicate as integrity.count_v2_integrity
    (version == 2, sil == 0, nic == 0 on the detail dict). Callsigns and
    squawks are collected from every point carrying a detail dict,
    regardless of ADS-B version, since a spoofed or degraded broadcast can
    flip versions mid-fragment and forensics wants everything observed.
    """
    summaries: list[FragmentSummary] = []
    for fragment in split_fragments(trace, base_ts, gap_secs):
        alt_values: list[float] = []
        saw_ground = False
        gs_values: list[float] = []
        v2_samples = v2_sil0 = v2_nic0 = 0
        callsigns: set[str] = set()
        squawks: set[str] = set()
        position_sources: Counter[str] = Counter()

        for _abs_ts, point in fragment:
            alt = point[3]
            if alt == "ground":
                saw_ground = True
            elif isinstance(alt, (int, float)):
                alt_values.append(alt)

            gs = point[4] if len(point) > 4 else None
            if isinstance(gs, (int, float)):
                gs_values.append(gs)

            detail = _point_detail(point)
            if detail:
                if detail.get("version") == 2:
                    v2_samples += 1
                    if detail.get("sil") == 0:
                        v2_sil0 += 1
                    if detail.get("nic") == 0:
                        v2_nic0 += 1
                flight = (detail.get("flight") or "").strip()
                if flight:
                    callsigns.add(flight)
                squawk = detail.get("squawk")
                if squawk:
                    squawks.add(str(squawk))

            pos_source = _point_position_source(point, detail)
            if pos_source:
                position_sources[pos_source] += 1

        alt_min = None
        alt_max = None
        if alt_values or saw_ground:
            min_candidates = alt_values + ([0] if saw_ground else [])
            alt_min = min(min_candidates)
            alt_max = max(alt_values) if alt_values else None

        start_ts, start_point = fragment[0]
        end_ts, end_point = fragment[-1]

        summaries.append(
            FragmentSummary(
                source=source,
                start_ts=start_ts,
                end_ts=end_ts,
                n_points=len(fragment),
                start_lat=start_point[1],
                start_lon=start_point[2],
                end_lat=end_point[1],
                end_lon=end_point[2],
                alt_min=alt_min,
                alt_max=alt_max,
                gs_min=min(gs_values) if gs_values else None,
                gs_max=max(gs_values) if gs_values else None,
                v2_samples=v2_samples,
                v2_sil0=v2_sil0,
                v2_nic0=v2_nic0,
                callsigns=sorted(callsigns),
                squawks=sorted(squawks),
                position_sources=dict(position_sources),
            )
        )
    return summaries


def squawk_timeline(base_ts: float, trace: list) -> list[tuple[float, str]]:
    """Squawk change points only: (abs_ts, squawk) for the first point and
    every point whose squawk differs from the previous squawk-bearing
    point. Points without a squawk in their detail dict are skipped
    entirely rather than treated as a "no squawk" state."""
    timeline: list[tuple[float, str]] = []
    last_squawk: str | None = None
    for point in trace:
        if not isinstance(point, list):
            continue
        detail = _point_detail(point)
        if not detail:
            continue
        squawk = detail.get("squawk")
        if not squawk:
            continue
        squawk = str(squawk)
        if squawk != last_squawk:
            timeline.append((base_ts + point[0], squawk))
            last_squawk = squawk
    return timeline


def callsign_timeline(base_ts: float, trace: list) -> list[tuple[float, str]]:
    """Callsign change points only, same change-point rule as
    squawk_timeline but keyed on detail["flight"] (stripped, blank
    treated as absent)."""
    timeline: list[tuple[float, str]] = []
    last_callsign: str | None = None
    for point in trace:
        if not isinstance(point, list):
            continue
        detail = _point_detail(point)
        if not detail:
            continue
        callsign = (detail.get("flight") or "").strip()
        if not callsign:
            continue
        if callsign != last_callsign:
            timeline.append((base_ts + point[0], callsign))
            last_callsign = callsign
    return timeline


def closest_approach(base_ts: float, trace: list, lat: float, lon: float) -> tuple[float, float, float | None] | None:
    """(dist_km, ts, alt) of the trace point nearest to (lat, lon), or
    None when the trace has no usable fixes."""
    best: tuple[float, float, float | None] | None = None
    for point in trace:
        if not isinstance(point, list):
            continue
        p_lat, p_lon = point[1], point[2]
        if not isinstance(p_lat, (int, float)) or not isinstance(p_lon, (int, float)):
            continue
        dist_km = haversine_km(lat, lon, p_lat, p_lon)
        if best is None or dist_km < best[0]:
            alt = point[3] if len(point) > 3 and isinstance(point[3], (int, float)) else None
            best = (dist_km, base_ts + point[0], alt)
    return best
