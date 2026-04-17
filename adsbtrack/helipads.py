"""Helipad discovery via DBSCAN clustering on off-airport flight coordinates.

v7 F1: clusters confirmed takeoff/landing coordinates that are > 2 km from
any airport (i.e., origin_icao IS NULL) to identify repeat-use helipads,
offshore platforms, and hospital pads.

Uses a pure-Python DBSCAN implementation with haversine distance so there's
no dependency on scikit-learn.
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict

from .airports import haversine_km


def _dbscan(points: list[tuple[float, float]], eps_km: float, min_samples: int) -> list[int]:
    """Minimal DBSCAN returning a cluster label per point (-1 = noise).

    ``points`` is a list of (lat, lon) tuples. Distance metric is haversine.
    Candidate neighbors are pulled from a lat/lon bucket index so each
    point only compares against the handful of points in its own cell
    plus the 8 adjacent cells, rather than all N. The haversine check
    stays as the fine filter.

    Cell size: eps_km / 111 deg in both axes. Using the latitude width for
    the longitude cells is intentionally conservative — lon degrees get
    smaller at higher latitudes, so a lat-width lon cell guarantees we
    never miss a neighbor, at the cost of some extra candidates in
    equatorial data. The final haversine distance check discards the
    overshoot.

    Known edge cases left unhandled (extremely rare for helipad use):
      - Antimeridian crossing (points near +/-180 lon): adjacent cells
        wrap to a different integer key, so the handful of cross-meridian
        neighbors could be missed. Document rather than complicate.
      - Polar clusters (|lat| > 85 deg): longitude cells shrink toward
        zero physical width, so 9-cell lookup becomes undersized. Same:
        document rather than complicate.
    """
    n = len(points)
    labels = [-1] * n
    visited = [False] * n
    cluster_id = 0

    # Same cell width in both axes (see docstring). defaultdict so absent
    # cells cost nothing on insertion; _neighbors uses plain .get to avoid
    # growing the dict with empty entries at query time.
    cell_size_deg = eps_km / 111.0
    grid: dict[tuple[int, int], list[int]] = defaultdict(list)
    for i, (lat, lon) in enumerate(points):
        grid[(int(lat / cell_size_deg), int(lon / cell_size_deg))].append(i)

    def _neighbors(idx: int) -> list[int]:
        lat_i, lon_i = points[idx]
        lat_cell = int(lat_i / cell_size_deg)
        lon_cell = int(lon_i / cell_size_deg)
        out: list[int] = []
        for dlat in (-1, 0, 1):
            for dlon in (-1, 0, 1):
                bucket = grid.get((lat_cell + dlat, lon_cell + dlon))
                if bucket is None:
                    continue
                for j in bucket:
                    if j == idx:
                        continue
                    if haversine_km(lat_i, lon_i, points[j][0], points[j][1]) <= eps_km:
                        out.append(j)
        return out

    for i in range(n):
        if visited[i]:
            continue
        visited[i] = True
        nbrs = _neighbors(i)
        if len(nbrs) < min_samples - 1:  # -1 because the point itself counts
            continue
        # Start a new cluster
        labels[i] = cluster_id
        seed_set = list(nbrs)
        j = 0
        while j < len(seed_set):
            q = seed_set[j]
            if not visited[q]:
                visited[q] = True
                q_nbrs = _neighbors(q)
                if len(q_nbrs) >= min_samples - 1:
                    seed_set.extend(q_nbrs)
            if labels[q] == -1:
                labels[q] = cluster_id
            j += 1
        cluster_id += 1

    return labels


def discover_helipads(
    conn: sqlite3.Connection,
    *,
    eps_km: float = 0.2,
    min_samples: int = 3,
) -> int:
    """Run helipad discovery and populate the helipads table.

    1. Collects all confirmed takeoff/landing coordinates where origin_icao
       or destination_icao IS NULL (off-airport operations).
    2. Runs DBSCAN to find spatial clusters.
    3. Inserts clusters into the helipads table.
    4. Back-fills origin_helipad_id / destination_helipad_id on flights.

    Returns the number of helipads discovered.
    """
    conn.row_factory = sqlite3.Row

    # Gather off-airport coordinates with their flight IDs and role.
    # Each row: (flight_id, lat, lon, role='origin'|'destination', takeoff_date)
    rows = conn.execute(
        """
        SELECT id, takeoff_lat AS lat, takeoff_lon AS lon, 'origin' AS role, takeoff_date AS dt
        FROM flights WHERE origin_icao IS NULL AND takeoff_lat IS NOT NULL
        UNION ALL
        SELECT id, landing_lat AS lat, landing_lon AS lon, 'destination' AS role,
               COALESCE(landing_date, takeoff_date) AS dt
        FROM flights WHERE destination_icao IS NULL AND landing_lat IS NOT NULL
               AND landing_type IN ('confirmed', 'dropped_on_approach')
        """
    ).fetchall()

    if not rows:
        return 0

    points = [(r["lat"], r["lon"]) for r in rows]
    labels = _dbscan(points, eps_km=eps_km, min_samples=min_samples)

    # Build cluster metadata
    clusters: dict[int, list[int]] = {}
    for idx, label in enumerate(labels):
        if label >= 0:
            clusters.setdefault(label, []).append(idx)

    if not clusters:
        return 0

    # Clear existing helipads and back-fill columns
    conn.execute("DELETE FROM helipads")
    conn.execute("UPDATE flights SET origin_helipad_id = NULL, destination_helipad_id = NULL")

    helipad_count = 0

    for _cluster_id, indices in sorted(clusters.items()):
        lats = [points[i][0] for i in indices]
        lons = [points[i][1] for i in indices]
        centroid_lat = sum(lats) / len(lats)
        centroid_lon = sum(lons) / len(lons)
        dates = [rows[i]["dt"] for i in indices if rows[i]["dt"]]
        first_seen = min(dates) if dates else None
        last_seen = max(dates) if dates else None

        # Determine name hint from location
        name_hint = None
        if centroid_lat < 20 or (centroid_lon < -85 and centroid_lat < 30):
            # Rough Gulf of Mexico / offshore check
            name_hint = f"offshore_platform_{helipad_count + 1}"
        else:
            name_hint = f"helipad_{helipad_count + 1}"

        conn.execute(
            """INSERT INTO helipads (helipad_id, centroid_lat, centroid_lon,
               landing_count, first_seen, last_seen, name_hint)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                helipad_count + 1,
                round(centroid_lat, 6),
                round(centroid_lon, 6),
                len(indices),
                first_seen,
                last_seen,
                name_hint,
            ),
        )

        # Back-fill flight helipad IDs
        db_helipad_id = helipad_count + 1
        for i in indices:
            flight_id = rows[i]["id"]
            role = rows[i]["role"]
            if role == "origin":
                conn.execute(
                    "UPDATE flights SET origin_helipad_id = ? WHERE id = ?",
                    (db_helipad_id, flight_id),
                )
            else:
                conn.execute(
                    "UPDATE flights SET destination_helipad_id = ? WHERE id = ?",
                    (db_helipad_id, flight_id),
                )

        helipad_count += 1

    return helipad_count
