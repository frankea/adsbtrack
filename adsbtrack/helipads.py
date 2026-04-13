"""Helipad discovery via DBSCAN clustering on off-airport flight coordinates.

v7 F1: clusters confirmed takeoff/landing coordinates that are > 2 km from
any airport (i.e., origin_icao IS NULL) to identify repeat-use helipads,
offshore platforms, and hospital pads.

Uses a pure-Python DBSCAN implementation with haversine distance so there's
no dependency on scikit-learn.
"""

from __future__ import annotations

import sqlite3

from .airports import haversine_km


def _dbscan(points: list[tuple[float, float]], eps_km: float, min_samples: int) -> list[int]:
    """Minimal DBSCAN returning a cluster label per point (-1 = noise).

    ``points`` is a list of (lat, lon) tuples. Distance metric is haversine.
    O(n^2) which is fine for the ~5,000 off-airport coordinates in a typical DB.
    """
    n = len(points)
    labels = [-1] * n
    visited = [False] * n
    cluster_id = 0

    def _neighbors(idx: int) -> list[int]:
        lat_i, lon_i = points[idx]
        return [j for j in range(n) if j != idx and haversine_km(lat_i, lon_i, points[j][0], points[j][1]) <= eps_km]

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
