"""
Loads the Luxembourg OSM pedestrian network into PostGIS and computes
pgRouting walk distances from every stop to every reachable residence.

Workflow called from pipeline.py after the main PG bulk-load:
    conn = psycopg2.connect(...)
    load_osm_walk_network(cache_path, conn)   # walk_nodes + walk_edges
    snap_points(conn)                          # stop_snap + residence_snap
    compute_walk_distances(conn)               # updates distance_m / color_class
"""
from __future__ import annotations

from pathlib import Path

import psycopg2
import psycopg2.extras

# Routing cutoff – residences beyond this from any stop keep their
# straight-line distance/color from the DuckDB phase.
MAX_WALK_M: float = 3_000.0

_NORTH, _SOUTH, _EAST, _WEST = 50.20, 49.40, 6.60, 5.70


# ── Walk network ──────────────────────────────────────────────────────────────

def load_osm_walk_network(cache_path: Path, conn, *, force: bool = False) -> None:
    """
    Download Luxembourg OSM walk network via osmnx and bulk-insert into
    walk_nodes / walk_edges.  Skips if already populated unless force=True.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM walk_edges")
        existing = cur.fetchone()[0]

    if existing > 0:
        if not force:
            print(f"  Walk network already in DB ({existing:,} edges) — skipping.")
            return
        print("  FORCE: truncating walk network tables...")
        with conn.cursor() as cur:
            cur.execute("TRUNCATE walk_edges, walk_nodes")
        conn.commit()

    import osmnx as ox  # noqa: PLC0415

    if cache_path.exists():
        print(f"  Loading cached walk graph from {cache_path.name}...")
        G = ox.load_graphml(cache_path)
    else:
        print("  Downloading Luxembourg walk network from OSM...")
        major = int(ox.__version__.split(".")[0])
        if major >= 2:
            G = ox.graph_from_bbox(
                (_WEST, _SOUTH, _EAST, _NORTH), network_type="walk", simplify=True
            )
        else:
            G = ox.graph_from_bbox(
                north=_NORTH, south=_SOUTH, east=_EAST, west=_WEST,
                network_type="walk", simplify=True, retain_all=False,
            )
        ox.save_graphml(G, cache_path)

    nodes_gdf, edges_gdf = ox.graph_to_gdfs(G)
    print(f"  {len(nodes_gdf):,} nodes, {len(edges_gdf):,} edges")

    print("  Inserting walk nodes...")
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO walk_nodes (id, geom) VALUES %s ON CONFLICT DO NOTHING",
            [
                (int(nid), f"SRID=4326;POINT({row.geometry.x} {row.geometry.y})")
                for nid, row in nodes_gdf.iterrows()
            ],
            template="(%s, ST_GeomFromEWKT(%s))",
            page_size=5_000,
        )

    print("  Inserting walk edges...")
    edge_rows = [
        (
            int(u), int(v),
            float(row.get("length", 0)) or row.geometry.length,
            float(row.get("length", 0)) or row.geometry.length,
            f"SRID=4326;{row.geometry.wkt}",
        )
        for (u, v, _k), row in edges_gdf.iterrows()
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO walk_edges (source, target, cost, reverse_cost, geom) VALUES %s",
            edge_rows,
            template="(%s, %s, %s, %s, ST_GeomFromEWKT(%s))",
            page_size=2_000,
        )
    conn.commit()
    print("  Walk network loaded.")


# ── Snap points ───────────────────────────────────────────────────────────────

def snap_points(conn, *, force: bool = False) -> None:
    """
    For each stop and residence, find the nearest walk edge and store:
      - fraction  : 0–1 position along the edge (ST_LineLocatePoint)
      - snap_geom : perpendicular intersection point on the edge (ST_ClosestPoint)
      - snap_dist_m: straight-line distance from the original point to snap_geom

    PIDs are global across both tables so pgr_withPointsDD can distinguish them:
      stop_snap     → pid  1 … N_stops
      residence_snap → pid  N_stops+1 … N_stops+N_residences
    """
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM residence_snap")
        if cur.fetchone()[0] > 0 and not force:
            print("  Snap points already computed — skipping.")
            return
        cur.execute("TRUNCATE stop_snap, residence_snap")

        print("  Snapping stops to walk edges...")
        cur.execute("""
            INSERT INTO stop_snap
                    (pid, stop_id, stop_type, edge_id, fraction, snap_geom, snap_dist_m)
            SELECT
                ROW_NUMBER() OVER (ORDER BY s.stop_id, s.stop_type)::BIGINT,
                s.stop_id,
                s.stop_type,
                e.id,
                ST_LineLocatePoint(e.geom, s.geom),
                ST_ClosestPoint(e.geom, s.geom),
                ST_Distance(s.geom::geography,
                            ST_ClosestPoint(e.geom, s.geom)::geography)
            FROM stops s
            CROSS JOIN LATERAL (
                SELECT id, geom FROM walk_edges ORDER BY geom <-> s.geom LIMIT 1
            ) e
        """)
        n_stops = cur.rowcount
        print(f"    {n_stops:,} stops snapped")

        print("  Snapping residences to walk edges...")
        cur.execute("""
            INSERT INTO residence_snap
                    (pid, residence_id, edge_id, fraction, snap_geom, snap_dist_m)
            SELECT
                %s + ROW_NUMBER() OVER (ORDER BY r.id)::BIGINT,
                r.id,
                e.id,
                ST_LineLocatePoint(e.geom, r.geom),
                ST_ClosestPoint(e.geom, r.geom),
                ST_Distance(r.geom::geography,
                            ST_ClosestPoint(e.geom, r.geom)::geography)
            FROM residence_accessibility r
            CROSS JOIN LATERAL (
                SELECT id, geom FROM walk_edges ORDER BY geom <-> r.geom LIMIT 1
            ) e
        """, (n_stops,))
        n_res = cur.rowcount
        print(f"    {n_res:,} residences snapped")

    conn.commit()


# ── pgRouting distances ───────────────────────────────────────────────────────

def compute_walk_distances(conn) -> None:
    """
    Run pgr_withPointsDD from every stop snap-point simultaneously.

    For each residence reached within MAX_WALK_M the total walk distance is:
        stop_snap.snap_dist_m + network agg_cost + residence_snap.snap_dist_m

    The residence is updated with the minimum distance across all stops, along
    with the corresponding stop_id and the reclassified color_class.
    Residences not reachable from any stop retain their DuckDB straight-line values.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM stop_snap")
        if cur.fetchone()[0] == 0:
            print("  No stop snap points — skipping routing.")
            return
        cur.execute("SELECT COUNT(*) FROM residence_snap")
        if cur.fetchone()[0] == 0:
            print("  No residence snap points — skipping routing.")
            return

    print(f"  Running pgr_withPointsDD (cutoff {MAX_WALK_M:.0f} m)...")
    with conn.cursor() as cur:
        # pgr_withPointsDD spreads from every stop snap-point (negative pid = point).
        # With details => true, the result includes rows where node is the negative
        # pid of a reached snap-point (stops OR residences).  We filter for residence
        # pids only (pid > MAX(stop_snap.pid)) to get stop→residence costs.
        cur.execute("""
            WITH routing AS (
                SELECT start_vid, node, agg_cost
                FROM pgr_withPointsDD(
                    'SELECT id, source, target, cost, reverse_cost FROM walk_edges',
                    'SELECT pid::BIGINT, edge_id::BIGINT, fraction
                       FROM stop_snap
                     UNION ALL
                     SELECT pid::BIGINT, edge_id::BIGINT, fraction
                       FROM residence_snap',
                    ARRAY(SELECT (-pid)::BIGINT FROM stop_snap),
                    %(max_walk)s,
                    directed     => false,
                    driving_side => 'b',
                    details      => true
                )
                WHERE node < 0
            ),
            stop_pid_max AS (SELECT COALESCE(MAX(pid), 0) AS n FROM stop_snap),
            candidates AS (
                SELECT
                    rs.residence_id,
                    ss.stop_id,
                    ss.snap_dist_m + r.agg_cost + rs.snap_dist_m AS total_m
                FROM routing r
                CROSS JOIN stop_pid_max sp
                JOIN stop_snap      ss ON ss.pid =  (-r.start_vid)::BIGINT
                JOIN residence_snap rs ON rs.pid =  (-r.node  )::BIGINT
                WHERE (-r.node)::BIGINT > sp.n
            ),
            best AS (
                SELECT DISTINCT ON (residence_id)
                    residence_id, stop_id, total_m
                FROM candidates
                ORDER BY residence_id, total_m ASC
            )
            UPDATE residence_accessibility ra
            SET
                distance_m  = b.total_m,
                color_class = CASE
                                  WHEN b.total_m <  1000 THEN 'green'
                                  WHEN b.total_m <= 2000 THEN 'yellow'
                                  ELSE                        'red'
                              END,
                stop_id     = b.stop_id
            FROM best b
            WHERE ra.id = b.residence_id
        """, {"max_walk": MAX_WALK_M})

        print(f"  {cur.rowcount:,} residences updated with pgRouting walk distances")
    conn.commit()
