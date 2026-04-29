from contextlib import asynccontextmanager
from typing import Optional
import json
import os

import asyncpg
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

DSN = (
    f"postgresql://{os.getenv('PG_USER','lux_user')}:{os.getenv('PG_PASS','lux_pass')}"
    f"@{os.getenv('PG_HOST','postgres')}:{os.getenv('PG_PORT','5432')}"
    f"/{os.getenv('PG_DB','lux_mobility')}"
)

pool: Optional[asyncpg.Pool] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = await asyncpg.create_pool(DSN, min_size=2, max_size=10)
    yield
    await pool.close()


app = FastAPI(title="Transit Accessibility API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


class AddressResult(BaseModel):
    id: int
    address: str
    longitude: float
    latitude: float
    stop_id: Optional[str]
    stop_name: Optional[str]
    distance_m: Optional[float]
    color_class: str
    road_connected: bool


@app.get("/search", response_model=list[AddressResult])
async def search(
    q: str = Query(..., min_length=2, description="Address search string"),
    limit: int = Query(10, ge=1, le=50),
):
    """
    Full-text trigram search over stored addresses.
    Returns up to `limit` results ordered by similarity.
    """
    rows = await pool.fetch(
        """
        SELECT id, address, longitude, latitude,
               stop_id, stop_name, distance_m, color_class, road_connected
        FROM residence_accessibility
        WHERE word_similarity($1, address) > 0.2
        ORDER BY word_similarity($1, address) DESC
        LIMIT $2
        """,
        q, limit,
    )
    return [AddressResult(**dict(r)) for r in rows]


@app.get("/address/{address_id}", response_model=AddressResult)
async def get_address(address_id: int):
    row = await pool.fetchrow(
        """
        SELECT id, address, longitude, latitude,
               stop_id, stop_name, distance_m, color_class, road_connected
        FROM residence_accessibility
        WHERE id = $1
        """,
        address_id,
    )
    if not row:
        raise HTTPException(status_code=404, detail="Address not found")
    return AddressResult(**dict(row))


@app.get("/geojson")
async def geojson_bbox(
    west:  float = Query(...),
    south: float = Query(...),
    east:  float = Query(...),
    north: float = Query(...),
    zoom:  int   = Query(12, ge=1, le=22),
):
    """
    Return residence points inside the given WGS-84 bounding box as GeoJSON.
    The zoom hint drives a server-side limit so the client is never overwhelmed.
    """
    limit = 500 if zoom < 12 else 3000 if zoom < 14 else 15000
    rows = await pool.fetch(
        """
        SELECT id, address, longitude, latitude,
               stop_name, distance_m, color_class, road_connected
        FROM residence_accessibility
        WHERE geom && ST_MakeEnvelope($1, $2, $3, $4, 4326)
        LIMIT $5
        """,
        west, south, east, north, limit,
    )
    features = [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [r["longitude"], r["latitude"]]},
            "properties": {
                "id":             r["id"],
                "address":        r["address"],
                "stop_name":      r["stop_name"],
                "distance_m":     round(r["distance_m"]) if r["distance_m"] is not None else None,
                "color_class":    r["color_class"],
                "road_connected": r["road_connected"],
            },
        }
        for r in rows
    ]
    return {"type": "FeatureCollection", "features": features}


@app.get("/path/{address_id}")
async def get_walk_path(address_id: int):
    """
    Return a GeoJSON LineString of the pedestrian path from a residence to its
    nearest stop, built from the pgRouting walk network.

    Path segments:
      residence → residence snap-point → [OSM walk edges] → stop snap-point → stop
    """
    snap = await pool.fetchrow("""
        SELECT
            ra.address, ra.stop_name, ra.distance_m, ra.color_class,
            ra.longitude              AS res_lon,
            ra.latitude               AS res_lat,
            rs.edge_id                AS res_edge,
            rs.fraction               AS res_frac,
            ST_X(rs.snap_geom)        AS res_snap_lon,
            ST_Y(rs.snap_geom)        AS res_snap_lat,
            ss.edge_id                AS stop_edge,
            ss.fraction               AS stop_frac,
            ST_X(ss.snap_geom)        AS stop_snap_lon,
            ST_Y(ss.snap_geom)        AS stop_snap_lat,
            s.lon                     AS stop_lon,
            s.lat                     AS stop_lat
        FROM residence_accessibility ra
        JOIN residence_snap rs ON rs.residence_id = ra.id
        JOIN stop_snap ss ON ss.stop_id = ra.stop_id
        JOIN stops s ON s.stop_id = ss.stop_id AND s.stop_type = ss.stop_type
        WHERE ra.id = $1
        LIMIT 1
    """, address_id)

    if not snap:
        raise HTTPException(status_code=404, detail="No routing data for this address")

    # Build pgRouting points SQL from DB values (integers + floats — not user input)
    points_sql = (
        f"SELECT 1::bigint AS pid, {int(snap['stop_edge'])}::bigint AS edge_id, "
        f"{float(snap['stop_frac'])}::double precision AS fraction "
        f"UNION ALL "
        f"SELECT 2::bigint AS pid, {int(snap['res_edge'])}::bigint AS edge_id, "
        f"{float(snap['res_frac'])}::double precision AS fraction"
    )

    # Route from res snap (-2) to stop snap (-1), collect ordered node coordinates,
    # and prepend/append the actual residence and stop locations.
    geojson_str = await pool.fetchval("""
        WITH route AS (
            SELECT r.seq, r.node
            FROM pgr_withPoints(
                'SELECT id, source, target, cost, reverse_cost FROM walk_edges',
                $1,
                -2::bigint, -1::bigint,
                directed     => false,
                driving_side => 'b'
            ) r
        ),
        node_geom AS (
            SELECT r.seq, wn.geom
            FROM route r
            JOIN walk_nodes wn ON wn.id = r.node
            WHERE r.node > 0
        ),
        all_points AS (
            SELECT -2 AS seq, ST_SetSRID(ST_MakePoint($2, $3), 4326) AS geom
            UNION ALL
            SELECT -1,         ST_SetSRID(ST_MakePoint($4, $5), 4326)
            UNION ALL
            SELECT seq, geom FROM node_geom
            UNION ALL
            SELECT 99998,      ST_SetSRID(ST_MakePoint($6, $7), 4326)
            UNION ALL
            SELECT 99999,      ST_SetSRID(ST_MakePoint($8, $9), 4326)
        )
        SELECT ST_AsGeoJSON(
            ST_MakeLine(ARRAY(SELECT geom FROM all_points ORDER BY seq))
        )
    """,
        points_sql,
        snap['res_lon'],       snap['res_lat'],
        snap['res_snap_lon'],  snap['res_snap_lat'],
        snap['stop_snap_lon'], snap['stop_snap_lat'],
        snap['stop_lon'],      snap['stop_lat'],
    )

    if not geojson_str:
        raise HTTPException(status_code=404, detail="Could not compute walking path")

    return {
        "path": json.loads(geojson_str),
        "stop_location": {
            "type": "Point",
            "coordinates": [float(snap['stop_lon']), float(snap['stop_lat'])],
        },
        "address":    snap['address'],
        "stop_name":  snap['stop_name'],
        "distance_m": snap['distance_m'],
        "color_class": snap['color_class'],
    }


@app.get("/health")
async def health():
    await pool.fetchval("SELECT 1")
    return {"status": "ok"}
