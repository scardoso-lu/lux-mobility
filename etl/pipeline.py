"""
DuckDB ETL pipeline: computes nearest bus stop for every Luxembourg address,
classifies by distance, and bulk-loads into PostgreSQL via DuckDB's postgres extension.
"""
import csv
import os
import sys
import xml.etree.ElementTree as ET
import duckdb
from dbfread import DBF
from pathlib import Path

RAW = Path(__file__).parent / "data" / "raw"
PROCESSED = Path(__file__).parent / "data" / "processed"
PROCESSED.mkdir(parents=True, exist_ok=True)

PG_HOST          = os.getenv("PG_HOST", "localhost")
PG_PORT          = os.getenv("PG_PORT", "5432")
PG_DB            = os.getenv("PG_DB",   "lux_mobility")
PG_USER          = os.getenv("PG_USER", "lux_user")
PG_PASS          = os.getenv("PG_PASS", "lux_pass")
FORCE_REPROCESS  = os.getenv("FORCE_REPROCESS", "false").lower() == "true"


def detect_latlon_cols(con: duckdb.DuckDBPyConnection, table: str) -> tuple[str, str]:
    cols = [r[0].lower() for r in con.execute(f"DESCRIBE {table}").fetchall()]
    lat_candidates = ["lat_wgs84", "latitude", "lat", "y"]
    lon_candidates = ["lon_wgs84", "longitude", "lon", "x"]
    lat = next((c for c in lat_candidates if c in cols), None)
    lon = next((c for c in lon_candidates if c in cols), None)
    if not lat or not lon:
        raise RuntimeError(f"Cannot detect lat/lon columns in {table}. Found: {cols}")
    return lat, lon


def detect_address_cols(con: duckdb.DuckDBPyConnection, table: str) -> str:
    cols = [r[0].lower() for r in con.execute(f"DESCRIBE {table}").fetchall()]
    # Build a concatenated address expression from whatever name/street fields exist
    parts = []
    for candidate in ["numero", "num", "house_number"]:
        if candidate in cols: parts.append(candidate); break
    for candidate in ["rue", "street", "rue_nom", "streetname"]:
        if candidate in cols: parts.append(candidate); break
    for candidate in ["localite", "locality", "commune", "city"]:
        if candidate in cols: parts.append(candidate); break
    for candidate in ["code_postal", "code_posta", "code_post", "codepostal", "postal_code", "postcode", "cp"]:
        if candidate in cols: parts.append(candidate); break
    if not parts:
        # fallback: first text column
        for r in con.execute(f"DESCRIBE {table}").fetchall():
            if "VARCHAR" in r[1].upper() or "TEXT" in r[1].upper():
                parts.append(r[0].lower())
                break
    if not parts:
        raise RuntimeError(f"Cannot find address columns in {table}. Found: {cols}")
    return " || ', ' || ".join(f"COALESCE(CAST({p} AS VARCHAR), '')" for p in parts)


def parse_inspire_gml(gml_path: Path) -> list[str]:
    """Parse INSPIRE TransportNetwork GML; return WKT LINESTRING list (EPSG:3035, X=Easting Y=Northing)."""
    GML_NS   = "http://www.opengis.net/gml/3.2"
    POSLIST  = f"{{{GML_NS}}}posList"
    LINE_TAGS = {f"{{{GML_NS}}}LineString", f"{{{GML_NS}}}LineStringSegment"}

    wkt_list: list[str] = []
    dim = 2

    for event, elem in ET.iterparse(str(gml_path), events=("start", "end")):
        if event == "start" and elem.tag in LINE_TAGS:
            dim = int(elem.get("srsDimension", 2))
        elif event == "end" and elem.tag == POSLIST and elem.text:
            vals   = elem.text.split()
            coords = [float(v) for v in vals]
            if len(coords) < dim * 2:
                elem.clear()
                continue
            # GML with srsName EPSG:3035 uses authority axis order: Northing first, Easting second.
            # Swap to (Easting, Northing) = (X, Y) so always_xy=true in ST_Transform is correct.
            pairs = [(coords[i + 1], coords[i]) for i in range(0, len(coords), dim)]
            if len(pairs) >= 2:
                wkt_list.append(
                    "LINESTRING (" + ", ".join(f"{x} {y}" for x, y in pairs) + ")"
                )
            elem.clear()

    return wkt_list


def detect_train_crs(train_dir: Path) -> str:
    prj = next(train_dir.glob("*.prj"), None)
    if prj:
        text = prj.read_text(errors="replace").upper()
        if "WGS_1984" in text or "WGS84" in text or '"WGS 84"' in text:
            return "EPSG:4326"
        if "3857" in text or "PSEUDO" in text or "MERCATOR" in text:
            return "EPSG:3857"
    return "EPSG:2169"  # Luxembourg national CRS default


def run():
    stops_file  = RAW / "stops.txt"
    train_dir   = RAW / "train_stops"
    addr_dbf    = RAW / "addresses.dbf"
    addr_csv    = PROCESSED / "addresses_normalized.csv"
    out_parquet = PROCESSED / "residence_accessibility.parquet"

    train_shp = next(train_dir.glob("*.shp"), None) if train_dir.exists() else None

    for f in [stops_file, addr_dbf]:
        if not f.exists():
            sys.exit(f"Missing {f}. Run etl/download.sh first.")
    if not train_shp:
        sys.exit(f"Missing train stops shapefile in {train_dir}. Run etl/download.sh first.")

    # Skip if the table is already populated (idempotency guard for restarts)
    pg_conn = f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASS}"
    _chk = duckdb.connect()
    _chk.execute("INSTALL postgres; LOAD postgres;")
    _chk.execute(f"ATTACH '{pg_conn}' AS _pg (TYPE POSTGRES)")
    existing = _chk.execute("SELECT COUNT(*) FROM _pg.residence_accessibility").fetchone()[0]
    if existing > 0:
        if FORCE_REPROCESS:
            print(f"FORCE_REPROCESS: truncating {existing:,} existing rows...")
            _chk.execute("DELETE FROM _pg.residence_snap")
            _chk.execute("DELETE FROM _pg.stop_snap")
            _chk.execute("DELETE FROM _pg.residence_accessibility")
            _chk.execute("DELETE FROM _pg.road_links")
            _chk.execute("DELETE FROM _pg.stops")
            _chk.execute("DELETE FROM _pg.walk_edges")
            _chk.execute("DELETE FROM _pg.walk_nodes")
        else:
            print(f"Table already has {existing:,} rows — skipping ETL.")
            _chk.close()
            return
    _chk.close()

    print("Connecting to DuckDB...")
    con = duckdb.connect(str(PROCESSED / "pipeline.duckdb"))
    con.execute("INSTALL spatial; LOAD spatial;")

    # ── Load bus stops (GTFS) ────────────────────────────────────────────────
    print("Loading bus stops...")
    con.execute(f"""
        CREATE OR REPLACE TABLE bus_stops AS
        SELECT
            stop_id,
            stop_name,
            TRY_CAST(stop_lat AS DOUBLE) AS stop_lat,
            TRY_CAST(stop_lon AS DOUBLE) AS stop_lon
        FROM read_csv_auto('{stops_file}', header=true)
        WHERE TRY_CAST(stop_lat AS DOUBLE) IS NOT NULL
    """)
    n_bus = con.execute("SELECT COUNT(*) FROM bus_stops").fetchone()[0]
    print(f"  {n_bus:,} bus stops loaded")

    # ── Load train stops (shapefile via ST_Read) ─────────────────────────────
    print("Loading train stops...")
    train_src_crs = detect_train_crs(train_dir)
    print(f"  Source CRS: {train_src_crs}")
    con.execute(f"""
        CREATE OR REPLACE TABLE train_stops AS
        SELECT
            'TRAIN_' || NOM_GARE || '_' || NO_LIGNE  AS stop_id,
            NOM_GARE                                  AS stop_name,
            ST_Y(ST_Transform(geom, '{train_src_crs}', 'EPSG:4326')) AS stop_lat,
            ST_X(ST_Transform(geom, '{train_src_crs}', 'EPSG:4326')) AS stop_lon
        FROM ST_Read('{train_shp}')
        WHERE NOM_GARE IS NOT NULL
    """)
    n_train = con.execute("SELECT COUNT(*) FROM train_stops").fetchone()[0]
    print(f"  {n_train:,} train stops loaded")

    # ── Merge into unified stops table ───────────────────────────────────────
    con.execute("""
        CREATE OR REPLACE TABLE stops AS
        SELECT stop_id, stop_name, stop_lat, stop_lon, 'bus'   AS stop_type FROM bus_stops
        UNION ALL
        SELECT stop_id, stop_name, stop_lat, stop_lon, 'train' AS stop_type FROM train_stops
    """)
    n_stops = con.execute("SELECT COUNT(*) FROM stops").fetchone()[0]
    print(f"  {n_stops:,} total stops (bus + train)")

    # ── Load INSPIRE road network (optional) ────────────────────────────────
    inspire_gml = RAW / "inspire_roads.gml"
    has_road_network = False
    if inspire_gml.exists():
        print("Loading INSPIRE road network...")
        road_wkt_list = parse_inspire_gml(inspire_gml)
        print(f"  {len(road_wkt_list):,} road links parsed from GML")
        if road_wkt_list:
            road_csv = PROCESSED / "road_links.csv"
            with open(road_csv, "w", newline="", encoding="utf-8") as fh:
                writer = csv.writer(fh, quoting=csv.QUOTE_ALL)
                writer.writerow(["wkt"])
                for wkt in road_wkt_list:
                    writer.writerow([wkt])
            # Transform EPSG:3035 → EPSG:2169; always_xy=true respects X=Easting, Y=Northing
            # as written in the GML file (which uses practical XY order, not the EPSG axis order)
            con.execute(f"""
                CREATE OR REPLACE TABLE road_links AS
                SELECT
                    ST_Transform(ST_GeomFromText(wkt), 'EPSG:3035', 'EPSG:2169', true) AS geom
                FROM read_csv('{road_csv}', header=true, quote='"')
                WHERE wkt IS NOT NULL
            """)
            con.execute("""
                CREATE OR REPLACE TABLE road_links AS
                SELECT geom,
                       ST_XMin(geom) AS xmin, ST_XMax(geom) AS xmax,
                       ST_YMin(geom) AS ymin, ST_YMax(geom) AS ymax
                FROM road_links
            """)
            n_links = con.execute("SELECT COUNT(*) FROM road_links").fetchone()[0]
            print(f"  {n_links:,} road links loaded (EPSG:2169)")
            has_road_network = True
    else:
        print("  inspire_roads.gml not found — road connectivity check skipped")

    # ── Load addresses (DBF → normalised CSV → DuckDB) ───────────────────────
    print("Loading addresses from DBF...")
    # DBF field names are UPPERCASE; lowercase them so column-detection works uniformly
    dbf_table = DBF(str(addr_dbf), encoding="utf-8", load=True)
    fields = [f.lower() for f in dbf_table.field_names]
    with open(addr_csv, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for record in dbf_table:
            writer.writerow({k.lower(): v for k, v in record.items()})

    con.execute(f"""
        CREATE OR REPLACE TABLE addresses AS
        SELECT *
        FROM read_csv_auto('{addr_csv}', header=true)
    """)
    n_addr = con.execute("SELECT COUNT(*) FROM addresses").fetchone()[0]
    print(f"  {n_addr:,} addresses loaded")

    lat_col, lon_col = detect_latlon_cols(con, "addresses")
    addr_expr = detect_address_cols(con, "addresses")
    print(f"  Using lat={lat_col}, lon={lon_col}, address='{addr_expr}'")

    # ── Add row ID if absent ────────────────────────────────────────────────
    addr_cols = [r[0].lower() for r in con.execute("DESCRIBE addresses").fetchall()]
    if "addr_id" not in addr_cols:
        con.execute("CREATE SEQUENCE IF NOT EXISTS seq_addr START 1")
        con.execute("ALTER TABLE addresses ADD COLUMN addr_id BIGINT DEFAULT nextval('seq_addr')")
        con.execute("UPDATE addresses SET addr_id = nextval('seq_addr')")

    # ── Nearest-stop spatial join ────────────────────────────────────────────
    # Pass 1: bbox pre-filter (fast; covers virtually all of Luxembourg)
    print("Computing nearest stops — pass 1: bbox-filtered...")
    _dist_expr = f"""ST_Distance(
                    ST_Transform(ST_Point(TRY_CAST(a.{lon_col} AS DOUBLE),
                                         TRY_CAST(a.{lat_col} AS DOUBLE)),
                                 'EPSG:4326', 'EPSG:2169'),
                    ST_Transform(ST_Point(s.stop_lon, s.stop_lat),
                                 'EPSG:4326', 'EPSG:2169')
                )"""
    _classify  = """CASE
                WHEN nn.distance_m <  1000 THEN 'green'
                WHEN nn.distance_m <= 2000 THEN 'yellow'
                ELSE                            'red'
            END"""
    _valid_coords = f"""TRY_CAST(a.{lon_col} AS DOUBLE) IS NOT NULL
          AND TRY_CAST(a.{lat_col} AS DOUBLE) IS NOT NULL"""

    con.execute(f"""
        CREATE OR REPLACE TABLE result AS
        SELECT
            a.addr_id,
            ({addr_expr})                        AS address,
            TRY_CAST(a.{lon_col} AS DOUBLE)      AS longitude,
            TRY_CAST(a.{lat_col} AS DOUBLE)      AS latitude,
            nn.stop_id,
            nn.stop_name,
            nn.distance_m,
            {_classify}                          AS color_class
        FROM addresses a,
        LATERAL (
            SELECT s.stop_id, s.stop_name, {_dist_expr} AS distance_m
            FROM stops s
            WHERE s.stop_lon BETWEEN TRY_CAST(a.{lon_col} AS DOUBLE) - 0.1
                                 AND TRY_CAST(a.{lon_col} AS DOUBLE) + 0.1
              AND s.stop_lat BETWEEN TRY_CAST(a.{lat_col} AS DOUBLE) - 0.1
                                 AND TRY_CAST(a.{lat_col} AS DOUBLE) + 0.1
            ORDER BY distance_m ASC
            LIMIT 1
        ) nn
        WHERE {_valid_coords}
    """)

    n_fast = con.execute("SELECT COUNT(*) FROM result").fetchone()[0]

    # Pass 2: fallback — full-scan nearest stop for any address the bbox missed.
    # This is slow per row but should touch only a handful of edge cases.
    print("Computing nearest stops — pass 2: fallback for bbox misses...")
    con.execute(f"""
        INSERT INTO result
        SELECT
            a.addr_id,
            ({addr_expr})                        AS address,
            TRY_CAST(a.{lon_col} AS DOUBLE)      AS longitude,
            TRY_CAST(a.{lat_col} AS DOUBLE)      AS latitude,
            nn.stop_id,
            nn.stop_name,
            nn.distance_m,
            {_classify}                          AS color_class
        FROM addresses a,
        LATERAL (
            SELECT s.stop_id, s.stop_name, {_dist_expr} AS distance_m
            FROM stops s
            ORDER BY distance_m ASC
            LIMIT 1
        ) nn
        WHERE {_valid_coords}
          AND a.addr_id NOT IN (SELECT addr_id FROM result)
    """)

    n_fallback = con.execute("SELECT COUNT(*) FROM result").fetchone()[0] - n_fast
    if n_fallback:
        print(f"  Fallback covered {n_fallback} address(es) outside the bbox window")

    # ── Road network connectivity check ─────────────────────────────────────
    # For each green/yellow address, verify at least one road link exists within
    # 500m. Addresses farther than that from the bus route network are isolated —
    # the straight-line distance to the nearest stop is misleading — so they are
    # reclassified to red and flagged with road_connected = false.
    if has_road_network:
        print("Checking road network connectivity...")
        # always_xy=true must match the road_links transform so both tables
        # share the same axis convention in EPSG:2169 (X=Easting, Y=Northing).
        con.execute("""
            CREATE OR REPLACE TABLE addr_proj AS
            SELECT
                r.addr_id,
                ST_Transform(ST_Point(r.longitude, r.latitude), 'EPSG:4326', 'EPSG:2169', true) AS addr_geom,
                ST_Transform(ST_Point(s.stop_lon, s.stop_lat), 'EPSG:4326', 'EPSG:2169', true) AS stop_geom
            FROM result r
            JOIN stops s ON s.stop_id = r.stop_id
            WHERE r.color_class IN ('green', 'yellow')
        """)
        road_ext = con.execute(
            "SELECT MIN(xmin), MAX(xmax), MIN(ymin), MAX(ymax) FROM road_links"
        ).fetchone()
        addr_ext = con.execute(
            "SELECT MIN(ST_X(addr_geom)), MAX(ST_X(addr_geom)), MIN(ST_Y(addr_geom)), MAX(ST_Y(addr_geom)) FROM addr_proj"
        ).fetchone()
        stop_ext = con.execute(
            "SELECT MIN(ST_X(stop_geom)), MAX(ST_X(stop_geom)), MIN(ST_Y(stop_geom)), MAX(ST_Y(stop_geom)) FROM addr_proj"
        ).fetchone()
        print(f"  road_links EPSG:2169 X=[{road_ext[0]:.0f}, {road_ext[1]:.0f}] Y=[{road_ext[2]:.0f}, {road_ext[3]:.0f}]")
        print(f"  addr_proj  EPSG:2169 X=[{addr_ext[0]:.0f}, {addr_ext[1]:.0f}] Y=[{addr_ext[2]:.0f}, {addr_ext[3]:.0f}]")
        print(f"  stop_proj  EPSG:2169 X=[{stop_ext[0]:.0f}, {stop_ext[1]:.0f}] Y=[{stop_ext[2]:.0f}, {stop_ext[3]:.0f}]")
        # An address is road-connected only when BOTH the address (≤20m) and its
        # nearest stop (≤5m) have a road link nearby — confirming the stop is on
        # the network and the address can reach it.
        con.execute("""
            CREATE OR REPLACE TABLE road_connected_ids AS
            SELECT DISTINCT a.addr_id
            FROM addr_proj a
            JOIN road_links rl_addr ON (
                rl_addr.xmin <= ST_X(a.addr_geom) + 20
                AND rl_addr.xmax >= ST_X(a.addr_geom) - 20
                AND rl_addr.ymin <= ST_Y(a.addr_geom) + 20
                AND rl_addr.ymax >= ST_Y(a.addr_geom) - 20
                AND ST_Distance(rl_addr.geom, a.addr_geom) < 20.0
            )
            JOIN road_links rl_stop ON (
                rl_stop.xmin <= ST_X(a.stop_geom) + 5
                AND rl_stop.xmax >= ST_X(a.stop_geom) - 5
                AND rl_stop.ymin <= ST_Y(a.stop_geom) + 5
                AND rl_stop.ymax >= ST_Y(a.stop_geom) - 5
                AND ST_Distance(rl_stop.geom, a.stop_geom) < 5.0
            )
        """)
        con.execute("""
            CREATE OR REPLACE TABLE result AS
            SELECT
                r.addr_id,
                r.address,
                r.longitude,
                r.latitude,
                r.stop_id,
                r.stop_name,
                r.distance_m,
                CASE
                    WHEN r.color_class IN ('green', 'yellow') AND rc.addr_id IS NULL
                    THEN 'red'
                    ELSE r.color_class
                END AS color_class,
                -- red addresses keep road_connected=true (distance is the reason, not isolation)
                (rc.addr_id IS NOT NULL OR r.color_class = 'red') AS road_connected
            FROM result r
            LEFT JOIN road_connected_ids rc ON rc.addr_id = r.addr_id
        """)
        n_disconnected = con.execute(
            "SELECT COUNT(*) FROM result WHERE NOT road_connected"
        ).fetchone()[0]
        print(f"  {n_disconnected:,} address(es) reclassified to red (address >20m or stop >5m from road link)")
    else:
        con.execute("ALTER TABLE result ADD COLUMN road_connected BOOLEAN DEFAULT true")


    n_result = con.execute("SELECT COUNT(*) FROM result").fetchone()[0]
    dist_stats = con.execute("""
        SELECT color_class, COUNT(*) as n,
               ROUND(AVG(distance_m)) as avg_m
        FROM result GROUP BY color_class ORDER BY color_class
    """).fetchall()
    print(f"  {n_result:,} records classified:")
    for row in dist_stats:
        print(f"    {row[0]}: {row[1]:,} (avg {row[2]}m)")

    # ── Export to Parquet ───────────────────────────────────────────────────
    print(f"Exporting to {out_parquet}...")
    con.execute(f"COPY result TO '{out_parquet}' (FORMAT PARQUET)")

    # ── Load into PostgreSQL ────────────────────────────────────────────────
    print("Loading into PostgreSQL...")
    pg_conn = f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASS}"
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{pg_conn}' AS pg (TYPE POSTGRES)")
    con.execute("""
        INSERT INTO pg.residence_accessibility
            (address, longitude, latitude, stop_id, stop_name, distance_m, color_class, road_connected)
        SELECT address, longitude, latitude, stop_id, stop_name, distance_m, color_class, road_connected
        FROM result
    """)
    if has_road_network:
        print("Loading road links into PostgreSQL...")
        con.execute("""
            INSERT INTO pg.road_links (geom)
            SELECT ST_Transform(geom, 'EPSG:2169', 'EPSG:4326', true)
            FROM road_links
        """)

    print("Loading stops into PostgreSQL...")
    con.execute("""
        INSERT INTO pg.stops (stop_id, stop_name, stop_type, lon, lat)
        SELECT stop_id, stop_name, stop_type, stop_lon, stop_lat
        FROM stops
    """)

    # ── Walk distances via pgRouting ────────────────────────────────────────
    # Loads the OSM pedestrian network into PostGIS, snaps every residence and
    # stop to its nearest walk edge (perpendicular intersection), then runs
    # pgr_withPointsDD from all stops simultaneously to compute true walk
    # distances and update distance_m / color_class / stop_id.
    try:
        import psycopg2
        import walk_network as wn

        print("Computing walk distances via pgRouting...")
        pg = psycopg2.connect(
            host=PG_HOST, port=int(PG_PORT), dbname=PG_DB,
            user=PG_USER, password=PG_PASS,
        )
        walk_cache = PROCESSED / "luxembourg_walk.graphml"
        wn.load_osm_walk_network(walk_cache, pg, force=FORCE_REPROCESS)
        wn.snap_points(pg, force=FORCE_REPROCESS)
        wn.compute_walk_distances(pg)
        pg.close()
    except ImportError as exc:
        print(f"  pgRouting step skipped ({exc}) — keeping straight-line distances")

    print("Done.")


if __name__ == "__main__":
    run()
