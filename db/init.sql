-- Run once as superuser to bootstrap the database
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS pgrouting;

CREATE TABLE IF NOT EXISTS residence_accessibility (
    id           BIGSERIAL PRIMARY KEY,
    address      TEXT          NOT NULL,
    longitude    DOUBLE PRECISION NOT NULL,
    latitude     DOUBLE PRECISION NOT NULL,
    geom         GEOMETRY(Point, 4326),
    stop_id      TEXT,
    stop_name    TEXT,
    distance_m   DOUBLE PRECISION,
    color_class     TEXT          NOT NULL CHECK (color_class IN ('green', 'yellow', 'red')),
    road_connected  BOOLEAN       NOT NULL DEFAULT true
);

-- Populate geom from lon/lat after bulk load
CREATE OR REPLACE FUNCTION sync_geom() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.geom := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_sync_geom ON residence_accessibility;
CREATE TRIGGER trg_sync_geom
    BEFORE INSERT OR UPDATE OF longitude, latitude
    ON residence_accessibility
    FOR EACH ROW EXECUTE FUNCTION sync_geom();

CREATE TABLE IF NOT EXISTS road_links (
    id   BIGSERIAL PRIMARY KEY,
    geom GEOMETRY(LineString, 4326)
);
CREATE INDEX IF NOT EXISTS idx_road_links_geom ON road_links USING GIST (geom);

-- Unified stops table (bus + train), used by walk network snapping
CREATE TABLE IF NOT EXISTS stops (
    stop_id   TEXT NOT NULL,
    stop_name TEXT,
    stop_type TEXT NOT NULL,  -- 'bus' or 'train'
    lon       DOUBLE PRECISION NOT NULL,
    lat       DOUBLE PRECISION NOT NULL,
    geom      GEOMETRY(Point, 4326),
    PRIMARY KEY (stop_id, stop_type)
);
CREATE INDEX IF NOT EXISTS idx_stops_geom ON stops USING GIST (geom);

CREATE OR REPLACE FUNCTION sync_stop_geom() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.geom := ST_SetSRID(ST_MakePoint(NEW.lon, NEW.lat), 4326);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_sync_stop_geom ON stops;
CREATE TRIGGER trg_sync_stop_geom
    BEFORE INSERT OR UPDATE OF lon, lat
    ON stops
    FOR EACH ROW EXECUTE FUNCTION sync_stop_geom();

-- OSM pedestrian walk network (loaded by ETL, used by pgRouting)
CREATE TABLE IF NOT EXISTS walk_nodes (
    id   BIGINT PRIMARY KEY,           -- OSM node ID
    geom GEOMETRY(Point, 4326)
);
CREATE INDEX IF NOT EXISTS idx_walk_nodes_geom ON walk_nodes USING GIST (geom);

CREATE TABLE IF NOT EXISTS walk_edges (
    id           BIGSERIAL PRIMARY KEY,
    source       BIGINT NOT NULL,      -- OSM node ID (matches walk_nodes.id)
    target       BIGINT NOT NULL,
    cost         FLOAT  NOT NULL,      -- metres, forward
    reverse_cost FLOAT  NOT NULL,      -- metres, backward (same: undirected network)
    geom         GEOMETRY(LineString, 4326)
);
CREATE INDEX IF NOT EXISTS idx_walk_edges_geom   ON walk_edges USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_walk_edges_source ON walk_edges (source);
CREATE INDEX IF NOT EXISTS idx_walk_edges_target ON walk_edges (target);

-- Pre-computed snap points: perpendicular projection of each stop onto nearest walk edge.
-- PIDs are assigned globally: stops first (1…N_stops), then residences (N_stops+1…).
CREATE TABLE IF NOT EXISTS stop_snap (
    pid          BIGINT PRIMARY KEY,
    stop_id      TEXT   NOT NULL,
    stop_type    TEXT   NOT NULL,
    edge_id      BIGINT NOT NULL,
    fraction     FLOAT  NOT NULL,      -- 0–1 along edge
    snap_geom    GEOMETRY(Point, 4326),
    snap_dist_m  FLOAT  NOT NULL,      -- straight-line from stop to snap point
    UNIQUE (stop_id, stop_type)
);

-- Pre-computed snap points for residences.
CREATE TABLE IF NOT EXISTS residence_snap (
    pid          BIGINT PRIMARY KEY,
    residence_id BIGINT UNIQUE NOT NULL,
    edge_id      BIGINT NOT NULL,
    fraction     FLOAT  NOT NULL,
    snap_geom    GEOMETRY(Point, 4326),
    snap_dist_m  FLOAT  NOT NULL
);

-- Indexes on residence_accessibility
CREATE INDEX IF NOT EXISTS idx_residence_geom
    ON residence_accessibility USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_residence_address_trgm
    ON residence_accessibility USING GIN (address gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_residence_color
    ON residence_accessibility (color_class);
