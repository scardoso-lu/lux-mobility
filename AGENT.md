# AGENT.md

This file provides guidance for AI agents working with the Lux-Mobility codebase.

## Project Overview

Lux-Mobility is a geospatial transit accessibility platform for Luxembourg. It classifies residential addresses by their proximity to public transport stops (bus/train), checks road network connectivity, and visualizes the results on an interactive map.

## Architecture

### Data Flow

```
Luxembourg Public Data Portals (GTFS, BD-Adresses, CFL train stops, INSPIRE roads GML)
  ↓ etl/download.sh (curl + unzip)
etl/data/raw/ (cached downloads)
  ↓ etl/pipeline.py (DuckDB)
PostgreSQL + PostGIS (residence_accessibility, road_links tables)
  ├─→ Martin (port 3000): serves MVT vector tiles directly from PostgreSQL
  └─→ FastAPI (port 8000): REST API for address search and GeoJSON queries
        ↓
Frontend (port 80): OpenLayers map consuming Martin tiles + FastAPI endpoints
```

### Components

**`/etl`** — Python data pipeline (runs once as an init container)
- `download.sh`: Downloads GTFS, BD-Adresses DBF, CFL train shapefile, INSPIRE road network GML
- `pipeline.py`: Core DuckDB ETL — loads stops and addresses, computes nearest stop per address, applies distance classification (green <1km / yellow 1-2km / red >2km), checks INSPIRE road connectivity, optionally computes actual walking distances via OSMnx, bulk-loads PostgreSQL using DuckDB's `postgres` extension
- `walking_distance.py`: OSMnx pedestrian routing (3km cutoff); used as an optional override for straight-line distances

**`/api`** — Async FastAPI backend (Python 3.12 + asyncpg)
- `/search`: Full-text trigram search on addresses (`pg_trgm`, similarity threshold 0.2)
- `/address/{id}`: Single address lookup
- `/geojson`: Bounding-box query returning GeoJSON (zoom-aware limits: 500/3000/15000)
- `/health`: Health check endpoint
- Uses a global asyncpg connection pool (2–10 connections) initialized via FastAPI lifespan

**`/martin`** — Martin vector tile server
- `config.yaml`: Exposes two PostgreSQL tables as MVT endpoints — `residence_accessibility` (points with address, distance_m, color_class, road_connected) and `road_links` (bus network linestrings)
- Reads directly from PostgreSQL; DB changes appear in tiles immediately

**`/frontend`** — Nginx-served vanilla JavaScript SPA
- `app.js`: OpenLayers 9 map initialized at Luxembourg Geoportail EPSG:2169 coordinates; two MVT layers; 300ms debounced address search; click-to-inspect info panel
- `docker-entrypoint.sh`: Injects `API_URL` and `TILE_URL` from environment into `/env.js` at nginx startup — changing these requires a container restart

**`/db`**
- `init.sql`: Runs automatically on first postgres boot — creates PostGIS/pg_trgm extensions, tables, GIST spatial index, GIN trigram index, and a trigger that auto-populates the `geom` column from `lon`/`lat`

## Key Implementation Details

- **Coordinate systems**: INSPIRE GML uses EPSG:3035 (Northing-first axis order) and is reprojected to EPSG:2169 (Luxembourg local); train shapefiles auto-detect CRS from `.prj`; all final output is EPSG:4326
- **Nearest-stop join**: Fast bbox pre-filter first, with fallback full-scan for edge cases
- **Road connectivity**: Verified by checking that both address and stop are within 20m/5m respectively of an INSPIRE road LINESTRING
- **OSMnx fallback**: If OSMnx routing fails or exceeds 3km, falls back to straight-line distance; osmnx import is optional (graceful degradation)
- **ETL idempotency**: Skips processing if `residence_accessibility` table already has data; `FORCE_REPROCESS=true` bypasses this check

## Running the Project

This is a fully Docker-based project. All services are orchestrated via docker-compose.

```bash
# First-time setup: copy environment file and configure
cp .env.example .env

# Start all services (postgres → etl → martin/api → frontend)
docker-compose up

# Force re-run the ETL pipeline (wipes intermediate files)
FORCE_REPROCESS=true docker-compose up etl

# Rebuild images after code changes
docker-compose up --build

# Tear down (preserves postgres volume)
docker-compose down

# Tear down including database data
docker-compose down -v
```

**Service startup order** is enforced by health checks:
1. `postgres` (PostGIS 16) — initializes schema via `db/init.sql`
2. `etl` — runs once, then exits; skips if DB already populated (unless `FORCE_REPROCESS=true`)
3. `martin` (port 3000) + `api` (port 8000) — both depend on postgres + etl
4. `frontend` (port 80) — depends on api

**Environment variables** (`.env`):
- `PG_USER`, `PG_PASS`, `PG_DB`, `PG_HOST`, `PG_PORT` — PostgreSQL credentials
- `FORCE_REPROCESS` — set `true` to wipe intermediate ETL files and re-process
- `TILE_URL` — public URL of martin tile server (injected into frontend at nginx startup)
- `API_URL` — public URL of FastAPI backend (injected into frontend at nginx startup)

## Development Guidelines

- **ETL**: Use DuckDB for efficient in-memory processing and PostgreSQL bulk loading
- **API**: Use asyncpg for PostgreSQL connections with connection pooling
- **Frontend**: Vanilla JavaScript with OpenLayers for mapping
- **Testing**: Use pytest for API and ETL tests

## Testing

Run tests with:

```bash
pytest
```

Tests are located in the `tests/` directory and cover API endpoints and ETL pipeline functionality.
