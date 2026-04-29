# Lux-Mobility

A geospatial transit accessibility platform for Luxembourg. Classifies residential addresses by proximity to public transport stops (bus/train), verifies road network connectivity, and visualizes results on an interactive map.

## Overview

Each address is assigned a color class based on its straight-line (or walking) distance to the nearest stop:

| Class | Distance |
|-------|----------|
| Green | < 1 km |
| Yellow | 1–2 km |
| Red | > 2 km |

Road connectivity is also checked — an address is flagged if neither it nor its nearest stop lies within reach of the INSPIRE road network.

## Architecture

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
- `download.sh` — downloads GTFS, BD-Adresses DBF, CFL train shapefile, INSPIRE road network GML
- `pipeline.py` — DuckDB ETL: loads stops and addresses, computes nearest stop per address, applies distance classification, checks INSPIRE road connectivity, bulk-loads PostgreSQL via DuckDB's `postgres` extension
- `walking_distance.py` — OSMnx pedestrian routing (3 km cutoff); optional override for straight-line distances

**`/api`** — Async FastAPI backend (Python 3.12 + asyncpg)
- `GET /search` — full-text trigram address search (`pg_trgm`, similarity ≥ 0.2)
- `GET /address/{id}` — single address lookup
- `GET /geojson` — bounding-box query returning GeoJSON (zoom-aware limits: 500 / 3000 / 15000 features)
- `GET /health` — health check

**`/martin`** — Martin vector tile server
- `config.yaml` — exposes `residence_accessibility` (address points) and `road_links` (bus network linestrings) as MVT endpoints; reads live from PostgreSQL

**`/frontend`** — Nginx-served vanilla JavaScript SPA
- `app.js` — OpenLayers 9 map in EPSG:2169; two MVT layers; 300 ms debounced search; click-to-inspect panel
- `docker-entrypoint.sh` — injects `API_URL` and `TILE_URL` from environment into `/env.js` at startup

**`/db`**
- `init.sql` — runs on first postgres boot; creates PostGIS/pg_trgm extensions, tables, GIST spatial index, GIN trigram index, and a trigger to auto-populate `geom` from `lon`/`lat`

## Getting Started

### Prerequisites

- Docker and Docker Compose

### Quick start

```bash
# Copy and configure environment variables
cp .env.example .env

# Start all services
docker-compose up
```

Open `http://localhost` in your browser.

### Environment variables (`.env`)

| Variable | Description |
|----------|-------------|
| `PG_USER` | PostgreSQL username |
| `PG_PASS` | PostgreSQL password |
| `PG_DB` | PostgreSQL database name |
| `PG_HOST` | PostgreSQL host |
| `PG_PORT` | PostgreSQL port |
| `FORCE_REPROCESS` | Set `true` to wipe intermediate ETL files and re-run the pipeline |
| `TILE_URL` | Public URL of the Martin tile server (injected into frontend) |
| `API_URL` | Public URL of the FastAPI backend (injected into frontend) |

### Service startup order

Health checks enforce this startup sequence:

1. `postgres` (PostGIS 16) — schema initialized via `db/init.sql`
2. `etl` — runs once and exits; skips if DB already populated
3. `martin` (port 3000) + `api` (port 8000)
4. `frontend` (port 80)

### Common commands

```bash
# Force re-run the ETL pipeline
FORCE_REPROCESS=true docker-compose up etl

# Rebuild images after code changes
docker-compose up --build

# Tear down (preserves postgres volume)
docker-compose down

# Tear down including database data
docker-compose down -v
```

## Development

### Running tests

```bash
pytest
```

Tests live in `tests/` and cover API endpoints and ETL pipeline functionality.

### Key implementation notes

- **Coordinate systems** — INSPIRE GML is in EPSG:3035 (Northing-first) and is reprojected to EPSG:2169 (Luxembourg local); all final output is EPSG:4326
- **Nearest-stop join** — bbox pre-filter with fallback full-scan for edge cases
- **Road connectivity** — address must be within 20 m and stop within 5 m of an INSPIRE road LINESTRING
- **OSMnx fallback** — if routing fails or exceeds 3 km, falls back to straight-line distance; `osmnx` import is optional (graceful degradation)
- **ETL idempotency** — pipeline skips if `residence_accessibility` already contains data; override with `FORCE_REPROCESS=true`

## Data Sources

- [GTFS Luxembourg](https://data.public.lu) — public transport feed
- [BD-Adresses](https://data.public.lu) — residential address database
- [CFL train stops](https://data.public.lu) — national rail stop shapefile
- [INSPIRE road network](https://data.public.lu) — national road network GML
