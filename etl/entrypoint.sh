#!/usr/bin/env bash
set -euo pipefail

if [ "${FORCE_REPROCESS:-false}" = "true" ]; then
    echo "==> FORCE_REPROCESS: removing processed files..."
    rm -f /app/data/processed/pipeline.duckdb \
          /app/data/processed/addresses_normalized.csv \
          /app/data/processed/residence_accessibility.parquet \
          /app/data/processed/road_links.csv \
          /app/data/processed/luxembourg_walk.graphml
fi

_train_shp=$(find /app/data/raw/train_stops -name '*.shp' 2>/dev/null | head -1)

if [ ! -f /app/data/raw/stops.txt ]      || \
   [ ! -f /app/data/raw/addresses.dbf ]  || \
   [ -z "$_train_shp" ]                  || \
   [ ! -f /app/data/raw/inspire_roads.gml ]; then
    echo "==> Downloading source data..."
    bash /app/download.sh
else
    echo "==> Source files already present, skipping download"
fi

python /app/pipeline.py
