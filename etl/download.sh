#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="$(dirname "$0")/data/raw"
mkdir -p "$DATA_DIR"

GTFS_URL="https://download.data.public.lu/resources/horaires-et-arrets-des-transport-publics-gtfs/20260423-044436/gtfs-20260422-20260508.zip"
ADDR_URL="https://data.public.lu/fr/datasets/r/2a17ddc2-b961-45ce-83a2-be751d3af9b8"
TRAIN_URL="https://data.public.lu/fr/datasets/r/76b84692-7d64-485b-82b2-c1a41874cb70"
ADDR_TMP="$DATA_DIR/addresses_raw.zip"

echo "==> Downloading GTFS..."
curl -fL "$GTFS_URL" -o "$DATA_DIR/gtfs.zip"
unzip -o "$DATA_DIR/gtfs.zip" stops.txt -d "$DATA_DIR/"
echo "    stops.txt extracted ($(wc -l < "$DATA_DIR/stops.txt") lines)"

echo "==> Downloading BD-Adresses..."
curl -fL "$ADDR_URL" -o "$ADDR_TMP"

# Locate the .dbf inside the zip (name varies between releases)
DBF_ENTRY=$(unzip -Z1 "$ADDR_TMP" | grep -i '\.dbf$' | head -1)
if [ -z "$DBF_ENTRY" ]; then
    echo "ERROR: no .dbf found in the downloaded archive" >&2
    unzip -Z1 "$ADDR_TMP" >&2
    exit 1
fi
unzip -o "$ADDR_TMP" "$DBF_ENTRY" -d "$DATA_DIR/addr_tmp/"
# Move to a stable name regardless of internal directory structure
find "$DATA_DIR/addr_tmp" -iname "*.dbf" | head -1 | xargs -I{} mv {} "$DATA_DIR/addresses.dbf"
rm -rf "$DATA_DIR/addr_tmp" "$ADDR_TMP"
echo "    addresses.dbf extracted from: $DBF_ENTRY"

echo "==> Downloading train stops shapefile..."
curl -fL "$TRAIN_URL" -o "$DATA_DIR/train_raw.zip"
mkdir -p "$DATA_DIR/train_stops"
unzip -o "$DATA_DIR/train_raw.zip" -d "$DATA_DIR/train_tmp/"
# Copy all shapefile components (.shp .shx .dbf .prj .cpg) to a stable directory
find "$DATA_DIR/train_tmp" -type f \( -iname "*.shp" -o -iname "*.shx" \
    -o -iname "*.dbf" -o -iname "*.prj" -o -iname "*.cpg" \) \
    -exec cp {} "$DATA_DIR/train_stops/" \;
rm -rf "$DATA_DIR/train_tmp" "$DATA_DIR/train_raw.zip"
SHP=$(find "$DATA_DIR/train_stops" -iname "*.shp" | head -1)
echo "    shapefile: $SHP"

echo "==> Downloading INSPIRE Transport Network (bus road links)..."
INSPIRE_URL="https://data.public.lu/fr/datasets/r/20517264-ca23-436a-9ac6-7a8bd4319e26"
curl -fL "$INSPIRE_URL" -o "$DATA_DIR/inspire_roads.gml"
echo "    inspire_roads.gml downloaded ($(du -sh "$DATA_DIR/inspire_roads.gml" | cut -f1))"

echo "Done. Files in $DATA_DIR"
