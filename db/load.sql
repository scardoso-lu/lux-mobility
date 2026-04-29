-- Run after bulk INSERT to ensure geom is populated and re-cluster indexes.
-- Typically called once after ETL pipeline completes.

-- If you bypassed the trigger (e.g. COPY or direct DuckDB insert without trigger),
-- back-fill geom for any rows that are missing it:
UPDATE residence_accessibility
SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
WHERE geom IS NULL;

-- Refresh index statistics
ANALYZE residence_accessibility;
