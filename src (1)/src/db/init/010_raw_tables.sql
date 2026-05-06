CREATE TABLE IF NOT EXISTS raw.parcel_raw (
    parcel_id TEXT PRIMARY KEY,
    category TEXT,
    area_ha DOUBLE PRECISION,
    geom geometry(Polygon, 4326),
    load_ts TIMESTAMP NOT NULL DEFAULT now(),
    record_source TEXT DEFAULT 'sample_data'
);

CREATE TABLE IF NOT EXISTS raw.settlement_raw (
    settlement_code TEXT PRIMARY KEY,
    name TEXT,
    geom geometry(MultiPolygon, 4326),
    load_ts TIMESTAMP NOT NULL DEFAULT now(),
    record_source TEXT DEFAULT 'sample_data'
);

CREATE TABLE IF NOT EXISTS raw.statistics_raw (
    district TEXT,
    year INTEGER,
    population INTEGER,
    employment_rate DOUBLE PRECISION,
    avg_income_index DOUBLE PRECISION,
    pm25 DOUBLE PRECISION,
    no2 DOUBLE PRECISION,
    water_quality DOUBLE PRECISION,
    ndvi DOUBLE PRECISION,
    load_ts TIMESTAMP NOT NULL DEFAULT now(),
    record_source TEXT DEFAULT 'sample_statistics'
);

CREATE TABLE IF NOT EXISTS raw.landuse_raw (
    landuse_id SERIAL PRIMARY KEY,
    osm_id TEXT,
    landuse_type TEXT,
    name TEXT,
    area_ha DOUBLE PRECISION,
    geom geometry(MultiPolygon, 4326),
    load_ts TIMESTAMP NOT NULL DEFAULT now(),
    record_source TEXT DEFAULT 'osm_landuse'
);

CREATE TABLE IF NOT EXISTS raw.roads_raw (
    road_id SERIAL PRIMARY KEY,
    osm_id TEXT,
    road_type TEXT,
    name TEXT,
    length_km DOUBLE PRECISION,
    geom geometry(MultiLineString, 4326),
    load_ts TIMESTAMP NOT NULL DEFAULT now(),
    record_source TEXT DEFAULT 'osm_roads'
);