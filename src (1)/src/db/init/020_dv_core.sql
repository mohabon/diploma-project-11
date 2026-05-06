-- =========================================================
-- Data Vault core model for Tartus spatial DWH prototype
-- Sources:
--   raw.landuse_raw
--   raw.roads_raw
--   raw.statistics_raw
--   raw.settlement_raw
-- =========================================================


-- ===============================
-- HUBS
-- ===============================

CREATE TABLE IF NOT EXISTS dv.hub_landuse (
  hk_landuse CHAR(32) PRIMARY KEY,
  bk_landuse TEXT UNIQUE,
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv.hub_road (
  hk_road CHAR(32) PRIMARY KEY,
  bk_road TEXT UNIQUE,
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv.hub_settlement (
  hk_settlement CHAR(32) PRIMARY KEY,
  bk_settlement TEXT UNIQUE,
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv.hub_statistics (
  hk_statistics CHAR(32) PRIMARY KEY,
  bk_statistics TEXT UNIQUE,
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);


-- ===============================
-- LINKS
-- ===============================

CREATE TABLE IF NOT EXISTS dv.link_landuse_settlement (
  hk_link_landuse_settlement CHAR(32) PRIMARY KEY,
  hk_landuse CHAR(32) NOT NULL REFERENCES dv.hub_landuse(hk_landuse),
  hk_settlement CHAR(32) NOT NULL REFERENCES dv.hub_settlement(hk_settlement),
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv.link_road_settlement (
  hk_link_road_settlement CHAR(32) PRIMARY KEY,
  hk_road CHAR(32) NOT NULL REFERENCES dv.hub_road(hk_road),
  hk_settlement CHAR(32) NOT NULL REFERENCES dv.hub_settlement(hk_settlement),
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);


-- ===============================
-- SATELLITES: LANDUSE
-- ===============================

CREATE TABLE IF NOT EXISTS dv.sat_landuse_attr (
  hk_landuse CHAR(32) NOT NULL REFERENCES dv.hub_landuse(hk_landuse),
  landuse_type TEXT,
  name TEXT,
  area_ha DOUBLE PRECISION,
  hashdiff CHAR(32) NOT NULL,
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv.sat_landuse_geom (
  hk_landuse CHAR(32) NOT NULL REFERENCES dv.hub_landuse(hk_landuse),
  geom geometry(MultiPolygon, 4326),
  hashdiff CHAR(32) NOT NULL,
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);


-- ===============================
-- SATELLITES: ROADS
-- ===============================

CREATE TABLE IF NOT EXISTS dv.sat_road_attr (
  hk_road CHAR(32) NOT NULL REFERENCES dv.hub_road(hk_road),
  road_type TEXT,
  name TEXT,
  length_km DOUBLE PRECISION,
  hashdiff CHAR(32) NOT NULL,
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv.sat_road_geom (
  hk_road CHAR(32) NOT NULL REFERENCES dv.hub_road(hk_road),
  geom geometry(MultiLineString, 4326),
  hashdiff CHAR(32) NOT NULL,
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);


-- ===============================
-- SATELLITES: STATISTICS / ENVIRONMENT
-- ===============================

CREATE TABLE IF NOT EXISTS dv.sat_statistics (
  hk_statistics CHAR(32) NOT NULL REFERENCES dv.hub_statistics(hk_statistics),
  district TEXT,
  year INTEGER,
  population INTEGER,
  employment_rate DOUBLE PRECISION,
  avg_income_index DOUBLE PRECISION,
  hashdiff CHAR(32) NOT NULL,
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv.sat_environment (
  hk_statistics CHAR(32) NOT NULL REFERENCES dv.hub_statistics(hk_statistics),
  district TEXT,
  year INTEGER,
  pm25 DOUBLE PRECISION,
  no2 DOUBLE PRECISION,
  water_quality DOUBLE PRECISION,
  ndvi DOUBLE PRECISION,
  hashdiff CHAR(32) NOT NULL,
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);


-- ===============================
-- DATA MART TABLES
-- ===============================

CREATE TABLE IF NOT EXISTS dm.f_landuse_summary (
  landuse_type TEXT,
  objects_count INTEGER,
  total_area_ha DOUBLE PRECISION,
  share_percent DOUBLE PRECISION,
  load_ts TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dm.f_road_summary (
  road_type TEXT,
  segments_count INTEGER,
  total_length_km DOUBLE PRECISION,
  share_percent DOUBLE PRECISION,
  load_ts TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dm.f_transport_accessibility (
  district TEXT,
  roads_nearby INTEGER,
  road_length_km DOUBLE PRECISION,
  load_ts TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dm.f_population (
  district TEXT,
  year INTEGER,
  population INTEGER,
  employment_rate DOUBLE PRECISION,
  unemployment_rate DOUBLE PRECISION,
  avg_income_index DOUBLE PRECISION,
  load_ts TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dm.f_eco_indicators (
  district TEXT,
  year INTEGER,
  pm25 DOUBLE PRECISION,
  no2 DOUBLE PRECISION,
  water_quality DOUBLE PRECISION,
  ndvi DOUBLE PRECISION,
  load_ts TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dm.f_chapter3_results (
  indicator TEXT,
  value_start DOUBLE PRECISION,
  value_end DOUBLE PRECISION,
  change_percent DOUBLE PRECISION,
  load_ts TIMESTAMP NOT NULL DEFAULT now()
);-- =========================================================
-- Data Vault core model for Tartus spatial DWH prototype
-- Sources:
--   raw.landuse_raw
--   raw.roads_raw
--   raw.statistics_raw
--   raw.settlement_raw
-- =========================================================


-- ===============================
-- HUBS
-- ===============================

CREATE TABLE IF NOT EXISTS dv.hub_landuse (
  hk_landuse CHAR(32) PRIMARY KEY,
  bk_landuse TEXT UNIQUE,
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv.hub_road (
  hk_road CHAR(32) PRIMARY KEY,
  bk_road TEXT UNIQUE,
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv.hub_settlement (
  hk_settlement CHAR(32) PRIMARY KEY,
  bk_settlement TEXT UNIQUE,
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv.hub_statistics (
  hk_statistics CHAR(32) PRIMARY KEY,
  bk_statistics TEXT UNIQUE,
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);


-- ===============================
-- LINKS
-- ===============================

CREATE TABLE IF NOT EXISTS dv.link_landuse_settlement (
  hk_link_landuse_settlement CHAR(32) PRIMARY KEY,
  hk_landuse CHAR(32) NOT NULL REFERENCES dv.hub_landuse(hk_landuse),
  hk_settlement CHAR(32) NOT NULL REFERENCES dv.hub_settlement(hk_settlement),
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv.link_road_settlement (
  hk_link_road_settlement CHAR(32) PRIMARY KEY,
  hk_road CHAR(32) NOT NULL REFERENCES dv.hub_road(hk_road),
  hk_settlement CHAR(32) NOT NULL REFERENCES dv.hub_settlement(hk_settlement),
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);


-- ===============================
-- SATELLITES: LANDUSE
-- ===============================

CREATE TABLE IF NOT EXISTS dv.sat_landuse_attr (
  hk_landuse CHAR(32) NOT NULL REFERENCES dv.hub_landuse(hk_landuse),
  landuse_type TEXT,
  name TEXT,
  area_ha DOUBLE PRECISION,
  hashdiff CHAR(32) NOT NULL,
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv.sat_landuse_geom (
  hk_landuse CHAR(32) NOT NULL REFERENCES dv.hub_landuse(hk_landuse),
  geom geometry(MultiPolygon, 4326),
  hashdiff CHAR(32) NOT NULL,
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);


-- ===============================
-- SATELLITES: ROADS
-- ===============================

CREATE TABLE IF NOT EXISTS dv.sat_road_attr (
  hk_road CHAR(32) NOT NULL REFERENCES dv.hub_road(hk_road),
  road_type TEXT,
  name TEXT,
  length_km DOUBLE PRECISION,
  hashdiff CHAR(32) NOT NULL,
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv.sat_road_geom (
  hk_road CHAR(32) NOT NULL REFERENCES dv.hub_road(hk_road),
  geom geometry(MultiLineString, 4326),
  hashdiff CHAR(32) NOT NULL,
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);


-- ===============================
-- SATELLITES: STATISTICS / ENVIRONMENT
-- ===============================

CREATE TABLE IF NOT EXISTS dv.sat_statistics (
  hk_statistics CHAR(32) NOT NULL REFERENCES dv.hub_statistics(hk_statistics),
  district TEXT,
  year INTEGER,
  population INTEGER,
  employment_rate DOUBLE PRECISION,
  avg_income_index DOUBLE PRECISION,
  hashdiff CHAR(32) NOT NULL,
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv.sat_environment (
  hk_statistics CHAR(32) NOT NULL REFERENCES dv.hub_statistics(hk_statistics),
  district TEXT,
  year INTEGER,
  pm25 DOUBLE PRECISION,
  no2 DOUBLE PRECISION,
  water_quality DOUBLE PRECISION,
  ndvi DOUBLE PRECISION,
  hashdiff CHAR(32) NOT NULL,
  load_ts TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);


-- ===============================
-- DATA MART TABLES
-- ===============================

CREATE TABLE IF NOT EXISTS dm.f_landuse_summary (
  landuse_type TEXT,
  objects_count INTEGER,
  total_area_ha DOUBLE PRECISION,
  share_percent DOUBLE PRECISION,
  load_ts TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dm.f_road_summary (
  road_type TEXT,
  segments_count INTEGER,
  total_length_km DOUBLE PRECISION,
  share_percent DOUBLE PRECISION,
  load_ts TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dm.f_transport_accessibility (
  district TEXT,
  roads_nearby INTEGER,
  road_length_km DOUBLE PRECISION,
  load_ts TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dm.f_population (
  district TEXT,
  year INTEGER,
  population INTEGER,
  employment_rate DOUBLE PRECISION,
  unemployment_rate DOUBLE PRECISION,
  avg_income_index DOUBLE PRECISION,
  load_ts TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dm.f_eco_indicators (
  district TEXT,
  year INTEGER,
  pm25 DOUBLE PRECISION,
  no2 DOUBLE PRECISION,
  water_quality DOUBLE PRECISION,
  ndvi DOUBLE PRECISION,
  load_ts TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dm.f_chapter3_results (
  indicator TEXT,
  value_start DOUBLE PRECISION,
  value_end DOUBLE PRECISION,
  change_percent DOUBLE PRECISION,
  load_ts TIMESTAMP NOT NULL DEFAULT now()
);