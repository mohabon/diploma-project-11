-- create_or_refresh_dm_parcel_usage.sql
TRUNCATE dm.f_parcel_usage;

INSERT INTO dm.f_parcel_usage (settlement_name, category, total_area_ha)
SELECT s.name, sp.category, SUM(sp.area_ha)
FROM dv.sat_parcel_thematic sp
JOIN dv.hub_parcel hp ON hp.hk_parcel = sp.hk_parcel
JOIN dv.link_parcel_settlement lps ON lps.hk_parcel = hp.hk_parcel
JOIN dv.hub_settlement hs ON hs.hk_settlement = lps.hk_settlement
JOIN (
  SELECT bk_settlement, MAX(load_ts) AS max_ts
  FROM dv.hub_settlement GROUP BY bk_settlement
) latest_s ON latest_s.bk_settlement = hs.bk_settlement
JOIN raw.settlement_raw s ON s.settlement_code = hs.bk_settlement
GROUP BY s.name, sp.category;
-- =========================
-- Population Data Mart
-- =========================

TRUNCATE dm.f_population;

INSERT INTO dm.f_population
(district, year, population, employment_rate, avg_income_index)
SELECT
    district,
    year,
    population,
    employment_rate,
    avg_income_index
FROM raw.statistics_raw;