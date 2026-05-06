-- Refresh analytical marts derived from the Data Vault layer.

TRUNCATE dm.f_parcel_usage;

INSERT INTO dm.f_parcel_usage
    (year, category, objects_count, total_area_ha, share_percent)
WITH parcel_current AS (
    SELECT
        year,
        category,
        area_ha
    FROM dv.sat_parcel_thematic
),
total AS (
    SELECT
        year,
        SUM(area_ha) AS total_area_ha
    FROM parcel_current
    GROUP BY year
)
SELECT
    p.year,
    p.category,
    COUNT(*)::integer AS objects_count,
    ROUND(SUM(p.area_ha)::numeric, 2)::double precision AS total_area_ha,
    ROUND((SUM(p.area_ha) / NULLIF(t.total_area_ha, 0) * 100)::numeric, 2)::double precision AS share_percent
FROM parcel_current p
JOIN total t ON t.year = p.year
GROUP BY p.year, p.category, t.total_area_ha
ORDER BY p.year, total_area_ha DESC;

TRUNCATE dm.f_population;

INSERT INTO dm.f_population
    (district, year, population, employment_rate, unemployment_rate, avg_income_index)
SELECT
    district,
    year,
    population,
    employment_rate,
    ROUND((100 - employment_rate)::numeric, 2)::double precision AS unemployment_rate,
    avg_income_index
FROM dv.sat_statistics
ORDER BY district, year;
