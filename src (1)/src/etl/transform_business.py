import os
import psycopg2

DSN = os.getenv("DSN", "postgresql://dwh_admin:changeme@localhost:5432/dwh")


def main():
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()

    # ===============================
    # Clean recalculated DV layer
    # ===============================
    cur.execute("""
        TRUNCATE
            dv.link_landuse_settlement,
            dv.link_road_settlement,
            dv.sat_landuse_attr,
            dv.sat_landuse_geom,
            dv.sat_road_attr,
            dv.sat_road_geom,
            dv.sat_statistics,
            dv.sat_environment,
            dv.hub_landuse,
            dv.hub_road,
            dv.hub_statistics
        CASCADE;
    """)

    # ===============================
    # HUBS
    # ===============================
    cur.execute("""
        INSERT INTO dv.hub_settlement (hk_settlement, bk_settlement, load_ts, record_source)
        SELECT md5(settlement_code), settlement_code, now(), 'raw.settlement_raw'
        FROM raw.settlement_raw
        ON CONFLICT (hk_settlement) DO NOTHING;
    """)

    cur.execute("""
        INSERT INTO dv.hub_landuse (hk_landuse, bk_landuse, load_ts, record_source)
        SELECT md5(landuse_id::text), landuse_id::text, now(), 'raw.landuse_raw'
        FROM raw.landuse_raw
        ON CONFLICT (hk_landuse) DO NOTHING;
    """)

    cur.execute("""
        INSERT INTO dv.hub_road (hk_road, bk_road, load_ts, record_source)
        SELECT md5(road_id::text), road_id::text, now(), 'raw.roads_raw'
        FROM raw.roads_raw
        ON CONFLICT (hk_road) DO NOTHING;
    """)

    cur.execute("""
        INSERT INTO dv.hub_statistics (hk_statistics, bk_statistics, load_ts, record_source)
        SELECT md5(district || '|' || year::text), district || '|' || year::text, now(), 'raw.statistics_raw'
        FROM raw.statistics_raw
        ON CONFLICT (hk_statistics) DO NOTHING;
    """)

    # ===============================
    # SATELLITES: landuse
    # ===============================
    cur.execute("""
        INSERT INTO dv.sat_landuse_attr
        (hk_landuse, landuse_type, name, area_ha, hashdiff, load_ts, record_source)
        SELECT
            md5(landuse_id::text),
            landuse_type,
            name,
            area_ha,
            md5(coalesce(landuse_type,'') || '|' || coalesce(name,'') || '|' || coalesce(area_ha::text,'')),
            now(),
            'raw.landuse_raw'
        FROM raw.landuse_raw;
    """)

    cur.execute("""
        INSERT INTO dv.sat_landuse_geom
        (hk_landuse, geom, hashdiff, load_ts, record_source)
        SELECT
            md5(landuse_id::text),
            geom,
            md5(ST_AsText(geom)),
            now(),
            'raw.landuse_raw'
        FROM raw.landuse_raw;
    """)

    # ===============================
    # SATELLITES: roads
    # ===============================
    cur.execute("""
        INSERT INTO dv.sat_road_attr
        (hk_road, road_type, name, length_km, hashdiff, load_ts, record_source)
        SELECT
            md5(road_id::text),
            road_type,
            name,
            length_km,
            md5(coalesce(road_type,'') || '|' || coalesce(name,'') || '|' || coalesce(length_km::text,'')),
            now(),
            'raw.roads_raw'
        FROM raw.roads_raw;
    """)

    cur.execute("""
        INSERT INTO dv.sat_road_geom
        (hk_road, geom, hashdiff, load_ts, record_source)
        SELECT
            md5(road_id::text),
            geom,
            md5(ST_AsText(geom)),
            now(),
            'raw.roads_raw'
        FROM raw.roads_raw;
    """)

    # ===============================
    # SATELLITES: statistics / environment
    # ===============================
    cur.execute("""
        INSERT INTO dv.sat_statistics
        (hk_statistics, district, year, population, employment_rate, avg_income_index,
         hashdiff, load_ts, record_source)
        SELECT
            md5(district || '|' || year::text),
            district,
            year,
            population,
            employment_rate,
            avg_income_index,
            md5(
                coalesce(district,'') || '|' ||
                coalesce(year::text,'') || '|' ||
                coalesce(population::text,'') || '|' ||
                coalesce(employment_rate::text,'') || '|' ||
                coalesce(avg_income_index::text,'')
            ),
            now(),
            'raw.statistics_raw'
        FROM raw.statistics_raw;
    """)

    cur.execute("""
        INSERT INTO dv.sat_environment
        (hk_statistics, district, year, pm25, no2, water_quality, ndvi,
         hashdiff, load_ts, record_source)
        SELECT
            md5(district || '|' || year::text),
            district,
            year,
            pm25,
            no2,
            water_quality,
            ndvi,
            md5(
                coalesce(district,'') || '|' ||
                coalesce(year::text,'') || '|' ||
                coalesce(pm25::text,'') || '|' ||
                coalesce(no2::text,'') || '|' ||
                coalesce(water_quality::text,'') || '|' ||
                coalesce(ndvi::text,'')
            ),
            now(),
            'raw.statistics_raw'
        FROM raw.statistics_raw;
    """)

    # ===============================
    # LINKS: spatial relations
    # ===============================
    cur.execute("""
        INSERT INTO dv.link_landuse_settlement
        (hk_link_landuse_settlement, hk_landuse, hk_settlement, load_ts, record_source)
        SELECT
            md5(md5(l.landuse_id::text) || md5(s.settlement_code)),
            md5(l.landuse_id::text),
            md5(s.settlement_code),
            now(),
            'spatial_intersection'
        FROM raw.landuse_raw l
        JOIN raw.settlement_raw s
          ON ST_Intersects(l.geom, s.geom)
        ON CONFLICT (hk_link_landuse_settlement) DO NOTHING;
    """)

    cur.execute("""
        INSERT INTO dv.link_road_settlement
        (hk_link_road_settlement, hk_road, hk_settlement, load_ts, record_source)
        SELECT
            md5(md5(r.road_id::text) || md5(s.settlement_code)),
            md5(r.road_id::text),
            md5(s.settlement_code),
            now(),
            'spatial_intersection'
        FROM raw.roads_raw r
        JOIN raw.settlement_raw s
          ON ST_Intersects(r.geom, s.geom)
        ON CONFLICT (hk_link_road_settlement) DO NOTHING;
    """)

    # ===============================
    # DATA MARTS from DV
    # ===============================
    cur.execute("""
        TRUNCATE
            dm.f_landuse_summary,
            dm.f_road_summary,
            dm.f_transport_accessibility,
            dm.f_population,
            dm.f_eco_indicators,
            dm.f_chapter3_results;
    """)

    cur.execute("""
        INSERT INTO dm.f_landuse_summary
        (landuse_type, objects_count, total_area_ha, share_percent)
        WITH landuse_current AS (
            SELECT
                a.hk_landuse,
                a.landuse_type,
                a.area_ha
            FROM dv.sat_landuse_attr a
        ),
        total AS (
            SELECT SUM(area_ha) AS total_area
            FROM landuse_current
        )
        SELECT
            landuse_type,
            COUNT(*)::integer,
            ROUND(SUM(area_ha)::numeric, 2)::double precision,
            ROUND((SUM(area_ha) / NULLIF((SELECT total_area FROM total), 0) * 100)::numeric, 2)::double precision
        FROM landuse_current
        GROUP BY landuse_type
        ORDER BY SUM(area_ha) DESC;
    """)

    cur.execute("""
        INSERT INTO dm.f_road_summary
        (road_type, segments_count, total_length_km, share_percent)
        WITH road_current AS (
            SELECT
                a.hk_road,
                a.road_type,
                a.length_km
            FROM dv.sat_road_attr a
        ),
        total AS (
            SELECT SUM(length_km) AS total_length
            FROM road_current
        )
        SELECT
            road_type,
            COUNT(*)::integer,
            ROUND(SUM(length_km)::numeric, 2)::double precision,
            ROUND((SUM(length_km) / NULLIF((SELECT total_length FROM total), 0) * 100)::numeric, 2)::double precision
        FROM road_current
        GROUP BY road_type
        ORDER BY SUM(length_km) DESC;
    """)

    cur.execute("""
        INSERT INTO dm.f_transport_accessibility
        (district, roads_nearby, road_length_km)
        SELECT
            hs.bk_settlement AS district,
            COUNT(DISTINCT lr.hk_road)::integer AS roads_nearby,
            ROUND(COALESCE(SUM(ra.length_km), 0)::numeric, 2)::double precision AS road_length_km
        FROM dv.hub_settlement hs
        LEFT JOIN dv.link_road_settlement lr
          ON hs.hk_settlement = lr.hk_settlement
        LEFT JOIN dv.sat_road_attr ra
          ON lr.hk_road = ra.hk_road
        GROUP BY hs.bk_settlement
        ORDER BY road_length_km DESC;
    """)

    cur.execute("""
        INSERT INTO dm.f_population
        (district, year, population, employment_rate, unemployment_rate, avg_income_index)
        SELECT
            district,
            year,
            population,
            employment_rate,
            ROUND((100 - employment_rate)::numeric, 2)::double precision,
            avg_income_index
        FROM dv.sat_statistics
        ORDER BY district, year;
    """)

    cur.execute("""
        INSERT INTO dm.f_eco_indicators
        (district, year, pm25, no2, water_quality, ndvi)
        SELECT
            district,
            year,
            pm25,
            no2,
            water_quality,
            ndvi
        FROM dv.sat_environment
        ORDER BY district, year;
    """)

    cur.execute("""
        INSERT INTO dm.f_chapter3_results
        (indicator, value_start, value_end, change_percent)
        WITH agg AS (
            SELECT
                st.year,
                SUM(st.population)::numeric AS population,
                AVG(st.employment_rate)::numeric AS employment_rate,
                AVG(st.avg_income_index)::numeric AS avg_income_index,
                AVG(en.pm25)::numeric AS pm25,
                AVG(en.no2)::numeric AS no2,
                AVG(en.water_quality)::numeric AS water_quality,
                AVG(en.ndvi)::numeric AS ndvi
            FROM dv.sat_statistics st
            JOIN dv.sat_environment en
              ON st.hk_statistics = en.hk_statistics
             AND st.district = en.district
             AND st.year = en.year
            GROUP BY st.year
        ),
        y2013 AS (SELECT * FROM agg WHERE year = 2013),
        y2018 AS (SELECT * FROM agg WHERE year = 2018),
        y2023 AS (SELECT * FROM agg WHERE year = 2023),
        land AS (
            SELECT
                SUM(area_ha)::numeric AS total_landuse_area_ha,
                SUM(CASE WHEN landuse_type = 'residential' THEN area_ha ELSE 0 END)::numeric AS residential_area_ha,
                SUM(CASE WHEN landuse_type = 'farmland' THEN area_ha ELSE 0 END)::numeric AS farmland_area_ha,
                SUM(CASE WHEN landuse_type = 'forest' THEN area_ha ELSE 0 END)::numeric AS forest_area_ha,
                SUM(CASE WHEN landuse_type = 'industrial' THEN area_ha ELSE 0 END)::numeric AS industrial_area_ha
            FROM dv.sat_landuse_attr
        ),
        roads AS (
            SELECT
                COUNT(*)::numeric AS road_segments,
                SUM(length_km)::numeric AS total_road_length_km
            FROM dv.sat_road_attr
        )
        SELECT 'Population growth 2013-2023',
               ROUND(y2013.population, 2)::double precision,
               ROUND(y2023.population, 2)::double precision,
               ROUND(((y2023.population - y2013.population) / NULLIF(y2013.population, 0) * 100), 2)::double precision
        FROM y2013, y2023

        UNION ALL
        SELECT 'Employment rate change 2013-2023',
               ROUND(y2013.employment_rate, 2)::double precision,
               ROUND(y2023.employment_rate, 2)::double precision,
               ROUND((y2023.employment_rate - y2013.employment_rate), 2)::double precision
        FROM y2013, y2023

        UNION ALL
        SELECT 'Average income index growth 2013-2023',
               ROUND(y2013.avg_income_index, 2)::double precision,
               ROUND(y2023.avg_income_index, 2)::double precision,
               ROUND(((y2023.avg_income_index - y2013.avg_income_index) / NULLIF(y2013.avg_income_index, 0) * 100), 2)::double precision
        FROM y2013, y2023

        UNION ALL
        SELECT 'PM2.5 growth 2018-2023',
               ROUND(y2018.pm25, 2)::double precision,
               ROUND(y2023.pm25, 2)::double precision,
               ROUND(((y2023.pm25 - y2018.pm25) / NULLIF(y2018.pm25, 0) * 100), 2)::double precision
        FROM y2018, y2023

        UNION ALL
        SELECT 'NO2 growth 2018-2023',
               ROUND(y2018.no2, 2)::double precision,
               ROUND(y2023.no2, 2)::double precision,
               ROUND(((y2023.no2 - y2018.no2) / NULLIF(y2018.no2, 0) * 100), 2)::double precision
        FROM y2018, y2023

        UNION ALL
        SELECT 'Water quality improvement 2018-2023',
               ROUND(y2018.water_quality, 2)::double precision,
               ROUND(y2023.water_quality, 2)::double precision,
               ROUND(((y2023.water_quality - y2018.water_quality) / NULLIF(y2018.water_quality, 0) * 100), 2)::double precision
        FROM y2018, y2023

        UNION ALL
        SELECT 'NDVI change 2018-2023',
               ROUND(y2018.ndvi, 2)::double precision,
               ROUND(y2023.ndvi, 2)::double precision,
               ROUND(((y2023.ndvi - y2018.ndvi) / NULLIF(y2018.ndvi, 0) * 100), 2)::double precision
        FROM y2018, y2023

        UNION ALL
        SELECT 'Residential land share',
               ROUND(total_landuse_area_ha, 2)::double precision,
               ROUND(residential_area_ha, 2)::double precision,
               ROUND((residential_area_ha / NULLIF(total_landuse_area_ha, 0) * 100), 2)::double precision
        FROM land

        UNION ALL
        SELECT 'Farmland share',
               ROUND(total_landuse_area_ha, 2)::double precision,
               ROUND(farmland_area_ha, 2)::double precision,
               ROUND((farmland_area_ha / NULLIF(total_landuse_area_ha, 0) * 100), 2)::double precision
        FROM land

        UNION ALL
        SELECT 'Forest share',
               ROUND(total_landuse_area_ha, 2)::double precision,
               ROUND(forest_area_ha, 2)::double precision,
               ROUND((forest_area_ha / NULLIF(total_landuse_area_ha, 0) * 100), 2)::double precision
        FROM land

        UNION ALL
        SELECT 'Industrial land share',
               ROUND(total_landuse_area_ha, 2)::double precision,
               ROUND(industrial_area_ha, 2)::double precision,
               ROUND((industrial_area_ha / NULLIF(total_landuse_area_ha, 0) * 100), 2)::double precision
        FROM land

        UNION ALL
        SELECT 'Total road length, km',
               road_segments::double precision,
               ROUND(total_road_length_km, 2)::double precision,
               NULL::double precision
        FROM roads;
    """)

    conn.commit()
    cur.close()
    conn.close()

    print("Transformed RAW to Data Vault and refreshed Data Marts.")


if __name__ == "__main__":
    main()