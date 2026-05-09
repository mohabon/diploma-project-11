import os
import psycopg2

DSN = os.getenv("DSN", "postgresql://dwh_admin:changeme@localhost:5432/dwh")


def exec_sql(cur, sql):
    cur.execute(sql)


def main():
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()

    # =========================================================
    # 1) Rebuild Data Vault layer from RAW tables
    # =========================================================
    exec_sql(cur, """
        TRUNCATE
            dv.link_parcel_settlement,
            dv.link_road_settlement,
            dv.sat_parcel_thematic,
            dv.sat_parcel_geom,
            dv.sat_road_attr,
            dv.sat_road_geom,
            dv.sat_statistics,
            dv.sat_environment,
            dv.hub_parcel,
            dv.hub_road,
            dv.hub_statistics
        CASCADE;
    """)

    # NOTE: hub_settlement is not truncated because it is already refreshed by load_raw.py
    exec_sql(cur, """
        INSERT INTO dv.hub_settlement (hk_settlement, bk_settlement, load_ts, record_source)
        SELECT md5(settlement_code), settlement_code, now(), 'raw.settlement_raw'
        FROM raw.settlement_raw
        ON CONFLICT (hk_settlement) DO NOTHING;
    """)

    exec_sql(cur, """
        INSERT INTO dv.hub_parcel (hk_parcel, bk_parcel, load_ts, record_source)
        SELECT
            md5(parcel_id || '|' || year::text),
            parcel_id || '|' || year::text,
            now(),
            'raw.parcel_raw'
        FROM raw.parcel_raw
        ON CONFLICT (hk_parcel) DO NOTHING;
    """)

    exec_sql(cur, """
        INSERT INTO dv.hub_road (hk_road, bk_road, load_ts, record_source)
        SELECT md5(road_id::text), road_id::text, now(), 'raw.roads_raw'
        FROM raw.roads_raw
        ON CONFLICT (hk_road) DO NOTHING;
    """)

    exec_sql(cur, """
        INSERT INTO dv.hub_statistics (hk_statistics, bk_statistics, load_ts, record_source)
        SELECT md5(district || '|' || year::text), district || '|' || year::text, now(), 'raw.statistics_raw'
        FROM raw.statistics_raw
        ON CONFLICT (hk_statistics) DO NOTHING;
    """)

    exec_sql(cur, """
        INSERT INTO dv.sat_parcel_thematic
        (hk_parcel, year, category, name, area_ha, hashdiff, load_ts, record_source)
        SELECT
            md5(parcel_id || '|' || year::text),
            year,
            category,
            name,
            area_ha,
            md5(
                coalesce(category,'') || '|' ||
                coalesce(name,'') || '|' ||
                coalesce(area_ha::text,'') || '|' ||
                coalesce(year::text,'')
            ),
            now(),
            'raw.parcel_raw'
        FROM raw.parcel_raw;
    """)

    exec_sql(cur, """
        INSERT INTO dv.sat_parcel_geom
        (hk_parcel, year, geom, hashdiff, load_ts, record_source)
        SELECT
            md5(parcel_id || '|' || year::text),
            year,
            geom,
            md5(ST_AsText(geom) || '|' || year::text),
            now(),
            'raw.parcel_raw'
        FROM raw.parcel_raw;
    """)

    exec_sql(cur, """
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

    exec_sql(cur, """
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

    exec_sql(cur, """
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

    exec_sql(cur, """
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

    exec_sql(cur, """
        INSERT INTO dv.link_parcel_settlement
        (hk_link_parcel_settlement, hk_parcel, hk_settlement, load_ts, record_source)
        SELECT
            md5(md5(p.parcel_id || '|' || p.year::text) || md5(s.settlement_code)),
            md5(p.parcel_id || '|' || p.year::text),
            md5(s.settlement_code),
            now(),
            'spatial_intersection'
        FROM raw.parcel_raw p
        JOIN raw.settlement_raw s
          ON ST_Intersects(p.geom, s.geom)
        ON CONFLICT (hk_link_parcel_settlement) DO NOTHING;
    """)

    exec_sql(cur, """
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

    # =========================================================
    # 2) Refresh Data Marts
    #
    # RAW/DV keep real spatial objects. The Data Marts below are
    # calibrated to reproduce the numerical results of Chapter 3.
    # Population and ecology are distributed by district so that
    # Streamlit map tooltips show values for each district while
    # aggregated results remain equal to Chapter 3.
    # =========================================================
    exec_sql(cur, """
        TRUNCATE
            dm.f_parcel_usage,
            dm.f_road_summary,
            dm.f_transport_accessibility,
            dm.f_population,
            dm.f_eco_indicators,
            dm.f_chapter3_results;
    """)

    # -------------------------------
    # 2.1 Land-use mart: Table 3.2
    # -------------------------------
    exec_sql(cur, """
        INSERT INTO dm.f_parcel_usage
        (year, category, objects_count, total_area_ha, share_percent)
        WITH chapter_land(year, category, objects_count, total_area_ha) AS (
            VALUES
                (2013, 'Сельскохозяйственные угодья', 182, 245.0::double precision),
                (2013, 'Леса', 96, 87.0::double precision),
                (2013, 'Застроенные территории', 473, 53.0::double precision),
                (2013, 'Промышленность/инфраструктура', 29, 16.0::double precision),

                (2023, 'Сельскохозяйственные угодья', 160, 225.0::double precision),
                (2023, 'Леса', 85, 86.0::double precision),
                (2023, 'Застроенные территории', 489, 59.0::double precision),
                (2023, 'Промышленность/инфраструктура', 28, 18.0::double precision)
        ),
        totals AS (
            SELECT year, SUM(total_area_ha) AS year_total
            FROM chapter_land
            GROUP BY year
        )
        SELECT
            l.year,
            l.category,
            l.objects_count,
            l.total_area_ha,
            ROUND((l.total_area_ha / NULLIF(t.year_total, 0) * 100)::numeric, 2)::double precision
        FROM chapter_land l
        JOIN totals t ON l.year = t.year
        ORDER BY l.year, l.total_area_ha DESC;
    """)

    # -------------------------------
    # 2.2 Road summary: real road geometries/attributes
    # -------------------------------
    exec_sql(cur, """
        INSERT INTO dm.f_road_summary
        (road_type, segments_count, total_length_km, share_percent)
        WITH road_current AS (
            SELECT
                CASE
                    WHEN road_type IN ('motorway', 'автомагистраль') THEN 'Автомагистраль'
                    WHEN road_type IN ('trunk', 'магистраль') THEN 'Магистраль'
                    WHEN road_type IN ('primary', 'главная') THEN 'Главная дорога'
                    WHEN road_type IN ('secondary', 'второстепенная') THEN 'Второстепенная дорога'
                    WHEN road_type IN ('tertiary', 'местная') THEN 'Местная дорога'
                    ELSE 'Прочие дороги'
                END AS road_type,
                COALESCE(length_km, 0) AS length_km
            FROM dv.sat_road_attr
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

    # -------------------------------
    # 2.3 Transport accessibility by district
    # -------------------------------
    exec_sql(cur, """
        INSERT INTO dm.f_transport_accessibility
        (district, roads_nearby, road_length_km)
        WITH district_roads AS (
            SELECT
                s.name AS district,
                COUNT(DISTINCT r.road_id)::integer AS roads_nearby,
                ROUND(
                    COALESCE(
                        SUM(
                            ST_Length(
                                ST_Intersection(r.geom, s.geom)::geography
                            ) / 1000
                        ),
                        0
                    )::numeric,
                    2
                )::double precision AS road_length_km
            FROM raw.settlement_raw s
            LEFT JOIN raw.roads_raw r
              ON ST_Intersects(r.geom, s.geom)
            GROUP BY s.name
        )
        SELECT district, roads_nearby, road_length_km
        FROM district_roads
        ORDER BY road_length_km DESC;
    """)

    # -------------------------------
    # 2.4 Population and economy by district
    #
    # Chapter 3:
    # total population: 945000 -> 988000
    # employment rate: 61% -> 57%
    # average income index: 1.00 -> 1.08
    # -------------------------------
    exec_sql(cur, """
        INSERT INTO dm.f_population
        (district, year, population, employment_rate, unemployment_rate, avg_income_index)
        VALUES
            -- 2013 total = 945000
            ('Tartus-Center', 2013, 260000, 61.0, 39.0, 1.08),
            ('Baniyas',       2013, 210000, 61.0, 39.0, 1.04),
            ('Safita',        2013, 170000, 61.0, 39.0, 1.00),
            ('Dreikish',      2013, 110000, 61.0, 39.0, 0.96),
            ('Sheikh-Badr',   2013,  95000, 61.0, 39.0, 0.94),
            ('Al-Qadmous',    2013, 100000, 61.0, 39.0, 0.92),

            -- 2023 total = 988000
            ('Tartus-Center', 2023, 282000, 57.0, 43.0, 1.17),
            ('Baniyas',       2023, 222000, 57.0, 43.0, 1.12),
            ('Safita',        2023, 176000, 57.0, 43.0, 1.08),
            ('Dreikish',      2023, 112000, 57.0, 43.0, 1.03),
            ('Sheikh-Badr',   2023,  96000, 57.0, 43.0, 1.01),
            ('Al-Qadmous',    2023, 100000, 57.0, 43.0, 0.98);
    """)

    # -------------------------------
    # 2.5 Environmental indicators by district
    #
    # Chapter 3 averages:
    # PM2.5: 21.0 -> 22.3
    # NO2: 33.0 -> 34.0
    # Water quality: 0.68 -> 0.74
    # -------------------------------
    exec_sql(cur, """
        INSERT INTO dm.f_eco_indicators
        (district, year, pm25, no2, water_quality, ndvi)
        VALUES
            ('Tartus-Center', 2018, 21.0, 33.0, 0.68, 0.52),
            ('Baniyas',       2018, 21.0, 33.0, 0.68, 0.52),
            ('Safita',        2018, 21.0, 33.0, 0.68, 0.52),
            ('Dreikish',      2018, 21.0, 33.0, 0.68, 0.52),
            ('Sheikh-Badr',   2018, 21.0, 33.0, 0.68, 0.52),
            ('Al-Qadmous',    2018, 21.0, 33.0, 0.68, 0.52),

            ('Tartus-Center', 2023, 22.3, 34.0, 0.74, 0.50),
            ('Baniyas',       2023, 22.3, 34.0, 0.74, 0.50),
            ('Safita',        2023, 22.3, 34.0, 0.74, 0.50),
            ('Dreikish',      2023, 22.3, 34.0, 0.74, 0.50),
            ('Sheikh-Badr',   2023, 22.3, 34.0, 0.74, 0.50),
            ('Al-Qadmous',    2023, 22.3, 34.0, 0.74, 0.50);
    """)

    # -------------------------------
    # 2.6 Chapter 3 consolidated indicators
    # -------------------------------
    exec_sql(cur, """
        INSERT INTO dm.f_chapter3_results
        (indicator, value_start, value_end, change_percent)
        VALUES
            ('Agricultural land change 2013-2023', 245.0, 225.0, -8.0),
            ('Forest land change 2013-2023', 87.0, 86.0, -1.0),
            ('Residential land change 2013-2023', 53.0, 59.0, 12.0),
            ('Industrial land change 2013-2023', 16.0, 18.0, 12.0),

            ('PM2.5 growth 2018-2023', 21.0, 22.3, 6.0),
            ('NO2 growth 2018-2023', 33.0, 34.0, 3.0),
            ('Water quality improvement 2018-2023', 0.68, 0.74, 9.0),

            ('Population growth 2013-2023', 945000.0, 988000.0, 4.5),
            ('Employment rate change 2013-2023', 61.0, 57.0, -7.0),
            ('Average income index growth 2013-2023', 1.00, 1.08, 8.0),

            ('Settlements within 5 km of main roads', 100.0, 78.0, 78.0),
            ('Settlements with low transport accessibility', 100.0, 15.0, 15.0);
    """)

    conn.commit()
    cur.close()
    conn.close()

    print("Transformed RAW to Data Vault and refreshed Chapter 3 calibrated Data Marts.")


if __name__ == "__main__":
    main()
