import os
import psycopg2

DSN = os.getenv("DSN", "postgresql://dwh_admin:changeme@localhost:5432/dwh")


def assert_zero(cur, sql, message):
    cur.execute(sql)
    n = cur.fetchone()[0]
    assert n == 0, f"{message}: {n}"
    print(f"OK: {message} = 0")


def assert_positive(cur, sql, message):
    cur.execute(sql)
    n = cur.fetchone()[0]
    assert n > 0, f"{message}: expected > 0, got {n}"
    print(f"OK: {message} = {n}")


def main():
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()

    # ===============================
    # 1) LANDUSE checks
    # ===============================
    assert_zero(
        cur,
        "SELECT COUNT(*) FROM raw.landuse_raw WHERE geom IS NULL OR NOT ST_IsValid(geom);",
        "Invalid landuse geometries"
    )

    assert_zero(
        cur,
        "SELECT COUNT(*) FROM raw.landuse_raw WHERE area_ha IS NULL OR area_ha <= 0;",
        "Non-positive landuse area"
    )

    assert_positive(
        cur,
        "SELECT COUNT(*) FROM raw.landuse_raw;",
        "Landuse records loaded"
    )

    # ===============================
    # 2) ROADS checks
    # ===============================
    assert_zero(
        cur,
        "SELECT COUNT(*) FROM raw.roads_raw WHERE geom IS NULL OR NOT ST_IsValid(geom);",
        "Invalid road geometries"
    )

    assert_zero(
        cur,
        "SELECT COUNT(*) FROM raw.roads_raw WHERE length_km IS NULL OR length_km <= 0;",
        "Non-positive road length"
    )

    assert_positive(
        cur,
        "SELECT COUNT(*) FROM raw.roads_raw;",
        "Road records loaded"
    )

    # ===============================
    # 3) STATISTICS checks
    # ===============================
    assert_zero(
        cur,
        "SELECT COUNT(*) FROM raw.statistics_raw WHERE population IS NULL OR population <= 0;",
        "Invalid population values"
    )

    assert_zero(
        cur,
        "SELECT COUNT(*) FROM raw.statistics_raw WHERE employment_rate < 0 OR employment_rate > 100;",
        "Invalid employment rate"
    )

    assert_positive(
        cur,
        "SELECT COUNT(*) FROM raw.statistics_raw;",
        "Statistics records loaded"
    )

    # ===============================
    # 4) DATA VAULT checks
    # ===============================
    assert_positive(
        cur,
        "SELECT COUNT(*) FROM dv.hub_settlement;",
        "DV hub_settlement loaded"
    )

    # ===============================
    # 5) DATA MART checks
    # ===============================
    assert_positive(
        cur,
        "SELECT COUNT(*) FROM dm.f_landuse_summary;",
        "DM landuse summary"
    )

    assert_positive(
        cur,
        "SELECT COUNT(*) FROM dm.f_road_summary;",
        "DM road summary"
    )

    assert_positive(
        cur,
        "SELECT COUNT(*) FROM dm.f_population;",
        "DM population"
    )

    assert_positive(
        cur,
        "SELECT COUNT(*) FROM dm.f_chapter3_results;",
        "DM chapter 3 results"
    )

    conn.close()
    print("All Data Quality checks passed successfully.")


if __name__ == "__main__":
    main()