import os
import sys

import psycopg2


DSN = os.getenv("DSN", "postgresql://dwh_admin:changeme@localhost:5432/dwh")


CHECKS = [
    ("raw.parcel_raw", "SELECT COUNT(*) FROM raw.parcel_raw"),
    ("raw.roads_raw", "SELECT COUNT(*) FROM raw.roads_raw"),
    ("raw.statistics_raw", "SELECT COUNT(*) FROM raw.statistics_raw"),
    ("raw.settlement_raw", "SELECT COUNT(*) FROM raw.settlement_raw"),
    ("dv.hub_parcel", "SELECT COUNT(*) FROM dv.hub_parcel"),
    ("dv.hub_road", "SELECT COUNT(*) FROM dv.hub_road"),
    ("dv.hub_settlement", "SELECT COUNT(*) FROM dv.hub_settlement"),
    ("dv.hub_statistics", "SELECT COUNT(*) FROM dv.hub_statistics"),
    ("dm.f_parcel_usage", "SELECT COUNT(*) FROM dm.f_parcel_usage"),
    ("dm.f_road_summary", "SELECT COUNT(*) FROM dm.f_road_summary"),
    ("dm.f_transport_accessibility", "SELECT COUNT(*) FROM dm.f_transport_accessibility"),
    ("dm.f_population", "SELECT COUNT(*) FROM dm.f_population"),
    ("dm.f_eco_indicators", "SELECT COUNT(*) FROM dm.f_eco_indicators"),
    ("dm.f_chapter3_results", "SELECT COUNT(*) FROM dm.f_chapter3_results"),
]


def main():
    failed = []

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            for name, sql in CHECKS:
                cur.execute(sql)
                rows = cur.fetchone()[0]
                status = "OK" if rows > 0 else "EMPTY"
                print(f"{status:5} {name:32} {rows}")

                if rows <= 0:
                    failed.append(name)

    if failed:
        print("\nEmpty required tables: " + ", ".join(failed), file=sys.stderr)
        return 1

    print("\nData warehouse verification passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
