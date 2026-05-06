import os
import csv
import json
import psycopg2
from shapely.geometry import shape

DSN = os.getenv("DSN", "postgresql://dwh_admin:changeme@localhost:5432/dwh")


def exec_sql(cur, sql, params=None):
    cur.execute(sql, params or ())


def load_geojson_boundaries(cur, geojson_path):
    with open(geojson_path, encoding="utf-8") as f:
        data = json.load(f)

    for feature in data["features"]:
        props = feature["properties"]

        name = props.get("name") or props.get("name:en") or "unknown"
        code = name.replace(" ", "_").lower()

        geom_json = json.dumps(feature["geometry"])

        sql = """
        INSERT INTO raw.settlement_raw
        (settlement_code, name, geom, load_ts, record_source)
        VALUES (
            %s,
            %s,
            ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)),
            now(),
            'geojson'
        )
        ON CONFLICT (settlement_code) DO UPDATE SET
            geom = EXCLUDED.geom,
            name = EXCLUDED.name,
            load_ts = now(),
            record_source = 'geojson'
        """
        exec_sql(cur, sql, [code, name, geom_json])


def load_statistics(cur, csv_path):
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            sql = """
            INSERT INTO raw.statistics_raw
              (district, year, population, employment_rate, avg_income_index,
               pm25, no2, water_quality, ndvi, load_ts, record_source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), 'sample_statistics')
            """

            exec_sql(
                cur,
                sql,
                [
                    row["district"],
                    int(row["year"]),
                    int(row["population"]),
                    float(row["employment_rate"]),
                    float(row["avg_income_index"]),
                    float(row["pm25"]),
                    float(row["no2"]),
                    float(row["water_quality"]),
                    float(row["ndvi"]),
                ],
            )


def load_landuse(cur, geojson_path):
    with open(geojson_path, encoding="utf-8") as f:
        data = json.load(f)

    loaded = 0
    skipped = 0

    for feature in data["features"]:
        props = feature.get("properties", {})
        geometry = feature.get("geometry")

        if not geometry:
            skipped += 1
            continue

        geom_type = geometry.get("type")

        if geom_type not in ("Polygon", "MultiPolygon"):
            skipped += 1
            continue

        landuse = props.get("landuse") or props.get("fclass") or props.get("natural") or "unknown"
        name = props.get("name")
        osm_id = props.get("@id")

        geom = shape(geometry)
        geom_wkt = geom.wkt

        sql = """
        INSERT INTO raw.landuse_raw
        (osm_id, landuse_type, name, area_ha, geom, load_ts, record_source)
        VALUES (%s, %s, %s,
                ST_Area(ST_GeomFromText(%s,4326)::geography)/10000,
                ST_Multi(ST_GeomFromText(%s,4326)),
                now(),
                'osm_landuse')
        """

        exec_sql(cur, sql, [osm_id, landuse, name, geom_wkt, geom_wkt])
        loaded += 1

    print(f"Loaded landuse: {loaded}, skipped non-polygon features: {skipped}")


def load_roads(cur, geojson_path):
    with open(geojson_path, encoding="utf-8") as f:
        data = json.load(f)

    loaded = 0
    skipped = 0

    for feature in data["features"]:
        props = feature.get("properties", {})
        geometry = feature.get("geometry")

        if not geometry:
            skipped += 1
            continue

        geom_type = geometry.get("type")

        if geom_type not in ("LineString", "MultiLineString"):
            skipped += 1
            continue

        road_type = props.get("highway") or "unknown"
        name = props.get("name")
        osm_id = props.get("@id")

        geom = shape(geometry)
        geom_wkt = geom.wkt

        sql = """
        INSERT INTO raw.roads_raw
        (osm_id, road_type, name, length_km, geom, load_ts, record_source)
        VALUES (%s, %s, %s,
                ST_Length(ST_GeomFromText(%s,4326)::geography)/1000,
                ST_Multi(ST_GeomFromText(%s,4326)),
                now(),
                'osm_roads')
        """

        exec_sql(cur, sql, [osm_id, road_type, name, geom_wkt, geom_wkt])
        loaded += 1

    print(f"Loaded roads: {loaded}, skipped non-line features: {skipped}")


def main():
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()

    cur.execute("DELETE FROM raw.settlement_raw")
    cur.execute("DELETE FROM raw.statistics_raw")
    cur.execute("DELETE FROM raw.landuse_raw")
    cur.execute("DELETE FROM raw.roads_raw")

    load_geojson_boundaries(cur, "/app/sample_data/tartus_district_boundaries.geojson")
    load_statistics(cur, "/app/sample_data/statistics.csv")
    load_landuse(cur, "/app/sample_data/landuse.geojson")
    load_roads(cur, "/app/sample_data/roads.geojson")

    conn.commit()
    cur.close()
    conn.close()

    print("Loaded RAW data: settlements + statistics + landuse + roads")


if __name__ == "__main__":
    main()