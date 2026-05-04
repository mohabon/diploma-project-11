import os
import json
import pandas as pd
import streamlit as st
import psycopg2
import folium
from streamlit_folium import folium_static
from shapely import wkt
from shapely.geometry import shape
from pyproj import Geod


# ==================== Settings ====================
DSN = os.getenv("DSN", "postgresql://dwh_admin:changeme@db:5432/dwh")

st.set_page_config(page_title="Tartus Spatial DWH Prototype", layout="wide")
st.title("🏛️ Прототип пространственного хранилища данных мухафазы Тартус")
st.caption(
    "Прототип | PostgreSQL/PostGIS | Raw Layer | Data Vault | "
    "Data Marts | Airflow | Prometheus | Streamlit"
)


# ==================== Database connection ====================
@st.cache_resource
def get_connection():
    try:
        return psycopg2.connect(DSN)
    except Exception as e:
        st.warning(f"⚠️ Ошибка подключения к базе данных: {e}")
        return None


conn = get_connection()


# ==================== Safe SQL reader ====================
def read_sql_safe(sql, connection, empty_columns=None):
    try:
        return pd.read_sql(sql, connection)
    except Exception:
        if connection:
            connection.rollback()
        return pd.DataFrame(columns=empty_columns or [])


# ==================== Load data from database ====================
@st.cache_data
def load_data():
    data = {}

    if not conn:
        return data

    data["landuse"] = read_sql_safe("""
        SELECT landuse_type, objects_count, total_area_ha, share_percent
        FROM dm.f_landuse_summary
        ORDER BY total_area_ha DESC
    """, conn)

    data["landuse_by_district"] = read_sql_safe("""
        SELECT district, landuse_type, objects_count, total_area_ha, share_percent
        FROM dm.f_landuse_by_district
        ORDER BY district, total_area_ha DESC
    """, conn)

    # ВАЖНО: слой землепользования берётся полностью из raw.landuse_raw.
    # Без обрезки по границам и без фильтрации точек, чтобы не терять объекты.
    data["landuse_map"] = read_sql_safe("""
        SELECT
            landuse_type,
            area_ha,
            ST_AsGeoJSON(
                CASE
                    WHEN ST_IsValid(geom) THEN geom
                    ELSE ST_MakeValid(geom)
                END
            ) AS geom
        FROM raw.landuse_raw
        WHERE geom IS NOT NULL
          AND NOT ST_IsEmpty(geom)
    """, conn)

    data["roads"] = read_sql_safe("""
        SELECT road_type, segments_count, total_length_km, share_percent
        FROM dm.f_road_summary
        ORDER BY total_length_km DESC
    """, conn)

    data["access"] = read_sql_safe("""
        SELECT district, roads_nearby, road_length_km
        FROM dm.f_transport_accessibility
        ORDER BY road_length_km DESC
    """, conn)

    data["population"] = read_sql_safe("""
        SELECT district, year, population, employment_rate, unemployment_rate, avg_income_index
        FROM dm.f_population
        ORDER BY district, year
    """, conn)

    data["eco"] = read_sql_safe("""
        SELECT district, year, pm25, no2, water_quality, ndvi
        FROM dm.f_eco_indicators
        ORDER BY district, year
    """, conn)

    data["roads_map"] = read_sql_safe("""
        SELECT road_type, length_km, ST_AsGeoJSON(geom) AS geom
        FROM raw.roads_raw
        WHERE geom IS NOT NULL
          AND road_type IN (
              'motorway', 'trunk', 'primary', 'secondary',
              'автомагистраль', 'магистраль', 'главная', 'второстепенная'
          )
        ORDER BY length_km DESC
    """, conn)

    data["dv_counts"] = read_sql_safe("""
        SELECT 'hub_landuse' AS name, COUNT(*) AS rows FROM dv.hub_landuse
        UNION ALL SELECT 'hub_road', COUNT(*) FROM dv.hub_road
        UNION ALL SELECT 'hub_settlement', COUNT(*) FROM dv.hub_settlement
        UNION ALL SELECT 'hub_statistics', COUNT(*) FROM dv.hub_statistics
        UNION ALL SELECT 'link_landuse_settlement', COUNT(*) FROM dv.link_landuse_settlement
        UNION ALL SELECT 'link_road_settlement', COUNT(*) FROM dv.link_road_settlement
        UNION ALL SELECT 'sat_landuse_attr', COUNT(*) FROM dv.sat_landuse_attr
        UNION ALL SELECT 'sat_landuse_geom', COUNT(*) FROM dv.sat_landuse_geom
        UNION ALL SELECT 'sat_road_attr', COUNT(*) FROM dv.sat_road_attr
        UNION ALL SELECT 'sat_road_geom', COUNT(*) FROM dv.sat_road_geom
        UNION ALL SELECT 'sat_statistics', COUNT(*) FROM dv.sat_statistics
        UNION ALL SELECT 'sat_environment', COUNT(*) FROM dv.sat_environment
    """, conn)

    data["raw_counts"] = read_sql_safe("""
        SELECT 'raw.landuse_raw' AS table_name, COUNT(*) AS rows FROM raw.landuse_raw
        UNION ALL SELECT 'raw.roads_raw', COUNT(*) FROM raw.roads_raw
        UNION ALL SELECT 'raw.statistics_raw', COUNT(*) FROM raw.statistics_raw
        UNION ALL SELECT 'raw.settlement_raw', COUNT(*) FROM raw.settlement_raw
        UNION ALL SELECT 'raw.parcel_raw', COUNT(*) FROM raw.parcel_raw
    """, conn)

    data["dq"] = read_sql_safe("""
        SELECT
            (SELECT COUNT(*) FROM raw.landuse_raw WHERE geom IS NOT NULL AND NOT ST_IsValid(geom)) AS invalid_landuse,
            (SELECT COUNT(*) FROM raw.roads_raw WHERE geom IS NOT NULL AND NOT ST_IsValid(geom)) AS invalid_roads,
            (SELECT COUNT(*) FROM raw.statistics_raw) AS statistics_rows
    """, conn)

    return data


data = load_data()


# ==================== Load sample files ====================
@st.cache_data
def load_sample_files():
    sample_dir = "/app/sample_data"

    if not os.path.exists(sample_dir):
        sample_dir = "sample_data"

    with open(os.path.join(sample_dir, "tartus_district_boundaries.geojson"), "r", encoding="utf-8") as f:
        geojson = json.load(f)

    settlements = pd.read_csv(os.path.join(sample_dir, "settlements.csv"))

    coords = {}
    for _, row in settlements.iterrows():
        try:
            poly = wkt.loads(row["wkt"])
            cent = poly.centroid
            coords[row["name"]] = {"lat": cent.y, "lon": cent.x}
        except Exception:
            pass

    return geojson, coords


districts_geojson, coords_dict = load_sample_files()


# ==================== Dictionaries ====================
district_ru = {
    "Tartus-Center": "Тартус",
    "Tartus": "Тартус",
    "Tartus District": "Тартус",
    "Tartous District": "Тартус",

    "Baniyas": "Банияс",
    "Banyas": "Банияс",
    "Banias": "Банияс",
    "Baniyas District": "Банияс",
    "Banyas District": "Банияс",
    "Banias District": "Банияс",

    "Safita": "Сафита",
    "Safita District": "Сафита",
    "As-Safita": "Сафита",
    "As-Safita District": "Сафита",
    "As Safita": "Сафита",
    "As Safita District": "Сафита",

    "Dreikish": "Дрейкиш",
    "Dreikish District": "Дрейкиш",
    "Dureikish": "Дрейкиш",
    "Dureikish District": "Дрейкиш",
    "Duraykish": "Дрейкиш",
    "Duraykish District": "Дрейкиш",
    "Drekish": "Дрейкиш",
    "Drekish District": "Дрейкиш",

    "Sheikh-Badr": "Шейх-Бадр",
    "Sheikh Badr": "Шейх-Бадр",
    "Sheikh Badr District": "Шейх-Бадр",
    "Ash-Sheikh Badr": "Шейх-Бадр",
    "Ash-Sheikh Badr District": "Шейх-Бадр",
    "Ash Sheikh Badr": "Шейх-Бадр",
    "Ash Sheikh Badr District": "Шейх-Бадр",

    "Al-Qadmous": "Аль-Кадмус",
    "Al Qadmous": "Аль-Кадмус",
    "Al-Qadmous District": "Аль-Кадмус",
    "Al Qadmous District": "Аль-Кадмус",
    "Qadmous": "Аль-Кадмус",
    "Qadmous District": "Аль-Кадмус",
    "Qadmus": "Аль-Кадмус",
    "Qadmus District": "Аль-Кадмус",

    "منطقة_طرطوس": "Тартус",
    "منطقة طرطوس": "Тартус",
    "منطقة_بانياس": "Банияс",
    "منطقة بانياس": "Банияс",
    "منطقة_صافيتا": "Сафита",
    "منطقة صافيتا": "Сафита",
    "منطقة_الدريكيش": "Дрейкиш",
    "منطقة الدريكيش": "Дрейкиш",
    "منطقة_دريكيش": "Дрейкиш",
    "منطقة دريكيش": "Дрейкиш",
    "منطقة_الشيخ_بدر": "Шейх-Бадр",
    "منطقة الشيخ بدر": "Шейх-Бадр",
    "منطقة_القدموس": "Аль-Кадмус",
    "منطقة القدموس": "Аль-Кадмус",
}

landuse_ru = {
    "residential": "Жилая застройка",
    "farmland": "Сельскохозяйственные земли",
    "agricultural": "Сельскохозяйственные земли",
    "agriculture": "Сельскохозяйственные земли",
    "forest": "Леса",
    "industrial": "Промышленные территории",

    "жилая": "Жилая застройка",
    "сельхоз": "Сельскохозяйственные земли",
    "сельскохозяйственные земли": "Сельскохозяйственные земли",
    "лес": "Леса",
    "леса": "Леса",
    "промышленность": "Промышленные территории",
}

road_ru = {
    "motorway": "Автомагистраль",
    "trunk": "Магистраль",
    "primary": "Главная дорога",
    "secondary": "Второстепенная дорога",
    "tertiary": "Местная дорога",
    "residential": "Жилая улица",

    "автомагистраль": "Автомагистраль",
    "магистраль": "Магистраль",
    "главная": "Главная дорога",
    "второстепенная": "Второстепенная дорога",
}

landuse_colors = {
    "residential": "#8B4513",
    "farmland": "#9CCC65",
    "agricultural": "#9CCC65",
    "agriculture": "#9CCC65",
    "forest": "#1B5E20",
    "industrial": "#546E7A",

    "жилая": "#8B4513",
    "сельхоз": "#9CCC65",
    "сельскохозяйственные земли": "#9CCC65",
    "лес": "#1B5E20",
    "леса": "#1B5E20",
    "промышленность": "#546E7A",
}


# ==================== Helper functions ====================
def get_district_key(name):
    if not name:
        return ""

    s = str(name).strip().lower()
    s = s.replace("_", " ").replace("-", " ")
    s = s.replace("district", "").strip()

    if "tartus" in s or "tartous" in s or "طرطوس" in s:
        return "Tartus-Center"

    if "baniyas" in s or "banyas" in s or "banias" in s or "بانياس" in s:
        return "Baniyas"

    if "safita" in s or "as safita" in s or "صافيتا" in s:
        return "Safita"

    if "dreikish" in s or "dureikish" in s or "duraykish" in s or "drekish" in s or "الدريكيش" in s or "دريكيش" in s:
        return "Dreikish"

    if "sheikh" in s or "badr" in s or "ash sheikh" in s or "shaykh" in s or "الشيخ" in s:
        return "Sheikh-Badr"

    if "qadmous" in s or "qadmus" in s or "kadmus" in s or "قدموس" in s:
        return "Al-Qadmous"

    return str(name)


def district_name_ru(name):
    key = get_district_key(name)
    return district_ru.get(name, district_ru.get(key, str(name)))


def get_population_color(value, min_value, max_value):
    if pd.isna(value):
        return "#DDDDDD"

    ratio = 1 if max_value == min_value else (value - min_value) / (max_value - min_value)

    if ratio >= 0.80:
        return "#4B1D0F"
    elif ratio >= 0.60:
        return "#7A2E12"
    elif ratio >= 0.40:
        return "#A0522D"
    elif ratio >= 0.20:
        return "#C68642"
    return "#E6C7A1"


def get_ecology_pressure(row):
    pm25_score = min(float(row["pm25"]) / 60, 1)
    no2_score = min(float(row["no2"]) / 80, 1)
    water_score = 1 - min(float(row["water_quality"]) / 100, 1)
    ndvi_score = 1 - min(float(row["ndvi"]) / 1, 1)

    return (
        pm25_score * 0.30 +
        no2_score * 0.25 +
        water_score * 0.25 +
        ndvi_score * 0.20
    )


def get_ecology_gray_color(value, min_value, max_value):
    if pd.isna(value):
        return "#D9D9D9"

    ratio = 0.5 if max_value == min_value else (value - min_value) / (max_value - min_value)

    if ratio >= 0.80:
        return "#1A1A1A"
    elif ratio >= 0.60:
        return "#4D4D4D"
    elif ratio >= 0.40:
        return "#808080"
    elif ratio >= 0.20:
        return "#BFBFBF"
    return "#F2F2F2"


def get_road_style_map(road_type):
    styles = {
        "motorway": {"color": "#000000", "weight": 6},
        "автомагистраль": {"color": "#000000", "weight": 6},

        "trunk": {"color": "#2C2C2C", "weight": 5},
        "магистраль": {"color": "#2C2C2C", "weight": 5},

        "primary": {"color": "#555555", "weight": 4},
        "главная": {"color": "#555555", "weight": 4},

        "secondary": {"color": "#888888", "weight": 2.5},
        "второстепенная": {"color": "#888888", "weight": 2.5},
    }

    return styles.get(road_type, {"color": "#AAAAAA", "weight": 1})


geod = Geod(ellps="WGS84")


def get_feature_name(feature):
    props = feature.get("properties", {})

    for field in [
        "name_en", "NAME_EN",
        "name", "NAME",
        "district", "District",
        "admin_name", "ADMIN_NAME",
    ]:
        if field in props and props[field]:
            return props[field]

    return ""


def feature_area_km2(feature):
    geom = shape(feature["geometry"])
    area_m2, _ = geod.geometry_area_perimeter(geom)
    return abs(area_m2) / 1_000_000


# ==================== Tabs ====================
tabs = st.tabs([
    "📊 Состояние системы",
    "✅ Качество данных",
    "🔄 Источники и ETL",
    "🏗️ Data Vault",
    "📈 Data Marts",
    "🚗 Транспорт",
    "🏞️ Землепользование",
    "🌿 Экология",
    "👥 Население и экономика",
    "🗺️ Карта",
])


# ==================== Tab 0: System status ====================
with tabs[0]:
    st.header("📊 Состояние системы")

    total_land = int(data["landuse"]["objects_count"].sum()) if "landuse" in data and not data["landuse"].empty else 0
    total_area = round(data["landuse"]["total_area_ha"].sum(), 2) if "landuse" in data and not data["landuse"].empty else 0
    total_roads = int(data["roads"]["segments_count"].sum()) if "roads" in data and not data["roads"].empty else 0
    total_length = round(data["roads"]["total_length_km"].sum(), 2) if "roads" in data and not data["roads"].empty else 0

    dv_tables = int(len(data["dv_counts"])) if "dv_counts" in data and not data["dv_counts"].empty else 0
    raw_tables = int(len(data["raw_counts"])) if "raw_counts" in data and not data["raw_counts"].empty else 0

    mart_count = 5
    if "landuse_by_district" in data and not data["landuse_by_district"].empty:
        mart_count = 6

    a, b, c, d = st.columns(4)
    a.metric("Объекты землепользования", f"{total_land:,}")
    b.metric("Дорожные сегменты", f"{total_roads:,}")
    c.metric("Таблицы Data Vault", dv_tables)
    d.metric("Витрины Data Marts", mart_count)

    e, f, g, h = st.columns(4)
    e.metric("Площадь землепользования, га", f"{total_area:,}")
    f.metric("Протяжённость дорог, км", f"{total_length:,}")
    g.metric("Raw-таблицы", raw_tables)
    h.metric("Цели мониторинга", "3")

    st.info(
        "Рабочий прототип построен по цепочке: Raw Layer → Data Vault → Data Marts → Streamlit Dashboard. "
        "ETL-процесс оркестрируется через Airflow, мониторинг выполняется через Prometheus, postgres-exporter и cAdvisor."
    )


# ==================== Tab 1: Data quality ====================
with tabs[1]:
    st.header("✅ Качество данных")

    if "dq" in data and not data["dq"].empty:
        invalid_landuse = int(data["dq"].iloc[0]["invalid_landuse"])
        invalid_roads = int(data["dq"].iloc[0]["invalid_roads"])
        statistics_rows = int(data["dq"].iloc[0]["statistics_rows"])

        a, b, c = st.columns(3)
        a.metric("Невалидные геометрии землепользования", invalid_landuse)
        b.metric("Невалидные геометрии дорог", invalid_roads)
        c.metric("Статистические записи", statistics_rows)

        dq_table = pd.DataFrame([
            ["Валидность геометрий", "raw.landuse_raw", invalid_landuse, "OK" if invalid_landuse == 0 else "Требует проверки"],
            ["Валидность геометрий", "raw.roads_raw", invalid_roads, "OK" if invalid_roads == 0 else "Требует проверки"],
            ["Полнота статистических данных", "raw.statistics_raw", statistics_rows, "OK" if statistics_rows > 0 else "Нет данных"],
        ], columns=["Проверка", "Слой данных", "Результат", "Статус"])

        st.subheader("Результаты проверок")
        st.dataframe(dq_table, use_container_width=True)
    else:
        st.warning("Нет данных о качестве данных.")


# ==================== Tab 2: Sources and ETL ====================
with tabs[2]:
    st.header("🔄 Источники данных и ETL-процесс")

    st.markdown(
        "Раздел показывает технический путь данных: от исходных файлов до аналитических витрин, "
        "с оркестрацией через Airflow и мониторингом через Prometheus и cAdvisor."
    )

    sources = pd.DataFrame([
        ["landuse.geojson", "raw.landuse_raw", "Объекты землепользования"],
        ["roads.geojson", "raw.roads_raw", "Дорожная сеть"],
        ["statistics.csv", "raw.statistics_raw", "Население, экономика, экология"],
        ["settlements.csv", "raw.settlement_raw", "Районы / населённые пункты"],
        ["tartus_district_boundaries.geojson", "sample_data", "Административные границы"],
    ], columns=["Источник", "Целевой слой / таблица", "Назначение"])

    st.subheader("Источники данных")
    st.dataframe(sources, use_container_width=True)

    pipeline = pd.DataFrame([
        ["1", "Загрузка исходных данных", "load_raw_data", "etl/load_raw.py", "Заполнение raw-таблиц"],
        ["2", "Преобразование данных", "transform_to_vault_and_marts", "etl/transform_business.py", "Data Vault + Data Marts"],
        ["3", "Контроль качества", "run_data_quality_checks", "etl/dq/test_cadastre.py", "Проверка геометрий и данных"],
    ], columns=["№", "Этап", "Airflow task", "Скрипт", "Результат"])

    st.subheader("Airflow ETL-пайплайн")
    st.dataframe(pipeline, use_container_width=True)

    st.markdown("**Схема обработки:** `Raw Layer → Data Vault → Data Marts → Streamlit Dashboard`")

    monitoring = pd.DataFrame([
        ["Airflow", "Оркестрация ETL-процесса", "http://localhost:8080"],
        ["Prometheus", "Сбор метрик PostgreSQL и контейнеров", "http://localhost:9090"],
        ["postgres-exporter", "Метрики PostgreSQL/PostGIS", "http://localhost:9187"],
        ["cAdvisor", "Метрики Docker-контейнеров", "http://localhost:8081"],
        ["Streamlit", "Аналитический дашборд", "http://localhost:8501"],
    ], columns=["Компонент", "Назначение", "Интерфейс"])

    st.subheader("Оркестрация и мониторинг")
    st.dataframe(monitoring, use_container_width=True)

    with st.expander("Техническое подтверждение запуска"):
        st.code("""
docker compose up -d db etl airflow-webserver airflow-scheduler postgres-exporter cadvisor prometheus

docker compose exec etl python /app/etl/load_raw.py
docker compose exec etl python /app/etl/transform_business.py
docker compose exec etl python /app/etl/dq/test_cadastre.py

docker compose exec etl streamlit run /app/etl/dashboard.py --server.address 0.0.0.0 --server.port 8501
        """, language="powershell")


# ==================== Tab 3: Data Vault ====================
with tabs[3]:
    st.header("🏗️ Data Vault")

    st.markdown(
        "Слой Data Vault фиксирует бизнес-ключи, связи между объектами и изменяемые атрибуты. "
        "В прототипе используются Hubs, Links и Satellites."
    )

    dv_catalog = pd.DataFrame([
        ["hub_landuse", "Hub", "Бизнес-ключи объектов землепользования"],
        ["hub_road", "Hub", "Бизнес-ключи дорожных объектов"],
        ["hub_settlement", "Hub", "Бизнес-ключи районов / населённых пунктов"],
        ["hub_statistics", "Hub", "Бизнес-ключи статистических записей"],
        ["link_landuse_settlement", "Link", "Пространственная связь землепользования с территорией"],
        ["link_road_settlement", "Link", "Пространственная связь дорожной сети с территорией"],
        ["sat_landuse_attr", "Satellite", "Атрибуты объектов землепользования"],
        ["sat_landuse_geom", "Satellite", "Геометрия объектов землепользования"],
        ["sat_road_attr", "Satellite", "Атрибуты дорожной сети"],
        ["sat_road_geom", "Satellite", "Геометрия дорожной сети"],
        ["sat_statistics", "Satellite", "Население и социально-экономические показатели"],
        ["sat_environment", "Satellite", "Экологические показатели"],
    ], columns=["Таблица", "Тип элемента", "Назначение"])

    if "dv_counts" in data and not data["dv_counts"].empty:
        dv_counts = data["dv_counts"].copy()
        dv_catalog = dv_catalog.merge(dv_counts, left_on="Таблица", right_on="name", how="left")
        dv_catalog = dv_catalog.drop(columns=["name"])
        dv_catalog = dv_catalog.rename(columns={"rows": "Количество строк"})
        dv_catalog["Количество строк"] = dv_catalog["Количество строк"].fillna(0).astype(int)

    a, b, c = st.columns(3)
    a.metric("Hubs", 4)
    b.metric("Links", 2)
    c.metric("Satellites", 6)

    st.subheader("Каталог Data Vault")
    st.dataframe(dv_catalog, use_container_width=True, height=600)


# ==================== Tab 4: Data Marts ====================
with tabs[4]:
    st.header("📈 Data Marts")

    st.markdown(
        "Витрины данных формируют аналитический слой прототипа. "
        "Они создаются на основе Data Vault и используются для тематического анализа землепользования, "
        "дорожной сети, транспортной доступности, населения, экономики и экологии."
    )

    mart_rows = [
        ["dm.f_landuse_summary", "Землепользование", "Тип землепользования", "Уровень всей мухафазы", "Землепользование"],
        ["dm.f_road_summary", "Дорожная сеть", "Тип дороги", "Уровень всей мухафазы", "Транспорт"],
        ["dm.f_transport_accessibility", "Транспортная доступность", "Район / территория", "Связана с территорией", "Транспорт"],
        ["dm.f_population", "Население и экономика", "Район + год", "Связана с районами", "Население и экономика, карта"],
        ["dm.f_eco_indicators", "Экология", "Район + год", "Связана с районами", "Экология, карта"],
    ]

    if "landuse_by_district" in data and not data["landuse_by_district"].empty:
        mart_rows.insert(
            1,
            ["dm.f_landuse_by_district", "Землепользование", "Район + тип землепользования", "Связана с районами", "Землепользование"]
        )

    mart_catalog = pd.DataFrame(
        mart_rows,
        columns=["Витрина данных", "Предметная область", "Гранулярность", "Пространственная связь", "Используется в разделе"]
    )

    a, b, c, d = st.columns(4)
    a.metric("Аналитический слой", "dm")
    b.metric("Тематические витрины", len(mart_catalog))
    c.metric("Источник данных", "Data Vault")
    d.metric("Пространственная детализация", "есть")

    st.subheader("Каталог витрин данных")
    st.dataframe(mart_catalog, use_container_width=True, height=330, hide_index=True)


# ==================== Tab 5: Transport ====================
with tabs[5]:
    st.header("🚗 Транспортная инфраструктура")

    if "access" in data and not data["access"].empty:
        acc = data["access"].copy()
        acc["district"] = acc["district"].apply(district_name_ru)

        acc = acc.rename(columns={
            "district": "Район",
            "roads_nearby": "Количество близлежащих дорог",
            "road_length_km": "Протяжённость дорог, км",
        })

        st.subheader("Доступность транспортной сети по районам")
        st.dataframe(acc, use_container_width=True)

        st.subheader("Протяжённость дорог по районам")
        st.bar_chart(acc.set_index("Район")["Протяжённость дорог, км"])
    else:
        st.warning("Нет данных о транспортной доступности.")

    if "roads" in data and not data["roads"].empty:
        roads = data["roads"].copy()
        roads["road_type"] = roads["road_type"].replace(road_ru)

        roads = roads.rename(columns={
            "road_type": "Тип дороги",
            "segments_count": "Количество сегментов",
            "total_length_km": "Общая протяжённость, км",
            "share_percent": "Доля, %",
        })

        st.subheader("Структура дорожной сети по типам дорог")
        st.dataframe(roads, use_container_width=True)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Количество сегментов по типам дорог**")
            st.bar_chart(roads.set_index("Тип дороги")["Количество сегментов"])

        with col2:
            st.markdown("**Протяжённость дорог по типам**")
            st.bar_chart(roads.set_index("Тип дороги")["Общая протяжённость, км"])

        st.subheader("Доля типов дорог в общей дорожной сети")
        st.bar_chart(roads.set_index("Тип дороги")["Доля, %"])
    else:
        st.warning("Нет данных о типах дорог.")


# ==================== Tab 6: Land use ====================
with tabs[6]:
    st.header("🏞️ Землепользование")

    if "landuse" in data and not data["landuse"].empty:
        lu = data["landuse"].copy()
        lu["landuse_type"] = lu["landuse_type"].replace(landuse_ru)

        lu = lu.rename(columns={
            "landuse_type": "Тип землепользования",
            "objects_count": "Количество объектов",
            "total_area_ha": "Общая площадь, га",
            "share_percent": "Доля, %",
        })

        overview_tab, district_tab = st.tabs(["Общая структура", "По районам"])

        with overview_tab:
            st.subheader("Структура землепользования на уровне мухафазы")
            st.dataframe(lu, use_container_width=True)

            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**Доля типов землепользования**")
                st.bar_chart(lu.set_index("Тип землепользования")["Доля, %"])
            with col2:
                st.markdown("**Площадь по типам землепользования**")
                st.bar_chart(lu.set_index("Тип землепользования")["Общая площадь, га"])

        with district_tab:
            if "landuse_by_district" in data and not data["landuse_by_district"].empty:
                lud = data["landuse_by_district"].copy()
                lud["district"] = lud["district"].apply(district_name_ru)
                lud["landuse_type"] = lud["landuse_type"].replace(landuse_ru)

                lud = lud.rename(columns={
                    "district": "Район",
                    "landuse_type": "Тип землепользования",
                    "objects_count": "Количество объектов",
                    "total_area_ha": "Площадь, га",
                    "share_percent": "Доля в районе, %",
                })

                districts = sorted(lud["Район"].unique())
                selected_district = st.selectbox("Район", districts)

                district_lu = lud[lud["Район"] == selected_district].copy()

                st.subheader(f"Землепользование: {selected_district}")
                st.dataframe(district_lu, use_container_width=True)

                st.bar_chart(district_lu.set_index("Тип землепользования")["Площадь, га"])

                st.subheader("Сравнение районов по выбранному типу землепользования")
                land_types = sorted(lud["Тип землепользования"].unique())
                selected_type = st.selectbox("Тип землепользования", land_types)

                compare = lud[lud["Тип землепользования"] == selected_type].copy()
                st.bar_chart(compare.set_index("Район")["Площадь, га"])
            else:
                st.info("Витрина землепользования по районам пока не загружена в схему dm.")
    else:
        st.warning("Нет данных о землепользовании.")


# ==================== Tab 7: Ecology ====================
with tabs[7]:
    st.header("🌿 Экологические показатели")

    if "eco" in data and not data["eco"].empty:
        eco = data["eco"].copy()
        eco["district"] = eco["district"].apply(district_name_ru)

        eco_ru = eco.rename(columns={
            "district": "Район",
            "year": "Год",
            "pm25": "PM2.5",
            "no2": "NO₂",
            "water_quality": "Качество воды",
            "ndvi": "NDVI",
        })

        st.subheader("Экологические данные по районам")
        st.dataframe(eco_ru, use_container_width=True)

        trend = eco.groupby("year")[["pm25", "no2", "water_quality", "ndvi"]].mean().reset_index()
        trend["year"] = trend["year"].astype(str)

        st.subheader("Динамика экологических показателей")

        a, b = st.columns(2)

        with a:
            st.markdown("**Динамика PM2.5**")
            st.line_chart(trend.set_index("year")["pm25"])

        with b:
            st.markdown("**Динамика NO₂**")
            st.line_chart(trend.set_index("year")["no2"])

        c, d = st.columns(2)

        with c:
            st.markdown("**Динамика качества воды**")
            st.line_chart(trend.set_index("year")["water_quality"])

        with d:
            st.markdown("**Динамика индекса растительности NDVI**")
            st.line_chart(trend.set_index("year")["ndvi"])
    else:
        st.warning("Нет экологических данных.")


# ==================== Tab 8: Population and economy ====================
with tabs[8]:
    st.header("👥 Население и экономика")

    if "population" in data and not data["population"].empty:
        pop = data["population"].copy()
        pop["district"] = pop["district"].apply(district_name_ru)

        pop_ru = pop.rename(columns={
            "district": "Район",
            "year": "Год",
            "population": "Численность населения",
            "employment_rate": "Уровень занятости, %",
            "unemployment_rate": "Уровень безработицы, %",
            "avg_income_index": "Индекс среднего дохода",
        })

        st.subheader("Показатели населения и экономики")
        st.dataframe(pop_ru, use_container_width=True)

        yearly = pop.groupby("year").agg({
            "population": "sum",
            "employment_rate": "mean",
            "unemployment_rate": "mean",
            "avg_income_index": "mean",
        }).reset_index()

        yearly = yearly.rename(columns={
            "year": "Год",
            "population": "Численность населения",
            "employment_rate": "Уровень занятости, %",
            "unemployment_rate": "Уровень безработицы, %",
            "avg_income_index": "Индекс среднего дохода",
        })

        yearly["Год"] = yearly["Год"].astype(str)

        st.subheader("Динамика социально-экономических показателей")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Динамика численности населения**")
            st.line_chart(yearly.set_index("Год")["Численность населения"])

        with col2:
            st.markdown("**Динамика уровня занятости**")
            st.line_chart(yearly.set_index("Год")["Уровень занятости, %"])

        col3, col4 = st.columns(2)

        with col3:
            st.markdown("**Динамика индекса среднего дохода**")
            st.line_chart(yearly.set_index("Год")["Индекс среднего дохода"])

        with col4:
            st.markdown("**Динамика уровня безработицы**")
            st.line_chart(yearly.set_index("Год")["Уровень безработицы, %"])

        latest_year = pop["year"].max()
        latest = pop[pop["year"] == latest_year].copy()

        latest_ru = latest.rename(columns={
            "district": "Район",
            "population": "Численность населения",
            "employment_rate": "Уровень занятости, %",
            "unemployment_rate": "Уровень безработицы, %",
            "avg_income_index": "Индекс среднего дохода",
        })

        st.subheader(f"Сравнение районов за {latest_year} год")

        col5, col6 = st.columns(2)

        with col5:
            st.markdown("**Численность населения по районам**")
            st.bar_chart(latest_ru.set_index("Район")["Численность населения"])

        with col6:
            st.markdown("**Уровень безработицы по районам**")
            st.bar_chart(latest_ru.set_index("Район")["Уровень безработицы, %"])
    else:
        st.warning("Нет данных о населении и экономике.")


# ==================== Tab 9: Map ====================
with tabs[9]:
    st.header("🗺️ Интерактивная карта пространственного развития")

    m = folium.Map(location=[34.95, 36.0], zoom_start=9, tiles=None)

    folium.TileLayer(
        "CartoDB positron",
        name="Базовая карта",
        control=True,
    ).add_to(m)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        show_population = st.checkbox("Население и экономика", value=False)

    with col2:
        show_landuse = st.checkbox("Землепользование", value=True)

    with col3:
        show_roads = st.checkbox("Дорожная сеть", value=True)

    with col4:
        show_ecology = st.checkbox("Экология", value=False)

    selected_eco_year = None

    if "eco" in data and not data["eco"].empty:
        available_years = sorted(data["eco"]["year"].unique())
        preferred_years = [2013, 2018, 2023]
        eco_year_options = [y for y in preferred_years if y in available_years]

        if not eco_year_options:
            eco_year_options = available_years

        selected_eco_year = st.selectbox(
            "Год экологического анализа",
            options=eco_year_options,
            index=len(eco_year_options) - 1,
        )

    # Population layer
    if show_population and districts_geojson and "population" in data and not data["population"].empty:
        latest_year = data["population"]["year"].max()
        latest_pop = data["population"][data["population"]["year"] == latest_year].copy()
        latest_pop["district_key"] = latest_pop["district"].apply(get_district_key)

        pop_dict = {
            row["district_key"]: {
                "population": row["population"],
                "employment_rate": row["employment_rate"],
                "unemployment_rate": row["unemployment_rate"],
                "avg_income_index": row["avg_income_index"],
            }
            for _, row in latest_pop.iterrows()
        }

        population_values = [
            v["population"]
            for v in pop_dict.values()
            if pd.notna(v["population"])
        ]

        min_population = min(population_values) if population_values else 0
        max_population = max(population_values) if population_values else 1

        population_group = folium.FeatureGroup(
            name=f"Численность населения и экономика, {latest_year}",
            show=True,
        )

        for feature in districts_geojson["features"]:
            name_raw = get_feature_name(feature)
            district_key = get_district_key(name_raw)
            ru_name = district_name_ru(name_raw)
            info = pop_dict.get(district_key)

            if info:
                area_km2 = feature_area_km2(feature)
                population = info["population"]
                density = population / area_km2 if area_km2 > 0 else 0
                employment = info["employment_rate"]
                unemployment = info["unemployment_rate"]
                income = info["avg_income_index"]

                color = get_population_color(population, min_population, max_population)

                tooltip = f"""
                <b>{ru_name}</b><br>
                Год: {latest_year}<br>
                Численность населения: {int(population):,} чел.<br>
                Плотность населения: {density:.1f} чел./км²<br>
                Уровень занятости: {employment:.1f}%<br>
                Уровень безработицы: {unemployment:.1f}%<br>
                Индекс среднего дохода: {income:.2f}
                """
            else:
                color = "#DDDDDD"
                tooltip = f"""
                <b>{ru_name}</b><br>
                Нет социально-экономических данных
                """

            folium.GeoJson(
                feature,
                style_function=lambda x, c=color: {
                    "fillColor": c,
                    "color": "#3B2A1A",
                    "weight": 2,
                    "fillOpacity": 0.25,
                },
                tooltip=folium.Tooltip(tooltip, sticky=True),
            ).add_to(population_group)

        population_group.add_to(m)

    # Land use layer
    if show_landuse and "landuse_map" in data and not data["landuse_map"].empty:
        landuse_group = folium.FeatureGroup(
            name="Землепользование",
            show=True,
        )

        landuse_map = data["landuse_map"].copy()

        for _, row in landuse_map.iterrows():
            try:
                if pd.isna(row["geom"]):
                    continue

                gj = json.loads(row["geom"])
                land_type = row["landuse_type"]
                ru_type = landuse_ru.get(land_type, land_type)
                color = landuse_colors.get(land_type, "#808080")

                folium.GeoJson(
                    gj,
                    style_function=lambda x, c=color: {
                        "color": c,
                        "fillColor": c,
                        "weight": 1.2,
                        "fillOpacity": 0.70,
                        "opacity": 0.95,
                    },
                    tooltip=folium.Tooltip(
                        f"<b>{ru_type}</b><br>Площадь: {row['area_ha']} га",
                        sticky=True,
                    ),
                ).add_to(landuse_group)

            except Exception as e:
                st.warning(f"Ошибка отображения землепользования: {e}")

        landuse_group.add_to(m)

    # Roads layer
    if show_roads and "roads_map" in data and not data["roads_map"].empty:
        roads_group = folium.FeatureGroup(
            name="Дорожная сеть по типам дорог",
            show=True,
        )

        roads_map = data["roads_map"].copy()

        if len(roads_map) > 2000:
            roads_map = roads_map.sort_values("length_km", ascending=False).head(2000)

        for _, row in roads_map.iterrows():
            try:
                gj = json.loads(row["geom"])
                style = get_road_style_map(row["road_type"])
                ru_type = road_ru.get(row["road_type"], row["road_type"])

                folium.GeoJson(
                    gj,
                    style_function=lambda x, s=style: {
                        "color": s["color"],
                        "weight": s["weight"],
                        "opacity": 0.90,
                    },
                    tooltip=f"""
                    <b>{ru_type}</b><br>
                    Протяжённость: {row["length_km"]} км
                    """,
                ).add_to(roads_group)

            except Exception:
                pass

        roads_group.add_to(m)

    # Ecology layer
    if (
        show_ecology
        and districts_geojson
        and "eco" in data
        and not data["eco"].empty
        and selected_eco_year is not None
    ):
        eco_year = data["eco"][data["eco"]["year"] == selected_eco_year].copy()
        eco_year["district_key"] = eco_year["district"].apply(get_district_key)
        eco_year["eco_pressure"] = eco_year.apply(get_ecology_pressure, axis=1)

        pressure_dict = {
            row["district_key"]: {
                "pm25": row["pm25"],
                "no2": row["no2"],
                "water_quality": row["water_quality"],
                "ndvi": row["ndvi"],
                "eco_pressure": row["eco_pressure"],
            }
            for _, row in eco_year.iterrows()
        }

        pressure_values = [
            v["eco_pressure"]
            for v in pressure_dict.values()
            if pd.notna(v["eco_pressure"])
        ]

        min_pressure = min(pressure_values) if pressure_values else 0
        max_pressure = max(pressure_values) if pressure_values else 1

        ecology_group = folium.FeatureGroup(
            name=f"Экологическое состояние, {selected_eco_year}",
            show=True,
        )

        for feature in districts_geojson["features"]:
            name_raw = get_feature_name(feature)
            district_key = get_district_key(name_raw)
            ru_name = district_name_ru(name_raw)
            info = pressure_dict.get(district_key)

            if info:
                pressure = info["eco_pressure"]
                color = get_ecology_gray_color(pressure, min_pressure, max_pressure)

                tooltip = f"""
                <b>{ru_name}</b><br>
                Год: {selected_eco_year}<br>
                PM2.5: {info["pm25"]}<br>
                NO₂: {info["no2"]}<br>
                Качество воды: {info["water_quality"]}<br>
                NDVI: {info["ndvi"]}<br>
                Экологическая нагрузка: {pressure:.2f}
                """
            else:
                color = "#DDDDDD"
                tooltip = f"""
                <b>{ru_name}</b><br>
                Нет экологических данных
                """

            folium.GeoJson(
                feature,
                style_function=lambda x, c=color: {
                    "fillColor": c,
                    "color": "#2C3E50",
                    "weight": 2,
                    "fillOpacity": 0.50,
                },
                tooltip=folium.Tooltip(tooltip, sticky=True),
            ).add_to(ecology_group)

        ecology_group.add_to(m)

    # Administrative boundaries are always on top
    if districts_geojson:
        boundaries_group = folium.FeatureGroup(
            name="Административные границы",
            show=True,
        )

        for feature in districts_geojson["features"]:
            name_raw = get_feature_name(feature)
            ru_name = district_name_ru(name_raw)

            folium.GeoJson(
                feature,
                style_function=lambda x: {
                    "fill": False,
                    "color": "#111111",
                    "weight": 2.8,
                    "opacity": 1,
                },
                tooltip=folium.Tooltip(f"<b>{ru_name}</b>", sticky=True),
            ).add_to(boundaries_group)

        boundaries_group.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)
    folium_static(m, width=1200, height=700)


# ==================== End ====================
st.sidebar.markdown("---\n**Прототип пространственного хранилища данных — Тартус**")