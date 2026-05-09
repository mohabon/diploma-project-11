# Пространственное хранилище данных мухафазы Тартус

Прототип пространственного хранилища данных на основе PostgreSQL/PostGIS, Data Vault 2.0 и аналитических витрин данных.

## Основные возможности

- PostgreSQL + PostGIS
- Raw Layer → Data Vault → Data Marts
- ETL/ELT на Python
- Airflow orchestration
- Data Quality checks
- RBAC в PostgreSQL
- Streamlit dashboard
- Prometheus + Grafana monitoring
- CI через GitHub Actions

## Запуск проекта

### Запуск контейнеров

```bash
docker compose up -d
```

### Загрузка исходных данных

```bash
docker compose exec etl python etl/load_raw.py
```

### Построение Data Vault и Data Marts

```bash
docker compose exec etl python etl/transform_business.py
```

### Проверки качества данных

```bash
docker compose exec etl python etl/dq/test_cadastre.py
```

### Проверка заполнения слоёв

```bash
docker compose exec etl python /app/verify_data.py
### Запуск dashboard

```bash
docker compose exec etl streamlit run etl/dashboard.py --server.address 0.0.0.0
```

## Доступ к сервисам

| Сервис | Адрес |
|---|---|
| Streamlit Dashboard | http://localhost:8501 |
| Airflow | http://localhost:8080 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |

## Основные сервисы

- PostgreSQL/PostGIS
- Airflow
- Streamlit
- Prometheus
- Grafana
- postgres-exporter
- cAdvisor

## Архитектура

```text
RAW → Data Vault → Data Marts → Dashboard
```
