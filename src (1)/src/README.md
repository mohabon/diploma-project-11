# Tartus DWH Prototype (Data Vault + PostGIS)

This is a **runnable prototype** of a spatial data warehouse using **PostgreSQL/PostGIS** and simple **ETL** scripts.
It demonstrates:
- Raw → Business (Data Vault) → Data Marts flow
- Spatial join (parcels ↔ settlements)
- Data Quality check (geometry validity)
- Ready to run with Docker Compose

## Quick start

1. Install Docker and Docker Compose.
2. Copy env:
   ```bash
   cp .env.example .env
   # optionally edit POSTGRES_PASSWORD in .env
   ```
3. Start DB and ETL container:
   ```bash
   docker compose up -d db etl
   ```
4. Load sample data to RAW and transform to DV + DM:
   ```bash
   docker compose exec etl python etl/load_raw.py
   docker compose exec etl python etl/transform_business.py
   ```
5. Run data-quality checks:
   ```bash
   docker compose exec etl python etl/dq/test_cadastre.py
   ```

## Connect to database

- Host: `localhost`
- Port: `${POSTGRES_PORT}` (default 5432)
- DB: `${POSTGRES_DB}`
- User: `${POSTGRES_USER}`
- Password: `${POSTGRES_PASSWORD}`

## Schemas
- `raw` — raw ingestion
- `dv` — Data Vault hubs/links/satellites
- `dm` — data marts (star-schema style)

Created on 2025-08-23.
