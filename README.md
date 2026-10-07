# Mini ELT Project: 2-Source ClickHouse Pipeline (Medallion Architecture)

An end-to-end production-grade ELT data pipeline into **ClickHouse** following the **Medallion Architecture (Bronze → Silver → Gold)**.

- **Real-Time Stream**: **Apache NiFi** polls the **Open-Meteo Weather API** every 60s for 5 major cities and streams raw payloads into ClickHouse Bronze via parameterized HTTP queries.
- **Batch Pipeline**: **Apache Airflow** schedules daily ingestion from the **Frankfurter Currency Exchange API** into ClickHouse Bronze, then immediately orchestrates **dbt** models to curate Silver and Gold layers.
- **Transformations**: **dbt** (`dbt-clickhouse` adapter) parses, cleans, deduplicates with `ReplacingMergeTree`, aggregates with `SummingMergeTree`, and produces cross-source analytical marts.

---

## Architecture Overview

```
                       +---------------------------------------+
                       | Open-Meteo API (5 Cities, 60s Poll)   |
                       +---------------------------------------+
                                           |
                                           v
                       +---------------------------------------+
                       | Apache NiFi (Real-Time Ingestion Flow)|
                       +---------------------------------------+
                                           |
                                           v
                                ClickHouse (Bronze Layer)
                   [bronze.weather_raw_events]  [bronze.fx_raw_events]
                                           ^               ^
                                           |               |
                                    (Stream)      (Daily Batch)
                                                           |
                                       +-----------------------+
                                       | Airflow PythonOperator|
                                       +-----------------------+
                                                   |
                                       +-----------------------+
                                       | Frankfurter FX API    |
                                       +-----------------------+
                                           |
                                           v
                               ClickHouse (Silver Layer)
                   [silver.weather_readings]    [silver.fx_rates]
                                           |
                                           v (dbt models)
                               ClickHouse (Gold Layer)
                   [gold.weather_hourly_summary] [gold.fx_daily_summary]
                                           \               /
                                            v             v
                                  [gold.daily_global_snapshot]
```

---

## Data Sources & Layers

### 1. Sources (100% Free, No API Keys)
- **Open-Meteo Weather API**:
  - `https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m&timezone=Asia%2FKolkata`
  - 5 Cities:
    - **Jammu** (32.7266, 74.857)
    - **Delhi** (28.6139, 77.2090)
    - **Mumbai** (19.0760, 72.8777)
    - **London** (51.5074, -0.1278)
    - **New York** (40.7128, -74.0060)
- **Frankfurter Currency Exchange API**:
  - `https://api.frankfurter.app/latest` (daily rates, base EUR)

### 2. Medallion Storage in ClickHouse
- **Bronze (Raw)**:
  - `bronze.weather_raw_events`: Raw JSON payload per city + ingestion timestamp.
  - `bronze.fx_raw_events`: Raw JSON rates object per base currency.
- **Silver (Cleaned & Deduplicated)**:
  - `silver.weather_readings`: Extracted metrics with `ReplacingMergeTree(ingested_at) ORDER BY (city, observed_at)`.
  - `silver.fx_rates`: Exploded currency rate pairs with `ReplacingMergeTree(ingested_at) ORDER BY (base_currency, quote_currency, rate_date)`.
- **Gold (Aggregated Marts)**:
  - `gold.weather_hourly_summary`: `SummingMergeTree` hourly weather rollups (min/max/avg temp, avg humidity, count).
  - `gold.fx_daily_summary`: Daily FX rates with day-over-day percentage change and division-by-zero safeguards.
  - `gold.daily_global_snapshot`: Cross-pipeline join correlating city average temperatures with EUR/USD and EUR/INR rates by date.

---

## Directory Structure

```
mini-elt-clickhouse/
├── docker-compose.yml          # ClickHouse, NiFi, Postgres, Airflow Webserver & Scheduler
├── .env.example                # Sample environment variables
├── README.md                   # Complete documentation
├── clickhouse/
│   └── init_db.sql             # Verified DDL for Bronze, Silver, and Gold
├── nifi/
│   ├── weather_flow_template.xml    # NiFi XML template
│   ├── setup_nifi_flow.py           # Automated NiFi flow creator via NiFi REST API
│   └── weather_nifi_simulator.py    # Python real-time streaming twin of NiFi flow
├── airflow/
│   └── dags/
│       └── daily_fx_pipeline.py     # Airflow DAG (Frankfurter Ingest -> dbt run stg_fx_rates+)
├── dbt/
│   ├── profiles.yml            # dbt-clickhouse connection config
│   ├── dbt_project.yml         # Project configurations (models, schemas)
│   ├── macros/
│   │   └── generate_schema_name.sql # Custom schema name override (prevents silver_silver)
│   └── models/
│       ├── staging/
│       │   ├── sources.yml
│       │   ├── stg_weather_readings.sql
│       │   └── stg_fx_rates.sql
│       └── marts/
│           ├── weather_hourly_summary.sql
│           ├── fx_daily_summary.sql
│           └── daily_global_snapshot.sql
└── scripts/
    └── verify_pipeline.py      # End-to-end verification and data validation script
```

---

## Quickstart Guide

### 1. Start the Docker Stack
```bash
docker compose up -d
```

Service URLs and credentials:
| Service | URL | Credentials |
| :--- | :--- | :--- |
| **ClickHouse HTTP** | `http://localhost:8123` | default / (no password) |
| **ClickHouse Native** | `localhost:9000` | default / (no password) |
| **Apache NiFi UI** | `http://localhost:8080/nifi` | None (configured for HTTP dev) |
| **Airflow Web UI** | `http://localhost:8085` | `admin` / `admin` |

### 2. Configure Real-Time NiFi Weather Stream
Once NiFi is online, run the automated provisioning script:
```bash
python nifi/setup_nifi_flow.py
```
*Alternatively, you can run `python nifi/weather_nifi_simulator.py` to stream weather data directly into ClickHouse without waiting for NiFi.*

### 3. Run Airflow Daily FX Ingestion
1. Open the Airflow Web UI at `http://localhost:8085`
2. Unpause and trigger `daily_fx_pipeline`
3. The DAG will:
   - Fetch the latest Frankfurter rates and write to `bronze.fx_raw_events`
   - Trigger `dbt run --select stg_fx_rates+` to build `silver.fx_rates`, `gold.fx_daily_summary`, and `gold.daily_global_snapshot`.

### 4. Run End-to-End Verification
To verify table creation, live API fetches, transformations, and gold marts at once:
```bash
python scripts/verify_pipeline.py
```

---

## Key Engineering Fixes & Safeguards Included

1. **Frankfurter API 403 Forbidden Fix**:
   - The Frankfurter API blocks standard automated Python requests. We added a custom `User-Agent: MiniELT-Airflow/1.0` header in the Airflow operator and scripts to ensure reliable 200 responses.
2. **dbt Custom Schema Override (`generate_schema_name.sql`)**:
   - Default dbt behavior concatenates target and custom schemas (creating `silver_gold`). Our custom macro cleanly writes models to `silver` and `gold` databases directly.
3. **Division-by-Zero Guard in FX Mart**:
   - `CASE WHEN prev_day_rate IS NULL OR prev_day_rate = 0 THEN NULL ...` prevents `inf` and crashes on the first day or zero baseline.
4. **ClickHouse Incomplete Payload Guard**:
   - `WHERE JSONHas(raw_response, 'current', 'time') AND JSONHas(raw_response, 'current', 'temperature_2m')` prevents `parseDateTimeBestEffort` crashes on missing weather fields.
5. **NiFi Parameterized HTTP Query**:
   - Uses `INSERT INTO ... SELECT {city:String} ... &param_city=...` to completely eliminate JSON character escaping bugs.
