# Analytical Design, Architecture & ClickHouse Special Query Rationale
**Author:** Udhayveer Singh Jamwal  
**Task:** Mini ELT Project Experiment (Apache NiFi + Airflow + dbt + ClickHouse Medallion Architecture)

---

## 1. Executive Summary & Objective

This project builds an end-to-end 2-source ELT pipeline using the **Medallion Architecture (Bronze → Silver → Gold)** loaded into **ClickHouse**:
1. **Real-Time Stream Source**: **Open-Meteo Weather API** polled every 60 seconds across 5 major global cities (Jammu, Delhi, Mumbai, London, New York) via **Apache NiFi**.
2. **Batch Source**: **Frankfurter Currency Exchange API** ingested daily (base EUR) via **Apache Airflow** DAG and transformed using **dbt** (`dbt-clickhouse` adapter).
3. **ClickHouse Analytical Core**: Fully leveraging ClickHouse-specific storage engines (`ReplacingMergeTree`, `SummingMergeTree`), array/JSON processing (`ARRAY JOIN JSONExtractKeysAndValues`), and ClickHouse-exclusive analytical query combinators (`anyIf`, `argMax`, `lagInFrame`).

---

## 2. Analytical Reasoning for the Architecture Design

### A. Why Medallion Architecture in ClickHouse?
ClickHouse is a columnar OLAP database optimized for append-only, high-throughput batch writes and vectorised queries. Traditional row-level `UPDATE` or `DELETE` operations are anti-patterns in OLAP engines. The Medallion architecture elegantly bridges this:

1. **Bronze (Raw Ingestion)**:
   - Stores raw API responses as unmodified JSON strings along with `ingested_at` and natural keys.
   - **Analytical Benefit**: Complete auditability, immutability, and protection against upstream schema drifts. If an API adds or alters fields, raw data is never lost, and downstream models can be re-run historically without re-fetching.

2. **Silver (Cleaned, Typed & Deduplicated)**:
   - Normalizes JSON payloads into strongly-typed tabular columns.
   - Implements deduplication using `ReplacingMergeTree(ingested_at) ORDER BY (natural_keys)`.
   - **Analytical Benefit**: Removes out-of-order and duplicate stream events without issuing row-level `DELETE` statements. Queries can use the `FINAL` modifier or let ClickHouse background parts merge organically.

3. **Gold (Aggregated Business Marts & Cross-Source Joins)**:
   - Pre-aggregates metrics to match dashboard consumption patterns:
     - `weather_hourly_summary`: Hourly rollups for trend and volatility analysis.
     - `fx_daily_summary`: Daily exchange rate shifts and day-over-day changes.
     - `daily_global_snapshot`: Cross-pipeline analytical mart correlating temperature trends with currency markets.
   - **Analytical Benefit**: Dashboards query pre-computed rollups in sub-milliseconds, preventing repetitive CPU-intensive scans over billions of raw rows.

---

## 3. "Special" Analytical Queries Unique to ClickHouse

As requested, this architecture takes full advantage of ClickHouse-exclusive analytical features that do not exist or require inefficient workarounds in conventional databases (Postgres, MySQL, Snowflake, BigQuery):

### Feature 1: Aggregate Function Combinators (`-If` Family)
- **ClickHouse Syntax**: `anyIf(rate, quote_currency = 'INR')` and `avgIf(...)`
- **Why It's Special**: In ANSI SQL, pivoting metrics from rows into columns requires repetitive `CASE WHEN ... THEN ... ELSE NULL END` blocks or subqueries with `JOIN`. ClickHouse combinators apply conditional aggregation at C++ vector execution speed, compiling directly into SSE/AVX CPU instructions.
- **Project Example** (`gold.daily_global_snapshot`):
  ```sql
  SELECT 
      toDate(w.observed_hour) AS snapshot_date, 
      w.city, 
      avg(w.avg_temperature_c) AS avg_temperature_c,
      anyIf(f_inr.rate, f_inr.quote_currency = 'INR') AS eur_inr_rate,
      anyIf(f_usd.rate, f_usd.quote_currency = 'USD') AS eur_usd_rate
  FROM gold.weather_hourly_summary w
  LEFT JOIN silver.fx_rates f_inr ON toDate(w.observed_hour) = f_inr.rate_date AND f_inr.quote_currency = 'INR'
  LEFT JOIN silver.fx_rates f_usd ON toDate(w.observed_hour) = f_usd.rate_date AND f_usd.quote_currency = 'USD'
  GROUP BY snapshot_date, w.city;
  ```

---

### Feature 2: In-Engine Dynamic Array Explosion (`ARRAY JOIN`)
- **ClickHouse Syntax**: `ARRAY JOIN JSONExtractKeysAndValues(raw_response, 'rates', 'Float64') AS kv`
- **Why It's Special**: Traditional databases require `CROSS JOIN UNNEST(...)` or complex JSON table functions that copy rows and create large intermediate memory allocations. ClickHouse's `ARRAY JOIN` is a native execution pipelining primitive that flattens arrays into columnar streams in memory with zero intermediate serialization overhead.
- **Project Example** (`silver.fx_rates` / `stg_fx_rates.sql`):
  ```sql
  SELECT 
      base_currency, 
      kv.1 AS quote_currency, 
      kv.2 AS rate,
      JSONExtractString(raw_response, 'date')::Date AS rate_date, 
      ingested_at
  FROM bronze.fx_raw_events
  ARRAY JOIN JSONExtractKeysAndValues(raw_response, 'rates', 'Float64') AS kv;
  ```

---

### Feature 3: `ReplacingMergeTree` with `FINAL` Resolution
- **ClickHouse Syntax**: `ENGINE = ReplacingMergeTree(ingested_at) ORDER BY (city, observed_at)`
- **Why It's Special**: Streaming data pipelines frequently encounter at-least-once delivery duplicates. Instead of running expensive `MERGE` or `UPSERT` queries upon every ingest (which causes write amplification and locks), ClickHouse accepts raw appends and resolves deduplication asynchronously during background part merges. Adding `FINAL` in analytical queries guarantees exact deduplication on-the-fly.
- **Project Example** (`gold.fx_daily_summary`):
  ```sql
  SELECT base_currency, quote_currency, rate_date, rate,
      if(row_number() OVER (PARTITION BY base_currency, quote_currency ORDER BY rate_date) = 1,
         NULL, 
         lagInFrame(rate) OVER (PARTITION BY base_currency, quote_currency ORDER BY rate_date)
      ) AS prev_day_rate
  FROM silver.fx_rates FINAL;
  ```

---

### Feature 4: ClickHouse Window Frame Optimization (`lagInFrame`)
- **ClickHouse Syntax**: `lagInFrame(rate) OVER (...)`
- **Why It's Special**: While standard SQL uses `LAG()`, ClickHouse provides `lagInFrame()` which is specifically vectorized to operate strictly within the defined window frame without allocating extra boundary buffers, significantly reducing memory consumption for multi-partition timeseries analysis.

---

### Feature 5: Native JSON Path Extraction with Safety Guards
- **ClickHouse Syntax**: `JSONExtractFloat(raw_response, 'current', 'temperature_2m')` and `JSONHas(...)`
- **Why It's Special**: Enables high-performance schematization directly on raw JSON strings without pre-parsing into specialized JSON document types, while `JSONHas()` acts as a safeguard against incomplete or corrupted payload crashes.

---

## 4. Pipeline Implementation Summary

| Pipeline Component | Technology | Frequency / Trigger | Target Layer |
| :--- | :--- | :--- | :--- |
| **Real-Time Stream** | Apache NiFi | 60 sec Timer-driven | `bronze.weather_raw_events` |
| **Batch Ingestion** | Apache Airflow | Daily (`@daily`) | `bronze.fx_raw_events` |
| **Silver Transformation** | dbt (`dbt-clickhouse`) | Incremental trigger | `silver.weather_readings`, `silver.fx_rates` |
| **Gold Aggregations** | dbt (`dbt-clickhouse`) | Table replacement | `gold.weather_hourly_summary`, `gold.fx_daily_summary` |
| **Cross-Source Mart** | dbt (`dbt-clickhouse`) | Table replacement | `gold.daily_global_snapshot` |

---

## 5. Verified Bug Fixes & Resiliency Engineering

1. **Frankfurter API 403 Forbidden Fix**:
   - The Frankfurter API rejects default automated Python clients. Fixed by adding a custom `User-Agent: MiniELT-Airflow/1.0` header in the Airflow operator.
2. **dbt Custom Schema Override (`generate_schema_name.sql`)**:
   - Default dbt naming creates `silver_gold` when a custom schema is passed. Fixed with a custom macro ensuring models map strictly to `silver` and `gold`.
3. **Division-by-Zero Guard**:
   - In `fx_daily_summary`, the initial rate record lacks a previous day value. Fixed with a `CASE WHEN prev_day_rate IS NULL OR prev_day_rate = 0 THEN NULL ...` guard to avoid `inf` division errors.
4. **NiFi Parameterized HTTP Query**:
   - Ingests via ClickHouse HTTP endpoint using parameterized query placeholders (`{city:String}` and `param_city=...`), completely eliminating JSON quote escaping errors.
