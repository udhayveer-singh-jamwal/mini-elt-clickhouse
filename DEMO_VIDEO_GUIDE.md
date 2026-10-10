# Demo Video Guide & Presentation Script
**Project:** Mini ELT Project (NiFi + Airflow + dbt + ClickHouse Medallion Architecture)  
**Presenter:** Udhayveer Singh Jamwal  
**Target Duration:** 3 to 5 Minutes  

---

## 🎬 1. Pre-Recording Checklist & Setup

Before starting your screen recording:

1. **Start Docker Desktop**:
   - Open **Docker Desktop** from your Windows Start Menu and wait until the whale icon turns steady green.
2. **Start the Stack**:
   - Open PowerShell and run:
     ```powershell
     cd C:\Users\uvjam\.gemini\antigravity\scratch\mini-elt-clickhouse
     docker compose up -d
     ```
3. **Open Browser Tabs**:
   - Tab 1: Your GitHub Repository (`https://github.com/<your-username>/mini-elt-clickhouse`)
   - Tab 2: Apache NiFi UI (`http://localhost:8080/nifi`)
   - Tab 3: Apache Airflow UI (`http://localhost:8085`) — Login: `admin` / `admin`
   - Tab 4: ClickHouse Play Web UI or DBeaver (`http://localhost:8123/play` or terminal)
4. **Recording Tool**:
   - Use **Loom**, **OBS Studio**, or Windows built-in screen recorder (**Windows Key + Alt + R** or **Windows Key + G**).

---

## 🎙️ 2. Word-by-Word Video Script (Step-by-Step)

### ⏱️ Segment 1: Introduction & Architecture (0:00 - 0:45)
**Screen to Show:** Architecture diagram in `README.md` or GitHub repo.

> *"Hello everyone and hello Ahmad Aufa. Today I am presenting my Mini ELT Project built with Medallion Architecture in ClickHouse.*  
> *The objective of this project is to implement a dual-source ELT pipeline using two free, public APIs:*  
> *1. A real-time streaming pipeline using Apache NiFi that polls the Open-Meteo Weather API every 60 seconds across 5 major global cities.*  
> *2. A batch pipeline scheduled daily with Apache Airflow that ingests exchange rates from the Frankfurter Currency API, followed by dbt transformations.*  
> *All data lands in ClickHouse through Bronze, Silver, and Gold layers, heavily utilizing ClickHouse-specific analytical features."*

---

### ⏱️ Segment 2: Real-Time Stream via Apache NiFi (0:45 - 1:45)
**Screen to Show:** Apache NiFi Canvas (`http://localhost:8080/nifi`) and ClickHouse terminal/UI.

> *"First, let's look at the real-time stream in Apache NiFi.*  
> *(Point to NiFi processors)*  
> *Here we have our pipeline chains for Jammu, Delhi, Mumbai, London, and New York.*  
> *Every 60 seconds, GenerateFlowFile triggers an HTTP GET to Open-Meteo.*  
> *The raw response is extracted and directly streamed to ClickHouse using an HTTP POST with parameterized queries. This eliminates JSON escaping issues entirely.*  
> *(Switch to terminal or ClickHouse Web UI)*  
> *Let's check ClickHouse: `SELECT count(), city, observed_at, temperature_c FROM silver.weather_readings`.*  
> *As you can see, records are continuously streaming in, deduplicated seamlessly using ClickHouse's `ReplacingMergeTree` engine."*

---

### ⏱️ Segment 3: Batch Pipeline via Airflow + dbt (1:45 - 2:45)
**Screen to Show:** Airflow Web UI (`http://localhost:8085`).

> *"Next is our batch ingestion and dbt transformation orchestrated by Apache Airflow.*  
> *(Show DAG `daily_fx_pipeline`)*  
> *Our DAG has two tasks:*  
> *Task 1: A PythonOperator that fetches the latest daily currency rates from Frankfurter API and inserts the raw payload into `bronze.fx_raw_events` using `clickhouse-connect`.*  
> *(Click 'Trigger DAG' to show live execution)*  
> *Notice that Task 1 succeeded. Now Task 2 automatically triggers `dbt run --select stg_fx_rates+`.*  
> *dbt parses the raw JSON, explodes the rates array into tabular rows, and builds our downstream Silver and Gold marts."*

---

### ⏱️ Segment 4: ClickHouse "Special" Analytical Queries (2:45 - 4:00)
**Screen to Show:** ClickHouse queries in terminal or SQL editor.

> *"Now let's examine the ClickHouse-exclusive analytical queries that make this pipeline powerful:*  
> *1. **ARRAY JOIN with JSON Extraction**: In `stg_fx_rates`, we use `ARRAY JOIN JSONExtractKeysAndValues(...)` to explode nested JSON rate objects into relational rows in a single in-memory scan.*  
> *2. **The `-If` Combinator Family**: In our Gold cross-source mart `daily_global_snapshot`, we use ClickHouse's unique `anyIf()` combinator, such as `anyIf(rate, quote_currency='INR')`, which pivots currencies at C++ vector execution speed without complex subqueries.*  
> *3. **Vectorized Window Optimization (`lagInFrame`)**: In `fx_daily_summary`, we calculate day-over-day percentage changes using `lagInFrame()` with a division-by-zero safeguard.*  
> *4. **Deduplication Engine**: All silver tables utilize `ReplacingMergeTree(ingested_at)` with `FINAL` resolution, eliminating expensive update queries."*

---

### ⏱️ Segment 5: Verification & Conclusion (4:00 - 4:30)
**Screen to Show:** Terminal running `python scripts/verify_pipeline.py` or final Gold Mart output.

> *(Show verified output table of `daily_global_snapshot`)*  
> *"Here is the final Gold mart correlating daily city temperatures with EUR/USD and EUR/INR rates.*  
> *All DDLs, dbt models, Airflow DAGs, and NiFi flows are documented, tested, and published to GitHub.*  
> *Thank you!"*

---

## 📤 3. Submission Message Template (For Teams)

Once your video is recorded and uploaded (to Google Drive, Loom, or YouTube Unlisted):

```text
Hi Ahmad Aufa,

I have completed the repository and recorded the demo video for the Mini ELT Project:

📁 GitHub Repository: https://github.com/<YOUR_USERNAME>/mini-elt-clickhouse
🎥 Demo Video Link: <YOUR_VIDEO_LINK_HERE>

Video Summary:
- Real-time NiFi streaming from Open-Meteo Weather API into Bronze & Silver.
- Batch Airflow DAG ingesting Frankfurter FX rates + automated dbt transformation.
- Demonstration of ClickHouse-exclusive features (anyIf combinators, ARRAY JOIN, ReplacingMergeTree, lagInFrame).
- Live query inspection of the Gold cross-source analytical mart (daily_global_snapshot).

Please let me know if you have any feedback!
```
