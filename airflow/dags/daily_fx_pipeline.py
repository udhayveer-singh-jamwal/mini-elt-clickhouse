"""
Airflow DAG: Daily FX Ingestion & Transformation Pipeline
Batch source: Frankfurter Currency Exchange API (base EUR) -> ClickHouse Bronze -> dbt Silver & Gold
"""

from datetime import datetime, timedelta
import json
import os
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

def fetch_and_insert_fx():
    """
    Pulls latest daily exchange rates from Frankfurter API
    and loads the raw JSON payload into ClickHouse bronze.fx_raw_events.
    Includes custom User-Agent to prevent HTTP 403 Forbidden.
    """
    import clickhouse_connect

    url = "https://api.frankfurter.app/latest"
    headers = {"User-Agent": "MiniELT-Airflow/1.0"}
    
    print(f"Fetching FX data from {url}...")
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    raw_payload = response.text
    parsed = response.json()
    base_curr = parsed.get("base", "EUR")
    print(f"Retrieved rates for base currency {base_curr} date {parsed.get('date')}")

    ch_host = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
    ch_port = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
    ch_user = os.environ.get("CLICKHOUSE_USER", "default")
    ch_pass = os.environ.get("CLICKHOUSE_PASSWORD", "")

    print(f"Connecting to ClickHouse at {ch_host}:{ch_port}...")
    client = clickhouse_connect.get_client(
        host=ch_host,
        port=ch_port,
        username=ch_user,
        password=ch_pass
    )

    client.command("""
        CREATE DATABASE IF NOT EXISTS bronze;
        CREATE TABLE IF NOT EXISTS bronze.fx_raw_events (
            base_currency String,
            ingested_at DateTime DEFAULT now(),
            raw_response String
        ) ENGINE = MergeTree 
        ORDER BY (base_currency, ingested_at);
    """)

    client.insert(
        table="fx_raw_events",
        database="bronze",
        data=[[base_curr, raw_payload]],
        column_names=["base_currency", "raw_response"]
    )
    print("Successfully inserted raw FX event into bronze.fx_raw_events")

with DAG(
    dag_id='daily_fx_pipeline',
    default_args=default_args,
    description='Daily FX rate ingestion from Frankfurter API into ClickHouse bronze and dbt transformation',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['elt', 'clickhouse', 'dbt', 'fx'],
) as dag:

    # Task 1: Fetch from Frankfurter API and insert into ClickHouse bronze
    ingest_fx_task = PythonOperator(
        task_id='ingest_fx_raw_to_bronze',
        python_callable=fetch_and_insert_fx,
    )

    # Task 2: Trigger dbt transformations for silver stg_fx_rates and downstream gold marts
    dbt_fx_task = BashOperator(
        task_id='dbt_run_fx_and_downstream',
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt --select stg_fx_rates+',
    )

    ingest_fx_task >> dbt_fx_task
