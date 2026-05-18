"""
weather_energy_pipeline.py
__________
Airflow DAG — Johannesburg Weather vs Energy Commodity Pipeline

Schedule: 08:00 Africa/Johannesburg daily
Tasks:    extract_weather --> extract_energy

Design decisions that I made:
- Weather runs before energy so that if weather extraction fails,
  we can't write a half complete day of data. Both succeed or
  the day's run is marked failed and retried cleanly.
- There'll be retries = 2, handling API failures (timeouts, rate limits)
- email_on_failure = True means a persistent failure surfaces
  immediately rather than being discovered the next morning
  when someone opens the dashboard.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os


# I made the extractors folder importable inside the Airflow container.
# This path matches the volume mount defined in docker-compose.yml.
sys.path.insert(0, "/opt/airflow/extractors")

import weather_extractor
import energy_extractor

# Default arguments applied to every task in this DAG

default_args = {
    "owner":             "Mandla_M",
    "depends_on_past":   False,       # each day's run is independent
    "email":             [],
    "email_on_failure":  False,       # when set to true in production ths alerts when all retries fail
    "email_on_retry":    False,       # only the final fail sends alert notification
    "retries":           2,           # retries twice before declaring failure
    "retry_delay":       timedelta(minutes=5),  # waits 5 min between retries
}




# The DAG definition

with DAG(
    dag_id="weather_energy_pipeline",
    default_args=default_args,
    description=(
        "Daily pipeline: Johannesburg weather vs energy commodity prices. "
        "Extracts from OpenWeatherMap + Open-Meteo and CoinGecko, "
        "validates, saves to file lake, loads to PostgreSQL."
    ),
    # Runs at 06:00 UTC = 08:00 Johannesburg time (UTC+2)
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,      # don't backfill missed runs on first deploy
    tags=["weather", "energy", "portfolio"],
) as dag:




    # Task 1: Extract weather

    extract_weather = PythonOperator(
        task_id="extract_weather",
        python_callable=weather_extractor.run,
        doc_md="""
        ### Extract Weather
        Pulls current conditions from OpenWeatherMap and precipitation
        data from Open-Meteo for Johannesburg and validates the response
        against a data contract before writing to PostgreSQL.
        Saves raw JSON to the file lake before loading to the database.
        """,
    )




    # Task 2: Extract energy

    extract_energy = PythonOperator(
        task_id="extract_energy",
        python_callable=energy_extractor.run,
        doc_md="""
        ### Extract Energy Commodity Prices
        Pulls Ethereum USD price from CoinGecko as
        energy-sensitive commodity proxies. Asset is fetched
        in a single API call and validates and upserts two rows per day —
        one per asset — into PostgreSQL.
        Saves raw JSON to the file lake before loading to the database.
        """,
    )





    # Task dependency : the execution order
    # weather must complete successfully before energy begins.
    # If weather fails, energy does not run and the day is incomplete
    # and Airflow marks the entire DAG run as failed.

    extract_weather >> extract_energy


