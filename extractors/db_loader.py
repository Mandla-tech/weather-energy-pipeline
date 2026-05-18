# db_loader.py
# Handles DB stuff for the pipeline
# I tried to keep this idempotent so reruns don’t mess things up (hopefully…)

import os
import logging
import psycopg2
from psycopg2.extras import Json
from datetime import date

# --- logging setup (might tweak format later) ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger("db_loader")


def get_connection():
    """
    Create a DB connection from env vars.
    Will complain loudly if something is missing.
    """
    needed_vars = [
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_DB",
        "POSTGRES_HOST",
        "POSTGRES_PORT"
    ]

    # checking env manually instead of just letting connect fail
    missing_stuff = []
    for v in needed_vars:
        if not os.getenv(v):
            missing_stuff.append(v)

    if len(missing_stuff) > 0:
        raise EnvironmentError("Missing env vars: " + str(missing_stuff))

    # grabbing vars one by one
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    db = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    pwd = os.getenv("POSTGRES_PASSWORD")

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=pwd
    )

    logger.info("Connected to DB")
    return conn



def upsert_weather(record: dict, raw_payload: dict):
    """
    Insert/update weather row.
    If it already exists for the same city + date, it just updates it.
    (so rerunning jobs shouldn't duplicate data)
    """

    query = """
        INSERT INTO raw_weather (
            city, country, weather_date,
            temp_celsius, feels_like, humidity_pct,
            wind_speed_ms, weather_main, weather_desc,
            precipitation_mm, raw_payload
        )
        VALUES (
            %(city)s, %(country)s, %(weather_date)s,
            %(temp_celsius)s, %(feels_like)s, %(humidity_pct)s,
            %(wind_speed_ms)s, %(weather_main)s, %(weather_desc)s,
            %(precipitation_mm)s, %(raw_payload)s
        )
        ON CONFLICT (weather_date, city)
        DO UPDATE SET
            temp_celsius = EXCLUDED.temp_celsius,
            feels_like = EXCLUDED.feels_like,
            humidity_pct = EXCLUDED.humidity_pct,
            wind_speed_ms = EXCLUDED.wind_speed_ms,
            weather_main = EXCLUDED.weather_main,
            weather_desc = EXCLUDED.weather_desc,
            precipitation_mm = EXCLUDED.precipitation_mm,
            raw_payload = EXCLUDED.raw_payload,
            ingested_at = NOW();
    """

    conn = get_connection()

    try:
        # using context manager but still keeping explicit structure
        with conn:
            with conn.cursor() as cursor:
                payload = {**record, "raw_payload": Json(raw_payload)}
                cursor.execute(query, payload)

        city = record.get("city")   # slightly safer than direct access
        w_date = record.get("weather_date")

        logger.info(f"Weather saved for {city} @ {w_date}")

    finally:
        # always close, even if something explodes above
        conn.close()


def upsert_energy(record: dict, raw_payload: dict):
    """
    Same idea as weather upsert, just for energy prices.
    Keeping these separate rather than a generic upsert
    to make each function's intent obvious at a glance.
    """
    query = """
        INSERT INTO raw_energy (
            energy_date, series_name,
            price_value, price_unit,
            source_api, raw_payload
        )
        VALUES (
            %(energy_date)s, %(series_name)s,
            %(price_value)s, %(price_unit)s,
            %(source_api)s, %(raw_payload)s
        )
        ON CONFLICT (energy_date, series_name)
        DO UPDATE SET
            price_value = EXCLUDED.price_value,
            price_unit  = EXCLUDED.price_unit,
            raw_payload = EXCLUDED.raw_payload,
            ingested_at = NOW();
    """
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(query, {**record, "raw_payload": Json(raw_payload)})
        logger.info(
            f"Energy updated: {record.get('series_name')} ({record.get('energy_date')})"
        )
    finally:
        conn.close()

def log_pipeline_alert(source: str, error_msg: str, raw_response: str = None):
    """
    Dumps errors into pipeline_alerts.
    This is mainly for dashboard visibility (Tableau etc).
    Might extend this later with severity levels or something.
    """

    insert_sql = """
        INSERT INTO pipeline_alerts (
            source, error_message, raw_response
        )
        VALUES (%s, %s, %s);
    """

    conn = get_connection()

    try:
        with conn:
            with conn.cursor() as c:   # yet another cursor name...
                # considered sanitizing msg here but leaving it raw for now
                c.execute(insert_sql, (source, error_msg, raw_response))

        logger.warning(
            f"[ALERT] source={source} | msg={error_msg}"
        )

    finally:
        conn.close()
