"""
backfill_historical.py
----------------------
Dear user/cloner 

I have made this ONE-TIME script to load 90 days of historical data.

If you need to, please run this ONCE after the pipeline is complete to populate
the database with historical data for meaningful analysis.

DO NOT run it more than once but if you do, the upsert pattern protects against duplicates if you
accidentally run it twice, but it is not intended for
repeated execution.

"""

import os
import json
import logging
import requests
import psycopg2
from psycopg2.extras import Json
from datetime import date, timedelta
from dotenv import load_dotenv

# Loading environment variables from .env file
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("backfill")

# Configuration
DAYS_BACK    = 90
LAT          = "-26.2041"
LON          = "28.0473"
CITY         = "Johannesburg"
OWM_API_KEY  = os.getenv("OPENWEATHER_API_KEY")


# The connection to database

def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )


# Weather backfill 

def fetch_historical_weather(days_back: int) -> dict:

    """ Getting 90 days of historical data from Open Meteo."""
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={LAT}&longitude={LON}"
        f"&daily=precipitation_sum,temperature_2m_max,temperature_2m_min,"
        f"windspeed_10m_max,weathercode"
        f"&timezone=Africa/Johannesburg"
        f"&past_days={days_back}"
    )
    logger.info(f"Fetching {days_back} days of historical weather from Open-Meteo")
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def load_weather_history(data: dict):
    """
    Parsing Open Meteo's response and upsert one row per day.
     This uses the same idempotent upsert as the live pipeline.
    """
    daily      = data["daily"]
    dates      = daily["time"]
    precip     = daily["precipitation_sum"]
    temp_max   = daily["temperature_2m_max"]
    temp_min   = daily["temperature_2m_min"]
    wind       = daily["windspeed_10m_max"]

    # WMO weather codes simplified mapping to match OWM style
    wmo_codes = {
        0: "Clear", 1: "Clear", 2: "Clouds", 3: "Clouds",
        45: "Fog", 48: "Fog",
        51: "Drizzle", 53: "Drizzle", 55: "Drizzle",
        61: "Rain", 63: "Rain", 65: "Rain",
        71: "Snow", 73: "Snow", 75: "Snow",
        80: "Rain", 81: "Rain", 82: "Rain",
        95: "Thunderstorm", 96: "Thunderstorm", 99: "Thunderstorm"
    }

    sql = """
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
            temp_celsius     = EXCLUDED.temp_celsius,
            wind_speed_ms    = EXCLUDED.wind_speed_ms,
            weather_main     = EXCLUDED.weather_main,
            weather_desc     = EXCLUDED.weather_desc,
            precipitation_mm = EXCLUDED.precipitation_mm,
            raw_payload      = EXCLUDED.raw_payload,
            ingested_at      = NOW();
    """

    
    conn    = get_connection()
    loaded  = 0
    skipped = 0

    try:
        with conn:
            with conn.cursor() as cur:
                for i, day_str in enumerate(dates):

                    # Skip any day where critical values are None
                    # This handles future forecast dates and missing data
                    if temp_max[i] is None or temp_min[i] is None:
                        logger.warning(
                            f"Skipping {day_str} — "
                            f"temp_max={temp_max[i]}, temp_min={temp_min[i]}"
                        )
                        skipped += 1
                        continue

                    if precip[i] is None:
                        precip_val = 0.0
                    else:
                        precip_val = precip[i]

                    if wind[i] is None:
                        wind_val = 0.0
                    else:
                        wind_val = wind[i]

                    weather_code = daily.get(
                        "weathercode", [0] * len(dates)
                    )[i]
                    weather_main = wmo_codes.get(
                        weather_code or 0, "Clear"
                    )
                    avg_temp = (temp_max[i] + temp_min[i]) / 2

                    record = {
                        "city":             CITY,
                        "country":          "ZA",
                        "weather_date":     day_str,
                        "temp_celsius":     round(avg_temp, 2),
                        "feels_like":       round(avg_temp - 1.5, 2),
                        "humidity_pct":     70,
                        "wind_speed_ms":    round(wind_val / 3.6, 2),
                        "weather_main":     weather_main,
                        "weather_desc":     weather_main.lower(),
                        "precipitation_mm": precip_val,
                        "raw_payload":      Json({
                            "open_meteo": "backfill",
                            "day_index":  i
                        }),
                    }
                    cur.execute(sql, record)
                    loaded += 1

        logger.info(
            f"Weather backfill complete — "
            f"{loaded} days loaded, {skipped} days skipped"
        )
    finally:
        conn.close()


# Energy backfill

def fetch_historical_energy(days_back: int) -> dict:
    """
    CoinGecko market_chart endpoint returns daily prices
    for the specified number of days with no API key.
    """
    url = (
        f"https://api.coingecko.com/api/v3/coins/ethereum/market_chart"
        f"?vs_currency=usd&days={days_back}&interval=daily"
    )
    logger.info(f"Fetching {days_back} days of ETH price history from CoinGecko")
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def load_energy_history(data: dict):
    """
    Parsing CoinGecko market chart response and upsert one row per day.
    CoinGecko returns timestamps in milliseconds — convert to date.
    """
    sql = """
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

    prices = data.get("prices", [])
    conn   = get_connection()
    loaded = 0

    try:
        with conn:
            with conn.cursor() as cur:
                for timestamp_ms, price in prices:
                    # Converting millisecond timestamp to date
                    energy_date = date.fromtimestamp(timestamp_ms / 1000)

                    record = {
                        "energy_date":  energy_date,
                        "series_name":  "Ethereum USD Price",
                        "price_value":  round(price, 4),
                        "price_unit":   "USD per ETH",
                        "source_api":   "CoinGecko v3 (historical)",
                    }
                    cur.execute(sql, {
                        **record,
                        "raw_payload": Json({"timestamp_ms": timestamp_ms,
                                             "price_usd": price})
                    })
                    loaded += 1
        logger.info(f"Energy backfill complete — {loaded} days loaded")
    finally:
        conn.close()


# Main block of this code run

def run():
    logger.info("=" * 60)
    logger.info(f"BACKFILL STARTED loading {DAYS_BACK} days of history")
    logger.info("=" * 60)

    # Weather
    weather_data = fetch_historical_weather(DAYS_BACK)
    load_weather_history(weather_data)

    # Energy
    energy_data = fetch_historical_energy(DAYS_BACK)
    load_energy_history(energy_data)

    logger.info("=" * 60)
    logger.info("BACKFILL COMPLETE run dbt to rebuild your mart")
    logger.info("Command: cd dbt/weather_energy && dbt run")
    logger.info("=" * 60)


if __name__ == "__main__":
    run()
