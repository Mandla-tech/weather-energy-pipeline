"""
weather_extractor.py
--------------------
Pulls daily weather data for Johannesburg from my two sources:
  1. OpenWeatherMap : current conditions (temp, humidity, wind, description)
  2. Open-Meteo : precipitation in mm (more reliable rainfall data)

Design pattern: extract -> validate -> save to file lake -> load to database
"""

import os
import json
import logging
import requests
from datetime import date, datetime
from pathlib import Path
from db_loader import upsert_weather, log_pipeline_alert

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("weather_extractor")

# My configuration
OWM_API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITY        = os.getenv("WEATHER_CITY", "Johannesburg")
LAT         = os.getenv("WEATHER_LAT", "-26.2041")
LON         = os.getenv("WEATHER_LON", "28.0473")
LAKE_PATH   = Path("/opt/airflow/data/lake")


# Step 1: Extraction

def fetch_openweather() -> dict:
    """Fetching current weather conditions from OpenWeatherMap."""
    url = (
        f"https://api.openweathermap.org/data/2.5/weather"
        f"?q={CITY}&appid={OWM_API_KEY}&units=metric"
    )
    logger.info(f"Fetching OpenWeatherMap data for {CITY}")
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


def fetch_open_meteo() -> dict:
    """Fetching daily precipitation from Open-Meteo (I dont need API key)."""
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={LAT}&longitude={LON}"
        f"&daily=precipitation_sum"
        f"&timezone=Africa/Johannesburg"
        f"&past_days=1"
    )
    logger.info("Fetching Open-Meteo precipitation data")
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


# Step 2: Validate data,  this iss the contract)

def validate_openweather(data: dict) -> list[str]:
    """
    Data contract for OpenWeatherMap response.
    Returns a list of error strings. Empty list = valid.
    """
    errors = []

    if "main" not in data:
        errors.append("Missing 'main' block in OWM response")
    else:
        if data["main"].get("temp") is None:
            errors.append("temp is null")
        if data["main"].get("humidity") is None:
            errors.append("humidity is null")

    if "weather" not in data or len(data["weather"]) == 0:
        errors.append("Missing 'weather' array in OWM response")

    if "wind" not in data:
        errors.append("Missing 'wind' block in OWM response")

    # Sanity range checks and catches garbage values
    if "main" in data:
        temp = data["main"].get("temp")
        if temp is not None and not (-10 <= temp <= 50):
            errors.append(f"Temperature {temp}°C is outside expected range (-10 to 50)")

        humidity = data["main"].get("humidity")
        if humidity is not None and not (0 <= humidity <= 100):
            errors.append(f"Humidity {humidity}% is outside valid range (0-100)")

    return errors


def validate_open_meteo(data: dict) -> list[str]:
    """Data contract for Open-Meteo response."""
    errors = []

    if "daily" not in data:
        errors.append("Missing 'daily' block in Open-Meteo response")
        return errors

    if "precipitation_sum" not in data["daily"]:
        errors.append("Missing precipitation_sum in Open-Meteo daily data")

    if "time" not in data["daily"]:
        errors.append("Missing time array in Open-Meteo daily data")

    return errors


# Step 3: Save to file lake

def save_to_lake(data: dict, source: str, today: date):
    """
    Saving raw JSON to the local file lake before loading to the database.
    Path pattern: data/lake/YYYY-MM-DD/weather_{source}.json
    I want to simulate the S3/GCS landing zone pattern used in cloud pipelines.
    """
    folder = LAKE_PATH / str(today)
    folder.mkdir(parents=True, exist_ok=True)

    file_path = folder / f"weather_{source}.json"
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    logger.info(f"Raw payload saved to lake: {file_path}")


# Step 4: Transform and Load

def parse_and_load(owm_data: dict, meteo_data: dict, today: date):
    """
    Merges data from both sources into one clean record
    and writes it to the database via the upsert loader.
    """
    # Extract precipitation from Open-Meteo
    # past days=1 returns yesterday as index 0
    try:
        precipitation = meteo_data["daily"]["precipitation_sum"][0] or 0.0
    except (KeyError, IndexError):
        precipitation = 0.0
        logger.warning("Could not parse precipitation — defaulting to 0.0")

    record = {
        "city":           owm_data.get("name", CITY),
        "country":        owm_data.get("sys", {}).get("country", "ZA"),
        "weather_date":   today,
        "temp_celsius":   owm_data["main"]["temp"],
        "feels_like":     owm_data["main"].get("feels_like"),
        "humidity_pct":   owm_data["main"]["humidity"],
        "wind_speed_ms":  owm_data.get("wind", {}).get("speed"),
        "weather_main":   owm_data["weather"][0]["main"],
        "weather_desc":   owm_data["weather"][0]["description"],
        "precipitation_mm": precipitation,
    }

    # Combined raw payload with both sources stored intact
    combined_payload = {
        "openweathermap": owm_data,
        "open_meteo":     meteo_data
    }

    upsert_weather(record, combined_payload)


# The main entrypoint

def run():
    today = date.today()
    logger.info(f"=== Weather extraction started for {today} ===")

    # 1. Extract
    try:
        owm_data   = fetch_openweather()
        meteo_data = fetch_open_meteo()
    except requests.RequestException as e:
        error_msg = f"API fetch failed: {str(e)}"
        logger.error(error_msg)
        log_pipeline_alert("weather_extractor", error_msg)
        raise

    # 2. Validate
    owm_errors   = validate_openweather(owm_data)
    meteo_errors = validate_open_meteo(meteo_data)
    all_errors   = owm_errors + meteo_errors

    if all_errors:
        error_msg = f"Data contract violations: {all_errors}"
        logger.error(error_msg)
        log_pipeline_alert(
            "weather_extractor",
            error_msg,
            json.dumps({"owm": owm_data, "meteo": meteo_data})
        )
        raise ValueError(error_msg)

    logger.info("Data contract validation passed")

    # 3. Save to file lake
    save_to_lake(owm_data,   "openweathermap", today)
    save_to_lake(meteo_data, "open_meteo",     today)

    # 4. Load to database
    parse_and_load(owm_data, meteo_data, today)

    logger.info(f"=== Weather extraction completed for {today} ===")


if __name__ == "__main__":
    run()

