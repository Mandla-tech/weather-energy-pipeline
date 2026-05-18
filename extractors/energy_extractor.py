"""
energy_extractor.py
-------------------
Pulls daily energy commodity price from CoinGecko
Note to self: This uses Ethereum as the energy commodity proxy, ETH mining and
transaction costs are directly tied to global energy prices, making
it a legitimate energy-sensitive financial instrument.
"""

import json
import logging
import requests
from datetime import date
from pathlib import Path
from db_loader import upsert_energy, log_pipeline_alert

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("energy_extractor")

# Configurations
COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/simple/price"
    "?ids=ethereum&vs_currencies=usd"
)
LAKE_PATH = Path("/opt/airflow/data/lake")


# Step 1: Extract

def fetch_coingecko() -> dict:
    """Fetch current Ethereum price from CoinGecko."""
    logger.info("Fetching CoinGecko energy commodity price")
    response = requests.get(COINGECKO_URL, timeout=10)
    response.raise_for_status()
    return response.json()


# Step 2: Validate

def validate_coingecko(data: dict) -> list[str]:
    """
    Data contract for CoinGecko response.
    Returns list of error strings. Empty = valid.
    """
    errors = []

    if "ethereum" not in data:
        errors.append("Missing 'ethereum' key in CoinGecko response")
        return errors

    price = data["ethereum"].get("usd")

    if price is None:
        errors.append("Ethereum USD price is null")
        return errors

    if price is not None and not (10 <= price <= 100000):
        errors.append(
            f"Ethereum price ${price} is outside expected range ($10–$100,000)"
        )

    return errors


#  Step 3: Save to file lake

def save_to_lake(data: dict, today: date):
    """Save raw CoinGecko response to the file lake."""
    folder = LAKE_PATH / str(today)
    folder.mkdir(parents=True, exist_ok=True)

    file_path = folder / "energy_coingecko.json"
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    logger.info(f"Raw payload saved to lake: {file_path}")


# Step 4: Load

def parse_and_load(data: dict, today: date):
    """Parse CoinGecko response and write to database."""
    price = data["ethereum"]["usd"]

    record = {
        "energy_date":  today,
        "series_name":  "Ethereum USD Price",
        "price_value":  price,
        "price_unit":   "USD per ETH",
        "source_api":   "CoinGecko v3",
    }

    upsert_energy(record, data)
    logger.info(f"Loaded Ethereum price: ${price:,.2f}")

# The main entrypoint

def run():
    today = date.today()
    logger.info(f"=== Energy extraction started for {today} ===")

    # 1. Extract
    try:
        data = fetch_coingecko()
    except requests.RequestException as e:
        error_msg = f"CoinGecko API fetch failed: {str(e)}"
        logger.error(error_msg)
        log_pipeline_alert("energy_extractor", error_msg)
        raise

    # 2. Validate
    errors = validate_coingecko(data)
    if errors:
        error_msg = f"Data contract violations: {errors}"
        logger.error(error_msg)
        log_pipeline_alert("energy_extractor", error_msg, json.dumps(data))
        raise ValueError(error_msg)

    logger.info("Data contract validation passed")

    # 3. Save to file lake
    save_to_lake(data, today)

    # 4. Load to database
    parse_and_load(data, today)

    logger.info(f"=== Energy extraction completed for {today} ===")


if __name__ == "__main__":
    run()
