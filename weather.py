import requests
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

URL = "https://api.open-meteo.com/v1/forecast"

PARAMS = {
    "latitude": 33.4054,
    "longitude": -86.8114,
    "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
    "temperature_unit": "fahrenheit",
}

MAX_ATTEMPTS = 3


def extract():
    """Pull weather data from the API, retrying on failure."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            logging.info(f"Attempt {attempt} of {MAX_ATTEMPTS}")

            response = requests.get(URL, params=PARAMS, timeout=10)
            response.raise_for_status()
            data = response.json()

            logging.info("Successfully fetched weather data")
            return data

        except requests.exceptions.Timeout:
            logging.warning(f"Attempt {attempt} timed out")

        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attempt} failed: {e}")

        except ValueError as e:
            logging.warning(f"Attempt {attempt} returned invalid JSON: {e}")

        if attempt < MAX_ATTEMPTS:
            wait = 2 ** attempt
            logging.info(f"Waiting {wait} seconds before retrying")
            time.sleep(wait)

    logging.error("All attempts failed. Giving up.")
    return None


def transform(data):
    """Pull the fields we care about out of the raw response."""
    current = data["current"]
    units = data["current_units"]

    return {
        "time": current["time"],
        "temperature": current["temperature_2m"],
        "temperature_unit": units["temperature_2m"],
        "humidity": current["relative_humidity_2m"],
        "wind_speed": current["wind_speed_10m"],
        "wind_speed_unit": units["wind_speed_10m"],
    }


def main():
    logging.info("Pipeline started")

    raw = extract()

    if raw is None:
        logging.error("Pipeline failed: no data to process")
        return

    record = transform(raw)
    logging.info(f"Transformed record: {record}")
    logging.info("Pipeline finished successfully")


if __name__ == "__main__":
    main()