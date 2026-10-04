import requests
import logging
import time
import sqlite3

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
DB_PATH="weather.db"

def extract():
    """Pull weather data from the API, retrying on failure."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            logging.info(f"Attempt {attempt} of {MAX_ATTEMPTS}")

            response = requests.get(URL, params=PARAMS, timeout=10)
            response.raise_for_status()
            data = response.json()

            logging.info("Successfully fetched weather data")
            logging.debug(data)
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

def load(record):
    connection=None
    try:
        connection=sqlite3.connect(DB_PATH)
        cursor=connection.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS weather (
                time              TEXT PRIMARY KEY,
                temperature       REAL,
                temperature_unit  TEXT,
                humidity          INTEGER,
                wind_speed        REAL,
                wind_speed_unit   TEXT,
                loaded_at         TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""INSERT OR REPLACE INTO weather
                (time, temperature, temperature_unit,
                 humidity, wind_speed, wind_speed_unit)
            VALUES(?,?,?,?,?,?)""",(record["time"],
            record["temperature"],
            record["temperature_unit"],
            record["humidity"],
            record["wind_speed"],
            record["wind_speed_unit"],))
        connection.commit()
        logging.info(f"loaded record for {record['time']} into {DB_PATH}")
        return True
    except  sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        return False
    finally:
        if connection:
            connection.close()

def main():
    logging.info("Pipeline started")
    
    raw = extract()

    if raw is None:
        logging.error("Pipeline failed: no data to process")
        return

    record = transform(raw)
    logging.info(f"Transformed record: {record}")
    if not load(record):
        logging.error("Pipeline failed: could not load record")
        return
    logging.info("Pipeline finished successfully")


if __name__ == "__main__":
    main()