# Weather ETL Pipeline

A fault-tolerant ETL pipeline that extracts current weather data from the Open-Meteo API, transforms the nested JSON response into a clean record, and logs every step of execution.

Built as a foundation for a production-style data pipeline, with an emphasis on reliability and observability rather than just a working happy path.

---

## Pipeline stages

| Stage | What it does |
|---|---|
| **Extract** | Fetches live weather data over HTTPS, with retry handling for transient failures |
| **Transform** | Flattens the nested JSON response into a clean, flat record with units attached |
| **Load** | In progress — persistence layer |

---

## Reliability features

- **Retry with exponential backoff** — up to 3 attempts, waiting 2s then 4s between tries, so a momentary network blip doesn't kill the run
- **Request timeout** — the request gives up after 10 seconds, so the job can never hang indefinitely on an unresponsive server
- **HTTP status validation** — `raise_for_status()` converts 4xx/5xx responses into catchable errors rather than letting an error page be parsed as data
- **Granular exception handling** — timeouts, network/DNS errors, and malformed JSON are each caught and logged separately, so failures are diagnosable rather than generic
- **Structured logging** — timestamped, levelled output written to both the console and `pipeline.log`, with UTF-8 encoding so non-ASCII characters (e.g. `°F`) are preserved
- **Graceful failure** — exits with an ERROR-level log rather than crashing with a stack trace

---

## Tech stack

Python 3.11 · `requests` · `logging`

---

## Running it

```bash
pip install requests
python weather.py
```

Configuration (location, measurements, units) lives in the `PARAMS` dictionary at the top of `weather.py`.

---

## Sample output

### Successful run

    2026-09-30 19:19:23 | INFO | Pipeline started
    2026-09-30 19:19:23 | INFO | Attempt 1 of 3
    2026-09-30 19:19:23 | INFO | Successfully fetched weather data
    2026-09-30 19:19:23 | INFO | Transformed record: {'time': '2026-10-01T00:15',
                                 'temperature': 81.4, 'temperature_unit': '°F',
                                 'humidity': 36, 'wind_speed': 9.1,
                                 'wind_speed_unit': 'km/h'}
    2026-09-30 19:19:23 | INFO | Pipeline finished successfully

### Failure and recovery

Captured by deliberately pointing the pipeline at an unreachable host to verify the retry logic:

    2026-09-30 19:19:47 | INFO    | Pipeline started
    2026-09-30 19:19:47 | INFO    | Attempt 1 of 3
    2026-09-30 19:19:47 | WARNING | Attempt 1 failed: Failed to resolve host
    2026-09-30 19:19:47 | INFO    | Waiting 2 seconds before retrying
    2026-09-30 19:19:49 | INFO    | Attempt 2 of 3
    2026-09-30 19:19:49 | WARNING | Attempt 2 failed: Failed to resolve host
    2026-09-30 19:19:49 | INFO    | Waiting 4 seconds before retrying
    2026-09-30 19:19:53 | INFO    | Attempt 3 of 3
    2026-09-30 19:19:53 | WARNING | Attempt 3 failed: Failed to resolve host
    2026-09-30 19:19:53 | ERROR   | All attempts failed. Giving up.
    2026-09-30 19:19:53 | ERROR   | Pipeline failed: no data to process

Note the timestamps — **47 → 49 → 53**. The 2-second and 4-second backoff intervals behave as designed, and the pipeline exits cleanly with a clear error rather than crashing.

---

## Design notes

**Why retries with backoff instead of a single attempt?**
Transient network failures are the most common cause of pipeline failure in production. Retrying immediately can worsen an already-struggling upstream service, so the wait interval doubles on each attempt.

**Why logging instead of `print()`?**
A scheduled job runs unattended. `print()` output disappears into a terminal nobody is watching; a log file persists with timestamps and severity levels, and can be filtered or shipped to a monitoring system.

**Why a timeout?**
Without one, a server that accepts a connection and then goes silent will hang the job indefinitely — no error, no crash, just a stalled process.

**Why `extract()` and `transform()` as separate functions?**
Separating the stages makes each one independently testable and keeps the pipeline readable as it grows.

---

## Roadmap

- [x] Extract from API with retry logic
- [x] Transform nested JSON into a flat record
- [x] Structured logging to file and console
- [ ] Load into PostgreSQL
- [ ] Containerise with Docker
- [ ] Orchestrate with Apache Airflow
- [ ] Add unit tests with `pytest`
- [ ] Data quality validation