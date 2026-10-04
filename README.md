# Weather ETL Pipeline

An end-to-end ETL pipeline that extracts current weather data from the Open-Meteo API, transforms the nested JSON response into a flat record, and loads it into a database with idempotent writes.

Built with an emphasis on reliability and observability rather than just a working happy path — the interesting part of a pipeline is what it does when things go wrong.

---

## Pipeline stages

| Stage | What it does |
|---|---|
| **Extract** | Fetches live weather data over HTTPS, with retry handling for transient failures |
| **Transform** | Flattens the nested JSON response into a clean record with units attached |
| **Load** | Idempotent upsert into SQLite, with audit metadata recording when each row was written |

---

## Reliability features

- **Retry with exponential backoff** — up to 3 attempts, waiting 2s then 4s between tries, so a momentary network blip doesn't kill the run
- **Request timeout** — the request gives up after 10 seconds, so the job can never hang indefinitely on an unresponsive server
- **HTTP status validation** — `raise_for_status()` converts 4xx/5xx responses into catchable errors rather than letting an error page be parsed as data
- **Granular exception handling** — timeouts, network/DNS errors, and malformed JSON are each caught and logged separately, so failures are diagnosable rather than generic
- **Idempotent loading** — `time` is a PRIMARY KEY and writes use `INSERT OR REPLACE`, so re-running the pipeline updates existing rows rather than creating duplicates
- **Parameterized queries** — all values are passed as bound parameters, never string-interpolated into SQL, preventing injection
- **Guaranteed connection cleanup** — the database connection closes in a `finally` block whether the write succeeded or failed
- **Structured logging** — timestamped, levelled output to both console and `pipeline.log`, UTF-8 encoded so non-ASCII characters (e.g. `°F`) survive
- **Graceful failure** — exits with an ERROR-level log rather than crashing with a stack trace

---

## Tech stack

Python 3.11 · `requests` · `sqlite3` · `logging`

---

## Running it

```bash
pip install requests
python weather.py
```

Configuration (location, measurements, units) lives in the `PARAMS` dictionary at the top of `weather.py`.

Inspect the loaded data:

```bash
python -c "import sqlite3; c=sqlite3.connect('weather.db'); [print(r) for r in c.execute('SELECT * FROM weather')]"
```

---

## Schema

| Column | Type | Notes |
|---|---|---|
| `time` | TEXT | PRIMARY KEY — the observation timestamp from the source |
| `temperature` | REAL | |
| `temperature_unit` | TEXT | |
| `humidity` | INTEGER | |
| `wind_speed` | REAL | |
| `wind_speed_unit` | TEXT | |
| `loaded_at` | TEXT | Audit metadata — when this row was written, distinct from when the observation occurred |

---

## Sample output

### Successful run

    2026-10-03 21:16:03 | INFO | Pipeline started
    2026-10-03 21:16:03 | INFO | Attempt 1 of 3
    2026-10-03 21:16:03 | INFO | Successfully fetched weather data
    2026-10-03 21:16:03 | INFO | Transformed record: {'time': '2026-10-04T02:15',
                                 'temperature': 78.8, 'temperature_unit': '°F',
                                 'humidity': 76, 'wind_speed': 5.4,
                                 'wind_speed_unit': 'km/h'}
    2026-10-03 21:16:03 | INFO | loaded record for 2026-10-04T02:15 into weather.db
    2026-10-03 21:16:03 | INFO | Pipeline finished successfully

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

### Idempotency

The pipeline was run four times in succession against the same 15-minute data interval:

    21:16:03 | INFO | loaded record for 2026-10-04T02:15 into weather.db
    21:16:32 | INFO | loaded record for 2026-10-04T02:15 into weather.db
    21:16:33 | INFO | loaded record for 2026-10-04T02:15 into weather.db
    21:16:34 | INFO | loaded record for 2026-10-04T02:15 into weather.db

Querying the table afterwards returns a single row, with `loaded_at` reflecting the most recent write:

    ('2026-10-04T02:15', 78.8, '°F', 76, 5.4, 'km/h', '2026-10-04 02:16:34')

Re-running the pipeline is safe and produces no duplicate records.

---

## Design notes

**Why retries with exponential backoff?**
Transient network failures are the most common cause of pipeline failure in production. Retrying immediately can worsen an already-struggling upstream service, so the wait interval doubles on each attempt.

**Why a request timeout?**
Without one, a server that accepts a connection and then goes silent will hang the job indefinitely — no error, no crash, just a stalled process nobody notices until the data is stale.

**Why idempotent writes?**
A scheduled pipeline will be re-run: after a failure, during a backfill, or by accident. Re-running must not corrupt the dataset. Making the source timestamp the primary key means a repeat run overwrites rather than duplicates.

**Why `loaded_at` separate from `time`?**
`time` is when the observation happened; `loaded_at` is when the pipeline wrote it. Keeping them distinct makes it possible to audit pipeline behaviour independently of the data itself — e.g. detecting a delayed or re-run load.

**Why logging instead of `print()`?**
A scheduled job runs unattended. `print()` output disappears into a terminal nobody is watching; a log file persists with timestamps and severity levels, and can be filtered or shipped to a monitoring system. The raw API payload is logged at DEBUG level rather than INFO, so it is available for troubleshooting without polluting normal runs — and so payload contents don't end up permanently in logs.

**Why separate `extract()`, `transform()`, and `load()` functions?**
Each stage is independently testable, and `main()` reads as a summary of the pipeline. Each returns a success value so the caller decides what happens on failure, rather than each stage deciding for itself.

---

## Roadmap

- [x] Extract from API with retry logic and exponential backoff
- [x] Transform nested JSON into a flat record
- [x] Structured logging to file and console
- [x] Load into SQLite with idempotent upsert
- [ ] Support multiple locations
- [ ] Migrate to PostgreSQL running in Docker
- [ ] Orchestrate with Apache Airflow
- [ ] Add unit tests with `pytest`
- [ ] Data quality validation
- [ ] Deploy to AWS