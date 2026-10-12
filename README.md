# Machine Temperature Streaming Analytics

A real-time streaming pipeline for one industrial machine:
CSV replay -> Kafka -> Python processor -> IsolationForest -> MySQL -> Streamlit.

Readings from the NAB machine-temperature dataset are replayed into Kafka, a Python processor keeps a
1-hour rolling window and scores every reading with an IsolationForest, MySQL stores the results, and a
Streamlit dashboard redraws itself every 2 seconds. The design and rules live in `CLAUDE.md` and `Resources/`.

Every command below was run on Ubuntu (bash). Where Windows differs, a PowerShell line is given; those
Windows lines follow the official tool docs but were not run on a Windows machine.

## Prerequisites
- Python 3.12 or newer (built and tested on 3.14.4) and Git
- Docker with the Compose plugin
  - Ubuntu: `sudo apt install -y docker.io docker-compose-v2 docker-buildx`, then
    `sudo usermod -aG docker $USER` and log out and back in
  - Windows / macOS: Docker Desktop (Windows needs WSL 2). Start it before any `docker` command.
  - Check: `docker run --rm hello-world`
- Free ports 9092 (Kafka), 3307 (MySQL) and 8501 (dashboard). Stop any Kafka left running from the labs.

## Setup (once, from an empty system)
Open a terminal in the project folder (the one with this README) and run the steps in order.

### 1. Python virtual environment
Linux / macOS:
```bash
# Ubuntu/Debian only: the venv module is a separate package (use your version, e.g. python3.12-venv)
sudo apt install -y python3.14-venv

python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
pip check                          # expect: No broken requirements found.
```
Windows (PowerShell):
```powershell
py -3 -m venv .venv
Set-ExecutionPolicy -Scope Process RemoteSigned   # only if activation is blocked
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
pip check
```
Every later command assumes the virtual environment is active (`(.venv)` in the prompt). In each new
terminal, activate it again: `source .venv/bin/activate` (Windows: `.venv\Scripts\Activate.ps1`).

### 2. Settings
All hosts, ports, the topic name and the database credentials live in `.env`.
Docker Compose and `src/config.py` both read it, so nothing else needs editing.
```bash
cp .env.example .env              # Windows: copy .env.example .env
```

### 3. Data
Both files are already in `data/` (from the [Numenta Anomaly Benchmark](https://github.com/numenta/NAB)):
`machine_temperature_system_failure.csv` (22,695 readings, one every 5 minutes) and `anomaly_windows.json`
(the 4 labelled failure windows). Nothing to do here.

### 4. Start Kafka and MySQL
Kafka 4 runs in KRaft mode, so there is no ZooKeeper: one container is the whole cluster.
```bash
docker compose up -d --wait       # first run downloads the images; waits until both are healthy
docker compose ps                 # broker and mysql should both show (healthy)
```

### 5. Create the topic
```bash
docker exec broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic machine-temperature --partitions 1 --replication-factor 1
```
Expect `Created topic machine-temperature.` One partition keeps every reading in order.

### 6. Create the tables
```bash
python -m src.db --init           # expect: Tables: forecasts, hourly_stats, metrics_current, readings
```

### 7. Train the model
```bash
python -m src.train_model
```
It fits an IsolationForest on the first 2,000 rows only (2 to 9 Dec 2013) and writes
`models/anomaly_model.joblib`. Expect `Training rows the model itself flags: 20 of 1988 (1.01%)`.
Retrain after any scikit-learn upgrade: the processor refuses a model saved by another version.

### 8. Dashboard settings
```bash
python scripts/make_secrets.py    # writes .streamlit/secrets.toml from .env (never committed)
```
Run it again after any change to the `MYSQL_*` values in `.env`.

### 9. Run the tests
```bash
pytest                            # expect: 41 passed
```

## Run the pipeline
Use four terminals, each with the virtual environment active, started in this order.

| Terminal | Command | What you see |
| --- | --- | --- |
| 1 | `python -m src.processor` | `Cold start…`, then `Consuming machine-temperature…` and one line per batch |
| 2 | `python -m src.forecaster` | `Consuming machine-temperature as group 'forecast'…`, then one line per forecast |
| 3 | `streamlit run dashboard/app.py` | open http://localhost:8501: "Waiting for data" |
| 4 | `python -m src.producer --start-at "2013-12-10 00:00" --rate 10` | one progress line every 50 events |

The forecaster is optional (the core pipeline works without it). It needs 288 readings (24 h of event
time, about 29 s at `--rate 10`) before its first forecast, then refits after every 12 new readings.

Within seconds the curve starts to draw itself and the tiles change with no clicks. The first red
points (readings the model flags) appear after about 27 s, at event time 10 Dec 22:40. About 3 minutes
in, the temperature collapses to about 2 and a cluster of red points appears.

Stop the producer with Ctrl+C: the dashboard keeps refreshing, but the numbers freeze and the status
line turns into "No new data for N s". Continue with `python -m src.producer --resume`.
Ctrl+C stops the processor after its current batch; restarted, it continues where it stopped.

### Producer options
```bash
python -m src.producer                                   # whole stream from seq 2000, 10 events/s
python -m src.producer --rate 5 --limit 50               # 50 events at 5 per second
python -m src.producer --start-at "2013-12-10 00:00"     # skip to the first failure window (seq 2049)
python -m src.producer --resume                          # continue after Ctrl+C
```
`--start-at` and `--resume` cannot be combined. The last sent seq is saved in `.producer_state` every
50 events and on exit. The whole stream is 20,695 events (about 34 minutes at 10/s, 104 s at 200/s).

### What each part does
- **Producer** (layer 1): replays CSV rows 2000 onward as JSON events, keyed by `machine-1`, paced by `--rate`.
- **Processor** (layers 3, 4, 5): reads the topic as consumer group `scoring`, keeps a 1-hour window
  (12 readings), drops repeated or out-of-order timestamps, scores each polled batch with one model call
  (the first 12 readings after a cold start are never flagged), and writes the batch to MySQL in one
  transaction before committing the Kafka offsets. A restart never stores a reading twice.
- **Forecaster** (extra): reads the same topic in its own consumer group `forecast`, keeps the last 288
  readings, and after every 12 new readings fits ARIMA(1,1,1) and stores the next 12 readings with a 95%
  interval in `forecasts`. Separate offsets mean it never slows the processor down (stop it, and only its
  own lag grows).
- **Hourly window** (in the processor): every batch is also grouped into fixed 1-hour windows of event
  time (a tumbling window) and merged into `hourly_stats`: count, mean, min, max and flagged readings.
- **Dashboard** (layer 6): one Streamlit fragment re-queries MySQL every 2 s (sidebar: 1 to 10 s, or
  pause). Every query goes to MySQL through `run_query()`; nothing is served from a cache.

### What the dashboard shows
- **Top:** machine state badge (Normal / Watch / Anomaly from the newest reading's score), the Live /
  "No new data" / "Replay complete" status line, replay progress (row X of 20,695), and 10 KPI tiles.
- **Temperature curve:** the newest 144 readings (12 h of machine time, chosen automatically): temperature, rolling average, volatility band (± 2 rolling std), red flags,
  failure windows and the ARIMA forecast. Hover any point to see its event time and when it arrived.
  Red points are flagged readings (anomaly score below 0), orange points are "watch" readings (score 0 to
  0.05). Directly under the curve, on the same time axis, the **anomaly score strip** shows what the model
  thought of every reading: a slow drift moves the temperature but leaves the score high; a sudden jump or
  spike pushes it below the dashed line at 0.
  Switch **Follow live → Browse history** to scroll back: the slider and the Earlier / Later buttons
  move through every stored reading while new data keeps arriving (the tiles stay live, the browsed
  chart stays still). **Back to live** returns to the newest readings.
- **Tabs** (only the open tab queries MySQL; the open tab is kept in the URL, e.g. `?tab=Pipeline`):
  *Model view* (anomaly score, score histogram, deviation vs delta scatter, the three window features),
  *Hourly view* (min/avg/max per hour, flags per hour, day × hour heatmap of the whole run),
  *Pipeline* (lag per reading, forecast vs actual with MAE, run timeline: readings per wall-clock
  minute), *Readings log* (scrollable table of the readings on the curve) and *Run report* (the run so far).
- **Final run report:** when the last stream row is stored, the page says "Replay complete" and shows
  the report at the top: total and active time (pauses left out), machine time replayed, replay speed,
  throughput, flags, windows caught with their detection delay, peak and minimum, lag, forecast
  accuracy, and a **Download report (.md)** button. The numbers come from `dashboard/run_report.py`
  (tested in `tests/test_run_report.py`) and match `report/results.md` after a full replay.
- **Zoomed temperature axis** (sidebar toggle, on by default): the axis fits the readings shown, in steps of
  10 and at least 40 wide, so normal variation is visible without making a slow drift look like a crash. It only
  changes when the readings cross a multiple of 10. Turn it off for the fixed 0–115 axis.
- The theme is in `.streamlit/config.toml` (no secrets in it).

## Evaluate the model against the labelled failures
Offline (no Kafka or MySQL needed): replays the stream rows in memory exactly as the pipeline does and
compares several detectors against NAB's four labelled failure windows.
```bash
python scripts/evaluate.py        # about 5 s
```
It prints a Markdown table and writes `report/evaluation.csv`, `report/stream_flags.png` (the whole stream,
windows shaded, flags in red) and `report/flags_outside_windows.png`. The run checks itself: the
"4 features, 0.01" row must equal the pipeline's saved model (284 flags on a full replay).

| Detector | Stream flagged | Windows hit | Flags outside windows |
| --- | --- | --- | --- |
| IsolationForest, 4 features, 0.005 | 0.93% | 4 of 4 | 125 |
| **IsolationForest, 4 features, 0.01 (the pipeline)** | 1.37% | 4 of 4 | 201 |
| IsolationForest, 4 features, 0.02 | 2.64% | 4 of 4 | 406 |
| IsolationForest, 4 features, 0.05 | 6.31% | 4 of 4 | 893 |
| IsolationForest, raw temperature, 0.01 (the course guide's approach) | 36.46% | 4 of 4 | 6,282 |
| Rolling z-score over 288 readings, cut-off 3 (no ML) | 4.65% | 4 of 4 | 843 |
| Rolling z-score over 288 readings, cut-off 4 (no ML) | 1.47% | 2 of 4 | 241 |

A flag outside a labelled window is unlabelled, not proven false. The dashboard's sidebar toggle
"Show known failure windows" shades the same windows behind the live curve, and a line under the tiles
counts how many windows reached so far got at least one flag.

## Live demo and report material
- `DEMO.md`: the step-by-step demo runbook (rehearsed and timed), with fallbacks and viva questions.
- Report numbers: after a full replay with the processor and forecaster running (see "Expected after a full
  replay" below), run
  ```bash
  python scripts/collect_results.py   # writes report/results.md from MySQL, Kafka offsets, the model and evaluation.csv
  ```
- `report/report_draft.md`: the report draft (abstract, architecture, dataset, the six layers with real code snippets,
  ML model, dashboard, results, conclusion, sources). Its numbers come only from `report/results.md`,
  `report/evaluation.csv` and `DEMO.md`; the TODO lines mark screenshots and the personal parts still to add.

## How to verify
Consumer lag of the processor (`LAG` 0 means it has processed everything on the topic):
```bash
docker exec broker /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group scoring
```
Both consumer groups, each with its own offsets (`scoring` = processor, `forecast` = forecaster):
```bash
docker exec broker /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group scoring --group forecast
```
(`--all-groups` works too, but also lists the throwaway groups that console consumers create.)
The newest forecast and the newest hours:
```bash
docker exec mysql sh -c 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "SELECT * FROM forecasts ORDER BY target_time DESC LIMIT 5; SELECT * FROM hourly_stats ORDER BY hour_start DESC LIMIT 5"'
```
Rows stored, duplicates and anomalies (`rows_stored` must equal `distinct_offsets`):
```bash
docker exec mysql sh -c 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "SELECT COUNT(*) AS rows_stored, COUNT(DISTINCT kafka_offset) AS distinct_offsets, SUM(is_anomaly) AS flagged FROM readings"'
```
The processor's running totals:
```bash
docker exec mysql sh -c 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "SELECT events_processed, anomalies_flagged, late_events_dropped, peak_temperature, min_temperature, pipeline_lag_ms FROM metrics_current"'
```
Flagged readings per day:
```bash
docker exec mysql sh -c 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "SELECT DATE(event_time) AS day, COUNT(*) AS flags FROM readings WHERE is_anomaly = 1 GROUP BY day ORDER BY day"'
```
Windows (PowerShell): run the same SQL through an interactive client, which asks for the password from
`.env` (`monitor_pass`): `docker exec -it mysql mysql -u monitor -p machine_monitor -e "SELECT COUNT(*) FROM readings"`

Messages on the topic (the newest offset is the number of events sent):
```bash
docker exec broker /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic machine-temperature
docker exec -it broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic machine-temperature --from-beginning --formatter-property print.key=true
```

**Expected after a full replay** (reset, processor, then `python -m src.producer --rate 200` with the
dashboard closed): 20,695 events sent, 20,683 rows stored, 12 late events dropped (12 readings on
7 Jan 2014 repeat earlier timestamps), and LAG 0 once the processor has caught up.

## When something is not running
Each script stops with one sentence instead of a stack trace. The processor first waits up to 60 s for
Kafka and MySQL, so it can be started together with `docker compose up`.

| Message starts with | Cause | Fix |
| --- | --- | --- |
| `Cannot reach Kafka at localhost:9092` | broker container not running | `docker compose up -d --wait` |
| `Cannot reach MySQL at 127.0.0.1:3307` | mysql container not running | `docker compose up -d --wait` |
| `Topic machine-temperature does not exist` | topic not created (or deleted) | Setup step 5, or `python scripts/reset_demo.py` |
| `Model file … not found` | model not trained yet | `python -m src.train_model` |
| `The model was saved with scikit-learn …` | library upgraded since training | `python -m src.train_model` |
| `Lost the connection to MySQL` (processor) | MySQL stopped mid-run | start MySQL, then the processor again; it re-reads the unfinished batch |
| Dashboard: `Database settings not found` | no `.streamlit/secrets.toml` | `python scripts/make_secrets.py`, reload the page |
| Dashboard: `Cannot reach MySQL` | MySQL not running | start it; the page recovers by itself |
| Dashboard: `The tables do not exist yet` | schema not applied | `python -m src.db --init` |

If the dashboard refreshes but the numbers do not move, check in this order: is the producer running,
is the processor's batch log moving, and does every query in `dashboard/app.py` go through `run_query()`
(nothing cached).

## Reset the demo (empty start)
Stop the processor first. Then:
```bash
python scripts/reset_demo.py      # recreate the topic, delete groups "scoring" and "forecast", empty all 4 tables, remove .producer_state
```
Safe to run twice in a row.

## Stop
Ctrl+C in the producer, forecaster, processor and dashboard terminals, then:
```bash
docker compose down               # keeps the data
docker compose down -v            # also deletes the Kafka and MySQL volumes (next start: Setup steps 4-6 again)
```
