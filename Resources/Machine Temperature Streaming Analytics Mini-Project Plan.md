# Machine Temperature Streaming Analytics: Mini-Project Plan

Oct 4, 2026 · @Sundareshwar

The project is a six-layer streaming pipeline (Kafka, Python, IsolationForest, MySQL, Streamlit) on a machine-temperature dataset with labelled failures. It is built in ten gated phases, each with one prompt to paste into Claude Code.

## What the brief requires

The project passes only if a streaming tool, an ML algorithm and an auto-refreshing dashboard are wired together and run end to end. Both files describe the same six layers: data source, streaming tool, stream processor, ML algorithm, database, visualization. Layers 2, 4 and 6 are mandatory categories; the other three are free choice, and the components table says the processor may reuse the streaming tool.

The guide's one warning matters more than any tool choice: streaming ingestion does not make a dashboard live. It is live only if new data keeps arriving and the view re-queries on its own.

Three deliverables:

1. Working code for all six layers.
2. A short report covering abstract, architecture, dataset, each layer, ML model, dashboard, results and conclusion.
3. A live demonstration or screen recording of the dashboard updating as data streams.

Submission checklist, copied from the guide's appendix:

- [ ] At least one streaming tool (Kafka, Storm, Spark or Flink) working
- [ ] At least one ML algorithm, trained and scoring live
- [ ] At least one visualization tool, auto-refreshing on a live data source
- [ ] A database serving layer between processor and dashboard
- [ ] The full pipeline runs end to end and the dashboard updates on its own
- [ ] A written report and a live demo or screen recording

## What we are building

A predictive-maintenance monitor for one industrial machine. Temperature readings are replayed into Kafka, a Python processor computes rolling metrics, an IsolationForest flags abnormal readings, MySQL stores the results, and a Streamlit dashboard redraws itself every 2 seconds.

The dataset is `machine_temperature_system_failure.csv` from the [Numenta Anomaly Benchmark](https://github.com/numenta/NAB/blob/master/data/README.md) (NAB). It was chosen over the guide's smart-meter example for one reason: it comes with labelled failures, so the report can state how many real failures the model caught instead of only showing red dots.

| Property | Value |
| --- | --- |
| Rows | 22,695, one reading every 5 minutes |
| Period | 2 Dec 2013 to 19 Feb 2014 |
| Columns | `timestamp`, `value` |
| Value range | 2.1 to 108.5 (the dataset does not state the unit) |
| File size | 732 KB |
| Labelled anomalies | 4, each with a window of about 2 days ([labels file](https://raw.githubusercontent.com/numenta/NAB/master/labels/combined_windows.json)) |
| Data quirk | 12 readings on 7 Jan 2014 repeat earlier timestamps, where the clock steps back 55 minutes |

NAB describes the anomalies as a planned shutdown, a hard-to-detect change, and the catastrophic failure that change led to.

The file is split once, by time. The first 2,000 rows (2 Dec to 9 Dec 2013, before the first labelled window) train the model offline. The remaining 20,695 rows are the live stream, so the model never scores data it was trained on.

## Architecture and pipeline

&#91;embedded content: pipeline architecture · 6 layers, offline training, 1 extra consumer\]

Data moves top to bottom and each layer talks only to its neighbours. The producer knows nothing about MySQL and the dashboard knows nothing about Kafka, which is why the demo can pause the producer while everything else keeps running.

One event on the topic `machine-temperature`:

```json
{
  "sensor_id": "machine-1",
  "seq": 2409,
  "event_time": "2013-12-11 06:00:00",
  "temperature": 102.736,
  "produced_at": "2026-10-10T09:15:03.120Z"
}
```

`event_time` is when the sensor took the reading, straight from the CSV. `produced_at` is when the producer sent it, so the processor can report pipeline lag. `seq` is the CSV row number. The message key is `sensor_id`, which keeps every reading for one machine in one partition, in order.

## Tech stack

Every layer uses a tool the brief lists, and the versions below are the current releases as of 4 Oct 2026. Kafka and MySQL run in Docker; all Python code runs on your machine in a virtual environment.

| Layer | Tool and version | Why this one |
| --- | --- | --- |
| 1 Data source | CSV replay, `producer.py` | The brief allows any source; a replay is repeatable for the demo |
| 2 Streaming tool | Apache Kafka 4.3.1, image `apache/kafka:4.3.1` | You already know Kafka from the labs; one container, no ZooKeeper |
| 3 Stream processor | Plain Python with `kafka-python` 3.0.11 | Windowing logic stays visible in about 100 lines, easy to explain in a viva |
| 4 ML algorithm | IsolationForest, scikit-learn 1.9.1 | Unsupervised, so it needs no labels to train; ARIMA is added as an extra |
| 5 Database | MySQL 8.4, image `mysql:8.4`, via PyMySQL 1.2.3 | The guide's choice; PyMySQL is pure Python, so nothing to compile on Windows |
| 6 Visualization | Streamlit 1.65.0 with Altair | Python only; `st.fragment` gives a real timed refresh |

Supporting pieces: Python 3.12 or newer (the current NumPy needs it), pandas 3.0, SQLAlchemy 2.1 for Streamlit's database connection, statsmodels 0.15 for the ARIMA extra, and Docker Desktop with Compose.

Two places where this plan deliberately departs from the guide:

- **No ZooKeeper.** The guide's run order starts ZooKeeper first. Kafka 4 removed ZooKeeper and runs in KRaft mode, so the broker is a single container. Say this in the report so nobody thinks a step is missing.
- **No meta-refresh tag.** The guide reloads the whole page every 2 seconds. A Streamlit fragment reruns only the live part, so sidebar controls keep their state.

## Key design decisions

The model scores four features per reading, not the raw temperature alone. I tested both on this dataset before writing the plan, and the guide's raw-value approach does not survive the train/stream split.

| Model | Trained on | Stream flagged | Labelled windows hit | Flags outside windows |
| --- | --- | --- | --- | --- |
| IsolationForest, raw value, contamination 0.01 | First 2,000 rows | 36.5% | 4 of 4 | 6,282 |
| IsolationForest, raw value, contamination 0.01 | Whole file (as in the guide) | 1.1% | 2 of 4 | 0 |
| IsolationForest, 4 features, contamination 0.005 | First 2,000 rows | 0.9% | 4 of 4 | 125 |
| **IsolationForest, 4 features, contamination 0.01** | First 2,000 rows | 1.4% | 4 of 4 | 201 |
| IsolationForest, 4 features, contamination 0.02 | First 2,000 rows | 2.6% | 4 of 4 | 406 |
| Rolling z-score baseline, 24 h, cut-off 3 | No training | 4.6% | 4 of 4 | 843 |
| Rolling z-score baseline, 24 h, cut-off 4 | No training | 1.5% | 2 of 4 | 241 |

The raw-value model fails because the training week only spans 52.7 to 94.4, while the machine later runs normally up to 108.5. Fitting on the whole file hides that problem but trains on the data it scores and misses two failures. The test followed this plan's feature definition exactly, with 200 trees. Four other random seeds also hit 4 of 4 and flagged 1.2% to 1.4%. Phase 6 reproduces the table inside the project. A flag outside a labelled window is not proven false, only unlabelled.

**The four features.** Temperature, deviation from the 1-hour rolling mean, 1-hour rolling standard deviation, and change since the previous reading. One hour is 12 readings, held in a `deque(maxlen=12)`.

**One feature class for training and serving.** `features.py` holds a single `RollingFeatures` class. `train_model.py` replays the training rows through it and `processor.py` feeds live events through it, so the two can never compute features differently.

**Score in batches.** One `predict` call took about 13 ms in my test whether it scored 1 row or 20, not microseconds as the guide says. The processor scores each polled batch with one call and writes it with one insert.

**Repeated timestamps are dropped.** An event whose `event_time` is not newer than the last accepted one is skipped and counted. A full replay should count 12.

**Restart-safe writes.** Kafka offsets are committed only after the MySQL write succeeds, and `readings.kafka_offset` is unique with `INSERT IGNORE`. A crash can replay a batch but cannot store it twice. On startup the processor refills its window from the last 12 stored rows.

**No anomaly flags during warm-up.** The first 12 events fill the window and are stored with `is_anomaly = 0`.

**Tables.**

| Table | Rows | Feeds |
| --- | --- | --- |
| `readings` | One per accepted event: event time, temperature, rolling mean, rolling std, deviation, anomaly score, flag, Kafka offset, produced and stored times | Live curve and anomaly markers |
| `metrics_current` | Exactly one, overwritten per batch: latest reading, rolling mean, running peak and minimum, events processed, anomalies flagged, late events dropped, pipeline lag | KPI tiles |
| `hourly_stats` (extra) | One per hour of event time: mean, minimum, maximum, anomaly count | Time-window panel |
| `forecasts` (extra) | One per forecast point: made at, target time, predicted value | Forecast line ahead of the curve |

**How the dashboard stays live.** The live part is a fragment with `run_every="2s"`. Every query passes `ttl=0`, because Streamlit's connection caches query results forever by default. The curve query reads only the newest 288 rows (24 hours of event time), so it stays fast as the table grows.

## Repository layout

One file per layer, so each report section can point at exactly one script.

```text
machine-temp-streaming/
├── CLAUDE.md                  project rules Claude Code reads every session
├── README.md                  setup and run order
├── docker-compose.yml         Kafka broker + MySQL
├── .env.example               ports, topic name, DB credentials
├── requirements.txt           pinned versions
├── data/
│   ├── machine_temperature_system_failure.csv
│   └── anomaly_windows.json   the 4 labelled windows from NAB
├── db/
│   └── schema.sql             all tables
├── src/
│   ├── config.py              reads .env, one place for every setting
│   ├── features.py            RollingFeatures, shared by training and serving
│   ├── db.py                  MySQL connection and batch writes
│   ├── producer.py            layer 1: CSV to Kafka, paced
│   ├── train_model.py         layer 4, offline: fit and save the model
│   ├── processor.py           layers 3 and 4, live: window, score, store
│   └── forecaster.py          extra: second consumer, ARIMA forecast
├── dashboard/
│   └── app.py                 layer 6: Streamlit
├── scripts/
│   ├── reset_demo.py          recreate topic, empty tables
│   └── evaluate.py            extra: score flags against NAB labels
├── models/                    anomaly_model.joblib (generated)
├── tests/
│   └── test_features.py
└── report/
    └── screenshots/
```

## Phases and gates

&#91;embedded content: build roadmap · 10 phases, a gate under each\]

Each phase ends at a gate you can check yourself in under a minute, and the next prompt is not sent until it passes. The project is submittable after Phase 5. Phases 6 and 7 are what lift it above the minimum.

## Claude Code prompts

Ten prompts, one per phase, pasted into Claude Code as written. Prompt 0 also writes `CLAUDE.md`, which holds every decision in this plan, so the later prompts stay short and Claude Code cannot drift from the design.

How to use them:

1. Start Claude Code in an empty folder named `machine-temp-streaming`, with the Context7 and Exa MCP servers connected.
2. Paste one prompt and let it finish. Run the gate check yourself rather than taking its word for it.
3. If the gate fails, paste the error back and say "fix this, stay inside Phase N".
4. Commit after every passed gate, then start a fresh session for the next phase. `CLAUDE.md` carries the context across sessions.

### Phase 0 · Setup

```text
You are setting up a university mini-project: a real-time streaming analytics pipeline. Do Phase 0 only and stop when its gate passes.

1. Create CLAUDE.md in the repo root with exactly the content between the two marker lines at the end of this message (leave the marker lines out).
2. Create the folders: data/, db/, src/, dashboard/, scripts/, models/, tests/, report/screenshots/.
3. Write docker-compose.yml with two services:
   - Kafka: image apache/kafka:4.3.1, one node in KRaft combined mode, default partitions 1. Publish port 9092, bind the broker listener to all interfaces and advertise localhost:9092, so clients on the host can connect. Follow the official single-node example on the apache/kafka Docker Hub page. Look it up with Exa or Context7; do not write it from memory.
   - MySQL: image mysql:8.4, host port 3307 mapped to container port 3306, database machine_monitor, an application user, a named volume and a healthcheck.
4. Write .env.example, copy it to .env, and write src/config.py, which reads .env and exposes every setting.
5. Write requirements.txt with the pinned versions from CLAUDE.md, create the virtual environment .venv and install into it.
6. Download the dataset CSV into data/. Download the labels file and save only the four machine_temperature windows to data/anomaly_windows.json. Check the CSV has 22,695 data rows.
7. Write db/schema.sql (CREATE TABLE IF NOT EXISTS for readings and metrics_current) and src/db.py with a command "python -m src.db --init" that applies it.
8. Write a first README.md with prerequisites and the setup commands used so far.
9. git init, add a .gitignore (.venv, .env, .producer_state, models/*.joblib, __pycache__, .streamlit/secrets.toml) and make the first commit.

Gate: run docker compose up -d, create the topic machine-temperature with 1 partition using kafka-topics.sh inside the broker container, run the schema init, and show me the real output of: docker compose ps, the topic description, SHOW TABLES, and a short kafka-python script run from the host that lists the topics (this proves the listener setup).

Do not write producer, processor, model or dashboard code yet.

--- CLAUDE.md START ---
# Machine Temperature Streaming Analytics

University mini-project for the course Data Stream Analytics.
Pipeline: CSV replay -> Kafka -> Python processor -> IsolationForest -> MySQL -> Streamlit.

## Rules for every session
- Work on one phase at a time. When its gate passes, stop and report. Never start the next phase unprompted.
- Before using any library API, check the current docs with the Context7 MCP server (kafka-python, Streamlit, scikit-learn, statsmodels, PyMySQL, SQLAlchemy). Use the Exa MCP server for anything Context7 lacks. Do not write library calls from memory.
- Every setting comes from src/config.py, which reads .env. No hard-coded hosts, ports, topics or credentials anywhere else.
- Keep the code simple and readable. It will be explained line by line in a viva. Plain functions, short files, comments that say why. No async, no ORM models, no class hierarchies, no libraries beyond the stack below.
- Do not change the event contract, table columns, topic name, feature list or model settings below without asking first.
- After each phase: run the gate check, show the real output, and add any new run step to README.md.
- Never say something works without running it. If a command fails, show the error.

## Stack (pinned)
- Python 3.12 or newer, virtual environment in .venv
- Kafka: Docker image apache/kafka:4.3.1, KRaft mode, no ZooKeeper, localhost:9092
- MySQL: Docker image mysql:8.4, host port 3307, database machine_monitor
- kafka-python 3.0.11, scikit-learn 1.9.1, pandas 3.0.x, numpy 2.x, joblib, PyMySQL 1.2.x, SQLAlchemy 2.1.x, streamlit 1.65.0, altair, statsmodels 0.15.0, python-dotenv, pytest

## Dataset
- data/machine_temperature_system_failure.csv from
  https://raw.githubusercontent.com/numenta/NAB/master/data/realKnownCause/machine_temperature_system_failure.csv
- Columns: timestamp, value. 22,695 rows, one every 5 minutes, 2013-12-02 21:15 to 2014-02-19 15:25.
- The unit is not stated. Label the value "temperature" with no unit.
- Labels: https://raw.githubusercontent.com/numenta/NAB/master/labels/combined_windows.json
  key realKnownCause/machine_temperature_system_failure.csv, four windows:
  2013-12-10 06:25 to 2013-12-12 05:35
  2013-12-15 17:50 to 2013-12-17 17:00
  2014-01-27 14:20 to 2014-01-29 13:30
  2014-02-07 14:55 to 2014-02-09 14:05
- Split by row order. Rows 0-1999 are TRAINING, used offline only. Rows 2000 onward are the STREAM.
  The producer never sends training rows and training never reads stream rows.
- Known quirk: 12 rows on 2014-01-07 repeat earlier timestamps. The producer sends them unchanged; the processor drops them.

## Event contract
Topic machine-temperature, 1 partition, message key = sensor_id, value = JSON:
  {"sensor_id": "machine-1", "seq": 2409, "event_time": "2013-12-11 06:00:00", "temperature": 102.736, "produced_at": "<UTC ISO-8601 send time>"}
seq is the zero-based CSV row number.

## Features (src/features.py, class RollingFeatures)
- Window: the last 12 accepted readings (1 hour of event time) in collections.deque(maxlen=12). The window includes the current reading.
- Feature order: temperature, deviation (temperature minus window mean), rolling_std (sample standard deviation of the window, 0.0 with fewer than 2 readings), delta (temperature minus previous reading, 0.0 for the first).
- train_model.py and processor.py both use this class. Nothing else may compute features.

## Model
- IsolationForest(n_estimators=200, contamination=0.01, random_state=42).
- Fitted on the features of training rows 12-1999 (rows 0-11 only warm up the window).
- Saved to models/anomaly_model.joblib together with the feature names and the scikit-learn version.
- The processor loads it once at startup, scores each polled batch with one call, and never retrains.
- anomaly_score is the decision_function value (negative means anomalous). is_anomaly is 1 when predict returns -1.
- The first 12 events after a cold start are stored with is_anomaly = 0.

## Processor behaviour
- Consumer group "scoring", auto commit off, start from the earliest offset.
- Loop: poll a batch -> drop events whose event_time is not newer than the last accepted event_time and count them -> update features -> score -> write the batch to MySQL in one transaction -> commit Kafka offsets.
- Writes are idempotent: readings.kafka_offset is UNIQUE and inserts use INSERT IGNORE.
- On startup, restore state if any exists: the window and the last accepted event_time from the newest 12 rows in readings, and the running peak, minimum and counters from metrics_current.
- Ctrl+C shuts down cleanly: finish the batch, commit, close.

## Tables (database machine_monitor)
- readings: id BIGINT AUTO_INCREMENT PK, kafka_offset BIGINT UNIQUE, seq INT, event_time DATETIME (indexed), temperature DOUBLE, rolling_avg DOUBLE, rolling_std DOUBLE, deviation DOUBLE, delta DOUBLE, anomaly_score DOUBLE NULL, is_anomaly TINYINT, produced_at DATETIME(3), stored_at DATETIME(3)
- metrics_current: id TINYINT PK (always 1), last_event_time, last_temperature, rolling_avg, peak_temperature, min_temperature, events_processed, anomalies_flagged, late_events_dropped, pipeline_lag_ms, updated_at
- Phase 7 adds hourly_stats and forecasts.

## Dashboard
- Streamlit. Everything live sits inside one function decorated with st.fragment(run_every=...).
- Database access through st.connection, with ttl=0 on every query.
- The curve reads only the newest 288 rows.

## Run order
1. docker compose up -d
2. python -m src.train_model (once)
3. python -m src.processor
4. streamlit run dashboard/app.py
5. python -m src.producer

## Phase gates
0 Setup: both containers healthy, topic exists, tables exist
1 Producer: console consumer shows JSON events at the set rate
2 Processor: readings row count climbs, metrics_current updates, no duplicates after a restart
3 ML model: flags appear on 2013-12-10 and 2013-12-16
4 Dashboard: the page redraws by itself while the producer runs
5 End to end: clean run from the README on a reset system, tests pass
6 Evaluation: comparison table of labelled windows hit and flags outside windows
7 Forecaster: forecast line drawn ahead of the live curve, two consumer groups
8 Demo runbook rehearsed
9 Report material assembled
--- CLAUDE.md END ---
```

### Phase 1 · Producer (layers 1 and 2)

```text
Read CLAUDE.md. Do Phase 1 only: the producer.

Check the kafka-python 3.x producer API with Context7 before writing.

Write src/producer.py:
- Reads data/machine_temperature_system_failure.csv and sends rows 2000 onward to the topic, one JSON event per row, following the event contract exactly.
- Options: --rate (events per second, default 10), --start-at "YYYY-MM-DD HH:MM" (skip stream rows before this event time), --limit N (stop after N events), --resume (continue after the seq saved in .producer_state).
- Saves the last sent seq to .producer_state every 50 events and on exit, so Ctrl+C followed by --resume continues where it stopped.
- Skips a row with a blank or non-numeric value instead of crashing, and logs it.
- Prints one progress line every 50 events: count sent, current event_time, actual rate.
- Flushes and closes the Kafka producer on Ctrl+C.

Gate: start a console consumer on the topic inside the broker container, run the producer with --rate 5 --limit 50, and show me the first 5 messages and the progress lines. Then show that --start-at "2013-12-10 00:00" begins at the right row, and that --resume continues from the saved seq.
```

### Phase 2 · Processor and database (layers 3 and 5)

```text
Read CLAUDE.md. Do Phase 2 only: the stream processor and the database layer. No ML yet.

Check the kafka-python 3.x consumer API (poll, manual commit) and PyMySQL with Context7 before writing.

1. src/features.py: the RollingFeatures class exactly as specified. Add tests/test_features.py with hand-calculated cases: a partly filled window, a full window that drops its oldest value, and delta on the first reading.
2. src/db.py: connect with PyMySQL using config; insert a batch of readings in one transaction with INSERT IGNORE; upsert the single metrics_current row; fetch the newest 12 readings.
3. src/processor.py: the loop described under "Processor behaviour". For this phase store anomaly_score as NULL and is_anomaly as 0. Keep running peak, running minimum, events processed and late events dropped, and write them to metrics_current with every batch. pipeline_lag_ms is stored_at minus produced_at for the newest event in the batch.
4. Log one line per batch: batch size, newest event_time, rolling average, lag.
5. scripts/reset_demo.py: delete and recreate the topic, empty all tables, remove .producer_state. It must be safe to run twice in a row.

Gate: run pytest. Start the processor, run the producer with --rate 20 --limit 400, and show me SELECT COUNT(*) FROM readings twice a few seconds apart, the metrics_current row, and the last 5 readings. Then stop and restart the processor mid-run and show that COUNT(*) equals COUNT(DISTINCT kafka_offset) and that rolling_avg continues smoothly across the restart.
```

### Phase 3 · ML model (layer 4)

```text
Read CLAUDE.md. Do Phase 3 only: offline training and live scoring.

Check the scikit-learn IsolationForest API with Context7 before writing.

1. src/train_model.py: read rows 0-1999 of the CSV, pass them through RollingFeatures in order, drop the 12 warm-up rows, fit the model from CLAUDE.md, and save model, feature names and scikit-learn version with joblib. Print the training time range, the row count, and the share of training rows the model itself flags.
2. src/processor.py: load the model once at startup. Exit with one clear sentence if the file is missing or the scikit-learn version differs from the saved one. Score each batch with one predict call and one decision_function call. Store anomaly_score and is_anomaly, keep warm-up events at is_anomaly = 0, and update anomalies_flagged in metrics_current.
3. Add a test proving that the feature rows built during training and by the processor for the same readings are identical.

Gate: train the model. Run reset_demo.py, start the processor, then run the producer with --start-at "2013-12-10 00:00" --rate 50 --limit 2200. Show me total rows, total flagged, and the count of flagged readings per day. An independent test of this design gave 8 flags on 2013-12-10 and 49 on 2013-12-16, when the temperature collapses to about 2. If your counts are far from that, stop and tell me why. Do not change the contamination value to make them match.
```

### Phase 4 · Dashboard (layer 6)

```text
Read CLAUDE.md. Do Phase 4 only: the Streamlit dashboard.

Check the current Streamlit docs with Context7 before writing: st.fragment with run_every, st.connection for MySQL through SQLAlchemy with the PyMySQL driver, st.metric, and Altair layered charts.

dashboard/app.py:
- A title and one caption line naming the pipeline.
- Sidebar: refresh interval (1 to 10 s, default 2), number of readings on the curve (default 288), and a pause-refresh toggle.
- One fragment that reruns on the chosen interval and holds everything live:
  - Six KPI tiles from metrics_current: latest temperature (delta against the rolling average), 1-hour rolling average, running peak, events processed, anomalies flagged, pipeline lag in ms.
  - A status line: "Live, last update N s ago", or when metrics_current.updated_at is more than 10 s old, a warning "No new data for N s. The dashboard is still refreshing."
  - An Altair chart of the newest readings by event_time: temperature line, rolling average line, red points where is_anomaly = 1. Fix the y-axis at 0 to 115 so it does not jump between refreshes.
  - A table of the 10 most recent flagged readings.
- Every query uses ttl=0. The curve query uses ORDER BY id DESC LIMIT n and is sorted ascending in pandas.
- When the tables are empty, show "Waiting for data" instead of an error.
- Generate .streamlit/secrets.toml from .env with a small script, document it in the README, and never commit it.

Gate: with the processor running, start the dashboard, then the producer with --start-at "2013-12-10 00:00" --rate 10. Give me the URL. I will check by eye that the curve extends and the tiles change with no clicks, that red points appear after about 30 seconds, and that stopping the producer turns the status line into the "No new data" warning while the page keeps refreshing.
```

### Phase 5 · End to end

After this phase the code meets every technical item on the submission checklist. Only the report and the demo remain.

```text
Read CLAUDE.md. Do Phase 5 only: make the whole pipeline reproducible and robust. No new features.

1. Prove a clean start: run docker compose down -v, then follow README.md step by step exactly as written, from an empty system to a live dashboard. Fix the README wherever a step is missing, wrong or out of order. Give Windows and Linux/macOS variants where a command differs.
2. Expected failures: producer, processor and dashboard must each print one clear sentence, with no stack trace, when Kafka or MySQL is not reachable, the topic is missing, or the model file is missing. The processor retries its connections for up to 60 s before giving up.
3. Add a "How to verify" section to the README with exact commands: consumer group lag for group scoring (kafka-consumer-groups.sh inside the broker container), row counts, and anomaly count.
4. Full replay: run reset_demo.py, then the whole stream at --rate 200 with the dashboard closed. Report events sent, rows stored (expected 20,683), late events dropped (expected 12), anomalies flagged, and the consumer lag when the producer finishes.
5. Run pytest and fix anything failing.

Gate: show me the final README, the output of step 4, and the pytest summary.
```

### Phase 6 · Evaluation (extra)

```text
Read CLAUDE.md. Do Phase 6 only: evaluate the model against the NAB labels. This is offline analysis plus one dashboard addition.

1. scripts/evaluate.py, run on the stream rows (2000 onward) through RollingFeatures, with repeated timestamps dropped the same way the processor drops them. Compare:
   - IsolationForest on the 4 features at contamination 0.005, 0.01, 0.02 and 0.05.
   - IsolationForest on raw temperature only at contamination 0.01, trained on the same training rows (the course guide's approach).
   - A no-ML baseline: rolling z-score against the previous 288 readings, with cut-offs 3 and 4.
   For each, report: share of the stream flagged, how many of the 4 labelled windows contain at least one flag, flags inside windows, flags outside windows, and the delay in minutes from each window's start to its first flag.
   Save the table to report/evaluation.csv and print it as Markdown.
2. Save two figures to report/: the full temperature series with the labelled windows shaded and the chosen model's flags marked, and a bar chart of flags outside windows per model.
3. Dashboard: shade the labelled windows behind the live curve, with a sidebar toggle "Show known failure windows", and add one line "Labelled failure windows with at least one flag: X of Y reached so far".
4. Do not change the model settings in CLAUDE.md because of these results. Tell me what you found and let me decide.

Gate: show me the Markdown table and both figures. An independent test gave: 4 features at 0.01 flags 1.4% of the stream and hits 4 of 4 windows with 201 flags outside; raw temperature flags 36.5%. Tell me if your numbers differ and why.
```

### Phase 7 · Forecaster and hourly window (extra)

```text
Read CLAUDE.md. Do Phase 7 only: two extras that show streaming ideas the core does not.

Check the statsmodels ARIMA API with Context7 before writing.

Part A, a second consumer group. src/forecaster.py:
- Consumes the same topic in its own consumer group "forecast", independently of the processor.
- Keeps the last 288 accepted readings. After every 12 new readings (1 hour of event time) it fits ARIMA with order (1,1,1) on that window and forecasts the next 12 readings with a confidence interval.
- Writes them to a new table forecasts (made_at_event_time, target_time, predicted_temperature, lower, upper), replacing any earlier forecast for the same target_time.
- If a fit fails, log it and carry on.

Part B, an event-time tumbling window. In the processor, aggregate accepted readings into 1-hour windows keyed by event_time and upsert one row per hour in a new table hourly_stats (hour_start, reading_count, avg_temperature, min_temperature, max_temperature, anomaly_count) with every batch.

Dashboard: draw the forecast as a dashed line ahead of the last reading with its interval as a faint band, and add a bar chart of anomaly_count per hour for the last 48 hours of event time.

Add both tables to db/schema.sql, CLAUDE.md and reset_demo.py, and the forecaster to the README run order.

Gate: run processor and forecaster together. Show me the kafka-consumer-groups.sh output listing both groups with their own offsets, sample rows from both new tables, and give me the dashboard URL so I can check that the dashed forecast stays ahead of the curve.
```

### Phase 8 · Demo

```text
Read CLAUDE.md. Do Phase 8 only: prepare the live demonstration. Write DEMO.md, a runbook I can follow while presenting, with every command in order for my operating system and what I should see after each one:

1. Before presenting: containers up, reset_demo.py run, model trained, terminals arranged, dashboard open and showing "Waiting for data".
2. Start the processor (and forecaster), then the producer with --start-at "2013-12-10 00:00" --rate 10. Calculate from the data how many seconds pass before the first flag and before the temperature collapse on 2013-12-16, and write those times in.
3. The pause: Ctrl+C the producer, point at the "No new data" warning while the page keeps refreshing, then restart with --resume.
4. Proof points: a console consumer on the topic, kafka-consumer-groups.sh showing lag, SELECT COUNT(*) climbing.
5. A fallback section: what to try if Kafka or MySQL does not start, and when to switch to the screen recording.

Then run the whole runbook yourself once, time each step, and correct DEMO.md wherever reality differed.
```

### Phase 9 · Report

This produces a draft built from real results. The screenshots and the reflection are yours to add.

```text
Read CLAUDE.md. Do Phase 9 only: assemble report material. Do not invent any number; every figure must come from a command you ran.

1. Run one full replay on a reset system and collect into report/results.md: events sent, rows stored, late events dropped, peak and minimum temperature with their event times, anomalies flagged, labelled windows detected, average and maximum pipeline lag, and the Phase 6 comparison table.
2. Write report/report_draft.md with the sections the course guide asks for: Abstract, Architecture, Dataset, one section per layer with a short real code snippet from this repo, ML model, Dashboard, Results, Conclusion. For each layer explain why it exists, not only what the code does. State that Kafka 4 runs in KRaft mode without ZooKeeper, and explain why the model uses four features instead of raw temperature.
3. Leave clearly marked TODO lines where I must add my own screenshots (live dashboard, console consumer, consumer groups, MySQL counts) and my own "what I learned" paragraph.
4. List every source used: the NAB dataset and its paper, and the Kafka, Streamlit and scikit-learn documentation.
```

## Demo and report

The demo starts the replay at 10 Dec 2013, which puts the first red markers on screen after about 27 seconds and the machine's collapse after about 3 minutes.

1. Before presenting: containers up, `reset_demo.py` run, model trained, dashboard open on "Waiting for data".
2. Start the processor, then the producer with `--start-at "2013-12-10 00:00" --rate 10`. The curve draws itself and the tiles tick over with no clicks.
3. About 27 s in, the first flags appear (event time 10 Dec 22:40), inside the first labelled window.
4. About 2 min 50 s in, flags begin as the temperature starts to fall. By 3 min 14 s it reaches 2.1, the lowest value in the file.
5. Stop the producer with Ctrl+C. The page keeps refreshing, the numbers freeze and the "No new data" warning appears. Restart with `--resume` and the curve continues. This is the guide's own teaching moment: live depends on data moving, not on the refresh timer.
6. Show the proof: a console consumer on the topic, consumer-group lag, and `SELECT COUNT(*)` climbing.

At `--rate 20` the same two moments arrive at about 14 s and 1 min 37 s. Record the screen during a rehearsal so there is a fallback.

The report follows the guide's eight sections. This is what each one holds for this project:

| Report section | Content |
| --- | --- |
| Abstract | What was built, and the headline result from your own Phase 6 run: labelled windows detected and share of readings flagged |
| Architecture | The six-layer diagram, the offline training step and the two consumer groups |
| Dataset | NAB machine temperature: format, one sample event, the train and stream split, the repeated-timestamp quirk |
| Each layer | One short section and one real snippet per file, saying why the layer exists |
| ML model | The four features, why raw temperature failed, the train and serve split, the contamination comparison |
| Dashboard | A screenshot, and the two things that keep it live: the timed fragment and `ttl=0` |
| Results | Events processed, peak and minimum, anomalies flagged, windows detected, pipeline lag, the comparison table |
| Conclusion | What you learned, the limits (one sensor, replayed data, unlabelled flags) and next steps |

## Risks and sources

None of these blocks the plan, and each is cheap to avoid once you know about it.

- **Port clashes.** A Kafka left running natively from the labs holds port 9092; stop it before `docker compose up`. MySQL is mapped to host port 3307 to stay clear of a local MySQL on 3306.
- **Docker on Windows.** Docker Desktop needs WSL 2 and must be running before any `docker compose` command. Pull both images before demo day so the demo needs no internet.
- **Client hangs on connect.** If the producer cannot reach the broker on Windows, a common fix is to use `127.0.0.1` instead of `localhost` in both the broker's advertised listener and `.env`.
- **Model file tied to its library version.** A joblib model loads reliably only under the scikit-learn version that saved it. Retrain after any upgrade.
- **Frozen dashboard.** Check in this order: is the producer running, is the processor's batch log moving, does the query pass `ttl=0`.
- **Flags outside labelled windows.** About 200 are expected over a full replay. Present them as unlabelled, not as errors.
- **Viva.** Claude Code writes the code, but you will be asked to explain it. After each phase, read the new file and ask it to explain anything unclear before you commit.

Sources, checked on 4 Oct 2026:

- Course files: `MiniProject_Guide.pdf` and `MINI_project_components_table.pdf`
- [Apache Kafka quickstart](https://kafka.apache.org/quickstart/): current release and Docker image tag
- [apache/kafka on Docker Hub](https://hub.docker.com/r/apache/kafka): single-node KRaft configuration and tool paths
- [mysql official image on Docker Hub](https://hub.docker.com/_/mysql): supported tags
- [NAB data README](https://github.com/numenta/NAB/blob/master/data/README.md), [labelled windows](https://raw.githubusercontent.com/numenta/NAB/master/labels/combined_windows.json) and [the CSV itself](https://raw.githubusercontent.com/numenta/NAB/master/data/realKnownCause/machine_temperature_system_failure.csv)
- [kafka-python](https://github.com/dpkp/kafka-python): producer, consumer and supported broker versions, read through Context7
- Streamlit docs, read through Context7: [fragments](https://docs.streamlit.io/develop/concepts/architecture/fragments) and [connecting to MySQL](https://docs.streamlit.io/develop/tutorials/databases/mysql)
- [PyPI](https://pypi.org/): package versions for kafka-python, streamlit, scikit-learn, statsmodels, pandas, numpy, PyMySQL and SQLAlchemy
