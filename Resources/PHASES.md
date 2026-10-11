# Machine Temperature Streaming Analytics: Phase-by-Phase Guide

This file explains what happens in each of the ten phases, in detail. It is the companion to the project plan, which holds the architecture, the design decisions and the Claude Code prompts.

For every phase you get:

- **Goal**: what exists at the end that did not exist before
- **What you do**: your own steps, outside Claude Code
- **What Claude Code builds**: the files and behaviour the phase prompt asks for
- **What to understand**: the ideas you should be able to explain in a viva
- **How to check the gate**: commands to run yourself
- **If it goes wrong**: the usual causes
- **Done when**: the condition for moving on

Rules that apply to every phase:

1. One phase at a time. Do not paste the next prompt until the gate passes.
2. Run the gate check yourself. Do not accept "it works" from Claude Code without seeing the output.
3. Commit after each passed gate: `git add -A && git commit -m "Phase N: <name>"`.
4. Start a fresh Claude Code session for each phase. `CLAUDE.md` carries the context.
5. Read every new file before committing. If you cannot explain a line, ask Claude Code to explain it.

The commands below assume the Kafka container is named `broker` and the MySQL container `mysql`. If Claude Code names them differently in `docker-compose.yml`, use the names shown by `docker compose ps`.

---

## Overview

| Phase | Name | Layers covered | Type | Main output |
| --- | --- | --- | --- | --- |
| 0 | Setup | Infrastructure | Core | Repo, Docker services, dataset, empty tables |
| 1 | Producer | 1, 2 | Core | `src/producer.py` sending events to Kafka |
| 2 | Processor and database | 3, 5 | Core | `src/features.py`, `src/db.py`, `src/processor.py` |
| 3 | ML model | 4 | Core | `src/train_model.py`, live scoring in the processor |
| 4 | Dashboard | 6 | Core | `dashboard/app.py` refreshing by itself |
| 5 | End to end | All | Core | Reproducible run from the README |
| 6 | Evaluation | 4 | Extra | Model comparison against real failure labels |
| 7 | Forecaster and hourly window | 2, 3, 4 | Extra | Second consumer group, ARIMA forecast |
| 8 | Demo | All | Delivery | `DEMO.md` runbook and a screen recording |
| 9 | Report | All | Delivery | Report draft with real numbers |

The project is submittable after Phase 5. Phases 6 and 7 are what lift it above the minimum.

---

## Phase 0: Setup

**Goal.** An empty but working skeleton: the repository, Kafka and MySQL running in Docker, the dataset downloaded, the tables created, and `CLAUDE.md` in place.

**What you do.**

1. Install Docker Desktop (Windows needs WSL 2 enabled) and start it.
2. Install Python 3.12 or newer and Git.
3. Create an empty folder named `machine-temp-streaming` and open Claude Code inside it.
4. Confirm the Context7 and Exa MCP servers are connected.
5. If a Kafka from the earlier lab experiments is still running natively, stop it. It holds port 9092.
6. Paste the Phase 0 prompt from the plan.

**What Claude Code builds.**

- `CLAUDE.md`: the project rules, stack versions, event contract, feature list, model settings, table definitions and phase gates. Every later session reads this first.
- The folder structure: `data/`, `db/`, `src/`, `dashboard/`, `scripts/`, `models/`, `tests/`, `report/screenshots/`.
- `docker-compose.yml` with two services:
  - Kafka `apache/kafka:4.3.1`, a single node in KRaft mode, reachable at `localhost:9092`.
  - MySQL `mysql:8.4`, on host port 3307, with database `machine_monitor`.
- `.env.example`, `.env` and `src/config.py`. All hosts, ports, topic names and credentials live here and nowhere else.
- `requirements.txt` with pinned versions, and a virtual environment in `.venv`.
- `data/machine_temperature_system_failure.csv` and `data/anomaly_windows.json`.
- `db/schema.sql` with the `readings` and `metrics_current` tables, and `src/db.py` with an init command.
- A first `README.md`, a `.gitignore`, and the first commit.

**What to understand.**

- **KRaft mode.** Kafka 4 no longer uses ZooKeeper. The broker manages its own cluster metadata, so one container is the whole cluster. The course guide still mentions ZooKeeper because it was written for older Kafka.
- **Listeners.** The broker has to *advertise* an address clients can reach. Since your Python code runs on the host, it advertises `localhost:9092`.
- **Why Docker.** One command starts both services, and they behave the same on Windows and Ubuntu.
- **Why one config file.** Changing a port or password in one place avoids hunting through five scripts.

**How to check the gate.**

```bash
docker compose ps
docker exec -it broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic machine-temperature
docker exec -it mysql mysql -u <app_user> -p machine_monitor -e "SHOW TABLES;"
```

You should see both containers running (MySQL marked healthy), the topic with 1 partition, and two tables. Claude Code should also show a short Python script run from the host that lists the topic. That proves the listener setup works from outside the container.

**If it goes wrong.**

- `port is already allocated`: something else is using 9092 or 3307. Stop it, or change the host port in `.env` and `docker-compose.yml`.
- `docker: command not found` or `cannot connect to the Docker daemon`: Docker Desktop is not running.
- The host script hangs but the in-container command works: the advertised listener is wrong. On Windows, try `127.0.0.1` instead of `localhost` in both the compose file and `.env`.
- The CSV does not have 22,695 data rows: the download was cut off. Download it again.

**Done when.** Both containers are up, the topic exists, the tables exist, the host script lists the topic, and the first commit is made.

---

## Phase 1: Producer (layers 1 and 2)

**Goal.** A script that turns the static CSV into a live stream of JSON events on the Kafka topic.

**What you do.** Paste the Phase 1 prompt. Keep a second terminal open with a console consumer so you can watch events arrive.

**What Claude Code builds.** `src/producer.py`, which:

- Reads the CSV and sends rows 2000 onward. Rows 0 to 1999 are reserved for training and are never sent.
- Sends one JSON event per row, matching the event contract: `sensor_id`, `seq`, `event_time`, `temperature`, `produced_at`.
- Uses `sensor_id` as the message key.
- Sleeps between sends so the rate matches `--rate` (default 10 events per second).
- Supports `--start-at "YYYY-MM-DD HH:MM"`, `--limit N` and `--resume`.
- Saves the last sent `seq` to `.producer_state` every 50 events and on exit.
- Skips rows with a blank or non-numeric value instead of crashing.
- Prints a progress line every 50 events and shuts down cleanly on Ctrl+C.

**What to understand.**

- **Why pacing matters.** Without the sleep, all 20,695 rows would be sent in a second or two and the dashboard would have nothing left to show. The delay is what makes a file behave like a sensor.
- **Topic, partition, offset.** A topic is a named log. Each message gets an increasing offset inside its partition. One partition means strict ordering, which the rolling window needs.
- **Message key.** Kafka sends all messages with the same key to the same partition. With one sensor this changes nothing, but it is the correct design if more machines were added.
- **Event time and processing time.** `event_time` is when the sensor took the reading (2013). `produced_at` is when your producer sent it (today). Streaming systems keep both.
- **Serialization.** Kafka carries bytes. The producer turns a Python dict into JSON bytes; the consumer reverses it.

**How to check the gate.**

Terminal 1:

```bash
docker exec -it broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic machine-temperature --from-beginning
```

Terminal 2:

```bash
python -m src.producer --rate 5 --limit 50
python -m src.producer --start-at "2013-12-10 00:00" --limit 5
python -m src.producer --resume --limit 5
```

Check that JSON lines appear in terminal 1 at about 5 per second, that the first run starts at `seq` 2000 with `event_time` `2013-12-09 19:55:00`, that `--start-at` begins exactly at `2013-12-10 00:00:00`, and that `--resume` continues from the next `seq`.

**If it goes wrong.**

- `NoBrokersAvailable`: the broker is down or the bootstrap address in `.env` is wrong.
- Messages appear all at once: the sleep is missing or the producer only flushes at the end.
- `temperature` arrives as a string: the value was not converted to float before sending.

**Done when.** The console consumer shows correctly shaped events at the chosen rate, and `--start-at` and `--resume` behave as described.

---

## Phase 2: Processor and database (layers 3 and 5)

**Goal.** A long-running consumer that reads events, computes rolling metrics in memory, and writes them to MySQL. No ML yet.

**What you do.** Paste the Phase 2 prompt. You will run three terminals: processor, producer, and a MySQL client.

**What Claude Code builds.**

- `src/features.py` with the `RollingFeatures` class. It holds the last 12 accepted readings in a `deque(maxlen=12)` and returns four numbers per reading:
  - `temperature`
  - `deviation`: temperature minus the window mean
  - `rolling_std`: sample standard deviation of the window
  - `delta`: temperature minus the previous reading
- `tests/test_features.py` with hand-calculated cases.
- `src/db.py`: batch insert into `readings` with `INSERT IGNORE`, upsert of the single `metrics_current` row, and a fetch of the newest 12 readings.
- `src/processor.py`, running this loop:
  1. Poll a batch of messages from Kafka.
  2. Drop any event whose `event_time` is not newer than the last accepted one, and count it.
  3. Update the rolling features.
  4. Write the batch to MySQL in one transaction.
  5. Commit the Kafka offsets.
- `scripts/reset_demo.py`: deletes and recreates the topic, empties the tables, removes `.producer_state`.

**What to understand.**

- **Stateful stream processing.** The processor remembers things between events: the window, the running peak, the running minimum, the counters. That memory is the "state".
- **Sliding window.** `deque(maxlen=12)` drops the oldest reading automatically when the 13th arrives. Twelve readings at 5-minute spacing is one hour of event time.
- **Consumer group and offsets.** The group `scoring` remembers how far it has read. Restarting the processor continues from the last committed offset, not from the beginning.
- **At-least-once delivery with idempotent writes.** Offsets are committed only after MySQL has the data. If the processor crashes between the two, the batch is replayed, and the unique `kafka_offset` column stops it being stored twice.
- **Late and duplicate events.** Twelve rows on 7 Jan 2014 repeat earlier timestamps. The processor drops them and counts them. This is a simple version of late-event handling.
- **Serving layer.** The processor writes and the dashboard reads. Neither waits for the other.
- **Why a single-row metrics table.** "Current state" is one thing, not a growing list. Updating one row is fast and makes the KPI query trivial.

**How to check the gate.**

```bash
pytest
python scripts/reset_demo.py
python -m src.processor                         # terminal 1
python -m src.producer --rate 20 --limit 400    # terminal 2
```

In MySQL, run these twice a few seconds apart:

```sql
SELECT COUNT(*) FROM readings;
SELECT * FROM metrics_current;
SELECT event_time, temperature, rolling_avg, rolling_std FROM readings ORDER BY id DESC LIMIT 5;
```

Then stop the processor with Ctrl+C while the producer is still running, start it again, and run:

```sql
SELECT COUNT(*), COUNT(DISTINCT kafka_offset) FROM readings;
```

The two numbers must be equal, and `rolling_avg` should not jump at the restart point.

**If it goes wrong.**

- Row count stays at zero: check the processor log first, then confirm the topic has messages with the console consumer.
- Duplicates after restart: `INSERT IGNORE` or the unique index is missing.
- `rolling_avg` jumps after restart: the processor did not restore its window from the last 12 stored rows.
- Peak or event counters reset to zero after restart: the processor did not restore them from `metrics_current`.

**Done when.** Tests pass, the row count climbs while the producer runs, `metrics_current` updates, and a mid-run restart produces no duplicates.

---

## Phase 3: ML model (layer 4)

**Goal.** An IsolationForest trained offline on the first 2,000 rows and used by the processor to score every live event.

**What you do.** Paste the Phase 3 prompt. Read the training output carefully; you will quote it in the report.

**What Claude Code builds.**

- `src/train_model.py`, which:
  1. Reads rows 0 to 1999.
  2. Passes them through the same `RollingFeatures` class the processor uses.
  3. Drops the first 12 warm-up rows.
  4. Fits `IsolationForest(n_estimators=200, contamination=0.01, random_state=42)`.
  5. Saves the model, the feature names and the scikit-learn version to `models/anomaly_model.joblib`.
- Changes to `src/processor.py`:
  - Load the model once at startup; exit with a clear message if it is missing.
  - Score each polled batch with one call.
  - Store `anomaly_score` and `is_anomaly` with each reading.
  - Keep the first 12 events after a cold start at `is_anomaly = 0`.
  - Update `anomalies_flagged` in `metrics_current`.
- A test proving that training and serving compute identical features for the same readings.

**What to understand.**

- **Train/serve split.** Training is slow and happens once, offline. Serving is fast and happens for every event. The stream never retrains the model.
- **Unsupervised anomaly detection.** IsolationForest needs no labels. It builds random trees that split the data; unusual points get isolated in fewer splits, so a short average path means "anomalous".
- **Contamination.** The share of training data the model treats as anomalous. It sets the threshold. Higher means more flags.
- **Why four features and not raw temperature.** The training week only covers 52.7 to 94.4, but the machine later runs normally up to 108.5. A raw-value model treats all of that as abnormal and flags about a third of the stream. Deviation, rolling std and delta describe *behaviour* (sudden change, instability), which stays comparable as the level drifts.
- **Train/serve skew.** If training and serving computed features differently, the model would score nonsense. Sharing one class makes that impossible.
- **No data leakage.** The model never sees the rows it later scores.

**How to check the gate.**

```bash
python -m src.train_model
python scripts/reset_demo.py
python -m src.processor                                                        # terminal 1
python -m src.producer --start-at "2013-12-10 00:00" --rate 50 --limit 2200    # terminal 2
```

```sql
SELECT COUNT(*) AS total_rows, SUM(is_anomaly) AS flagged FROM readings;
SELECT DATE(event_time) AS day, COUNT(*) AS flags FROM readings WHERE is_anomaly = 1 GROUP BY day ORDER BY day;
```

A test of this design gave 8 flags on 2013-12-10 and 49 on 2013-12-16, the day the temperature collapses to about 2. Your numbers should be close.

**If it goes wrong.**

- Zero flags: the processor is not calling the model, or the warm-up rule is applied to every event.
- A third or more of the rows flagged: the features are not being computed (only raw temperature is passed), or the feature order differs between training and serving.
- Version mismatch error on startup: retrain the model in the current virtual environment.
- Do not "fix" odd numbers by changing the contamination value. Find the cause.

**Done when.** The model file exists, flags appear on 10 Dec and 16 Dec in roughly the expected counts, and the identical-features test passes.

---

## Phase 4: Dashboard (layer 6)

**Goal.** A Streamlit page that shows the pipeline's state and redraws itself with no clicks.

**What you do.** Paste the Phase 4 prompt. The gate for this phase is checked by eye, so watch the page yourself.

**What Claude Code builds.** `dashboard/app.py`, with:

- A title and a caption naming the pipeline.
- A sidebar: refresh interval (default 2 s), number of readings on the curve (default 288), and a pause-refresh toggle.
- One fragment that reruns on the chosen interval and holds everything live:
  - Six KPI tiles: latest temperature, 1-hour rolling average, running peak, events processed, anomalies flagged, pipeline lag in ms.
  - A status line: "Live, last update N s ago", or a warning "No new data for N s. The dashboard is still refreshing." once the last update is more than 10 s old.
  - A chart with the temperature line, the rolling average line, and red points on flagged readings, with the y-axis fixed at 0 to 115.
  - A table of the 10 most recent flagged readings.
- "Waiting for data" when the tables are empty.
- `.streamlit/secrets.toml` generated from `.env` (never committed).

**What to understand.**

- **The two reasons a dashboard looks frozen.** Either no new data is arriving, or the view is not re-querying. You must solve both. This is the guide's central warning.
- **Fragments.** `st.fragment(run_every=...)` reruns only that function on a timer. The guide's meta-refresh tag reloads the whole page and resets the sidebar.
- **Query caching.** Streamlit's connection caches query results with no expiry by default. Every query needs `ttl=0`, or the page will keep showing the first result.
- **Bounded queries.** The curve reads only the newest 288 rows (24 hours of event time), so it stays fast however large the table becomes.
- **The rolling average lags the raw line.** That visible lag is the window doing its job, and it is worth pointing out in the demo.

**How to check the gate.**

```bash
python scripts/reset_demo.py
python -m src.processor                                              # terminal 1
streamlit run dashboard/app.py                                       # terminal 2
python -m src.producer --start-at "2013-12-10 00:00" --rate 10       # terminal 3
```

Watch for:

1. "Waiting for data" before the producer starts.
2. The curve extending and the tiles changing with no clicks.
3. Red points appearing after about 30 seconds.
4. After Ctrl+C on the producer, the status line turning into the "No new data" warning while the page keeps refreshing.
5. After restarting with `--resume`, the curve continuing.

**If it goes wrong.**

- The page only updates when you click or press R: the live content is outside the fragment, or `run_every` is not set.
- The page refreshes but shows the same numbers: `ttl=0` is missing, or the processor has stopped.
- The chart rescales and jumps on every refresh: the y-axis domain is not fixed.
- Connection error on load: `.streamlit/secrets.toml` is missing or points at port 3306 instead of 3307.

**Done when.** All five observations above hold.

---

## Phase 5: End to end

**Goal.** Anyone can clone the repository, follow the README, and reach a live dashboard. Expected failures produce clear messages.

**What you do.** Paste the Phase 5 prompt. Afterwards, follow the README yourself from a clean state, without Claude Code's help. If you get stuck, the README is not finished.

**What Claude Code does.**

1. Runs `docker compose down -v` and follows `README.md` from scratch, fixing every missing or wrong step.
2. Makes the producer, processor and dashboard print one clear sentence when Kafka or MySQL is down, the topic is missing, or the model file is missing. The processor retries its connections for up to 60 s.
3. Adds a "How to verify" section to the README.
4. Runs the whole stream at `--rate 200` and reports the totals.
5. Runs the tests and fixes failures.

**What to understand.**

- **Reproducibility.** A project that only runs on the machine it was built on will fail in a demo on another laptop.
- **Consumer lag.** The gap between the newest message in the topic and the last one the group has processed. Lag near zero means the processor keeps up.
- **Full-replay numbers.** These are the headline figures for the report.

**How to check the gate.**

```bash
docker compose down -v
# now follow README.md line by line
python scripts/reset_demo.py
python -m src.processor                 # terminal 1
python -m src.producer --rate 200       # terminal 2, about 2 minutes
docker exec -it broker /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group scoring
pytest
```

```sql
SELECT events_processed, anomalies_flagged, late_events_dropped, peak_temperature, min_temperature FROM metrics_current;
SELECT COUNT(*) FROM readings;
```

Expected after a full replay: 20,695 events sent, 20,683 rows stored, 12 late events dropped, and lag at 0 once the processor has caught up. A test of this design flagged about 284 readings.

**If it goes wrong.**

- Stored rows are not 20,683: check `late_events_dropped`. If it is not 12, the drop rule is wrong.
- Lag keeps growing at rate 200: the processor is scoring or inserting one row at a time instead of per batch.
- README steps fail on your OS: tell Claude Code which OS you use and have it add that variant.

**Done when.** A clean run from the README works, the full-replay numbers match, and the tests pass. At this point every technical item on the submission checklist is met.

---

## Phase 6: Evaluation (extra)

**Goal.** Evidence, not just red dots: how many of the four real failure windows the model caught, and how it compares with simpler approaches.

**What you do.** Paste the Phase 6 prompt. Read the results and decide for yourself whether to keep contamination at 0.01.

**What Claude Code builds.**

- `scripts/evaluate.py`, which runs these on the stream rows:
  - IsolationForest on the 4 features at contamination 0.005, 0.01, 0.02 and 0.05.
  - IsolationForest on raw temperature only (the guide's approach).
  - A no-ML baseline: rolling z-score against the previous 288 readings, with cut-offs 3 and 4.
- For each: share of the stream flagged, labelled windows hit (out of 4), flags inside windows, flags outside windows, and detection delay per window.
- `report/evaluation.csv` and two figures in `report/`.
- A dashboard addition: labelled windows shaded behind the curve, with a sidebar toggle, and a line showing how many labelled windows have been flagged so far.

**What to understand.**

- **The trade-off.** Higher contamination catches failures sooner but raises more flags outside the labelled windows. There is no free setting; you choose a point on the curve and justify it.
- **Labelled windows.** NAB gives a window of about two days around each known anomaly. A flag anywhere inside counts as a detection.
- **Flags outside windows are unlabelled, not proven false.** The machine may have behaved oddly at times nobody recorded. Say this in the report.
- **Why a baseline.** If a rolling z-score did just as well, the ML model would add nothing. The comparison shows the model catches all four windows with far fewer flags.
- **Detection delay.** How long after a window opens the first flag appears. Useful for the predictive-maintenance story.

**How to check the gate.**

```bash
python scripts/evaluate.py
```

Reference numbers from an independent test of this design:

| Model | Stream flagged | Windows hit | Flags outside windows |
| --- | --- | --- | --- |
| IsolationForest, raw value, 0.01 | 36.5% | 4 of 4 | 6,282 |
| IsolationForest, 4 features, 0.005 | 0.9% | 4 of 4 | 125 |
| IsolationForest, 4 features, 0.01 | 1.4% | 4 of 4 | 201 |
| IsolationForest, 4 features, 0.02 | 2.6% | 4 of 4 | 406 |
| Rolling z-score, cut-off 3 | 4.6% | 4 of 4 | 843 |
| Rolling z-score, cut-off 4 | 1.5% | 2 of 4 | 241 |

Your table should be close. Then open the dashboard and confirm the shaded windows appear and the toggle hides them.

**If it goes wrong.**

- Numbers far from the reference: the evaluation script is computing features differently from `RollingFeatures`, or it is not dropping the 12 repeated timestamps.
- Claude Code changes the model settings to get better numbers: revert it. The prompt forbids that.

**Done when.** The table and both figures exist, the numbers are explained, and the dashboard shows the labelled windows.

---

## Phase 7: Forecaster and hourly window (extra)

**Goal.** Two additions that show streaming ideas the core does not: a second independent consumer, and an event-time tumbling window.

**What you do.** Paste the Phase 7 prompt. You now run four processes: processor, forecaster, dashboard, producer.

**What Claude Code builds.**

- **Part A, `src/forecaster.py`.**
  - Consumes the same topic in its own consumer group, `forecast`.
  - Keeps the last 288 accepted readings.
  - After every 12 new readings, fits ARIMA(1,1,1) and forecasts the next 12 readings (one hour) with a confidence interval.
  - Writes them to a new `forecasts` table.
- **Part B, in the processor.** Aggregates readings into 1-hour windows by `event_time` and upserts one row per hour into a new `hourly_stats` table: count, mean, minimum, maximum, anomaly count.
- **Dashboard.** A dashed forecast line ahead of the last reading with a faint interval band, and a bar chart of anomalies per hour for the last 48 hours of event time.
- Both tables added to `db/schema.sql`, `CLAUDE.md` and `reset_demo.py`.

**What to understand.**

- **Consumer groups.** Two groups read the same topic independently, each with its own offsets. Neither slows the other. This is the main reason to put a broker in the middle instead of calling the processor directly.
- **Tumbling and sliding windows.** The 1-hour rolling mean is a sliding window: it moves with every event. The hourly stats are tumbling windows: fixed, non-overlapping hours.
- **Event-time windows.** The hours are those of the sensor's clock (2013), not the wall clock.
- **ARIMA(1,1,1).** One autoregressive term, one differencing step, one moving-average term. It forecasts the short-term trend. It is refitted regularly on recent data because the series drifts.
- **Why the forecaster is separate.** Fitting a model is slower than scoring. Keeping it in its own process means a slow fit never delays anomaly detection.

**How to check the gate.**

```bash
python scripts/reset_demo.py
python -m src.processor      # terminal 1
python -m src.forecaster     # terminal 2
streamlit run dashboard/app.py   # terminal 3
python -m src.producer --start-at "2013-12-10 00:00" --rate 10   # terminal 4
docker exec -it broker /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups
```

```sql
SELECT * FROM forecasts ORDER BY target_time DESC LIMIT 5;
SELECT * FROM hourly_stats ORDER BY hour_start DESC LIMIT 5;
```

You should see two groups, `scoring` and `forecast`, each with its own offset, rows in both tables, and a dashed line on the dashboard that stays ahead of the live curve.

**If it goes wrong.**

- No forecast for the first few minutes: expected. The forecaster needs enough readings before its first fit.
- Forecast line is flat or wild during the collapse on 16 Dec: expected. ARIMA cannot predict a failure; note this as a limitation.
- Only one consumer group listed: the forecaster is using the processor's group id.

**Done when.** Both groups appear with separate offsets, both tables fill, and the forecast is drawn ahead of the curve.

---

## Phase 8: Demo

**Goal.** A rehearsed, timed demonstration and a screen recording as backup.

**What you do.** This phase is mostly yours.

1. Paste the Phase 8 prompt so Claude Code writes and test-runs `DEMO.md`.
2. Follow `DEMO.md` yourself, start to finish, at least twice.
3. Record the screen on the second run.
4. Pull both Docker images in advance so the demo needs no internet.

**The demo sequence.**

| Step | Action | What the audience sees |
| --- | --- | --- |
| 1 | Before presenting: containers up, `reset_demo.py`, model trained, dashboard open | "Waiting for data" |
| 2 | Start processor, then producer with `--start-at "2013-12-10 00:00" --rate 10` | Curve draws itself, tiles tick over |
| 3 | About 27 s in | First red markers, inside the first labelled window |
| 4 | About 2 min 50 s to 3 min 14 s in | Temperature collapses to 2.1, a cluster of red markers |
| 5 | Ctrl+C the producer | Page keeps refreshing, numbers freeze, "No new data" warning |
| 6 | Restart with `--resume` | Curve continues from where it stopped |
| 7 | Show the proof points | Console consumer output, consumer-group lag, `COUNT(*)` climbing |

At `--rate 20` the moments in steps 3 and 4 arrive at about 14 s and 1 min 37 s.

**What to be ready to explain.**

- Why pausing the producer freezes the numbers but not the refresh: live depends on data moving, not on the timer.
- What each of the six layers does and why it exists.
- Why Kafka sits between producer and processor.
- Why the model was trained on only the first 2,000 rows.
- Why four features instead of raw temperature.
- What contamination means and what happens when you change it.
- Why there is no ZooKeeper.

**If it goes wrong on the day.**

- A container will not start: `docker compose down`, then `docker compose up -d`, then `reset_demo.py`.
- Dashboard shows an error: restart Streamlit; the processor and Kafka are unaffected.
- Nothing recovers within a minute: play the recording and talk over it.

**Done when.** You have run the demo twice without looking anything up, and the recording shows the pause and resume.

---

## Phase 9: Report

**Goal.** The written report, built from real results.

**What you do.**

1. Paste the Phase 9 prompt so Claude Code collects the numbers and writes a draft.
2. Take your own screenshots: live dashboard, console consumer, consumer groups, MySQL counts.
3. Rewrite the draft in your own words, and write the "what I learned" section yourself.
4. Check every number in the report against `report/results.md`.

**What Claude Code produces.**

- `report/results.md`: events sent, rows stored, late events dropped, peak and minimum with their event times, anomalies flagged, labelled windows detected, average and maximum pipeline lag, and the Phase 6 comparison table.
- `report/report_draft.md` with the eight sections the guide asks for, and marked TODO lines for your screenshots and reflection.

**Report structure.**

| Section | Content |
| --- | --- |
| Abstract | What was built and the headline result, in 2 to 4 sentences |
| Architecture | The six-layer diagram, the offline training step, the two consumer groups |
| Dataset | NAB machine temperature: format, one sample event, the train and stream split, the repeated-timestamp quirk |
| Each layer | One short section and one real code snippet per file, saying why the layer exists |
| ML model | The four features, why raw temperature failed, the train and serve split, the contamination comparison |
| Dashboard | A screenshot, and the two things that keep it live: the timed fragment and `ttl=0` |
| Results | Events processed, peak and minimum, anomalies flagged, windows detected, pipeline lag, the comparison table |
| Conclusion | What you learned, the limits (one sensor, replayed data, unlabelled flags), next steps |

**Points worth making that most reports will not have.**

- A measured comparison against real failure labels, not just a screenshot of red dots.
- A documented case where the guide's approach fails on this data, and why.
- Restart-safe processing: offsets committed after the database write, idempotent inserts.
- Handling of repeated timestamps as late events.
- Two independent consumer groups on one topic.

**Final submission checklist.**

- [ ] At least one streaming tool (Kafka) working
- [ ] At least one ML algorithm (IsolationForest) trained and scoring live
- [ ] At least one visualization tool (Streamlit) auto-refreshing on a live data source
- [ ] A database serving layer (MySQL) between processor and dashboard
- [ ] The full pipeline runs end to end and the dashboard updates on its own
- [ ] A written report
- [ ] A live demo or screen recording

**Done when.** Every box above is ticked and every number in the report traces back to a command you ran.
