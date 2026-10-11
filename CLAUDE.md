# Machine Temperature Streaming Analytics

University mini-project for the course Data Stream Analytics.
Pipeline: CSV replay -> Kafka -> Python processor -> IsolationForest -> MySQL -> Streamlit.
A predictive-maintenance monitor for one industrial machine: readings are replayed into Kafka, a Python
processor computes rolling metrics, an IsolationForest flags abnormal readings, MySQL stores the results,
and a Streamlit dashboard redraws itself every 2 seconds.

## Current state (update after every passed gate)
- Last passed phase: **Phase 6 Evaluation** (2026-10-11). Earlier: Phase 5 `63398c7`, Phase 4 `2361911`, Phase 3 `d1015aa`, Phase 2 `a0f8d98`, Phase 1 `145079d`, Phase 0 `87a0b36`.
- **The project is submittable** (every technical checklist item is met). **Next step: Phase 7, forecaster and hourly
  window (extra).** Its prompt is under "Phase 7 · Forecaster and hourly window (extra)" in the plan file; gate checks are
  in `Resources/PHASES.md` § Phase 7. Contamination stays 0.01 unless the user decides otherwise after reading Phase 6.
- The model is trained (`models/anomaly_model.joblib`, gitignored; retrain with `python -m src.train_model` on a fresh clone).
- `.streamlit/secrets.toml` is generated (`python scripts/make_secrets.py`, gitignored).
- After Phase 6, MySQL and the topic hold a 2,200-event replay from 2013-12-10 (left for the user's visual check of the
  window shading). Before any gate run, use `python scripts/reset_demo.py` (stop the processor first).
- Git repo on branch `main`. After each passed gate, commit with the message `Phase N: <name>`.
- Infrastructure: containers `broker` (Kafka) and `mysql` from docker-compose.yml. At the start of a session run
  `docker compose ps`; if they are not `(healthy)`, run `docker compose up -d --wait`.

## What already exists (reuse it, do not rewrite it)
- `src/config.py`: every setting (see Settings below).
- `src/db.py`: `connect()`, `init_schema()` (CLI `python -m src.db --init`), `READING_COLUMNS` (tuple order of a readings row),
  `write_batch(conn, readings, metrics)` (one transaction: INSERT IGNORE readings + upsert metrics_current id=1, rollback on error),
  `fetch_latest_readings(conn, n=12)` (oldest first), `fetch_metrics(conn)` (dict or None), `empty_tables(conn)`.
- `src/features.py`: `WINDOW_SIZE = 12`, `FEATURE_NAMES = ["temperature", "deviation", "rolling_std", "delta"]` (model column order),
  `RollingFeatures.update(t)` -> dict with the 4 features + `rolling_avg`; `RollingFeatures.restore(temps)` refills the window (oldest first).
- `src/features.py` also has `model_row(f)`: the feature dict -> list in `FEATURE_NAMES` order. Training and serving both use it.
- `src/train_model.py`: `load_training_rows()` -> (event_time, temperature) for rows 0-1999, `build_training_rows(temps)` ->
  model rows with the 12 warm-up rows dropped; model constants `N_ESTIMATORS`, `CONTAMINATION`, `RANDOM_STATE`.
  Saves `{"model", "feature_names", "sklearn_version"}` with joblib.
- `src/checks.py`: `retry(what, attempt, errors, fix, wait_s=0, every_s=3)` (retries only the given connection errors,
  then `sys.exit` with one sentence: what, reason, fix), `reason(error)`, `connect_mysql(wait_s)`, `require_topic(wait_s)`
  (KafkaAdminClient with `bootstrap_timeout_ms=5000`, then checks the topic is listed), `START_HINT`.
- `src/producer.py` calls `checks.require_topic()` before creating its producer (`allow_auto_create_topics=False`).
- `src/processor.py` main order: `load_model()` -> `checks.connect_mysql(60)` -> `checks.require_topic(60)` -> restore ->
  consumer (`allow_auto_create_topics=False`) -> signal handlers. A MySQL `OperationalError` on write exits with one
  sentence (the batch's offsets are not committed, so a restart re-reads it). `STARTUP_WAIT_S = 60`.
- `src/processor.py` (functions): `load_model()` (exits with one sentence if the file is missing, the scikit-learn version differs or the
  feature names differ), `make_consumer()`, `new_state()` (cold-start state, `warmup_left = 12`), `restore_state()` (also restores
  `anomalies_flagged`, sets `warmup_left = 0`), `process_batch(messages, features, state, model)` (one `predict` + one
  `decision_function` per batch), `main()`.
- `scripts/evaluate.py`: `load_stream()` (csv + float(), like the producer), `load_windows()`, `accept_in_order()` (the
  processor's drop rule), `stream_model_rows()`, `isolation_forest_flags(train, stream, c)`, `without_warm_up()`,
  `zscore_flags(temps, cutoff, history=288)`, `score_against_windows(times, flags, windows)`, `markdown_table()`,
  `save_figures()`. Writes report/evaluation.csv, report/stream_flags.png, report/flags_outside_windows.png.
- `dashboard/app.py` (Phase 6 additions): imports `src.config` via one `sys.path.insert` (for `ANOMALY_WINDOWS`),
  `load_windows()` (`@st.cache_data`), sidebar toggle "Show known failure windows" (default on), shading = `mark_rect`
  layer of the windows overlapping the curve, clipped to its range; `show_window_progress()` prints
  "Labelled failure windows with at least one flag: X of Y reached so far" (Y = windows with start <= last_event_time).
- `dashboard/app.py`: Streamlit page. Sidebar (refresh 1-10 s, curve rows 12-2016 default 288, pause toggle), then one
  fragment `live_view()` with `run_every=None if paused else refresh_s`: status line, 6 KPI tiles, Altair chart, last 10 flags.
  Query helpers `load_metrics()`, `load_curve(n)`, `load_recent_flags()` all use `ttl=0, show_spinner=False`.
- `scripts/make_secrets.py`: writes `.streamlit/secrets.toml` (`[connections.mysql]`, dialect mysql, driver pymysql) from config.
- `scripts/reset_demo.py`: recreate topic, delete group `scoring`, empty tables, remove `.producer_state`. Safe to run twice.
- `tests/test_checks.py` (4 tests: retry succeeds after failures, gives up with one sentence, `wait_s=0` tries once,
  unexpected errors are not hidden). `tests/test_evaluate.py` (5 hand-calculated tests: inside/outside/delay scoring,
  drop rule, z-score flag / no flag / flat history). Total: 21 tests.
- `tests/test_features.py` (5 hand-calculated tests), `tests/test_scoring.py` (7 tests: train/serve rows identical, warm-up
  across batches, no warm-up after restore, one call per batch, missing/old model refused; uses a `FakeModel` that records
  its input) + `pytest.ini` (`pythonpath = .`, so plain `pytest` works).
- `db/schema.sql`: `readings` and `metrics_current`, applied and verified.
- `data/anomaly_windows.json`: `{"source": url, "key": nab_key, "windows": [{"start": ..., "end": ...}, ...]}`,
  4 windows, timestamps formatted like `"2013-12-10 06:25:00.000000"`.
- Topic `machine-temperature` exists with 1 partition.
- `src/producer.py`: `load_rows()`, `make_event()`, `read_state()`/`write_state()`, `choose_rows()`; CLI options `--rate`, `--limit`, `--start-at` | `--resume` (mutually exclusive). Fresh run starts at seq 2000 (`2013-12-09 19:55:00`); `--start-at "2013-12-10 00:00"` starts at seq 2049. `temperature` is sent with the CSV's full precision (not rounded).

## Settings (.env -> src/config.py)
Import them as `from src import config`, then `config.KAFKA_TOPIC` and so on. To add a setting, add it to
`.env.example`, `.env` and `config.py` together.
| .env key | config.py name | Value now |
| --- | --- | --- |
| KAFKA_BOOTSTRAP_SERVERS | `KAFKA_BOOTSTRAP_SERVERS` (str) | localhost:9092 |
| KAFKA_TOPIC | `KAFKA_TOPIC` (str) | machine-temperature |
| SENSOR_ID | `SENSOR_ID` (str) | machine-1 |
| CONSUMER_GROUP | `CONSUMER_GROUP` (str) | scoring |
| MYSQL_HOST / MYSQL_PORT | `MYSQL_HOST` (str) / `MYSQL_PORT` (int) | 127.0.0.1 / 3307 |
| MYSQL_DATABASE / MYSQL_USER / MYSQL_PASSWORD | same names (str) | machine_monitor / monitor / (in .env) |
| MYSQL_ROOT_PASSWORD | not exposed (used only by docker-compose.yml) | (in .env) |
| DATA_CSV | `DATA_CSV` (absolute Path) | data/machine_temperature_system_failure.csv |
| ANOMALY_WINDOWS | `ANOMALY_WINDOWS` (absolute Path) | data/anomaly_windows.json |
| MODEL_PATH | `MODEL_PATH` (absolute Path) | models/anomaly_model.joblib |
| (none) | `SCHEMA_SQL`, `ROOT` (absolute Paths) | db/schema.sql, repo root |
| (none, design constant) | `STREAM_START_SEQ` (int) | 2000: first stream row; rows before it are training only |
| (none) | `PRODUCER_STATE` (absolute Path) | .producer_state (gitignored) |

## Library notes learned while building (kafka-python 3.0.11)
- Serializers live in `kafka`: `DefaultSerializer()` (str <-> UTF-8 bytes) and `JsonSerializer()` (dict <-> JSON bytes).
  The same classes deserialize: the producer serializes with them and the processor's consumer uses
  `key_deserializer=DefaultSerializer()` and `value_deserializer=JsonSerializer()` (checked in Context7, working).
- 3.0 producer defaults: `acks='all'`, `enable_idempotence=True`, `linger_ms=0`. `send()` is asynchronous: call `flush()`
  before recording progress, as the producer does before writing `.producer_state`.
- 3.0 removed `NoBrokersAvailable`. An unreachable broker shows up as a timeout (`kafka.errors.KafkaTimeoutError`,
  after `bootstrap_timeout_ms`, default 30 s). Phase 5's clear-error handling must catch that, not `NoBrokersAvailable`.
- 3.0 changed the consumer default `session_timeout_ms` to 45 s (relevant when restarting the processor in Phase 2).
- KafkaProducer, KafkaConsumer and KafkaAdminClient all work as context managers (`with ... as producer:`).
- Consumer defaults are `auto_offset_reset='latest'` and `enable_auto_commit=True`; the processor overrides both.
- Admin: `list_groups()` returns dicts (`g["group_id"]`). `delete_groups([g])` returns a **dict** `{group_id: "OK" or error name}`
  even though its docstring says "list of tuples" (the stale docstring caused a reset_demo bug; trust the real value).
- After a `kill -9` of the processor, the restarted one waits up to `session_timeout_ms` (45 s; measured 27 s) for the dead
  member to expire before it gets the partition. A clean Ctrl+C stop rejoins at once.

## Processor decisions made in Phase 2 (keep them)
- All wall-clock times (`produced_at`, `stored_at`, `updated_at`) are stored as **naive UTC** in DATETIME(3).
- `events_processed` = accepted and stored events. Late ones go to `late_events_dropped`, so processed + dropped = consumed.
- Ctrl+C/SIGTERM only set a flag; the loop finishes the batch (MySQL write, then Kafka commit) and closes.
- On startup the newest stored `kafka_offset` is restored; messages at or below it are crash replays and are skipped quietly
  (not re-stored, not counted as late). This assumes topic and tables are reset together, as reset_demo.py does.
- A batch where every event is late still upserts metrics_current, keeping the last known temperature/rolling_avg/lag.
- MySQL: `STORED` is a reserved word, so do not use it as a column alias in queries.

## Model decisions made in Phase 3 (keep them)
- The model is fitted on a plain list of lists (no DataFrame), and the processor scores a list of lists, so there are no
  feature-name warnings. Column order is guarded three ways: `model_row()`, `FEATURE_NAMES` saved in the joblib file and
  checked by `load_model()`, and the train/serve test.
- Warm-up readings are still scored and their `anomaly_score` is stored; only `is_anomaly` is forced to 0. Warm-up counts
  accepted readings across batches (`state["warmup_left"]`), not per batch.
- `load_model()` silences scikit-learn's `InconsistentVersionWarning` and compares the saved version itself, so a version
  mismatch gives one clear sentence instead of a warning followed by unsupported behaviour.
- Training output (quote it in the report): rows 12-1999 (2013-12-02 22:15 to 2013-12-09 19:50), 1,988 rows, fitted in
  0.19 s, the model flags 20 of its own 1,988 training rows (1.01%).

## Dashboard decisions made in Phase 4 (keep them)
- `st.connection("mysql", type="sql")` reads `.streamlit/secrets.toml`; the dashboard does not import `src`.
- Every query passes `ttl=0` (otherwise cached forever) and `show_spinner=False` (otherwise a spinner flashes every refresh).
- Status age = naive-UTC now minus `metrics_current.updated_at` (stored as naive UTC). Over 10 s -> warning.
- The pause toggle sets `run_every=None`. Sidebar widgets are outside the fragment, so changing one reruns the whole
  script (Streamlit docs: "widget outside a fragment -> full script rerun") and the fragment picks up the new value.
- Chart: `transform_fold` of temperature + rolling_avg into coloured lines, a red point layer from `is_anomaly == 1`,
  y fixed with `alt.Y(...).scale(domain=[0, 115])`. The page footer shows its own refresh time.
- Verified headless with `streamlit.testing.v1.AppTest` against the real MySQL (empty -> "Waiting for data"; stale ->
  warning; live -> "Live"); the chart's datasets were read back for slider 12/288/1000/2016 and equal the newest N rows.
- `streamlit run` binds all interfaces (Network URL shown); add `--server.address localhost` to keep it local.

## Robustness decisions made in Phase 5 (keep them)
- The broker has `auto.create.topics.enable=true` (default). Our producer and consumer pass `allow_auto_create_topics=False`
  and check the topic at startup, so a missing topic gives one sentence instead of being silently recreated (verified:
  after the drill only `__consumer_offsets` existed). docker-compose.yml is unchanged.
- kafka-python 3.0.11: an unreachable broker raises `KafkaTimeoutError: Unable to bootstrap from localhost:9092` from
  `KafkaAdminClient`/`KafkaConsumer`/`KafkaProducer` after `bootstrap_timeout_ms` (measured 3.0 s at 3000, 5.2 s at 5000).
  No kafka log noise is printed.
- PyMySQL: refused connection -> `OperationalError` 2003 immediately; MySQL stopped mid-run -> 2013 "Lost connection".
  `write_batch` only rolls back if `conn.open` (a dead connection cannot roll back); the processor closes `conn` only if open.
- Dashboard: pandas 3 wraps errors raised while a query runs in `pandas.errors.DatabaseError` (the SQLAlchemy error is
  `__cause__`); connection failures before the query arrive as `sqlalchemy.exc.OperationalError` directly. `live_view()`
  unwraps and shows one `st.error` sentence (MySQL down / tables missing); the fragment keeps its timer and recovers.
  Missing secrets: `st.connection` raises `StreamlitAPIException` -> `st.error` + `st.stop()`.
- AppTest: `at.secrets` does NOT reach `st.connection`; it reads `.streamlit/secrets.toml` from the working directory.
  Run AppTest checks from the repo root (or from an empty folder to simulate missing secrets), with an absolute script path.
- README is one ordered path (Setup 1-9, then three terminals). Windows lines are given but were not run on Windows.

## Evaluation results and decisions from Phase 6 (the user decides on contamination; nothing was tuned)
- Method: stream rows 2000+, 12 repeated timestamps dropped (20,683 scored), RollingFeatures from a cold start, first 12
  never flagged; every IsolationForest uses n_estimators=200, random_state=42 and training rows 12-1999 (raw-temperature
  model: the same rows' temperatures). z-score: previous 288 accepted readings, sample std, |z| > cut-off, first 288 not
  scored, std 0 never flags. A flag is inside a window when start <= t <= end. Delay = minutes from window start to the
  first flag inside it.
- Self-check: "IF 4 features c=0.01" equals the saved pipeline model on the same rows (284 flags = the Phase 5 replay).

| Detector | Flagged | Windows | Flags | Inside | Outside | Delay w1/w2/w3/w4 (min) |
| --- | --- | --- | --- | --- | --- | --- |
| IF 4 features c=0.005 | 0.93% | 4 of 4 | 192 | 67 | 125 | 975 / 1325 / 1760 / 2700 |
| **IF 4 features c=0.01** | 1.37% | 4 of 4 | 284 | 83 | 201 | 975 / 165 / 210 / 2695 |
| IF 4 features c=0.02 | 2.64% | 4 of 4 | 547 | 141 | 406 | 960 / 135 / 205 / 2695 |
| IF 4 features c=0.05 | 6.31% | 4 of 4 | 1305 | 412 | 893 | 0 / 135 / 200 / 0 |
| IF raw temperature c=0.01 | 36.46% | 4 of 4 | 7541 | 1259 | 6282 | 0 / 0 / 1365 / 25 |
| z-score 288 cut-off 3 | 4.65% | 4 of 4 | 961 | 118 | 843 | 1035 / 100 / 0 / 2700 |
| z-score 288 cut-off 4 | 1.47% | 2 of 4 | 304 | 63 | 241 | - / 100 / - / 2700 |
  Every reference number from the plan is reproduced (0.9/125, 1.4/201, 2.6/406, 36.5%/6,282, 4.6/843, 1.5% 2 of 4 241).
- Finding for the report: the features describe sudden change, so slow drifts are caught late. Window 4 (the
  catastrophic failure, a slow decline from 7 Feb) is first flagged 2,695 min (about 45 h) after it opens, at the fast
  recovery jump near its end; window 1 is first flagged at 10 Dec 22:40 (975 min in), on the fast rise. Only c=0.05
  catches windows 1 and 4 at their start, at the price of 893 flags outside windows.
- Figures are written with Altair + vl-convert-python (added to requirements.txt with the user's approval; Altair
  needs it to save PNG). `alt.data_transformers.disable_max_rows()` is needed for the 20,683-point series.
- Dashboard check (AppTest, real MySQL): after the 2,200-event replay from 2013-12-10 the line says "2 of 2" and the
  curve has a rect layer for Window 2 clipped to the shown range; toggle off removes it. After the rest of the stream
  (--resume): "4 of 4", and no rect in the last 288 readings (no window there). Starting at 2013-12-10 instead of seq 2000
  gives 275 flags, not 284, because the cold-start warm-up then falls on different readings.

## How to run gate checks from Claude Code (lessons from Phase 2)
- Run long-lived processes with `run_in_background` and `python -u` (unbuffered), so their log lines appear live.
- Foreground `sleep` is blocked: wait for a condition with a background `until ...; do sleep 1; done` loop that also
  stops on errors and after a time limit.
- Find the processor PID with `pgrep -f "^.venv/bin/python -u -m src.processor"`. A looser pattern also matches the
  shell running pgrep itself.
- Never fire dependent steps as parallel tool calls: if the reset fails, the steps after it run against a half-reset
  system. In Phase 2 this made the processor skip new messages (new offsets 0-39 were below the stored last offset 999).
- Prove results independently where possible (e.g. recompute rolling stats with pandas, not RollingFeatures).
  `pd.read_sql` on a PyMySQL connection works but warns; use SQLAlchemy for real code (Phase 4 does).
- pandas 3.0's default CSV float parser differs from Python `float()` in the last bit for some values (327 of 3,797
  stream rows, max 1.4e-14). The producer uses `float()`, so stored temperatures equal
  `pd.read_csv(..., float_precision="round_trip")`. Use that (or the csv module) for exact comparisons (Phase 6).
- To stop Streamlit, match the process exactly (e.g. `pgrep -f "^.venv/bin/python .venv/bin/streamlit"` or kill the
  background task); a loose `pkill -f "streamlit run"` pattern also matches the calling shell.

## Project docs (read these when a detail is missing here)
- `Resources/Machine Temperature Streaming Analytics Mini-Project Plan.md`: the full design, the evaluation table, and the exact prompt for each phase. This is the source of truth.
- `Resources/PHASES.md`: for each phase, the goal, what to understand for the viva, the gate commands and the usual failures.
- `Resources/MiniProject_Guide.pdf`: the course brief (smart-meter worked example, report structure, submission checklist).
- `Resources/MINI_project_components_table.pdf`: the course's table of allowed tools per layer.

## What the brief requires
- Six layers: 1 data source, 2 streaming tool, 3 stream processor, 4 ML algorithm, 5 database, 6 visualization.
- Layers 2, 4 and 6 are mandatory categories: one of Kafka/Storm/Spark/Flink, one ML algorithm, one auto-refreshing viz tool.
- Deliverables: (1) working code for all six layers, (2) a short report, (3) a live demo or screen recording of the dashboard updating.
- **Key idea:** streaming ingestion does not make a dashboard live. It is live only if new data keeps arriving
  AND the view re-queries on its own. Pausing the producer must freeze the numbers while the page keeps refreshing.
- Submission checklist:
  - [x] Streaming tool (Kafka) working (Phase 1: producer -> topic verified with the console consumer)
  - [x] ML algorithm trained and scoring live (Phase 3: IsolationForest trained offline, every batch scored in the processor)
  - [x] Visualization auto-refreshing on a live data source (Phase 4: Streamlit fragment re-queries MySQL every 2 s)
  - [x] Database serving layer between processor and dashboard (Phase 2: processor writes readings + metrics_current)
  - [x] Full pipeline runs end to end; dashboard updates on its own (Phase 5: clean run from the README + full replay)
  - [ ] Written report + live demo or screen recording

## Deliberate departures from the course guide (say so in the report)
- **No ZooKeeper.** Kafka 4 removed it and runs in KRaft mode, so the broker is one container.
- **No meta-refresh tag.** `st.fragment(run_every=...)` reruns only the live part, so the sidebar keeps its state.
- **Four features, not raw temperature**, and the model is trained on the first 2,000 rows only, not the whole file (see below).
- **Batch scoring.** One `predict` call takes about 13 ms whether it scores 1 row or 20, so score each polled batch in one call.

## Rules for every session
- Work on one phase at a time. When its gate passes, stop and report. Never start the next phase unprompted.
- Before using any library API, check the current docs with the Context7 MCP server (kafka-python, Streamlit, scikit-learn, statsmodels, PyMySQL, SQLAlchemy). Use the Exa MCP server for anything Context7 lacks. Do not write library calls from memory.
- Every setting comes from src/config.py, which reads .env. No hard-coded hosts, ports, topics or credentials anywhere else.
- Keep the code simple and readable. It will be explained line by line in a viva. Plain functions, short files, comments that say why. No async, no ORM models, no class hierarchies, no libraries beyond the stack below.
- Do not change the event contract, table columns, topic name, feature list or model settings below without asking first.
- After each phase: run the gate check, show the real output, and add any new run step to README.md.
- Never say something works without running it. If a command fails, show the error.
- Never tune contamination (or anything else) to make numbers match the expected ones. If results differ, stop and explain why.

## Stack (pinned)
- Python 3.12 or newer, virtual environment in .venv
- Kafka: Docker image apache/kafka:4.3.1, KRaft mode, no ZooKeeper, localhost:9092
- MySQL: Docker image mysql:8.4, host port 3307, database machine_monitor
- kafka-python 3.0.11, scikit-learn 1.9.1, pandas 3.0.x, numpy 2.x, joblib, PyMySQL 1.2.x, SQLAlchemy 2.1.x, streamlit 1.65.0, altair, statsmodels 0.15.0, python-dotenv, pytest
- vl-convert-python 1.9.0.post1 (added in Phase 6 with the user's approval: Altair's PNG exporter for report figures)

## Environment notes (this machine)
- Ubuntu 26.04, system Python 3.14.4. `.venv` is built on it (python3.14-venv installed) and all pins in requirements.txt are installed and verified (`pip check` clean, IsolationForest and ARIMA smoke-tested) as of 2026-10-11. Use `.venv/bin/python` or activate the venv.
- Docker Engine 29.1.3 + Compose v2.40.3 installed from Ubuntu's repo (docker.io, docker-compose-v2, docker-buildx), service enabled, user `esh` in the `docker` group (docker works without sudo).
- `.env` holds local demo credentials (app user `monitor`); docker-compose.yml reads the MYSQL_* values from it. PyMySQL authenticates to MySQL 8.4 without the `cryptography` package.
- db/schema.sql is split on semicolons by `src/db.py`, so never put a semicolon inside a SQL comment.
- `sudo` cannot prompt inside Claude Code; use `pkexec <command>` (graphical password dialog) for system installs.
- Docker container names are `broker` (Kafka) and `mysql` (fixed by `container_name` in docker-compose.yml).
- Port clashes: a native Kafka from the labs holds 9092, so stop it first. MySQL uses host port 3307 to avoid a local 3306.
- Kafka CLI tools are inside the broker at `/opt/kafka/bin/` (kafka-topics.sh, kafka-console-consumer.sh, kafka-consumer-groups.sh).
- Node/npx are not installed. Context7 and Exa are reached through hosted HTTP MCP endpoints.

## Dataset
- data/machine_temperature_system_failure.csv from
  https://raw.githubusercontent.com/numenta/NAB/master/data/realKnownCause/machine_temperature_system_failure.csv
- Columns: timestamp, value. 22,695 rows, one every 5 minutes, 2013-12-02 21:15 to 2014-02-19 15:25. Value range 2.1 to 108.5.
- The unit is not stated. Label the value "temperature" with no unit.
- Labels: https://raw.githubusercontent.com/numenta/NAB/master/labels/combined_windows.json
  key realKnownCause/machine_temperature_system_failure.csv, four windows:
  2013-12-10 06:25 to 2013-12-12 05:35
  2013-12-15 17:50 to 2013-12-17 17:00
  2014-01-27 14:20 to 2014-01-29 13:30
  2014-02-07 14:55 to 2014-02-09 14:05
  (NAB: a planned shutdown, a hard-to-detect change, and the catastrophic failure that change led to.)
- Split by row order. Rows 0-1999 are TRAINING, used offline only. Rows 2000 onward are the STREAM (20,695 rows).
  The producer never sends training rows and training never reads stream rows.
- Known quirk: 12 rows on 2014-01-07 repeat earlier timestamps (the clock steps back 55 minutes). The producer sends them unchanged; the processor drops them.

## Event contract
Topic machine-temperature, 1 partition, message key = sensor_id, value = JSON:
  {"sensor_id": "machine-1", "seq": 2409, "event_time": "2013-12-11 06:00:00", "temperature": 102.736, "produced_at": "<UTC ISO-8601 send time>"}
seq is the zero-based CSV row number. event_time = when the sensor took the reading (from the CSV);
produced_at = when the producer sent it (used to compute pipeline lag).

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

### Why four features (evidence from the plan; Phase 6 reproduces it)
| Model | Trained on | Stream flagged | Windows hit | Flags outside windows |
| --- | --- | --- | --- | --- |
| IF, raw value, c=0.01 | first 2,000 rows | 36.5% | 4 of 4 | 6,282 |
| IF, raw value, c=0.01 | whole file (guide) | 1.1% | 2 of 4 | 0 |
| **IF, 4 features, c=0.01** | first 2,000 rows | 1.4% | 4 of 4 | 201 |
| Rolling z-score 24 h, cut-off 3 | none | 4.6% | 4 of 4 | 843 |
Raw value fails because the training week spans only 52.7-94.4, while the machine later runs normally up to 108.5.
A flag outside a labelled window is unlabelled, not proven false.

### Expected numbers (reference checks, never targets to tune towards)
- Phase 3 (replay from 2013-12-10, 2,200 events): about 8 flags on 2013-12-10 and 49 on 2013-12-16 (temperature collapses to about 2).
- Phase 5 full replay: 20,683 rows stored, 12 late events dropped. Measured: exactly that, plus 284 flagged (the
  PHASES.md reference is about 284), peak 108.51 at 2013-12-26 15:45, minimum 2.08 at 2013-12-16 17:25, LAG 0 when the
  producer finished, max pipeline lag 57 ms at --rate 200. A full replay from seq 2000 also flags 9 readings on
  2013-12-09 21:30-22:10 (a fast rise right after the stream starts; outside the labelled windows).
- Demo at --rate 10 from 2013-12-10: first flags after about 27 s (event time 10 Dec 22:40), the collapse starts about 2 min 50 s in, and the minimum of 2.1 arrives at about 3 min 14 s.

## Processor behaviour
- Consumer group "scoring", auto commit off, start from the earliest offset.
- Loop: poll a batch -> drop events whose event_time is not newer than the last accepted event_time and count them -> update features -> score -> write the batch to MySQL in one transaction -> commit Kafka offsets.
- Writes are idempotent: readings.kafka_offset is UNIQUE and inserts use INSERT IGNORE. Offsets are committed only after the MySQL write succeeds.
- On startup, restore state if any exists: the window and the last accepted event_time from the newest 12 rows in readings, and the running peak, minimum and counters from metrics_current.
- Ctrl+C shuts down cleanly: finish the batch, commit, close.

## Tables (database machine_monitor)
- readings: id BIGINT AUTO_INCREMENT PK, kafka_offset BIGINT UNIQUE, seq INT, event_time DATETIME (indexed), temperature DOUBLE, rolling_avg DOUBLE, rolling_std DOUBLE, deviation DOUBLE, delta DOUBLE, anomaly_score DOUBLE NULL, is_anomaly TINYINT, produced_at DATETIME(3), stored_at DATETIME(3)
- metrics_current: id TINYINT PK (always 1), last_event_time, last_temperature, rolling_avg, peak_temperature, min_temperature, events_processed, anomalies_flagged, late_events_dropped, pipeline_lag_ms, updated_at
- Phase 7 adds hourly_stats (hour_start, reading_count, avg_temperature, min_temperature, max_temperature, anomaly_count) and forecasts (made_at_event_time, target_time, predicted_temperature, lower, upper).

## Dashboard
- Streamlit. Everything live sits inside one function decorated with st.fragment(run_every=...).
- Database access through st.connection, with ttl=0 on every query (the connection caches forever by default).
- The curve reads only the newest 288 rows (ORDER BY id DESC LIMIT n, then sorted ascending in pandas).
- Y-axis fixed at 0-115. "Waiting for data" when tables are empty. A "No new data for N s" warning when updated_at is more than 10 s old.
- If the dashboard looks frozen, check in this order: is the producer running, is the processor's batch log moving, does the query pass ttl=0.

## Repository layout
One file per layer, so each report section can point at exactly one script.
`✓` = exists now. `(Phase N)` = created in that phase. Empty folders hold a `.gitkeep` until they get real files (then delete it).

```text
Mini_Project_DSA/
├── Resources/                 ✓ course brief and project plan (read-only reference)
│   ├── Machine Temperature Streaming Analytics Mini-Project Plan.md   design plan + phase prompts
│   ├── PHASES.md              phase-by-phase guide and gate checks
│   ├── MiniProject_Guide.pdf  course brief
│   └── MINI_project_components_table.pdf   allowed tools per layer
├── CLAUDE.md                  ✓ project rules Claude Code reads every session
├── README.md                  ✓ setup and run order (grows every phase)
├── DEMO.md                    (Phase 8) live demo runbook
├── docker-compose.yml         ✓ Kafka broker + MySQL
├── .env.example               ✓ ports, topic name, DB credentials
├── .env                       ✓ local copy of .env.example (gitignored)
├── .gitignore                 ✓
├── requirements.txt           ✓ pinned versions
├── pytest.ini                 ✓ lets plain `pytest` import src
├── skills-lock.json           ✓ sources of the installed skills (skills.sh)
├── .claude/
│   └── skills/                ✓ project skills (see "Skills available")
├── .streamlit/
│   └── secrets.toml           ✓ generated from .env by scripts/make_secrets.py (gitignored)
├── data/
│   ├── machine_temperature_system_failure.csv   ✓ identical to the NAB upstream file
│   └── anomaly_windows.json   ✓ the 4 labelled windows from NAB
├── db/
│   └── schema.sql             ✓ readings + metrics_current (Phase 7 adds 2 tables)
├── src/
│   ├── __init__.py            ✓ makes `python -m src.<module>` work
│   ├── config.py              ✓ reads .env, one place for every setting
│   ├── checks.py              ✓ start-up checks: retry, MySQL, Kafka topic (one-sentence failures)
│   ├── db.py                  ✓ connection, schema init, batch writes, state reads
│   ├── producer.py            ✓ layer 1: CSV to Kafka, paced
│   ├── features.py            ✓ RollingFeatures, shared by training and serving
│   ├── processor.py           ✓ layers 3 and 4, live: window, score, store
│   ├── train_model.py         ✓ layer 4, offline: fit and save the model
│   └── forecaster.py          (Phase 7) extra: second consumer, ARIMA forecast
├── dashboard/
│   └── app.py                 ✓ layer 6: Streamlit
├── scripts/
│   ├── reset_demo.py          ✓ recreate topic, delete consumer group, empty tables
│   ├── make_secrets.py        ✓ writes .streamlit/secrets.toml from .env
│   └── evaluate.py            ✓ extra: score flags against NAB labels, write the table and figures
├── models/                    ✓ anomaly_model.joblib (generated by train_model, gitignored)
├── tests/
│   ├── test_features.py       ✓ 5 hand-calculated feature tests
│   ├── test_scoring.py        ✓ 7 tests: train/serve identical rows, warm-up, model loading
│   ├── test_checks.py         ✓ 4 tests: retry loop and one-sentence exit
│   └── test_evaluate.py       ✓ 5 tests: window scoring, drop rule, z-score baseline
└── report/
    ├── screenshots/           ✓ (empty)
    ├── evaluation.csv         ✓ comparison table (scripts/evaluate.py)
    ├── stream_flags.png       ✓ whole stream, windows shaded, flags in red
    ├── flags_outside_windows.png  ✓ bar chart per detector
    ├── results.md             (Phase 9) real numbers
    └── report_draft.md        (Phase 9) report draft
```
.gitignore: .venv, .env, .producer_state, models/*.joblib, __pycache__, *.pyc, .pytest_cache, .streamlit/secrets.toml

## Run order
One-time setup (already done on this machine; full commands in README.md "Setup"):
`cp .env.example .env` -> `docker compose up -d --wait` -> create the topic with
`kafka-topics.sh --create --if-not-exists --topic machine-temperature --partitions 1 --replication-factor 1` -> `python -m src.db --init`.

Every run:
1. docker compose up -d
   (fresh start: `python scripts/reset_demo.py` with the processor stopped; tests: `pytest`)
2. python -m src.train_model (once) and python scripts/make_secrets.py (once)
3. python -m src.processor
4. streamlit run dashboard/app.py
5. python -m src.producer   (options: --rate N, --start-at "YYYY-MM-DD HH:MM", --limit N, --resume)
Phase 7 adds `python -m src.forecaster` (consumer group "forecast") alongside the processor.

## Handy commands (for checking work, all tested)
```bash
docker compose ps                                                     # both (healthy)?
docker exec broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic machine-temperature
docker exec -it broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic machine-temperature --from-beginning
docker exec broker /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group scoring
docker exec mysql sh -c 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "SELECT COUNT(*) FROM readings;"'
```
The MySQL line reads the credentials from the container's own environment, so no password is typed
(its "Using a password on the command line" warning is harmless).
Count messages on the topic: `docker exec broker /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic machine-temperature`.
Right after reset_demo.py (before the processor runs) the consumer-groups line says "Group scoring not found": expected, not a bug.
Drop `-it` from the console consumer and add `--timeout-ms 5000` when running it non-interactively (e.g. from Claude Code).

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
The project is submittable after Phase 5. Phases 6 and 7 lift it above the minimum.

## Skills available (.claude/skills)
- `verification-before-completion`: before claiming any gate passed. Run the check and show the output.
- `systematic-debugging`: when a gate fails or behaviour is unexpected. Find the root cause before fixing.
- `test-driven-development`: for features.py, db helpers and other testable logic.
- `writing-plans` / `executing-plans`: to break a phase into steps and carry them out.
- `developing-with-streamlit`: required for all dashboard work (Phase 4, 6 and 7 dashboard parts).
- `scikit-learn`: IsolationForest training and scoring (Phase 3) and the evaluation (Phase 6).
- `python-testing-patterns`: pytest structure and fixtures for tests/.
- `python-error-handling`: the clear one-sentence failures and connection retries (Phase 5).
- The skills still follow CLAUDE.md: if a skill suggests extra libraries, classes or async code, the rules above win.

## Progress (tick when the gate passes and the phase is committed)
- [x] Phase 0 Setup (gate passed 2026-10-11: broker + mysql healthy, topic machine-temperature with 1 partition, tables readings + metrics_current, host lists topics via kafka-python)
- [x] Phase 1 Producer (gate passed 2026-10-11: 5/s pacing confirmed from Kafka timestamps, --start-at -> seq 2049, --resume continues from saved seq, Ctrl+C saves state, bad-value guard tested, topic count = events sent)
- [x] Phase 2 Processor and database (gate passed 2026-10-11: 5 tests pass; reset safe twice; count climbs 104 -> 180 in 3 s at 20/s; Ctrl+C restart and kill -9 restart: COUNT = DISTINCT offsets (1000 = 1000), rolling_avg/std match an independent pandas recompute to 1e-12; 12 repeats on 2014-01-07 dropped (seq 10149-10160); lag 0)
- [x] Phase 3 ML model (gate passed 2026-10-11: 12 tests pass; trained on 1,988 rows, 1.01% of training flagged; replay from
  2013-12-10, 2,200 events at 50/s: 60 flagged, 8 on 12-10 and 49 on 12-16 (exactly the reference), 1 on 12-15, 2 on 12-17,
  all 60 inside labelled windows, first flag 2013-12-10 22:40; an independent pandas recompute gives identical flags and
  scores; COUNT = DISTINCT = 2,200; lag 0; restart restores "60 flagged"; missing model file -> one sentence, exit 1)
- [x] Phase 4 Dashboard (gate passed 2026-10-11: user watched the page update live at --rate 10 with no clicks; first flag
  in MySQL at event time 10 Dec 22:40 about 27 s in; AppTest: waiting / stale warning / live states, tiles equal
  metrics_current; chart contents for slider 12/288/1000/2016 equal the newest N rows; data re-verified against the
  NAB upstream CSV (same sha256), pandas feature recompute and offline model scoring; 12 tests pass)
- [x] Phase 5 End to end (gate passed 2026-10-11: `docker compose down -v`, then README followed literally in a copy of
  the repo with no .venv/.env/model/secrets: every step printed what the README says, 16 tests pass; full replay at
  --rate 200 in 103 s: 20,695 sent, 20,683 stored (= distinct offsets), 12 late dropped, 284 flagged, LAG 0; failure
  drills with no traceback: Kafka down (producer 5 s; processor waits 60 s or recovers), MySQL down (processor waits and
  recovers, mid-run loss exits cleanly and restart loses nothing; dashboard error then recovery), topic missing (not
  auto-created), model missing, secrets missing, tables missing)
- [x] Phase 6 Evaluation (gate passed 2026-10-11: 21 tests pass; evaluate.py reproduces every reference number (4 features
  0.01: 1.37%, 4 of 4, 201 outside; raw 36.46%, 6,282 outside) and its 0.01 row equals the saved model (284); both figures
  written and checked; dashboard shading + toggle + "2 of 2" / "4 of 4" line verified with AppTest on real data)
- [ ] Phase 7 Forecaster and hourly window (extra)
- [ ] Phase 8 Demo
- [ ] Phase 9 Report
