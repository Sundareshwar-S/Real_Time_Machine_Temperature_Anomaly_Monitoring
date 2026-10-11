# Machine Temperature Streaming Analytics

A real-time streaming pipeline for one industrial machine:
CSV replay -> Kafka -> Python processor -> IsolationForest -> MySQL -> Streamlit.

This README grows phase by phase. The design and rules live in `CLAUDE.md` and `Resources/`.

## Prerequisites
- Python 3.12 or newer (built and tested on 3.14.4)
- Git
- Docker Engine with the Compose plugin (needed from Phase 0's gate onward for Kafka and MySQL)
  - Ubuntu: `sudo apt install -y docker.io docker-compose-v2 docker-buildx`, then
    `sudo usermod -aG docker $USER` and log out and back in
  - Windows / macOS: Docker Desktop (Windows needs WSL 2)
  - Check: `docker run --rm hello-world`

## Setup

### 1. Python virtual environment

Linux / macOS:
```bash
# Ubuntu/Debian only: the venv module is a separate package
sudo apt install -y python3.14-venv        # use your Python version, e.g. python3.12-venv

python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
pip check                                   # expect: No broken requirements found.
```

Windows (PowerShell):
```powershell
py -3 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 2. Settings
All hosts, ports, the topic name and the database credentials live in `.env`.
Docker Compose and `src/config.py` both read it.
```bash
cp .env.example .env              # Windows: copy .env.example .env
```

### 3. Data
Both files are already in `data/` (from the [Numenta Anomaly Benchmark](https://github.com/numenta/NAB)):
- `data/machine_temperature_system_failure.csv`: 22,695 readings, one every 5 minutes
- `data/anomaly_windows.json`: the 4 labelled failure windows for this file

To download the CSV again:
```bash
curl -o data/machine_temperature_system_failure.csv https://raw.githubusercontent.com/numenta/NAB/master/data/realKnownCause/machine_temperature_system_failure.csv
```

### 4. Start Kafka and MySQL
Kafka 4 runs in KRaft mode, so there is no ZooKeeper: one container is the whole cluster.
Stop any Kafka already running on port 9092 first.
```bash
docker compose up -d --wait       # waits until both containers are healthy
docker compose ps                 # broker and mysql should show (healthy)
```

### 5. Create the topic (once)
```bash
docker exec broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic machine-temperature --partitions 1 --replication-factor 1
```

### 6. Create the tables (once, safe to repeat)
```bash
python -m src.db --init           # expect: Tables: metrics_current, readings
```

## Check the setup
```bash
# Topic exists with 1 partition
docker exec broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic machine-temperature

# Tables exist
docker exec mysql sh -c 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "SHOW TABLES;"'

# The host can reach Kafka (proves the advertised listener works)
python -c "from kafka import KafkaAdminClient; print(KafkaAdminClient(bootstrap_servers='localhost:9092').list_topics())"
```

## Run the producer (layer 1)
Replays the stream rows (seq 2000 onward; rows 0-1999 are kept for training) into Kafka as JSON events.
```bash
python -m src.producer                                   # whole stream, 10 events/s
python -m src.producer --rate 5 --limit 50               # 50 events at 5 per second
python -m src.producer --start-at "2013-12-10 00:00"     # skip to the first failure window (seq 2049)
python -m src.producer --resume                          # continue after Ctrl+C
```
- `--start-at` and `--resume` cannot be combined.
- The last sent seq is saved in `.producer_state` every 50 events and on exit (Ctrl+C is safe).

Watch the events arrive in a second terminal:
```bash
docker exec -it broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic machine-temperature --from-beginning --property print.key=true
```
The first event of a fresh run is seq 2000, event_time `2013-12-09 19:55:00`, key `machine-1`.

## Run the tests
```bash
pytest                            # rolling-feature maths, train/serve identical features, scoring rules
```

## Reset the demo (empty start)
Stop the processor first. Then:
```bash
python scripts/reset_demo.py      # recreate the topic, delete group "scoring", empty the tables, remove .producer_state
```
Safe to run twice in a row.

## Train the model (layer 4, once)
```bash
python -m src.train_model         # writes models/anomaly_model.joblib (not committed)
```
It fits an IsolationForest on the first 2,000 rows only (2 to 9 Dec 2013), using the same
`RollingFeatures` class the processor uses. Retrain after any scikit-learn upgrade: the processor
refuses a model saved by another scikit-learn version.

## Run the processor (layers 3, 4 and 5)
Train the model first. Then start the processor before (or after) the producer, in its own terminal:
```bash
python -m src.processor           # Ctrl+C stops after the current batch
```
It reads the topic as consumer group `scoring`, keeps a 1-hour rolling window (12 readings),
drops repeated or out-of-order timestamps, scores each batch with the model (the first 12 readings
after a cold start are never flagged), and writes each batch to MySQL in one transaction
before committing the Kafka offsets. On restart it picks up where it stopped (window and counters
are restored from MySQL), so a restart never stores a reading twice.

Watch the data arrive:
```bash
docker exec mysql sh -c 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "SELECT COUNT(*) FROM readings; SELECT * FROM metrics_current\G"'
docker exec broker /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group scoring
```

Flags per day (the Phase 3 check: reset, start the processor, then
`python -m src.producer --start-at "2013-12-10 00:00" --rate 50 --limit 2200`; expect flags on 10 and 16 Dec):
```bash
docker exec mysql sh -c 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "SELECT DATE(event_time) AS day, COUNT(*) AS flags FROM readings WHERE is_anomaly = 1 GROUP BY day ORDER BY day"'
```

## Stop
```bash
docker compose down               # keeps the data
docker compose down -v            # also deletes the Kafka and MySQL volumes
```
