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

## Stop
```bash
docker compose down               # keeps the data
docker compose down -v            # also deletes the Kafka and MySQL volumes
```
