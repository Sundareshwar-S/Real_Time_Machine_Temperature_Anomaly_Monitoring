"""All settings in one place, read from the .env file in the repo root.

Every other module imports from here, so no host, port, topic or password
is ever hard-coded anywhere else.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

# The repo root is one level above src/. Loading .env by absolute path means
# the settings are found no matter which folder a script is started from.
ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

# Kafka (layer 2)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "machine-temperature")
SENSOR_ID = os.getenv("SENSOR_ID", "machine-1")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "scoring")

# MySQL (layer 5)
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3307"))
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "machine_monitor")
MYSQL_USER = os.getenv("MYSQL_USER", "monitor")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")

# Files, turned into absolute paths from the repo root
DATA_CSV = ROOT / os.getenv("DATA_CSV", "data/machine_temperature_system_failure.csv")
ANOMALY_WINDOWS = ROOT / os.getenv("ANOMALY_WINDOWS", "data/anomaly_windows.json")
MODEL_PATH = ROOT / os.getenv("MODEL_PATH", "models/anomaly_model.joblib")
SCHEMA_SQL = ROOT / "db" / "schema.sql"

# Project constants. These are design decisions, not machine settings, so they
# are not in .env.
# Rows 0-1999 train the model offline; rows 2000 onward are the live stream.
# The producer never sends a row before this, and training never reads one after it.
STREAM_START_SEQ = 2000
# The producer saves the last sent seq here so --resume can continue after Ctrl+C.
PRODUCER_STATE = ROOT / ".producer_state"
