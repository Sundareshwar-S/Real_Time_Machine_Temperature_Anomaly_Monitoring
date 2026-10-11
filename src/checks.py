"""Start-up checks shared by the producer and the processor.

When Kafka or MySQL is not running, or the topic is missing, the scripts stop
with one clear sentence that says what is wrong and how to fix it, instead of
a stack trace. The processor waits up to 60 s for the services first, so it can
be started at the same time as `docker compose up`.
"""
import sys
import time

import pymysql
from kafka import KafkaAdminClient
from kafka.errors import KafkaError

from src import config, db

KAFKA_TIMEOUT_MS = 5000   # how long one attempt to reach Kafka may take
RETRY_EVERY_S = 3         # pause between attempts
START_HINT = "Start it with: docker compose up -d"


def reason(error):
    """The readable part of an error: PyMySQL errors are (code, message) pairs."""
    if isinstance(error, pymysql.err.MySQLError) and len(error.args) == 2:
        return error.args[1]
    return str(error)


def retry(what, attempt, errors, fix, wait_s=0, every_s=RETRY_EVERY_S):
    """Call attempt() until it works or wait_s seconds have passed.

    Only the given connection errors are retried; any other error is a bug and
    is raised as usual. After the last failed attempt the script exits with one
    sentence: what could not be reached, why, and how to fix it.
    """
    deadline = time.monotonic() + wait_s
    while True:
        try:
            return attempt()
        except errors as error:
            if time.monotonic() >= deadline:
                waited = f" after waiting {wait_s} s" if wait_s else ""
                sys.exit(f"Cannot reach {what}{waited} ({reason(error)}). {fix}")
            print(f"Waiting for {what} ({reason(error)})...")
            time.sleep(every_s)


def connect_mysql(wait_s=0):
    """Return an open MySQL connection, or exit with one sentence."""
    what = f"MySQL at {config.MYSQL_HOST}:{config.MYSQL_PORT}"
    return retry(what, db.connect, pymysql.err.OperationalError, START_HINT, wait_s)


def require_topic(wait_s=0):
    """Return once Kafka answers and the topic exists, or exit with one sentence.

    The broker would create a missing topic by itself the first time a client
    used it (auto.create.topics.enable is on), with no warning. Checking first
    turns that silent surprise into a clear message.
    """
    def list_topics():
        admin = KafkaAdminClient(bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                                 bootstrap_timeout_ms=KAFKA_TIMEOUT_MS)
        try:
            return admin.list_topics()
        finally:
            admin.close()

    topics = retry(f"Kafka at {config.KAFKA_BOOTSTRAP_SERVERS}", list_topics, KafkaError, START_HINT, wait_s)
    if config.KAFKA_TOPIC not in topics:
        sys.exit(f"Topic {config.KAFKA_TOPIC} does not exist. Create it with the kafka-topics.sh command "
                 "in README.md (Setup, step 5) or run: python scripts/reset_demo.py")
