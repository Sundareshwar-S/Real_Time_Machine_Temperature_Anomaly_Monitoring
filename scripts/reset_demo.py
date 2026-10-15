"""Put the pipeline back to an empty start, ready for a demo or a test run.

First it checks that nothing is running: no consumer in the processor's or the
forecaster's group, no reading stored in the last 10 s, and no new messages on
the topic (the producer). If anything runs it stops with one sentence and
deletes nothing, because a half-reset system makes the processor skip new data.
Then:
1. Delete and recreate the Kafka topic (all old messages are gone).
2. Delete both consumer groups (processor and forecaster), so they start again from offset 0.
3. Empty every MySQL table.
4. Remove .producer_state, so the producer starts again from seq 2000.

It never touches the dataset (data/), db/schema.sql, the trained model or the settings.
The dashboard's "Clear data and start again" button calls reset_all() too.
Safe to run twice in a row.
    python scripts/reset_demo.py
"""
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Let `python scripts/reset_demo.py` import the src package from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition  # noqa: E402

from src import config, db  # noqa: E402

FRESH_S = 10   # a reading stored less than this long ago means the pipeline is still running
GROUPS = (config.CONSUMER_GROUP, config.FORECAST_GROUP)
STOP_HINT = "Stop the producer, processor and forecaster with Ctrl+C first, then try again."


class ResetError(Exception):
    """The reset was refused or failed; the message is one sentence for the user."""


def topic_growing(wait_s=1.5):
    """True if new messages arrive on the topic within wait_s seconds (the producer is running)."""
    partition = TopicPartition(config.KAFKA_TOPIC, 0)
    consumer = KafkaConsumer(bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS)
    try:
        if partition.topic not in consumer.topics():
            return False
        before = consumer.end_offsets([partition])[partition]
        time.sleep(wait_s)
        return consumer.end_offsets([partition])[partition] > before
    finally:
        consumer.close()


def check_idle(admin, conn):
    """Raise ResetError if any part of the pipeline is still running. Changes nothing."""
    # describe_groups also answers for a group that does not exist (state "Dead", no members).
    busy = [group for group, info in admin.describe_groups(list(GROUPS)).items() if info["members"]]
    if busy:
        raise ResetError(f"Consumer group {' and '.join(busy)} is still running (the processor or forecaster). "
                         + STOP_HINT)
    metrics = db.fetch_metrics(conn)
    if metrics and metrics["updated_at"] is not None:
        age_s = (datetime.now(timezone.utc).replace(tzinfo=None) - metrics["updated_at"]).total_seconds()
        if age_s < FRESH_S:
            raise ResetError(f"A reading was stored {age_s:.0f} s ago, so the pipeline is still running. " + STOP_HINT)
    if topic_growing():
        raise ResetError("New messages are still arriving on the topic, so the producer is running. " + STOP_HINT)


def recreate_topic(admin, lines):
    topic = config.KAFKA_TOPIC
    if topic in admin.list_topics():
        admin.delete_topics([topic])
        # Deletion finishes in the background, so wait until the topic is gone
        # before creating it again.
        for _ in range(30):
            if topic not in admin.list_topics():
                break
            time.sleep(0.5)
        else:
            raise ResetError(f"Topic {topic} was still being deleted after 15 s. Run the reset again.")
        lines.append(f"Deleted topic {topic}")
    # One partition keeps every reading in order, which the rolling window needs.
    admin.create_topics({topic: {"num_partitions": 1, "replication_factor": 1}}, wait_for_metadata=True)
    lines.append(f"Created topic {topic} (1 partition)")


def delete_consumer_group(admin, group, lines):
    # list_groups() returns one dict per group in kafka-python 3.x.
    if group not in [g["group_id"] for g in admin.list_groups()]:
        lines.append(f"Consumer group {group} does not exist (nothing to delete)")
        return
    # delete_groups() returns {group_id: "OK" or the error name}. (Its docstring in
    # kafka-python 3.0.11 still says "list of tuples", but the real value is a dict.)
    result = admin.delete_groups([group])[group]
    if result != "OK":
        raise ResetError(f"Could not delete consumer group {group} ({result}). " + STOP_HINT)
    lines.append(f"Deleted consumer group {group}")


def reset_all(admin, conn):
    """Check that nothing runs, then reset everything. Returns the lines to show; raises ResetError."""
    check_idle(admin, conn)   # first, so a refusal deletes nothing
    lines = []
    recreate_topic(admin, lines)
    for group in GROUPS:
        delete_consumer_group(admin, group, lines)
    db.empty_tables(conn)
    lines.append("Emptied tables readings, metrics_current, hourly_stats and forecasts")
    if config.PRODUCER_STATE.exists():
        config.PRODUCER_STATE.unlink()
        lines.append("Removed .producer_state")
    else:
        lines.append(".producer_state not present (nothing to remove)")
    lines.append("Reset complete.")
    return lines


def main():
    admin = KafkaAdminClient(bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS)
    try:
        with db.connect() as conn:
            lines = reset_all(admin, conn)
    except ResetError as error:
        sys.exit(str(error))
    finally:
        admin.close()
    print("\n".join(lines))


if __name__ == "__main__":
    main()
