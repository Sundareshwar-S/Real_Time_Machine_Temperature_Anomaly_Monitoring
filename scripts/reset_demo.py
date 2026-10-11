"""Put the pipeline back to an empty start, ready for a demo or a test run.

1. Delete and recreate the Kafka topic (all old messages are gone).
2. Delete the processor's consumer group, so it starts again from offset 0.
3. Empty every MySQL table.
4. Remove .producer_state, so the producer starts again from seq 2000.

Safe to run twice in a row. Stop the processor first.
    python scripts/reset_demo.py
"""
import sys
import time
from pathlib import Path

# Let `python scripts/reset_demo.py` import the src package from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from kafka import KafkaAdminClient  # noqa: E402

from src import config, db  # noqa: E402


def recreate_topic(admin):
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
            sys.exit(f"Topic {topic} was still being deleted after 15 s. Run the reset again.")
        print(f"Deleted topic {topic}")
    # One partition keeps every reading in order, which the rolling window needs.
    admin.create_topics({topic: {"num_partitions": 1, "replication_factor": 1}}, wait_for_metadata=True)
    print(f"Created topic {topic} (1 partition)")


def delete_consumer_group(admin):
    group = config.CONSUMER_GROUP
    # list_groups() returns one dict per group in kafka-python 3.x.
    if group not in [g["group_id"] for g in admin.list_groups()]:
        print(f"Consumer group {group} does not exist (nothing to delete)")
        return
    # delete_groups() returns {group_id: "OK" or the error name}. (Its docstring in
    # kafka-python 3.0.11 still says "list of tuples", but the real value is a dict.)
    result = admin.delete_groups([group])[group]
    if result != "OK":
        sys.exit(f"Could not delete consumer group {group} ({result}). "
                 "Stop the processor first, then run the reset again.")
    print(f"Deleted consumer group {group}")


def main():
    admin = KafkaAdminClient(bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS)
    try:
        recreate_topic(admin)
        delete_consumer_group(admin)
    finally:
        admin.close()

    with db.connect() as conn:
        db.empty_tables(conn)
    print("Emptied tables readings and metrics_current")

    if config.PRODUCER_STATE.exists():
        config.PRODUCER_STATE.unlink()
        print("Removed .producer_state")
    else:
        print(".producer_state not present (nothing to remove)")
    print("Reset complete.")


if __name__ == "__main__":
    main()
