"""Layer 3: the stream processor.

Reads events from Kafka, keeps a 1-hour sliding window in memory, and writes
every accepted reading plus the current KPIs to MySQL. This is where the state
of the stream lives: the window, the running peak and minimum, and the counters.

Loop: poll a batch -> drop late events -> update features -> write the batch
to MySQL in one transaction -> commit the Kafka offsets.
Offsets are committed only after MySQL has the data, so a crash can make a
batch be read twice but never lost; the UNIQUE kafka_offset stops it being
stored twice.

    python -m src.processor          (Ctrl+C stops after the current batch)
"""
import signal
from datetime import datetime, timezone

from kafka import DefaultSerializer, JsonSerializer, KafkaConsumer

from src import config, db
from src.features import RollingFeatures

# Set by Ctrl+C (SIGINT) or SIGTERM. The loop checks it between batches, so
# a batch is never cut in half.
stopping = False


def request_stop(signum, frame):
    global stopping
    stopping = True
    print("\nStop requested: finishing the current batch...")


def utc_now():
    """Current UTC time without a timezone, the form stored in DATETIME(3) columns."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def parse_produced_at(text):
    """'2026-10-11T05:52:21.955Z' -> naive UTC datetime."""
    return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)


def make_consumer():
    return KafkaConsumer(
        config.KAFKA_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        group_id=config.CONSUMER_GROUP,
        # We commit by hand, after the MySQL write (the library default is True).
        enable_auto_commit=False,
        # A brand-new group starts from the first message on the topic (default is "latest").
        auto_offset_reset="earliest",
        key_deserializer=DefaultSerializer(),
        value_deserializer=JsonSerializer(),
    )


def restore_state(conn, features):
    """Pick up where the last run stopped, using what MySQL already holds.

    Returns the state dict the loop keeps between batches.
    """
    state = {
        "last_event_time": None,   # newest accepted event_time (for the late-event check)
        "last_offset": -1,         # newest stored kafka_offset (for skipping replays)
        "peak": None,
        "minimum": None,
        "events_processed": 0,
        "late_events_dropped": 0,
    }
    latest = db.fetch_latest_readings(conn, 12)
    metrics = db.fetch_metrics(conn)
    if not latest or metrics is None:
        print("Cold start: no stored readings yet.")
        return state

    # Refill the window from the newest 12 stored readings, so rolling_avg
    # continues smoothly instead of starting again from one reading.
    features.restore([row["temperature"] for row in latest])
    state["last_event_time"] = latest[-1]["event_time"]
    state["last_offset"] = latest[-1]["kafka_offset"]
    state["peak"] = metrics["peak_temperature"]
    state["minimum"] = metrics["min_temperature"]
    state["events_processed"] = metrics["events_processed"]
    state["late_events_dropped"] = metrics["late_events_dropped"]
    print(
        f"Restored: window of {len(features.window)} readings, last event_time {state['last_event_time']}, "
        f"last offset {state['last_offset']}, {state['events_processed']} processed, "
        f"{state['late_events_dropped']} late dropped"
    )
    return state


def process_batch(messages, features, state):
    """Turn one polled batch into reading rows. Updates `state` in place.

    Returns (rows, newest accepted event, number dropped as late, rolling_avg).
    """
    rows = []
    newest = None
    late = 0
    rolling_avg = None
    for message in messages:
        event = message.value

        # Already stored before a crash (the Kafka commit never happened):
        # skip it quietly, it is neither new nor late.
        if message.offset <= state["last_offset"]:
            continue

        event_time = datetime.strptime(event["event_time"], "%Y-%m-%d %H:%M:%S")
        # Late or repeated event: not newer than the last accepted one. Drop and count it.
        if state["last_event_time"] is not None and event_time <= state["last_event_time"]:
            late += 1
            continue

        temperature = event["temperature"]
        f = features.update(temperature)
        rolling_avg = f["rolling_avg"]
        state["last_event_time"] = event_time
        state["peak"] = temperature if state["peak"] is None else max(state["peak"], temperature)
        state["minimum"] = temperature if state["minimum"] is None else min(state["minimum"], temperature)
        state["events_processed"] += 1
        newest = event

        # No ML yet (Phase 3): anomaly_score is NULL and is_anomaly is 0.
        # stored_at is filled in just before the write.
        rows.append([
            message.offset, event["seq"], event_time, temperature, f["rolling_avg"], f["rolling_std"],
            f["deviation"], f["delta"], None, 0, parse_produced_at(event["produced_at"]), None,
        ])
    state["late_events_dropped"] += late
    return rows, newest, late, rolling_avg


def main():
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    conn = db.connect()
    features = RollingFeatures()
    state = restore_state(conn, features)
    consumer = make_consumer()
    print(f"Consuming {config.KAFKA_TOPIC} as group '{config.CONSUMER_GROUP}'. Ctrl+C to stop.")

    batch_number = 0
    try:
        while not stopping:
            # Wait up to 1 s for messages; an empty result just loops again,
            # which is also how a stop request is noticed quickly.
            polled = consumer.poll(timeout_ms=1000)
            messages = [m for partition_messages in polled.values() for m in partition_messages]
            if not messages:
                continue
            batch_number += 1

            rows, newest, late, rolling_avg = process_batch(messages, features, state)
            stored_at = utc_now()
            for row in rows:
                row[-1] = stored_at

            # Lag = how long the newest event in this batch took from producer to MySQL.
            lag_ms = None
            if newest is not None:
                lag_ms = int((stored_at - parse_produced_at(newest["produced_at"])).total_seconds() * 1000)

            metrics = {
                "last_event_time": state["last_event_time"],
                "last_temperature": newest["temperature"] if newest else None,
                "rolling_avg": rolling_avg,
                "peak_temperature": state["peak"],
                "min_temperature": state["minimum"],
                "events_processed": state["events_processed"],
                "anomalies_flagged": 0,
                "late_events_dropped": state["late_events_dropped"],
                "pipeline_lag_ms": lag_ms,
                "updated_at": stored_at,
            }
            if newest is None:
                # Every event in this batch was late or a replay: keep the last
                # known reading in metrics_current instead of blanking it.
                previous = db.fetch_metrics(conn) or {}
                for key in ("last_temperature", "rolling_avg", "pipeline_lag_ms"):
                    metrics[key] = previous.get(key)

            # MySQL first, Kafka second: the offsets only move on once the data is safe.
            db.write_batch(conn, [tuple(r) for r in rows], metrics)
            if rows:
                state["last_offset"] = rows[-1][0]
            consumer.commit()

            line = f"batch {batch_number}: {len(rows)} stored"
            if late:
                line += f", {late} late dropped"
            if newest is not None:
                line += f" | newest {state['last_event_time']} | rolling_avg {rolling_avg:.2f} | lag {lag_ms} ms"
            print(line)
    finally:
        consumer.close()
        conn.close()
        print(
            f"Processor stopped cleanly: {state['events_processed']} processed, "
            f"{state['late_events_dropped']} late dropped."
        )


if __name__ == "__main__":
    main()
