"""Layer 3: the stream processor.

Reads events from Kafka, keeps a 1-hour sliding window in memory, and writes
every accepted reading plus the current KPIs to MySQL. This is where the state
of the stream lives: the window, the running peak and minimum, and the counters.

Loop: poll a batch -> drop late events -> update features -> score the batch
with the IsolationForest -> group it into 1-hour event-time windows -> write
the batch to MySQL in one transaction -> commit the Kafka offsets.
Offsets are committed only after MySQL has the data, so a crash can make a
batch be read twice but never lost; the UNIQUE kafka_offset stops it being
stored twice.

    python -m src.processor          (Ctrl+C stops after the current batch)
"""
import signal
import sys
import warnings
from datetime import datetime, timezone

import joblib
import pymysql
import sklearn
from kafka import DefaultSerializer, JsonSerializer, KafkaConsumer
from sklearn.exceptions import InconsistentVersionWarning

from src import checks, config, db
from src.features import FEATURE_NAMES, WINDOW_SIZE, RollingFeatures, model_row

# How long to keep retrying Kafka and MySQL at startup before giving up.
STARTUP_WAIT_S = 60

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
        # main() has already checked that the topic exists; never create it by accident.
        allow_auto_create_topics=False,
        key_deserializer=DefaultSerializer(),
        value_deserializer=JsonSerializer(),
    )


def load_model():
    """Load the trained model once, or stop with one clear sentence."""
    if not config.MODEL_PATH.exists():
        sys.exit(f"Model file {config.MODEL_PATH} not found. Train it first: python -m src.train_model")
    with warnings.catch_warnings():
        # scikit-learn warns when a model was saved by another version; the
        # check below turns that into one clear sentence instead.
        warnings.simplefilter("ignore", InconsistentVersionWarning)
        saved = joblib.load(config.MODEL_PATH)
    if saved["sklearn_version"] != sklearn.__version__:
        sys.exit(f"The model was saved with scikit-learn {saved['sklearn_version']} but "
                 f"{sklearn.__version__} is installed. Retrain it: python -m src.train_model")
    if saved["feature_names"] != FEATURE_NAMES:
        sys.exit(f"The model was trained on {saved['feature_names']}, not {FEATURE_NAMES}. "
                 "Retrain it: python -m src.train_model")
    return saved["model"]


def new_state():
    """The state of a processor that has never stored anything (a cold start)."""
    return {
        "last_event_time": None,   # newest accepted event_time (for the late-event check)
        "last_offset": -1,         # newest stored kafka_offset (for skipping replays)
        "peak": None,
        "minimum": None,
        "events_processed": 0,
        "anomalies_flagged": 0,
        "late_events_dropped": 0,
        # The first 12 readings only fill the window, so they are never flagged
        # (training skipped them too).
        "warmup_left": WINDOW_SIZE,
    }


def restore_state(conn, features):
    """Pick up where the last run stopped, using what MySQL already holds.

    Returns the state dict the loop keeps between batches.
    """
    state = new_state()
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
    state["anomalies_flagged"] = metrics["anomalies_flagged"]
    state["late_events_dropped"] = metrics["late_events_dropped"]
    # The window came back full, so there is nothing left to warm up.
    state["warmup_left"] = 0
    print(
        f"Restored: window of {len(features.window)} readings, last event_time {state['last_event_time']}, "
        f"last offset {state['last_offset']}, {state['events_processed']} processed, "
        f"{state['anomalies_flagged']} flagged, {state['late_events_dropped']} late dropped"
    )
    return state


def process_batch(messages, features, state, model):
    """Turn one polled batch into scored reading rows. Updates `state` in place.

    Returns (rows, newest accepted event, number dropped as late, rolling_avg).
    """
    rows = []
    model_rows = []   # the model's input, one row per accepted reading
    warming_up = []   # True for readings that only fill the window
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

        model_rows.append(model_row(f))
        warming_up.append(state["warmup_left"] > 0)
        if state["warmup_left"] > 0:
            state["warmup_left"] -= 1

        # anomaly_score and is_anomaly are filled in below, after scoring.
        # stored_at is filled in just before the write.
        rows.append([
            message.offset, event["seq"], event_time, temperature, f["rolling_avg"], f["rolling_std"],
            f["deviation"], f["delta"], None, 0, parse_produced_at(event["produced_at"]), None,
        ])

    if rows:
        # One call each for the whole batch: a call costs about the same for
        # 1 row as for 20, so scoring row by row would fall behind at high rates.
        predictions = model.predict(model_rows)
        scores = model.decision_function(model_rows)
        for row, prediction, score, warm_up in zip(rows, predictions, scores, warming_up):
            row[8] = float(score)   # negative = anomalous
            row[9] = 1 if prediction == -1 and not warm_up else 0
            state["anomalies_flagged"] += row[9]

    state["late_events_dropped"] += late
    return rows, newest, late, rolling_avg


def hourly_rows(rows):
    """Group a batch's reading rows into 1-hour tumbling windows of event time.

    Tumbling = fixed, non-overlapping hours (10:00-10:59, 11:00-11:59, ...),
    unlike the sliding 1-hour window behind rolling_avg, which moves with every
    reading. Returns one dict per hour, ready to merge into hourly_stats.
    """
    hours = {}
    for row in rows:
        event_time, temperature, is_anomaly = row[2], row[3], row[9]
        hour_start = event_time.replace(minute=0, second=0, microsecond=0)
        hours.setdefault(hour_start, []).append((temperature, is_anomaly))
    return [
        {
            "hour_start": hour_start,
            "reading_count": len(readings),
            "avg_temperature": sum(t for t, _ in readings) / len(readings),
            "min_temperature": min(t for t, _ in readings),
            "max_temperature": max(t for t, _ in readings),
            "anomaly_count": sum(flag for _, flag in readings),
        }
        for hour_start, readings in sorted(hours.items())
    ]


def main():
    model = load_model()   # once, at startup: the stream never retrains
    # Wait up to 60 s for MySQL and Kafka, so the processor can be started
    # together with `docker compose up`. Each gives up with one clear sentence.
    conn = checks.connect_mysql(wait_s=STARTUP_WAIT_S)
    checks.require_topic(wait_s=STARTUP_WAIT_S)
    features = RollingFeatures()
    state = restore_state(conn, features)
    consumer = make_consumer()

    # From here on Ctrl+C only asks the loop to stop after the current batch.
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
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

            rows, newest, late, rolling_avg = process_batch(messages, features, state, model)
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
                "anomalies_flagged": state["anomalies_flagged"],
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
            try:
                db.write_batch(conn, [tuple(r) for r in rows], metrics, hourly_rows(rows))
            except pymysql.err.OperationalError as error:
                # The offsets of this batch are not committed, so a restarted
                # processor reads the batch again: nothing is lost.
                sys.exit(f"Lost the connection to MySQL ({checks.reason(error)}). {checks.START_HINT}, "
                         "then start the processor again; it continues where it stopped.")
            if rows:
                state["last_offset"] = rows[-1][0]
            consumer.commit()

            line = f"batch {batch_number}: {len(rows)} stored"
            flagged = sum(row[9] for row in rows)
            if flagged:
                line += f", {flagged} flagged"
            if late:
                line += f", {late} late dropped"
            if newest is not None:
                line += f" | newest {state['last_event_time']} | rolling_avg {rolling_avg:.2f} | lag {lag_ms} ms"
            print(line)
    finally:
        consumer.close()
        if conn.open:
            conn.close()
        print(
            f"Processor stopped: {state['events_processed']} processed, "
            f"{state['anomalies_flagged']} flagged, {state['late_events_dropped']} late dropped."
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        # Ctrl+C while still waiting for Kafka or MySQL at startup.
        sys.exit("Stopped before the processor started.")
