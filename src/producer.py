"""Layer 1: replay the CSV into Kafka as a paced, live stream.

Each stream row (seq 2000 onward) becomes one JSON event on the topic, keyed by
sensor_id so every reading of this machine lands in the same partition, in order.
The sleep between sends is what turns a static file into something that behaves
like a sensor.

Examples:
    python -m src.producer                         # whole stream at 10 events/s
    python -m src.producer --rate 5 --limit 50
    python -m src.producer --start-at "2013-12-10 00:00"
    python -m src.producer --resume                # continue after Ctrl+C
"""
import argparse
import csv
import sys
import time
from datetime import datetime, timezone

from kafka import DefaultSerializer, JsonSerializer, KafkaProducer

from src import config

PROGRESS_EVERY = 50  # print progress and save state every 50 events


def load_rows():
    """Return (seq, event_time, raw_value) for every stream row of the CSV.

    seq is the zero-based row number, so it matches the row's position in the file.
    The value stays a string here; it is checked when the row is sent.
    """
    with open(config.DATA_CSV, newline="") as f:
        rows = [(seq, row["timestamp"], row["value"]) for seq, row in enumerate(csv.DictReader(f))]
    return [r for r in rows if r[0] >= config.STREAM_START_SEQ]


def make_event(seq, event_time, temperature):
    """Build one event exactly as the event contract in CLAUDE.md describes."""
    # produced_at is the wall-clock send time in UTC, e.g. 2026-10-11T09:15:03.120Z.
    # The processor subtracts it from its own clock to measure pipeline lag.
    produced_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return {
        "sensor_id": config.SENSOR_ID,
        "seq": seq,
        "event_time": event_time,
        "temperature": temperature,
        "produced_at": produced_at,
    }


def read_state():
    """Return the last sent seq saved by a previous run, or None if there is none."""
    if not config.PRODUCER_STATE.exists():
        return None
    return int(config.PRODUCER_STATE.read_text().strip())


def write_state(seq):
    config.PRODUCER_STATE.write_text(f"{seq}\n")


def choose_rows(rows, start_at, resume):
    """Cut the stream rows down to where this run should begin."""
    if resume:
        last_seq = read_state()
        if last_seq is None:
            sys.exit("Nothing to resume: .producer_state does not exist yet. Run without --resume first.")
        return [r for r in rows if r[0] > last_seq]
    if start_at:
        # Timestamps in the CSV are "YYYY-MM-DD HH:MM:SS", so comparing the
        # parsed datetimes keeps every row at or after the requested time.
        start = datetime.strptime(start_at, "%Y-%m-%d %H:%M")
        return [r for r in rows if datetime.strptime(r[1], "%Y-%m-%d %H:%M:%S") >= start]
    return rows


def parse_args():
    parser = argparse.ArgumentParser(description="Replay the machine temperature CSV into Kafka.")
    parser.add_argument("--rate", type=float, default=10, help="events per second (default 10)")
    parser.add_argument("--limit", type=int, help="stop after sending this many events")
    # Both options pick the starting row, so only one of them may be given.
    start = parser.add_mutually_exclusive_group()
    start.add_argument("--start-at", help='skip stream rows before this event time, "YYYY-MM-DD HH:MM"')
    start.add_argument("--resume", action="store_true", help="continue after the seq saved in .producer_state")
    return parser.parse_args()


def main():
    args = parse_args()
    rows = choose_rows(load_rows(), args.start_at, args.resume)
    if not rows:
        sys.exit("No rows left to send from that starting point.")

    producer = KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=DefaultSerializer(),  # the key "machine-1" as UTF-8 bytes
        value_serializer=JsonSerializer(),   # the event dict as JSON bytes
    )
    print(f"Sending to topic {config.KAFKA_TOPIC} from seq {rows[0][0]} ({rows[0][1]}) at {args.rate} events/s")

    sent = 0
    last_seq = None
    started = time.monotonic()
    try:
        for seq, event_time, raw_value in rows:
            if args.limit is not None and sent >= args.limit:
                break

            # Real sensor data has gaps: skip a bad reading instead of crashing.
            try:
                temperature = float(raw_value)
            except ValueError:
                print(f"Skipping seq {seq}: bad value {raw_value!r}")
                continue

            # Pacing: event number `sent` is due at started + sent / rate.
            # Sleeping until that moment keeps the real rate on target without drift.
            delay = started + sent / args.rate - time.monotonic()
            if delay > 0:
                time.sleep(delay)

            producer.send(config.KAFKA_TOPIC, key=config.SENSOR_ID, value=make_event(seq, event_time, temperature))
            sent += 1
            last_seq = seq

            if sent % PROGRESS_EVERY == 0:
                # send() is asynchronous, so flush first: the saved seq then only
                # covers events Kafka has confirmed.
                producer.flush()
                write_state(last_seq)
                actual_rate = sent / (time.monotonic() - started)
                print(f"sent {sent} | event_time {event_time} | rate {actual_rate:.1f}/s")
    except KeyboardInterrupt:
        print("\nStopping (Ctrl+C)...")
    finally:
        # Deliver anything still buffered, remember where we stopped, and close.
        producer.flush()
        producer.close()
        if last_seq is not None:
            write_state(last_seq)
            print(f"Done: sent {sent} events, last seq {last_seq}. Continue with: python -m src.producer --resume")
        else:
            print("Done: no events sent.")


if __name__ == "__main__":
    main()
