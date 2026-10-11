"""MySQL access for the pipeline (layer 5, the serving layer).

The processor writes through these functions; the dashboard only reads.
    python -m src.db --init      create the tables
"""
import argparse

import pymysql
import pymysql.cursors

from src import config

# Column order of one readings row. The processor builds tuples in this order.
READING_COLUMNS = (
    "kafka_offset", "seq", "event_time", "temperature", "rolling_avg", "rolling_std",
    "deviation", "delta", "anomaly_score", "is_anomaly", "produced_at", "stored_at",
)

# INSERT IGNORE skips a row whose kafka_offset (UNIQUE) is already stored, so a
# batch replayed after a crash can never be stored twice.
INSERT_READINGS = (
    f"INSERT IGNORE INTO readings ({', '.join(READING_COLUMNS)}) "
    f"VALUES ({', '.join(['%s'] * len(READING_COLUMNS))})"
)

# metrics_current always holds exactly one row (id = 1): insert it the first
# time, overwrite it every time after that.
UPSERT_METRICS = """
    INSERT INTO metrics_current
        (id, last_event_time, last_temperature, rolling_avg, peak_temperature, min_temperature,
         events_processed, anomalies_flagged, late_events_dropped, pipeline_lag_ms, updated_at)
    VALUES
        (1, %(last_event_time)s, %(last_temperature)s, %(rolling_avg)s, %(peak_temperature)s,
         %(min_temperature)s, %(events_processed)s, %(anomalies_flagged)s, %(late_events_dropped)s,
         %(pipeline_lag_ms)s, %(updated_at)s) AS new
    ON DUPLICATE KEY UPDATE
        last_event_time = new.last_event_time, last_temperature = new.last_temperature,
        rolling_avg = new.rolling_avg, peak_temperature = new.peak_temperature,
        min_temperature = new.min_temperature, events_processed = new.events_processed,
        anomalies_flagged = new.anomalies_flagged, late_events_dropped = new.late_events_dropped,
        pipeline_lag_ms = new.pipeline_lag_ms, updated_at = new.updated_at
"""


# Merge one hour of a batch into hourly_stats. The stored hour may already hold
# readings from earlier batches, so counts add up, min/max compare, and the
# average is re-weighted by the counts. MySQL applies these assignments left to
# right (a later one sees the updated value), so avg_temperature must come
# before reading_count changes.
UPSERT_HOURLY = """
    INSERT INTO hourly_stats
        (hour_start, reading_count, avg_temperature, min_temperature, max_temperature, anomaly_count)
    VALUES
        (%(hour_start)s, %(reading_count)s, %(avg_temperature)s, %(min_temperature)s,
         %(max_temperature)s, %(anomaly_count)s) AS new
    ON DUPLICATE KEY UPDATE
        avg_temperature = (hourly_stats.avg_temperature * hourly_stats.reading_count
                           + new.avg_temperature * new.reading_count)
                          / (hourly_stats.reading_count + new.reading_count),
        reading_count = hourly_stats.reading_count + new.reading_count,
        min_temperature = LEAST(hourly_stats.min_temperature, new.min_temperature),
        max_temperature = GREATEST(hourly_stats.max_temperature, new.max_temperature),
        anomaly_count = hourly_stats.anomaly_count + new.anomaly_count
"""

# A forecast point for a target_time that already has one replaces it.
UPSERT_FORECAST = """
    INSERT INTO forecasts (target_time, made_at_event_time, predicted_temperature, lower, upper)
    VALUES (%s, %s, %s, %s, %s) AS new
    ON DUPLICATE KEY UPDATE
        made_at_event_time = new.made_at_event_time, predicted_temperature = new.predicted_temperature,
        lower = new.lower, upper = new.upper
"""


def connect():
    """Open a new connection to the machine_monitor database."""
    return pymysql.connect(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        database=config.MYSQL_DATABASE,
    )


def init_schema():
    """Create every table in db/schema.sql. Safe to run more than once."""
    sql = config.SCHEMA_SQL.read_text()
    # Drop comment lines, then run one statement at a time (PyMySQL runs a
    # single statement per execute call unless multi-statements are enabled).
    lines = [line for line in sql.splitlines() if not line.strip().startswith("--")]
    statements = [s.strip() for s in "\n".join(lines).split(";") if s.strip()]

    with connect() as conn:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)
            cur.execute("SHOW TABLES")
            tables = [row[0] for row in cur.fetchall()]
        conn.commit()
    return tables


def write_batch(conn, readings, metrics, hourly=()):
    """Store one polled batch in a single transaction: all of it, or none of it.

    readings: list of tuples in READING_COLUMNS order (may be empty if every
    event in the batch was dropped as late). metrics: dict for metrics_current.
    hourly: one dict per event-time hour touched by the batch (for hourly_stats).
    Returns the number of reading rows actually inserted.
    """
    try:
        with conn.cursor() as cur:
            inserted = cur.executemany(INSERT_READINGS, readings) if readings else 0
            cur.execute(UPSERT_METRICS, metrics)
            if hourly:
                cur.executemany(UPSERT_HOURLY, hourly)
        conn.commit()
    except Exception:
        # Leave the database exactly as it was before this batch, then let the
        # caller see the error. The Kafka offsets are not committed either, so
        # the batch will be read again.
        if conn.open:   # a connection that has died has nothing left to roll back
            conn.rollback()
        raise
    return inserted or 0


def write_forecasts(conn, rows):
    """Store one forecast: rows of (target_time, made_at_event_time, predicted, lower, upper)."""
    try:
        with conn.cursor() as cur:
            cur.executemany(UPSERT_FORECAST, rows)
        conn.commit()
    except Exception:
        if conn.open:
            conn.rollback()
        raise


def fetch_latest_readings(conn, n=12):
    """Return the newest n stored readings, oldest first (to refill the window)."""
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            "SELECT kafka_offset, event_time, temperature FROM readings ORDER BY id DESC LIMIT %s",
            (n,),
        )
        rows = cur.fetchall()
    return list(reversed(rows))


def fetch_metrics(conn):
    """Return the metrics_current row as a dict, or None if nothing has been stored yet."""
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute("SELECT * FROM metrics_current WHERE id = 1")
        return cur.fetchone()


def empty_tables(conn):
    """Delete every row from every pipeline table (used by scripts/reset_demo.py)."""
    with conn.cursor() as cur:
        for table in ("readings", "metrics_current", "hourly_stats", "forecasts"):
            cur.execute(f"TRUNCATE TABLE {table}")
    conn.commit()


def main():
    parser = argparse.ArgumentParser(description="MySQL helpers for the pipeline.")
    parser.add_argument("--init", action="store_true", help="create the tables from db/schema.sql")
    args = parser.parse_args()

    if args.init:
        tables = init_schema()
        print(f"Schema applied to {config.MYSQL_DATABASE}. Tables: {', '.join(tables)}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
