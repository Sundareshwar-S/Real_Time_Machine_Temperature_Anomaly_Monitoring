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


def write_batch(conn, readings, metrics):
    """Store one polled batch in a single transaction: all of it, or none of it.

    readings: list of tuples in READING_COLUMNS order (may be empty if every
    event in the batch was dropped as late). metrics: dict for metrics_current.
    Returns the number of reading rows actually inserted.
    """
    try:
        with conn.cursor() as cur:
            inserted = cur.executemany(INSERT_READINGS, readings) if readings else 0
            cur.execute(UPSERT_METRICS, metrics)
        conn.commit()
    except Exception:
        # Leave the database exactly as it was before this batch, then let the
        # caller see the error. The Kafka offsets are not committed either, so
        # the batch will be read again.
        if conn.open:   # a connection that has died has nothing left to roll back
            conn.rollback()
        raise
    return inserted or 0


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
        for table in ("readings", "metrics_current"):
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
