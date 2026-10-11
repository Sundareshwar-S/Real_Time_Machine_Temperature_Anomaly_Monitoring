-- Tables for database machine_monitor (layer 5, the serving layer).
-- The processor writes here; the dashboard only reads.
-- Safe to run again: every statement uses IF NOT EXISTS.
-- src/db.py splits this file on semicolons, so use them only to end statements (never in comments).

-- One row per accepted event. Feeds the live curve and the anomaly markers.
CREATE TABLE IF NOT EXISTS readings (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    kafka_offset  BIGINT NOT NULL,          -- UNIQUE + INSERT IGNORE: a replayed batch is never stored twice
    seq           INT NOT NULL,             -- zero-based CSV row number
    event_time    DATETIME NOT NULL,        -- when the sensor took the reading
    temperature   DOUBLE NOT NULL,
    rolling_avg   DOUBLE NOT NULL,          -- mean of the last 12 readings (1 hour)
    rolling_std   DOUBLE NOT NULL,
    deviation     DOUBLE NOT NULL,          -- temperature minus rolling_avg
    delta         DOUBLE NOT NULL,          -- temperature minus previous reading
    anomaly_score DOUBLE NULL,              -- IsolationForest decision_function, negative = anomalous
    is_anomaly    TINYINT NOT NULL DEFAULT 0,
    produced_at   DATETIME(3) NOT NULL,     -- when the producer sent it
    stored_at     DATETIME(3) NOT NULL,     -- when the processor wrote it
    UNIQUE KEY uq_readings_kafka_offset (kafka_offset),
    KEY idx_readings_event_time (event_time)
);

-- Exactly one row (id = 1), overwritten with every batch. Feeds the KPI tiles.
CREATE TABLE IF NOT EXISTS metrics_current (
    id                  TINYINT PRIMARY KEY,
    last_event_time     DATETIME NULL,
    last_temperature    DOUBLE NULL,
    rolling_avg         DOUBLE NULL,
    peak_temperature    DOUBLE NULL,
    min_temperature     DOUBLE NULL,
    events_processed    BIGINT NOT NULL DEFAULT 0,
    anomalies_flagged   BIGINT NOT NULL DEFAULT 0,
    late_events_dropped BIGINT NOT NULL DEFAULT 0,
    pipeline_lag_ms     BIGINT NULL,
    updated_at          DATETIME(3) NULL
);

-- One row per hour of event time: a tumbling window (fixed, non-overlapping hours).
-- The processor merges each batch into it. Feeds the anomalies-per-hour bar chart.
CREATE TABLE IF NOT EXISTS hourly_stats (
    hour_start      DATETIME PRIMARY KEY,   -- event_time rounded down to the hour
    reading_count   INT NOT NULL,
    avg_temperature DOUBLE NOT NULL,
    min_temperature DOUBLE NOT NULL,
    max_temperature DOUBLE NOT NULL,
    anomaly_count   INT NOT NULL
);

-- One row per forecast point, written by the forecaster (consumer group forecast).
-- A newer forecast for the same target_time replaces the older one. Feeds the dashed line.
CREATE TABLE IF NOT EXISTS forecasts (
    target_time           DATETIME PRIMARY KEY,   -- the event time being predicted
    made_at_event_time    DATETIME NOT NULL,      -- event time of the newest reading the fit used
    predicted_temperature DOUBLE NOT NULL,
    lower                 DOUBLE NOT NULL,        -- 95% interval
    upper                 DOUBLE NOT NULL,
    KEY idx_forecasts_made_at (made_at_event_time)
);
