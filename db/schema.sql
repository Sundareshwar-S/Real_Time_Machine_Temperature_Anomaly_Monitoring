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
