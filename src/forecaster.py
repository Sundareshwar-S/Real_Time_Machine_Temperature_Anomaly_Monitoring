"""Extra: a second, independent consumer that forecasts the next hour with ARIMA.

It reads the same topic as the processor but in its own consumer group
("forecast"), so Kafka keeps separate offsets for it: it never slows the
processor down, and the processor never waits for it. Fitting a model is much
slower than scoring one, which is why it runs in its own process.

Every 12 new readings (1 hour of event time) it fits ARIMA(1,1,1) on the last
288 readings (24 hours) and forecasts the next 12 readings with a 95% interval.
The dashboard draws them as a dashed line ahead of the live curve.

    python -m src.forecaster          (Ctrl+C stops after the current batch)
"""
import math
import signal
import sys
import time
import warnings
from collections import deque
from datetime import datetime, timedelta

import pymysql
from kafka import DefaultSerializer, JsonSerializer, KafkaConsumer
from statsmodels.tsa.arima.model import ARIMA

from src import checks, config, db

HISTORY = 288                 # readings the model is fitted on (24 hours)
EVERY = 12                    # refit after this many new readings (1 hour)
HORIZON = 12                  # readings forecast ahead (1 hour)
ORDER = (1, 1, 1)             # ARIMA(p, d, q): 1 autoregressive term, 1 difference, 1 moving-average term
STEP = timedelta(minutes=5)   # time between two readings
STARTUP_WAIT_S = 60

stopping = False


def request_stop(signum, frame):
    global stopping
    stopping = True
    print("\nStop requested: finishing the current batch...")


def new_state():
    return {
        "window": deque(maxlen=HISTORY),   # the last 288 accepted temperatures
        "last_event_time": None,           # newest accepted event_time (repeats are skipped)
        "new_since_fit": 0,                # readings since the last fit
        "late_events_dropped": 0,
        "fits": 0,
        "failed_fits": 0,
    }


def fit_forecast(temperatures):
    """Fit ARIMA on the window and forecast HORIZON readings.

    Returns (mean, lower, upper) as lists, or None if the fit fails, so one bad
    window never stops the forecaster.
    """
    # statsmodels does not refuse a window of NaN values: it returns a forecast
    # of zeros. So check the input as well as the output.
    if not all(isinstance(t, (int, float)) and math.isfinite(t) for t in temperatures):
        print(f"Window holds a missing or infinite reading; waiting for the next {EVERY} readings.")
        return None
    try:
        with warnings.catch_warnings():
            # statsmodels warns about convergence on some windows; a usable
            # forecast is checked below instead.
            warnings.simplefilter("ignore")
            result = ARIMA(list(temperatures), order=ORDER).fit()
            forecast = result.get_forecast(steps=HORIZON)
            mean = [float(v) for v in forecast.predicted_mean]
            interval = forecast.conf_int(alpha=0.05)   # 95% interval, one (lower, upper) pair per step
            lower = [float(lo) for lo, _ in interval]
            upper = [float(hi) for _, hi in interval]
    except Exception as error:
        print(f"Fit failed ({type(error).__name__}: {error}); waiting for the next {EVERY} readings.")
        return None
    if not all(math.isfinite(v) for v in mean + lower + upper):
        print(f"Fit gave no usable numbers; waiting for the next {EVERY} readings.")
        return None
    return mean, lower, upper


def forecast_rows(made_at, mean, lower, upper):
    """One row per forecast step: (target_time, made_at_event_time, predicted, lower, upper)."""
    return [(made_at + STEP * (k + 1), made_at, mean[k], lower[k], upper[k]) for k in range(len(mean))]


def handle_batch(messages, state, fit=fit_forecast):
    """Add a polled batch to the window and fit wherever a refit is due.

    Returns the forecast rows to store (empty if no fit was due or every fit failed).
    """
    rows = []
    for message in messages:
        event = message.value
        event_time = datetime.strptime(event["event_time"], "%Y-%m-%d %H:%M:%S")
        # Same rule as the processor: a reading not newer than the last one is dropped.
        if state["last_event_time"] is not None and event_time <= state["last_event_time"]:
            state["late_events_dropped"] += 1
            continue
        state["window"].append(event["temperature"])
        state["last_event_time"] = event_time
        state["new_since_fit"] += 1

        # Fit once the window holds a full 24 hours, then after every 12 new readings.
        if len(state["window"]) == HISTORY and state["new_since_fit"] >= EVERY:
            state["new_since_fit"] = 0
            result = fit(state["window"])
            if result is None:
                state["failed_fits"] += 1
                continue
            state["fits"] += 1
            rows += forecast_rows(event_time, *result)
    return rows


def make_consumer():
    return KafkaConsumer(
        config.KAFKA_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        group_id=config.FORECAST_GROUP,   # its own group: its own offsets, independent of the processor
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        allow_auto_create_topics=False,
        key_deserializer=DefaultSerializer(),
        value_deserializer=JsonSerializer(),
    )


def main():
    conn = checks.connect_mysql(wait_s=STARTUP_WAIT_S)
    checks.require_topic(wait_s=STARTUP_WAIT_S)
    consumer = make_consumer()
    state = new_state()
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    print(f"Consuming {config.KAFKA_TOPIC} as group '{config.FORECAST_GROUP}'. "
          f"First forecast after {HISTORY} readings. Ctrl+C to stop.")

    try:
        while not stopping:
            polled = consumer.poll(timeout_ms=1000)
            messages = [m for partition_messages in polled.values() for m in partition_messages]
            if not messages:
                continue
            started = time.perf_counter()
            rows = handle_batch(messages, state)
            if rows:
                try:
                    db.write_forecasts(conn, rows)
                except pymysql.err.OperationalError as error:
                    sys.exit(f"Lost the connection to MySQL ({checks.reason(error)}). {checks.START_HINT}, "
                             "then start the forecaster again.")
                newest = rows[-HORIZON:]
                print(f"forecast at {newest[0][1]}: next {HORIZON} readings from {newest[0][0]} "
                      f"= {newest[0][2]:.2f} [{newest[0][3]:.2f}, {newest[0][4]:.2f}] "
                      f"to {newest[-1][2]:.2f} ({(time.perf_counter() - started) * 1000:.0f} ms)")
            # Offsets move on only after the forecast is stored.
            consumer.commit()
    finally:
        consumer.close()
        if conn.open:
            conn.close()
        print(f"Forecaster stopped: {state['fits']} forecasts, {state['failed_fits']} failed fits, "
              f"{state['late_events_dropped']} late dropped.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("Stopped before the forecaster started.")
