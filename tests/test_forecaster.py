"""Checks for src/forecaster.py: forecast timing, the ARIMA wrapper and the refit cadence."""
import math
from datetime import datetime, timedelta
from types import SimpleNamespace

from src import forecaster

START = datetime(2013, 12, 10, 0, 0)


def messages(count, first_offset=0, start=START):
    """Kafka-like messages, one reading every 5 minutes."""
    return [SimpleNamespace(offset=first_offset + i, value={
        "event_time": (start + timedelta(minutes=5 * i)).strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": 90.0 + math.sin(i / 10)})
        for i in range(count)]


def test_forecast_rows_cover_the_next_hour_in_5_minute_steps():
    made_at = datetime(2013, 12, 16, 12, 0)
    mean, lower, upper = [80.0] * 12, [78.0] * 12, [82.0] * 12

    rows = forecaster.forecast_rows(made_at, mean, lower, upper)

    assert len(rows) == 12
    assert rows[0] == (datetime(2013, 12, 16, 12, 5), made_at, 80.0, 78.0, 82.0)
    assert rows[-1][0] == datetime(2013, 12, 16, 13, 0)   # 12 x 5 min = 1 hour ahead


def test_fit_forecast_returns_12_values_inside_their_interval():
    series = [90.0 + 2 * math.sin(i / 10) for i in range(forecaster.HISTORY)]

    mean, lower, upper = forecaster.fit_forecast(series)

    assert len(mean) == len(lower) == len(upper) == forecaster.HORIZON
    assert all(lo <= m <= hi for m, lo, hi in zip(mean, lower, upper))


def broken_arima(*args, **kwargs):
    """Stands in for statsmodels raising, as it does for text values (TypeError)."""
    raise TypeError("bad input")


def test_a_failed_fit_returns_none_instead_of_raising(monkeypatch):
    monkeypatch.setattr(forecaster, "ARIMA", broken_arima)
    assert forecaster.fit_forecast([90.0] * forecaster.HISTORY) is None


def test_a_window_with_missing_or_infinite_readings_is_not_fitted():
    # statsmodels does not refuse NaN (it forecasts zeros) and turns inf into NaN forecasts.
    assert forecaster.fit_forecast([float("nan")] * forecaster.HISTORY) is None
    assert forecaster.fit_forecast([90.0] * 287 + [float("inf")]) is None


def fake_fit(temperatures):
    """Stands in for ARIMA: records the window it was given."""
    fake_fit.windows.append(list(temperatures))
    return [1.0] * 12, [0.0] * 12, [2.0] * 12


def test_first_fit_needs_a_full_window_then_one_fit_per_12_new_readings():
    fake_fit.windows = []
    state = forecaster.new_state()

    # 300 readings in two batches: a full window at reading 288, then 12 more.
    rows = forecaster.handle_batch(messages(150), state, fit=fake_fit)
    rows += forecaster.handle_batch(messages(150, first_offset=150, start=START + timedelta(minutes=5 * 150)),
                                    state, fit=fake_fit)

    assert len(fake_fit.windows) == 2
    assert all(len(w) == forecaster.HISTORY for w in fake_fit.windows)
    made_at = sorted({row[1] for row in rows})
    assert made_at == [START + timedelta(minutes=5 * 287), START + timedelta(minutes=5 * 299)]


def test_repeated_timestamps_are_skipped_like_in_the_processor():
    state = forecaster.new_state()
    batch = messages(3)
    batch.append(SimpleNamespace(offset=3, value=dict(batch[1].value)))   # 00:05 again

    forecaster.handle_batch(batch, state, fit=fake_fit)

    assert len(state["window"]) == 3
    assert state["late_events_dropped"] == 1
