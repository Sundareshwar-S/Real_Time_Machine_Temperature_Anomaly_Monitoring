"""Hand-calculated checks for the dashboard's run report (dashboard/run_report.py)."""
from datetime import datetime, timedelta

import pandas as pd
import pytest

from dashboard import run_report

T0 = datetime(2026, 10, 12, 16, 0, 0)   # wall clock (UTC) when the first reading was sent


def wall(seconds):
    return T0 + timedelta(seconds=seconds)


def readings_fixture():
    """Six readings, one every 5 min of event time, stored 1 s apart, with one 20 s pause before the 5th."""
    event = [datetime(2013, 12, 10, 6, 0) + timedelta(minutes=5 * i) for i in range(6)]
    produced = [wall(s) for s in (0, 1, 2, 3, 24, 25)]
    # Lag per reading: 50, 50, 100, 50, 50, 300 ms -> average 100, maximum 300.
    lag_ms = [50, 50, 100, 50, 50, 300]
    return pd.DataFrame({
        "event_time": event,
        "temperature": [80.0, 81.0, 20.0, 82.0, 108.5, 79.0],
        "is_anomaly": [0, 0, 1, 0, 1, 0],
        "produced_at": produced,
        "stored_at": [p + timedelta(milliseconds=ms) for p, ms in zip(produced, lag_ms)],
    })


def test_format_duration_picks_readable_units():
    assert run_report.format_duration(45) == "45 s"
    assert run_report.format_duration(125) == "2 min 05 s"
    assert run_report.format_duration(3725) == "1 h 02 min"
    assert run_report.format_duration(1723.5 * 3600) == "71 d 19 h"


def test_active_time_leaves_out_pauses_longer_than_the_threshold():
    # Gaps 1, 1, 28, 1 s: only the 28 s gap is a pause (> 10 s).
    times = pd.Series([wall(s) for s in (0, 1, 2, 30, 31)])

    active_s, pauses = run_report.active_time(times, pause_after_s=10)

    assert active_s == pytest.approx(31 - 28)
    assert pauses == 1


def test_run_summary_counts_time_flags_windows_and_lag():
    readings = readings_fixture()
    # One window from 06:05 to 06:15 event time: the flag at 06:10 is inside (5 min after it opens),
    # the flag at 06:20 is outside.
    windows = [(datetime(2013, 12, 10, 6, 5), datetime(2013, 12, 10, 6, 15))]
    forecasts = pd.DataFrame({"predicted_temperature": [80.0, 90.0], "lower": [70.0, 85.0],
                              "upper": [90.0, 95.0], "actual": [82.0, 80.0]})

    s = run_report.run_summary(readings, windows, forecasts, late_dropped=2, n_forecasts=1, pause_after_s=10)

    # Wall clock: first send at 0 s, last store at 25.3 s; the 21 s gap between stores is a pause.
    assert s["total_s"] == pytest.approx(25.3)
    assert s["active_s"] == pytest.approx(25.3 - (24.05 - 3.05))
    assert s["pauses"] == 1
    assert s["readings"] == 6
    assert s["late_dropped"] == 2
    # 25 minutes of event time covered.
    assert s["event_span_s"] == 25 * 60
    assert s["speedup"] == pytest.approx(25 * 60 / s["active_s"])
    assert s["flags"] == 2
    assert s["flag_pct"] == pytest.approx(100 * 2 / 6)
    assert (s["windows_hit"], s["windows_reached"], s["inside"], s["outside"]) == (1, 1, 1, 1)
    assert s["window_rows"][0]["delay_min"] == 5
    assert s["window_rows"][0]["flags_inside"] == 1
    assert (s["peak"], s["peak_time"]) == (108.5, datetime(2013, 12, 10, 6, 20))
    assert (s["minimum"], s["min_time"]) == (20.0, datetime(2013, 12, 10, 6, 10))
    assert s["lag_avg_ms"] == pytest.approx(100)
    assert s["lag_max_ms"] == pytest.approx(300)
    # Forecast errors 2 and 10 -> MAE 6; only the first actual (82 in 70-90) is inside its interval.
    assert s["forecast_mae"] == pytest.approx(6)
    assert s["forecast_coverage_pct"] == pytest.approx(50)


def test_windows_the_stream_has_not_reached_are_left_out():
    readings = readings_fixture()
    windows = [(datetime(2013, 12, 10, 6, 5), datetime(2013, 12, 10, 6, 15)),
               (datetime(2014, 2, 7, 14, 55), datetime(2014, 2, 9, 14, 5))]   # after the last reading

    s = run_report.run_summary(readings, windows, pd.DataFrame(), late_dropped=0, n_forecasts=0)

    assert (s["windows_hit"], s["windows_reached"]) == (1, 1)
    assert s["forecast_mae"] is None


def test_summary_markdown_names_the_headline_numbers():
    s = run_report.run_summary(readings_fixture(), [], pd.DataFrame(), late_dropped=0, n_forecasts=0)

    text = run_report.summary_markdown(s, complete=True)

    assert text.startswith("# Machine temperature replay: final run report")
    assert "| Readings stored | 6 |" in text
    assert "| Readings flagged | 2 (33.33%) |" in text
