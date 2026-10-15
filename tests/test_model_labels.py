"""Checks for the dashboard's label of each reading and its sustained-alert rule (dashboard/model_labels.py)."""
from datetime import datetime, timedelta

from dashboard.model_labels import alert_episodes, alert_states, flag_label

WATCH = 0.0345   # the watch limit the model file holds (2nd percentile of its training scores)


def test_a_flagged_reading_is_flagged():
    assert flag_label(-0.03, 1, WATCH) == "Flagged"


def test_a_score_between_0_and_the_watch_limit_is_watch():
    assert flag_label(0.0, 0, WATCH) == "Watch"
    assert flag_label(WATCH, 0, WATCH) == "Watch"


def test_a_clearly_normal_score_is_normal():
    assert flag_label(0.04, 0, WATCH) == "Normal"   # above the model's watch limit


def test_a_warm_up_reading_with_a_negative_score_is_not_watch():
    # The first 12 readings after a cold start are scored but never flagged
    # (is_anomaly = 0). A negative score there is not "close to the cut-off".
    assert flag_label(-0.08, 0, WATCH) == "Normal"


def test_a_missing_score_is_normal():
    assert flag_label(None, 0, WATCH) == "Normal"
    assert flag_label(float("nan"), 0, WATCH) == "Normal"


def test_isolated_flags_never_raise_an_alert():
    # One flag, two normal, one flag, ...: never 3 flags among the last 6 readings.
    flags = [1, 0, 0, 1, 0, 0, 1, 0, 0, 1]
    assert alert_states(flags) == [0] * 10


def test_three_flags_among_the_last_six_raise_an_alert():
    # Readings 1, 3 and 5 are flagged: the third flag (index 4) is the 3rd within 6 -> alert from there.
    flags = [1, 0, 1, 0, 1, 0, 0, 0, 0]
    # index:  0  1  2  3  4  5  6  7  8
    # last 6 at index 4 = [1,0,1,0,1] -> 3 -> alert; index 5 -> [1,0,1,0,1,0] 3 -> alert;
    # index 6 -> [0,1,0,1,0,0] 2 -> off.
    assert alert_states(flags) == [0, 0, 0, 0, 1, 1, 0, 0, 0]


def test_a_run_of_flags_alerts_and_ends_when_fewer_than_three_remain():
    flags = [1, 1, 1, 1, 0, 0, 0, 0, 0]
    # index 2: 3 flags -> on. index 5: last 6 = [1,1,1,1,0,0] 4 -> on.
    # index 6: [1,1,1,0,0,0] 3 -> on. index 7: [1,1,0,0,0,0] 2 -> off.
    assert alert_states(flags) == [0, 0, 1, 1, 1, 1, 1, 0, 0]


def test_alert_episodes_are_the_runs_of_alert_readings():
    start = datetime(2013, 12, 16, 0, 0)
    times = [start + timedelta(minutes=5 * i) for i in range(9)]
    states = [0, 1, 1, 0, 0, 1, 1, 1, 0]
    assert alert_episodes(times, states) == [(times[1], times[2]), (times[5], times[7])]


def test_an_alert_still_open_at_the_end_runs_to_the_last_reading():
    times = [datetime(2014, 2, 8, 0, 5 * i) for i in range(4)]
    assert alert_episodes(times, [0, 0, 1, 1]) == [(times[2], times[3])]
    assert alert_episodes(times, [0, 0, 0, 0]) == []
