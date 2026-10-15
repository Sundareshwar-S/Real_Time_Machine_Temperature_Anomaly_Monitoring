"""Checks for the dashboard's label of each reading (dashboard/model_labels.py)."""
from dashboard.model_labels import WATCH_SCORE, flag_label


def test_a_flagged_reading_is_flagged():
    assert flag_label(-0.03, 1) == "Flagged"


def test_a_score_between_0_and_the_watch_limit_is_watch():
    assert flag_label(0.0, 0) == "Watch"
    assert flag_label(WATCH_SCORE, 0) == "Watch"


def test_a_clearly_normal_score_is_normal():
    assert flag_label(0.12, 0) == "Normal"


def test_a_warm_up_reading_with_a_negative_score_is_not_watch():
    # The first 12 readings after a cold start are scored but never flagged
    # (is_anomaly = 0). A negative score there is not "close to the cut-off".
    assert flag_label(-0.08, 0) == "Normal"


def test_a_missing_score_is_normal():
    assert flag_label(None, 0) == "Normal"
    assert flag_label(float("nan"), 0) == "Normal"
