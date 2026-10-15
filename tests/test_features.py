"""Hand-calculated checks for RollingFeatures (src/features.py).

Each expected number below was worked out by hand, so a test failing means
the feature maths changed, not that the test copied the code.
"""
import math

import pytest

from src.features import FEATURE_NAMES, LONG_WINDOW, RollingFeatures


def test_feature_order_matches_the_model_columns():
    # The model in Phase 3 is trained on columns in exactly this order.
    assert FEATURE_NAMES == ["temperature", "deviation", "rolling_std", "delta", "drift", "daily_std"]


def test_first_reading_has_no_spread_and_no_change():
    features = RollingFeatures()

    result = features.update(50.0)

    assert result["temperature"] == 50.0
    assert result["rolling_avg"] == 50.0
    assert result["deviation"] == 0.0     # 50 - mean(50)
    assert result["rolling_std"] == 0.0   # fewer than 2 readings
    assert result["delta"] == 0.0         # no previous reading
    assert result["drift"] == 0.0         # 50 - mean(50) over the 24-hour window
    assert result["daily_std"] == 0.0     # fewer than 2 readings


def test_partly_filled_window():
    # Window [10, 20, 30]: mean 20.
    # Sample std = sqrt(((10-20)^2 + 0 + (30-20)^2) / (3-1)) = sqrt(200 / 2) = 10.
    features = RollingFeatures()
    features.update(10.0)
    features.update(20.0)

    result = features.update(30.0)

    assert result["rolling_avg"] == pytest.approx(20.0)
    assert result["deviation"] == pytest.approx(10.0)    # 30 - 20
    assert result["rolling_std"] == pytest.approx(10.0)
    assert result["delta"] == pytest.approx(10.0)        # 30 - 20


def test_full_window_drops_its_oldest_value():
    # Feed 1, 2, ..., 13. After the 13th reading the window holds 2..13
    # (the 1 was pushed out). Mean = (2 + 13) / 2 = 7.5.
    # Sample variance of 12 consecutive integers = n(n+1)/12 = 12*13/12 = 13,
    # so the sample std is sqrt(13).
    features = RollingFeatures()
    for value in range(1, 13):
        features.update(float(value))

    result = features.update(13.0)

    assert len(features.window) == 12
    assert list(features.window) == [float(v) for v in range(2, 14)]
    assert result["rolling_avg"] == pytest.approx(7.5)
    assert result["deviation"] == pytest.approx(5.5)          # 13 - 7.5
    assert result["rolling_std"] == pytest.approx(math.sqrt(13))
    assert result["delta"] == pytest.approx(1.0)              # 13 - 12


def test_restore_continues_exactly_like_an_uninterrupted_run():
    # The processor rebuilds its windows from the newest stored readings (up to
    # 288) after a restart. The next result must match a run that never stopped.
    readings = [70.0, 72.5, 71.0, 69.8, 73.2, 74.0, 75.5, 74.9, 76.1, 77.0, 76.4, 78.2, 79.9, 81.3]
    uninterrupted = RollingFeatures()
    for value in readings:
        uninterrupted.update(value)
    expected = uninterrupted.update(85.0)

    restarted = RollingFeatures()
    restarted.restore(readings)
    result = restarted.update(85.0)

    assert result == pytest.approx(expected)
    # Hand check, so the comparison above can't pass on two empty results:
    # delta is measured from the last restored reading, 85.0 - 81.3 = 3.7.
    assert result["delta"] == pytest.approx(3.7)
    # Window after 85.0 arrives: the last 11 readings (69.8 ... 81.3, sum 836.3)
    # plus 85.0, so the mean is 921.3 / 12.
    assert result["rolling_avg"] == pytest.approx(921.3 / 12)


def test_drift_equals_deviation_while_both_windows_hold_the_same_readings():
    # Before the 13th reading the 1-hour and the 24-hour window hold the same
    # readings, so drift = deviation and daily_std = rolling_std.
    # Window [10, 20, 30]: mean 20, sample std 10 (see test_partly_filled_window).
    features = RollingFeatures()
    features.update(10.0)
    features.update(20.0)

    result = features.update(30.0)

    assert result["drift"] == pytest.approx(10.0)
    assert result["daily_std"] == pytest.approx(10.0)


def test_drift_compares_with_24_hours_while_deviation_compares_with_1_hour():
    # 24 readings of 10, then 12 readings of 20 (a step one hour ago).
    # 1-hour window: twelve 20s -> mean 20, deviation 0, rolling_std 0.
    # 24-hour window: 36 readings, 24 x 10 + 12 x 20 = 480 -> mean 13.333,
    # so drift = 20 - 40/3 = 6.667: the slow change the 1-hour features no longer see.
    features = RollingFeatures()
    for _ in range(24):
        features.update(10.0)
    for _ in range(11):
        features.update(20.0)

    result = features.update(20.0)

    assert result["deviation"] == pytest.approx(0.0)
    assert result["rolling_std"] == pytest.approx(0.0)
    assert result["drift"] == pytest.approx(20 - 40 / 3)
    assert result["daily_std"] > 0


def test_full_24_hour_window_drops_its_oldest_value():
    # One 20 followed by 288 readings of 10: the 20 has left the 288-reading
    # window, so the window is all 10s: drift 0, daily_std 0.
    assert LONG_WINDOW == 288
    features = RollingFeatures()
    features.update(20.0)
    for _ in range(287):
        features.update(10.0)
    # The 20 is still the oldest of 288 readings: mean (20 + 287 x 10) / 288.
    assert features.long_window[0] == 20.0
    assert sum(features.long_window) / len(features.long_window) == pytest.approx((20 + 287 * 10) / 288)

    result = features.update(10.0)

    assert len(features.long_window) == 288
    assert result["drift"] == pytest.approx(0.0)
    assert result["daily_std"] == pytest.approx(0.0)


def test_restore_refills_both_windows():
    # After a restart the processor passes up to 288 stored readings, oldest first.
    # The next result must match a run that never stopped, the 24-hour features included.
    readings = [60.0 + (i % 17) * 0.7 for i in range(300)]
    uninterrupted = RollingFeatures()
    for value in readings:
        uninterrupted.update(value)
    expected = uninterrupted.update(85.0)

    restarted = RollingFeatures()
    restarted.restore(readings[-LONG_WINDOW:])
    result = restarted.update(85.0)

    assert list(restarted.window) == readings[-11:] + [85.0]
    assert result == pytest.approx(expected)
