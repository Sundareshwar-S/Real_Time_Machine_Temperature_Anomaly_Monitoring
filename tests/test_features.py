"""Hand-calculated checks for RollingFeatures (src/features.py).

Each expected number below was worked out by hand, so a test failing means
the feature maths changed, not that the test copied the code.
"""
import math

import pytest

from src.features import FEATURE_NAMES, RollingFeatures


def test_feature_order_matches_the_model_columns():
    # The model in Phase 3 is trained on columns in exactly this order.
    assert FEATURE_NAMES == ["temperature", "deviation", "rolling_std", "delta"]


def test_first_reading_has_no_spread_and_no_change():
    features = RollingFeatures()

    result = features.update(50.0)

    assert result["temperature"] == 50.0
    assert result["rolling_avg"] == 50.0
    assert result["deviation"] == 0.0     # 50 - mean(50)
    assert result["rolling_std"] == 0.0   # fewer than 2 readings
    assert result["delta"] == 0.0         # no previous reading


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
    # The processor rebuilds its window from the newest 12 stored readings
    # after a restart. The next result must match a run that never stopped.
    readings = [70.0, 72.5, 71.0, 69.8, 73.2, 74.0, 75.5, 74.9, 76.1, 77.0, 76.4, 78.2, 79.9, 81.3]
    uninterrupted = RollingFeatures()
    for value in readings:
        uninterrupted.update(value)
    expected = uninterrupted.update(85.0)

    restarted = RollingFeatures()
    restarted.restore(readings[-12:])
    result = restarted.update(85.0)

    assert result == pytest.approx(expected)
    # Hand check, so the comparison above can't pass on two empty results:
    # delta is measured from the last restored reading, 85.0 - 81.3 = 3.7.
    assert result["delta"] == pytest.approx(3.7)
    # Window after 85.0 arrives: the last 11 readings (69.8 ... 81.3, sum 836.3)
    # plus 85.0, so the mean is 921.3 / 12.
    assert result["rolling_avg"] == pytest.approx(921.3 / 12)
