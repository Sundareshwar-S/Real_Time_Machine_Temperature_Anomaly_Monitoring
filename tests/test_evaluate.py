"""Hand-calculated checks for scripts/evaluate.py: how flags are scored against
the labelled windows, the processor's drop rule, and the z-score baseline."""
from datetime import datetime

import pytest

from scripts import evaluate


def t(text):
    return datetime.strptime(text, "%Y-%m-%d %H:%M")


WINDOWS = [
    (t("2014-01-01 10:00"), t("2014-01-01 12:00")),
    (t("2014-01-02 10:00"), t("2014-01-02 12:00")),
]


def test_flags_are_counted_inside_and_outside_the_windows():
    times = [t("2014-01-01 09:00"),   # before window 1: outside
             t("2014-01-01 10:10"),   # inside window 1, 10 minutes after its start
             t("2014-01-01 11:00"),   # inside window 1
             t("2014-01-01 12:00"),   # exactly the end of window 1: inside
             t("2014-01-01 13:00"),   # after window 1: outside
             t("2014-01-02 11:00")]   # inside window 2, but not flagged
    flags = [1, 1, 1, 1, 1, 0]

    result = evaluate.score_against_windows(times, flags, WINDOWS)

    assert result["flags"] == 5
    assert result["inside"] == 3
    assert result["outside"] == 2
    assert result["windows_hit"] == 1
    assert result["flagged_pct"] == pytest.approx(5 / 6 * 100)
    assert result["delays_min"] == [10, None]   # window 2 was missed


def test_a_reading_not_newer_than_the_last_accepted_one_is_dropped():
    rows = [(0, t("2014-01-07 10:00"), 50.0),
            (1, t("2014-01-07 10:05"), 51.0),
            (2, t("2014-01-07 10:05"), 52.0),   # same timestamp again
            (3, t("2014-01-07 09:30"), 53.0),   # clock stepped back
            (4, t("2014-01-07 10:10"), 54.0)]

    accepted, dropped = evaluate.accept_in_order(rows)

    assert [seq for seq, _, _ in accepted] == [0, 1, 4]
    assert dropped == 2


def test_zscore_flags_a_reading_far_from_its_history():
    # History [10, 10, 12, 10]: mean 10.5. Sample variance =
    # (0.25 + 0.25 + 2.25 + 0.25) / 3 = 1, so std 1.
    # 30 is (30 - 10.5) / 1 = 19.5 std away: flagged.
    temps = [10, 10, 12, 10, 30]
    assert evaluate.zscore_flags(temps, cutoff=3, history=4) == [0, 0, 0, 0, 1]


def test_zscore_does_not_flag_a_reading_close_to_its_history():
    # Same history, 11 is (11 - 10.5) / 1 = 0.5 std away.
    temps = [10, 10, 12, 10, 11]
    assert evaluate.zscore_flags(temps, cutoff=3, history=4) == [0, 0, 0, 0, 0]


def test_zscore_never_flags_against_a_flat_history():
    # A flat history has std 0, so z is undefined: no flag.
    temps = [10, 10, 10, 10, 50]
    assert evaluate.zscore_flags(temps, cutoff=3, history=4) == [0, 0, 0, 0, 0]
