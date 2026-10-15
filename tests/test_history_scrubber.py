"""Hand-calculated checks for the history scrubber's time <-> position maths (dashboard/history_scrubber.py)."""
from datetime import datetime

from dashboard.history_scrubber import flag_steps, from_step, steps_between, to_step

FIRST = datetime(2013, 12, 9, 19, 55)   # the stream's first event time
LAST = datetime(2013, 12, 10, 19, 55)   # 24 hours later = 288 steps of 5 minutes


def test_24_hours_are_288_steps():
    assert steps_between(FIRST, LAST) == 288


def test_a_time_maps_to_its_5_minute_step_and_back():
    # 9 Dec 21:00 is 65 minutes after 19:55 = 13 steps.
    t = datetime(2013, 12, 9, 21, 0)
    assert to_step(t, FIRST, LAST) == 13
    assert from_step(13, FIRST, LAST) == t


def test_a_time_between_steps_rounds_to_the_nearest_step():
    # 21:02 is 67 minutes in: 13.4 steps -> 13.  21:03 is 68 minutes: 13.6 -> 14.
    assert to_step(datetime(2013, 12, 9, 21, 2), FIRST, LAST) == 13
    assert to_step(datetime(2013, 12, 9, 21, 3), FIRST, LAST) == 14


def test_positions_outside_the_stream_are_clamped():
    assert to_step(datetime(2013, 12, 1), FIRST, LAST) == 0
    assert to_step(datetime(2014, 3, 1), FIRST, LAST) == 288
    assert from_step(-5, FIRST, LAST) == FIRST
    assert from_step(10_000, FIRST, LAST) == LAST


def test_flag_marks_are_sorted_steps_without_repeats():
    # Two flags in the same 5-minute step (a repeated timestamp) give one mark.
    times = [datetime(2013, 12, 10, 0, 0), datetime(2013, 12, 9, 20, 0), datetime(2013, 12, 9, 20, 0)]
    # 20:00 -> 1 step; 00:00 -> 245 minutes -> 49 steps.
    assert flag_steps(times, FIRST, LAST) == [1, 49]


def test_no_flags_gives_no_marks():
    assert flag_steps([], FIRST, LAST) == []
