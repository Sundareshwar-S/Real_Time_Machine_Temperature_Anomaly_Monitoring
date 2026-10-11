"""Hand-calculated checks for the processor's 1-hour tumbling window (hourly_stats)."""
from datetime import datetime

import pytest

from src import processor


def reading(event_time, temperature, is_anomaly):
    """A readings row in db.READING_COLUMNS order (only the fields hourly_rows uses are filled)."""
    when = datetime.strptime(event_time, "%Y-%m-%d %H:%M")
    return [0, 0, when, temperature, None, None, None, None, None, is_anomaly, None, None]


def test_readings_are_grouped_into_whole_event_time_hours():
    rows = [
        reading("2013-12-16 10:00", 50.0, 0),   # 10:00 belongs to the 10:00 hour
        reading("2013-12-16 10:30", 60.0, 1),
        reading("2013-12-16 10:55", 70.0, 0),
        reading("2013-12-16 11:00", 80.0, 1),   # exactly 11:00 opens the next hour
    ]

    hours = processor.hourly_rows(rows)

    assert [h["hour_start"] for h in hours] == [datetime(2013, 12, 16, 10), datetime(2013, 12, 16, 11)]
    ten, eleven = hours
    # Hour 10: 50, 60, 70 -> count 3, mean 60, min 50, max 70, one flagged.
    assert ten["reading_count"] == 3
    assert ten["avg_temperature"] == pytest.approx(60.0)
    assert ten["min_temperature"] == 50.0
    assert ten["max_temperature"] == 70.0
    assert ten["anomaly_count"] == 1
    # Hour 11: just 80, flagged.
    assert eleven == {"hour_start": datetime(2013, 12, 16, 11), "reading_count": 1, "avg_temperature": 80.0,
                      "min_temperature": 80.0, "max_temperature": 80.0, "anomaly_count": 1}


def test_an_empty_batch_has_no_hours():
    assert processor.hourly_rows([]) == []
