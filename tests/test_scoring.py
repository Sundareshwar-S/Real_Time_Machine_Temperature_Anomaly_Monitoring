"""Checks for Phase 3: training and serving build the same model input, and the
processor applies the scoring rules (one call per batch, warm-up, flag count).

FakeModel stands in for the IsolationForest so the tests do not need the
trained model file. It records exactly what the processor asks it to score.
"""
from types import SimpleNamespace

import pytest

from src import db, processor, train_model
from src.features import WINDOW_SIZE


class FakeModel:
    """Answers every row with the same prediction and records each call."""

    def __init__(self, prediction):
        self.prediction = prediction          # 1 = normal, -1 = anomaly
        self.predict_calls = []
        self.decision_calls = []

    def predict(self, rows):
        self.predict_calls.append([list(r) for r in rows])
        return [self.prediction] * len(rows)

    def decision_function(self, rows):
        self.decision_calls.append([list(r) for r in rows])
        # Same sign convention as IsolationForest: negative means anomalous.
        return [0.05 * self.prediction] * len(rows)


def make_messages(readings, first_offset=0):
    """Kafka-like messages (offset + value) for (event_time, temperature) pairs."""
    return [
        SimpleNamespace(offset=first_offset + i, value={
            "sensor_id": "machine-1", "seq": first_offset + i, "event_time": event_time,
            "temperature": temperature, "produced_at": "2026-10-11T10:00:00.000Z",
        })
        for i, (event_time, temperature) in enumerate(readings)
    ]


def first_training_readings(n):
    """The first n training rows of the real CSV as (event_time, temperature)."""
    return train_model.load_training_rows()[:n]


def test_processor_scores_exactly_the_rows_training_fitted_on():
    readings = first_training_readings(40)
    training_rows = train_model.build_training_rows([t for _, t in readings])

    model = FakeModel(prediction=1)
    processor.process_batch(make_messages(readings), processor.RollingFeatures(), processor.new_state(), model)

    scored = model.predict_calls[0]
    # Training drops the 12 warm-up rows; the processor still scores them
    # (they are stored with is_anomaly = 0). Everything after must be identical,
    # value for value and column for column, not just approximately equal.
    assert len(training_rows) == 40 - WINDOW_SIZE
    assert scored[WINDOW_SIZE:] == training_rows


def test_training_drops_exactly_the_warm_up_rows():
    rows = train_model.build_training_rows([float(t) for t in range(1, 21)])
    assert len(rows) == 20 - WINDOW_SIZE
    # The first kept row is reading 13: window 2..13, so deviation 5.5 and delta 1
    # (see test_full_window_drops_its_oldest_value in test_features.py).
    assert rows[0][0] == 13.0
    assert rows[0][1] == pytest.approx(5.5)
    assert rows[0][3] == pytest.approx(1.0)


def test_one_predict_and_one_decision_function_call_per_batch():
    model = FakeModel(prediction=1)
    processor.process_batch(make_messages(first_training_readings(25)), processor.RollingFeatures(),
                            processor.new_state(), model)
    assert len(model.predict_calls) == 1
    assert len(model.decision_calls) == 1
    assert len(model.predict_calls[0]) == 25


def test_warm_up_after_a_cold_start_spans_batches():
    # The model calls every reading an anomaly. After a cold start the first
    # 12 accepted readings must still be stored as normal, even when they
    # arrive split over two polled batches (5 + 15 readings).
    readings = first_training_readings(20)
    model = FakeModel(prediction=-1)
    features = processor.RollingFeatures()
    state = processor.new_state()

    rows_a = processor.process_batch(make_messages(readings[:5]), features, state, model)[0]
    rows_b = processor.process_batch(make_messages(readings[5:], first_offset=5), features, state, model)[0]

    flags = [row[9] for row in rows_a + rows_b]
    assert flags == [0] * 12 + [1] * 8
    assert state["anomalies_flagged"] == 8
    # The score is stored for every reading, warm-up included.
    assert all(row[8] is not None and row[8] < 0 for row in rows_a + rows_b)


def test_no_warm_up_after_a_restore(monkeypatch):
    # After a restart the window comes back full from MySQL, so the very first
    # new reading may be flagged, and the flag counter continues from MySQL.
    readings = first_training_readings(13)
    stored = [{"kafka_offset": i, "event_time": processor.datetime.strptime(t, "%Y-%m-%d %H:%M:%S"),
               "temperature": temp} for i, (t, temp) in enumerate(readings[:12])]
    metrics = {"peak_temperature": 80.0, "min_temperature": 60.0, "events_processed": 12,
               "anomalies_flagged": 7, "late_events_dropped": 0}
    monkeypatch.setattr(db, "fetch_latest_readings", lambda conn, n: stored)
    monkeypatch.setattr(db, "fetch_metrics", lambda conn: metrics)

    features = processor.RollingFeatures()
    state = processor.restore_state(None, features)
    rows = processor.process_batch(make_messages(readings[12:], first_offset=12), features, state,
                                   FakeModel(prediction=-1))[0]

    assert [row[9] for row in rows] == [1]
    assert state["anomalies_flagged"] == 8


def test_missing_model_file_exits_with_one_sentence(monkeypatch, tmp_path):
    monkeypatch.setattr(processor.config, "MODEL_PATH", tmp_path / "missing.joblib")
    with pytest.raises(SystemExit) as stop:
        processor.load_model()
    assert "python -m src.train_model" in str(stop.value)


def test_model_saved_by_another_scikit_learn_version_is_refused(monkeypatch, tmp_path):
    path = tmp_path / "old.joblib"
    processor.joblib.dump({"model": None, "feature_names": processor.FEATURE_NAMES,
                           "sklearn_version": "0.0.1"}, path)
    monkeypatch.setattr(processor.config, "MODEL_PATH", path)
    with pytest.raises(SystemExit) as stop:
        processor.load_model()
    assert "0.0.1" in str(stop.value)


def test_warm_up_continues_after_a_restore_with_fewer_than_12_readings(monkeypatch):
    # The processor stopped after 5 readings, so the restored window holds only 5.
    # The warm-up must go on for the 7 readings still missing, not end at once.
    readings = first_training_readings(20)
    stored = [{"kafka_offset": i, "event_time": processor.datetime.strptime(t, "%Y-%m-%d %H:%M:%S"),
               "temperature": temp} for i, (t, temp) in enumerate(readings[:5])]
    metrics = {"peak_temperature": 80.0, "min_temperature": 60.0, "events_processed": 5,
               "anomalies_flagged": 0, "late_events_dropped": 0}
    monkeypatch.setattr(db, "fetch_latest_readings", lambda conn, n: stored)
    monkeypatch.setattr(db, "fetch_metrics", lambda conn: metrics)

    features = processor.RollingFeatures()
    state = processor.restore_state(None, features)
    rows = processor.process_batch(make_messages(readings[5:], first_offset=5), features, state,
                                   FakeModel(prediction=-1))[0]

    assert [row[9] for row in rows] == [0] * 7 + [1] * 8
