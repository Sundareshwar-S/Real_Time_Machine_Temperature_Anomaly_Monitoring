"""Layer 4, offline: fit the IsolationForest once and save it.

Reads only the training rows (0-1999, 2 to 9 Dec 2013), so the model never
sees a reading it will later score in the stream. The rows go through the same
RollingFeatures class the processor uses, so training and serving compute the
features identically.

    python -m src.train_model
"""
import csv
import time

import joblib
import sklearn
from sklearn.ensemble import IsolationForest

from src import config
from src.features import FEATURE_NAMES, WINDOW_SIZE, RollingFeatures, model_row

# Model settings from CLAUDE.md. Do not change them to make results look better.
N_ESTIMATORS = 200     # number of random trees
CONTAMINATION = 0.01   # share of training rows treated as anomalous: sets the threshold
RANDOM_STATE = 42      # same trees on every run, so results are repeatable


def load_training_rows():
    """Return (event_time, temperature) for CSV rows 0-1999, in file order."""
    with open(config.DATA_CSV, newline="") as f:
        rows = [(row["timestamp"], float(row["value"])) for row in csv.DictReader(f)]
    return rows[:config.STREAM_START_SEQ]


def build_training_rows(temperatures):
    """Turn readings into model rows, dropping the first 12 (the window is still filling)."""
    features = RollingFeatures()
    rows = [model_row(features.update(t)) for t in temperatures]
    return rows[WINDOW_SIZE:]


def main():
    readings = load_training_rows()
    rows = build_training_rows([t for _, t in readings])
    first_used, last_used = readings[WINDOW_SIZE][0], readings[-1][0]
    print(f"Training rows: 0-{len(readings) - 1} ({readings[0][0]} to {last_used})")
    print(f"Fitting on rows {WINDOW_SIZE}-{len(readings) - 1} ({first_used} to {last_used}): "
          f"{len(rows)} rows, features {FEATURE_NAMES}")

    model = IsolationForest(n_estimators=N_ESTIMATORS, contamination=CONTAMINATION, random_state=RANDOM_STATE)
    started = time.perf_counter()
    model.fit(rows)
    print(f"Fitted in {time.perf_counter() - started:.2f} s")

    # predict gives -1 for an anomaly and 1 for a normal reading. On its own
    # training data the model flags about `contamination` of the rows.
    flagged = sum(1 for p in model.predict(rows) if p == -1)
    print(f"Training rows the model itself flags: {flagged} of {len(rows)} ({flagged / len(rows):.2%})")

    # Save the feature names and the library version with the model, so the
    # processor can refuse a model built for other columns or another scikit-learn.
    config.MODEL_PATH.parent.mkdir(exist_ok=True)
    joblib.dump(
        {"model": model, "feature_names": FEATURE_NAMES, "sklearn_version": sklearn.__version__},
        config.MODEL_PATH,
    )
    print(f"Saved {config.MODEL_PATH.relative_to(config.ROOT)} (scikit-learn {sklearn.__version__})")


if __name__ == "__main__":
    main()
