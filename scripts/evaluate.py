"""Evaluate the anomaly detector against NAB's four labelled failure windows.

Offline analysis, no Kafka or MySQL: the stream rows (2000 onward) are replayed
in memory exactly as the pipeline sees them (repeated timestamps dropped, then
RollingFeatures from a cold start), and several detectors are compared:
  - IsolationForest on the 4 features at contamination 0.005, 0.01, 0.02, 0.05
    (0.01 is the model the pipeline uses)
  - IsolationForest on raw temperature only (the course guide's approach)
  - a no-ML baseline: rolling z-score against the previous 288 readings (24 h)
A flag outside a labelled window is unlabelled, not proven false.

    python scripts/evaluate.py     writes report/evaluation.csv and two figures
"""
import csv
import json
import sys
from datetime import datetime
from pathlib import Path

# Let `python scripts/evaluate.py` import the src package from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import altair as alt  # noqa: E402
import joblib  # noqa: E402
import pandas as pd  # noqa: E402
from sklearn.ensemble import IsolationForest  # noqa: E402

from src import config, train_model  # noqa: E402
from src.features import WINDOW_SIZE, RollingFeatures, model_row  # noqa: E402

REPORT = config.ROOT / "report"
ZSCORE_HISTORY = 288   # previous readings the z-score compares against (24 h)


def load_stream():
    """(seq, event_time, temperature) for every stream row, parsed like the producer does."""
    with open(config.DATA_CSV, newline="") as f:
        rows = [(seq, datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S"), float(row["value"]))
                for seq, row in enumerate(csv.DictReader(f))]
    return [r for r in rows if r[0] >= config.STREAM_START_SEQ]


def load_windows():
    """The labelled failure windows as (start, end) datetimes."""
    data = json.loads(config.ANOMALY_WINDOWS.read_text())
    parse = lambda text: datetime.strptime(text, "%Y-%m-%d %H:%M:%S.%f")  # noqa: E731
    return [(parse(w["start"]), parse(w["end"])) for w in data["windows"]]


def accept_in_order(rows):
    """The processor's rule: drop a reading that is not newer than the last accepted one.

    Returns (accepted rows, number dropped).
    """
    accepted = []
    for row in rows:
        if accepted and row[1] <= accepted[-1][1]:
            continue
        accepted.append(row)
    return accepted, len(rows) - len(accepted)


def stream_model_rows(temperatures):
    """Model rows for the stream, from a cold-start window, exactly as a full replay builds them."""
    features = RollingFeatures()
    return [model_row(features.update(t)) for t in temperatures]


def isolation_forest_flags(train_rows, stream_rows, contamination):
    """Fit an IsolationForest with the pipeline's settings except contamination, and flag the stream."""
    model = IsolationForest(n_estimators=train_model.N_ESTIMATORS, contamination=contamination,
                            random_state=train_model.RANDOM_STATE)
    model.fit(train_rows)
    flags = [1 if p == -1 else 0 for p in model.predict(stream_rows)]
    return without_warm_up(flags)


def without_warm_up(flags):
    """The processor never flags the first 12 readings after a cold start (the window is filling)."""
    return [0] * WINDOW_SIZE + flags[WINDOW_SIZE:]


def zscore_flags(temperatures, cutoff, history=ZSCORE_HISTORY):
    """Flag a reading more than `cutoff` sample standard deviations from the mean of the
    previous `history` readings. Readings without a full history are not flagged, and
    neither is a reading compared against a flat history (standard deviation 0)."""
    s = pd.Series(temperatures, dtype=float)
    # shift(1): the window ends at the previous reading, so a reading is not
    # compared against itself.
    mean = s.rolling(history).mean().shift(1)
    std = s.rolling(history).std(ddof=1).shift(1)
    flagged = ((s - mean).abs() > cutoff * std) & (std > 0)
    return [int(f) for f in flagged]


def score_against_windows(times, flags, windows):
    """How the flags line up with the labelled windows."""
    flagged_times = [t for t, f in zip(times, flags) if f]
    inside = [t for t in flagged_times if any(start <= t <= end for start, end in windows)]
    delays = []
    for start, end in windows:
        hits = [t for t in inside if start <= t <= end]
        # Minutes from the window opening to its first flag; None if never flagged.
        delays.append(int((min(hits) - start).total_seconds() // 60) if hits else None)
    return {
        "flagged_pct": 100 * len(flagged_times) / len(times),
        "windows_hit": sum(d is not None for d in delays),
        "flags": len(flagged_times),
        "inside": len(inside),
        "outside": len(flagged_times) - len(inside),
        "delays_min": delays,
    }


def markdown_table(df):
    """A pandas DataFrame as a Markdown table (no extra library needed)."""
    lines = ["| " + " | ".join(df.columns) + " |", "| " + " | ".join("---" for _ in df.columns) + " |"]
    lines += ["| " + " | ".join("" if pd.isna(v) else str(v) for v in row) + " |" for row in df.itertuples(index=False)]
    return "\n".join(lines)


def save_figures(times, temperatures, chosen_flags, windows, table):
    alt.data_transformers.disable_max_rows()   # the full stream is 20,683 points
    series = pd.DataFrame({"event_time": times, "temperature": temperatures, "flag": chosen_flags})
    shaded = pd.DataFrame({"start": [s for s, _ in windows], "end": [e for _, e in windows]})
    rects = alt.Chart(shaded).mark_rect(color="orange", opacity=0.25).encode(x="start:T", x2="end:T")
    line = alt.Chart(series).mark_line(strokeWidth=0.6).encode(
        x=alt.X("event_time:T", title="Event time"),
        y=alt.Y("temperature:Q", title="Temperature").scale(domain=[0, 115]))
    points = alt.Chart(series[series["flag"] == 1]).mark_point(color="red", filled=True, size=14).encode(
        x="event_time:T", y="temperature:Q")
    (rects + line + points).properties(
        width=1100, height=320,
        title="Stream (rows 2000+), labelled failure windows shaded, red = flagged by IsolationForest (4 features, 0.01)",
    ).save(REPORT / "stream_flags.png", scale_factor=2)

    base = alt.Chart(table).encode(y=alt.Y("model:N", sort=None, title=None),
                                   x=alt.X("flags_outside:Q", title="Flags outside the labelled windows"))
    (base.mark_bar() + base.mark_text(align="left", dx=3).encode(text="flags_outside:Q")).properties(
        width=600, height=260, title="Flags outside the labelled windows, per detector",
    ).save(REPORT / "flags_outside_windows.png", scale_factor=2)


def main():
    accepted, dropped = accept_in_order(load_stream())
    times = [t for _, t, _ in accepted]
    temperatures = [temp for _, _, temp in accepted]
    windows = load_windows()
    print(f"Stream: {len(accepted) + dropped} rows, {dropped} dropped as repeated timestamps, {len(accepted)} scored")

    training = train_model.load_training_rows()
    train_rows = train_model.build_training_rows([temp for _, temp in training])      # rows 12-1999
    raw_train = [[temp] for _, temp in training[WINDOW_SIZE:]]                          # same rows, raw value
    stream_rows = stream_model_rows(temperatures)

    detectors = []
    for c in (0.005, 0.01, 0.02, 0.05):
        detectors.append((f"IF 4 features c={c}", "rows 12-1999", isolation_forest_flags(train_rows, stream_rows, c)))
    detectors.append(("IF raw temperature c=0.01", "rows 12-1999",
                      isolation_forest_flags(raw_train, [[t] for t in temperatures], 0.01)))
    for cutoff in (3, 4):
        detectors.append((f"z-score 288 cut-off {cutoff}", "no training", zscore_flags(temperatures, cutoff)))

    # Sanity check: the 0.01 row must be exactly what the pipeline's saved model flags.
    saved = joblib.load(config.MODEL_PATH)["model"]
    pipeline_flags = without_warm_up([1 if p == -1 else 0 for p in saved.predict(stream_rows)])
    chosen = detectors[1][2]
    print(f"IF 4 features c=0.01 equals the saved pipeline model: {chosen == pipeline_flags} "
          f"({sum(chosen)} flags; the Phase 5 full replay stored 284)")

    rows = []
    for name, trained_on, flags in detectors:
        score = score_against_windows(times, flags, windows)
        rows.append({
            "model": name, "trained_on": trained_on,
            "stream_flagged_pct": round(score["flagged_pct"], 2),
            "windows_hit": f"{score['windows_hit']} of {len(windows)}",
            "flags": score["flags"], "flags_inside": score["inside"], "flags_outside": score["outside"],
            **{f"delay_w{i}_min": d for i, d in enumerate(score["delays_min"], start=1)},
        })
    table = pd.DataFrame(rows)
    # "Int64" keeps whole minutes and leaves a missed window empty (plain int cannot hold a gap).
    for column in [c for c in table.columns if c.startswith("delay_")]:
        table[column] = table[column].astype("Int64")
    REPORT.mkdir(exist_ok=True)
    table.to_csv(REPORT / "evaluation.csv", index=False)
    print()
    print(markdown_table(table))
    print("\nWindows: " + "; ".join(f"w{i} {s:%Y-%m-%d %H:%M} to {e:%Y-%m-%d %H:%M}"
                                      for i, (s, e) in enumerate(windows, start=1)))

    save_figures(times, temperatures, chosen, windows, table)
    print("Saved report/evaluation.csv, report/stream_flags.png, report/flags_outside_windows.png")


if __name__ == "__main__":
    main()
