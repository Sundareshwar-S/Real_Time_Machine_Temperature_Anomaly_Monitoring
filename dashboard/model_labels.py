"""What the model thought of one reading, as the dashboard labels it.

Plain Python, no Streamlit, so the rule can be unit-tested
(tests/test_model_labels.py). The curve's markers, the score strip, the
histogram and the machine-state badge all use this one rule.
"""
import math

# An anomaly score between 0 and this is close to the model's cut-off at 0: "Watch".
WATCH_SCORE = 0.05


def flag_label(score, is_anomaly):
    """'Flagged', 'Watch' or 'Normal' for one reading.

    Flagged: the processor stored is_anomaly = 1 (the score is below 0).
    Watch: not flagged, score from 0 to WATCH_SCORE.
    A warm-up reading (the first 12 after a cold start) can have a negative
    score but is never flagged; it is not "close to the cut-off", so it is Normal.
    """
    if is_anomaly == 1:
        return "Flagged"
    if score is not None and not math.isnan(score) and 0 <= score <= WATCH_SCORE:
        return "Watch"
    return "Normal"
