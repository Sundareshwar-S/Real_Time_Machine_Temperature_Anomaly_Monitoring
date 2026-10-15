"""What the model thought of the readings, as the dashboard shows it.

Plain Python, no Streamlit, so the rules can be unit-tested (tests/test_model_labels.py).

Two ideas:
- The label of one reading: Flagged (the processor stored is_anomaly = 1),
  Watch (score from 0 up to the model's watch limit) or Normal.
- A sustained alert: at least ALERT_NEED of the last ALERT_WINDOW readings are
  flagged. In a slow decline the score can hover right at the cut-off, so single
  flags switch on and off from one reading to the next; monitoring tools wait for
  several flags close together before they raise an alert ("debounce"). The
  stored flags are not changed; this is only how the dashboard groups them.
"""
import math

# The model file holds its own watch limit (src/train_model.py); this is only the
# fallback for a model file saved before 2026-10-15.
DEFAULT_WATCH_SCORE = 0.05
ALERT_WINDOW = 6   # readings looked at: 30 minutes of event time
ALERT_NEED = 3     # flagged readings among them that make a sustained alert


def flag_label(score, is_anomaly, watch):
    """'Flagged', 'Watch' or 'Normal' for one reading.

    A warm-up reading (the first 12 after a cold start) can have a negative
    score but is never flagged; it is not "close to the cut-off", so it is Normal.
    """
    if is_anomaly == 1:
        return "Flagged"
    if score is not None and not math.isnan(score) and 0 <= score <= watch:
        return "Watch"
    return "Normal"


def alert_states(flags, window=ALERT_WINDOW, need=ALERT_NEED):
    """1 for each reading where at least `need` of the last `window` readings (itself included) are flagged."""
    states = []
    for i in range(len(flags)):
        recent = flags[max(0, i - window + 1):i + 1]
        states.append(1 if sum(recent) >= need else 0)
    return states


def alert_episodes(times, states):
    """The sustained alerts as (first, last) event time of each run of alert readings."""
    episodes = []
    start = None
    for i, (t, on) in enumerate(zip(times, states)):
        if on and start is None:
            start = t
        if start is not None and (not on or i == len(states) - 1):
            episodes.append((start, t if on else times[i - 1]))
            start = None
    return episodes
