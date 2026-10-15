"""The rolling features, shared by training (train_model.py) and serving (processor.py).

Both scripts push readings through this one class, so the model is always
scored on features computed exactly the way it was trained on them.
"""
import statistics
from collections import deque

# 12 readings at 5-minute spacing = 1 hour of event time.
WINDOW_SIZE = 12
# 288 readings = 24 hours of event time, for the slow-drift features.
LONG_WINDOW = 288

# The order of the model's input columns. Training and serving both use exactly this order.
# The first four describe sudden change within the last hour; drift and daily_std
# describe slow change over the last day, which the 1-hour features cannot see.
FEATURE_NAMES = ["temperature", "deviation", "rolling_std", "delta", "drift", "daily_std"]


def model_row(features):
    """One model input row: the feature values in FEATURE_NAMES order.

    Training and serving both build their rows with this, so the columns can
    never end up in a different order on one side.
    """
    return [features[name] for name in FEATURE_NAMES]


class RollingFeatures:
    """Two sliding windows: the last 12 accepted readings (1 hour) and the last 288 (24 hours)."""

    def __init__(self):
        # deque(maxlen=...) drops the oldest reading by itself when a new one arrives on a full window.
        self.window = deque(maxlen=WINDOW_SIZE)
        self.long_window = deque(maxlen=LONG_WINDOW)
        self.previous = None  # the last reading, for delta

    def update(self, temperature):
        """Add one reading and return its features (plus the rolling average)."""
        self.window.append(temperature)
        self.long_window.append(temperature)

        rolling_avg = sum(self.window) / len(self.window)
        # Sample standard deviation needs at least 2 values.
        rolling_std = statistics.stdev(self.window) if len(self.window) >= 2 else 0.0
        delta = temperature - self.previous if self.previous is not None else 0.0
        self.previous = temperature
        # Until 288 readings have arrived the 24-hour window is simply shorter,
        # so these work from the first reading and the warm-up stays 12 readings.
        daily_avg = sum(self.long_window) / len(self.long_window)
        daily_std = statistics.stdev(self.long_window) if len(self.long_window) >= 2 else 0.0

        return {
            "temperature": temperature,
            "rolling_avg": rolling_avg,
            "deviation": temperature - rolling_avg,
            "rolling_std": rolling_std,
            "delta": delta,
            "drift": temperature - daily_avg,
            "daily_std": daily_std,
        }

    def restore(self, temperatures):
        """Refill both windows after a restart, from up to 288 stored readings in time order (oldest first)."""
        self.window.clear()
        self.long_window.clear()
        self.window.extend(temperatures)        # keeps only the last 12
        self.long_window.extend(temperatures)   # keeps up to the last 288
        self.previous = temperatures[-1] if temperatures else None
