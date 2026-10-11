"""The rolling features, shared by training (train_model.py) and serving (processor.py).

Both scripts push readings through this one class, so the model is always
scored on features computed exactly the way it was trained on them.
"""
import statistics
from collections import deque

# 12 readings at 5-minute spacing = 1 hour of event time.
WINDOW_SIZE = 12

# The order of the model's input columns. Phase 3 trains on exactly this order.
FEATURE_NAMES = ["temperature", "deviation", "rolling_std", "delta"]


class RollingFeatures:
    """A sliding window over the last 12 accepted readings."""

    def __init__(self):
        # deque(maxlen=12) drops the oldest reading by itself when the 13th arrives.
        self.window = deque(maxlen=WINDOW_SIZE)
        self.previous = None  # the last reading, for delta

    def update(self, temperature):
        """Add one reading and return its features (plus the rolling average)."""
        self.window.append(temperature)

        rolling_avg = sum(self.window) / len(self.window)
        # Sample standard deviation needs at least 2 values.
        rolling_std = statistics.stdev(self.window) if len(self.window) >= 2 else 0.0
        delta = temperature - self.previous if self.previous is not None else 0.0
        self.previous = temperature

        return {
            "temperature": temperature,
            "rolling_avg": rolling_avg,
            "deviation": temperature - rolling_avg,
            "rolling_std": rolling_std,
            "delta": delta,
        }

    def restore(self, temperatures):
        """Refill the window after a restart, from stored readings in time order (oldest first)."""
        self.window.clear()
        self.window.extend(temperatures)
        self.previous = temperatures[-1] if temperatures else None
