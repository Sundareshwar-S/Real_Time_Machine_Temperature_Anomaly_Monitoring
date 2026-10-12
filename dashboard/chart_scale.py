"""The temperature axis of the dashboard charts.

A fixed 0-115 axis never jumps, but a healthy machine moves only about 17 units
in 24 hours, so the curve looks like a flat line. Zoomed, the axis fits the
readings shown, rounded out to whole tens: it only changes when the readings
cross a multiple of 10, so it still stays put between 2-second refreshes.
"""
import math

FULL_SCALE = (0, 115)   # every reading in the file fits (2.1 to 108.5)


def y_domain(values, zoom, full=FULL_SCALE, step=10, pad=2, min_span=20):
    """(low, high) for the y-axis of a chart showing these temperatures."""
    values = [v for v in values if v is not None and not math.isnan(v)]
    if not zoom or not values:
        return full
    low = max(full[0], math.floor((min(values) - pad) / step) * step)
    high = min(full[1], math.ceil((max(values) + pad) / step) * step)
    # Never zoom in closer than min_span, so sensor noise is not blown up into waves.
    while high - low < min_span:
        if high < full[1]:
            high = min(full[1], high + step)
        else:
            low = max(full[0], low - step)
    return low, high
