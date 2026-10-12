"""The temperature axis of the dashboard charts.

A fixed 0-115 axis never jumps, but a healthy machine moves only about 11 units
in 12 hours, so the curve looks like a flat line. Zoomed, the axis fits the
readings shown, rounded out to whole tens: it only changes when the readings
cross a multiple of 10, so it still stays put between 2-second refreshes.

The axis is never narrower than 40 units. With a 20-unit axis a slow, harmless
drift of 14 units filled 70% of the chart and looked like a crash; at 40 it
takes about a third, so the picture stays honest about the size of a change.
"""
import math

FULL_SCALE = (0, 115)   # every reading in the file fits (2.1 to 108.5)


def y_domain(values, zoom, full=FULL_SCALE, step=10, pad=2, min_span=40):
    """(low, high) for the y-axis of a chart showing these temperatures."""
    values = [v for v in values if v is not None and not math.isnan(v)]
    if not zoom or not values:
        return full
    low = max(full[0], math.floor((min(values) - pad) / step) * step)
    high = min(full[1], math.ceil((max(values) + pad) / step) * step)
    # Widen one step at a time, bottom and top in turn, so the readings stay in the middle.
    # A side that has reached the full scale stops and the other side keeps growing.
    grow_low = True
    while high - low < min_span and (low > full[0] or high < full[1]):
        if grow_low and low > full[0] or high >= full[1]:
            low = max(full[0], low - step)
        else:
            high = min(full[1], high + step)
        grow_low = not grow_low
    return low, high
