"""Hand-calculated checks for the dashboard's zoomed temperature axis (dashboard/chart_scale.py)."""
from dashboard.chart_scale import y_domain


def test_a_calm_stretch_is_zoomed_and_widened_to_the_minimum_span():
    # 92 - 2 = 90 -> 90; 97 + 2 = 99 -> 100. Span 10 < 20, so the top grows by one step to 110.
    assert y_domain([92, 97], zoom=True) == (90, 110)


def test_the_collapse_zooms_out_by_itself():
    # 2.1 - 2 = 0.1 -> 0; 102 + 2 = 104 -> 110.
    assert y_domain([2.1, 102], zoom=True) == (0, 110)


def test_the_axis_never_goes_past_the_full_scale():
    # 50 - 2 = 48 -> 40; 108.5 + 2 = 110.5 -> 120, capped at 115.
    assert y_domain([50, 108.5], zoom=True) == (40, 115)


def test_zoom_off_keeps_the_fixed_scale():
    assert y_domain([92, 97], zoom=False) == (0, 115)


def test_no_readings_gives_the_fixed_scale():
    assert y_domain([], zoom=True) == (0, 115)
