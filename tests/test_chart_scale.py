"""Hand-calculated checks for the dashboard's zoomed temperature axis (dashboard/chart_scale.py)."""
from dashboard.chart_scale import y_domain


def test_a_calm_stretch_is_widened_to_40_on_both_sides():
    # 92 - 2 = 90 -> 90; 97 + 2 = 99 -> 100. Span 10 < 40: low 80, high 110, low 70 -> span 40.
    assert y_domain([92, 97], zoom=True) == (70, 110)


def test_a_slow_drift_fills_well_under_half_the_axis():
    # 22-23 Dec: 84.8 - 2 = 82.8 -> 80; 98.8 + 2 = 100.8 -> 110. Span 30 < 40: low 70.
    # The 14-unit drift then takes 35% of the height instead of 70% on a 20-unit axis.
    assert y_domain([84.8, 98.8], zoom=True) == (70, 110)


def test_the_collapse_zooms_out_by_itself():
    # 2.1 - 2 = 0.1 -> 0; 102 + 2 = 104 -> 110.
    assert y_domain([2.1, 102], zoom=True) == (0, 110)


def test_the_axis_never_goes_past_the_full_scale():
    # 50 - 2 = 48 -> 40; 108.5 + 2 = 110.5 -> 120, capped at 115.
    assert y_domain([50, 108.5], zoom=True) == (40, 115)


def test_a_capped_side_lets_the_other_side_grow():
    # 104 - 2 = 102 -> 100; 108 + 2 = 110 -> 110. Span 10: low 90, high 115 (cap), low 80, low 70 -> span 45.
    assert y_domain([104, 108], zoom=True) == (70, 115)


def test_zoom_off_keeps_the_fixed_scale():
    assert y_domain([92, 97], zoom=False) == (0, 115)


def test_no_readings_gives_the_fixed_scale():
    assert y_domain([], zoom=True) == (0, 115)
