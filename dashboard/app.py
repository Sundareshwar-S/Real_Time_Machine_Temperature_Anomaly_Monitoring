"""Layer 6: the live dashboard.

The page is live for two reasons, and both are needed:
1. New data keeps arriving: the producer and processor keep writing to MySQL.
2. The view re-queries on its own: everything live sits in one fragment that
   Streamlit reruns on a timer, and every query goes to MySQL (nothing is
   answered from a cache).
Stop the producer and the numbers freeze while the page keeps refreshing.

    python scripts/make_secrets.py      (once: database settings from .env)
    streamlit run dashboard/app.py
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, ProgrammingError
from streamlit.errors import StreamlitAPIException

# `streamlit run dashboard/app.py` only puts dashboard/ on the import path; add
# the repo root so the page can read its file settings from src/config.py.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src import config  # noqa: E402

STALE_AFTER_S = 10      # older than this, the status line warns that no new data arrives
Y_DOMAIN = [0, 115]     # fixed temperature axis, so the chart does not jump between refreshes
RECENT_FLAGS = 10       # rows in the table of flagged readings

st.set_page_config(page_title="Machine temperature monitor", page_icon=":material/thermostat:", layout="wide")
st.title("Machine temperature monitor")
st.caption("Live pipeline: CSV replay → Kafka → Python processor → IsolationForest → MySQL → Streamlit")

with st.sidebar:
    refresh_s = st.slider("Refresh every (seconds)", min_value=1, max_value=10, value=2)
    curve_rows = st.slider("Readings on the curve", min_value=12, max_value=2016, value=288, step=12,
                           help="288 readings = 24 hours of event time (one every 5 minutes).")
    paused = st.toggle("Pause refresh", help="Stops the timed refresh. The data keeps arriving in MySQL.")
    show_windows = st.toggle("Show known failure windows", value=True,
                             help="Shade NAB's four labelled failure windows behind the curve.")

# Database settings come from .streamlit/secrets.toml ([connections.mysql]).
try:
    conn = st.connection("mysql", type="sql")
except StreamlitAPIException:
    st.error("Database settings not found in .streamlit/secrets.toml. "
             "Create the file with: python scripts/make_secrets.py, then reload this page.")
    st.stop()


@st.cache_data
def load_windows():
    """NAB's labelled failure windows. A static file, so it is read once and cached."""
    windows = json.loads(config.ANOMALY_WINDOWS.read_text())["windows"]
    return pd.DataFrame({
        "window": [f"Window {i}" for i in range(1, len(windows) + 1)],
        "start": pd.to_datetime([w["start"] for w in windows]),
        "end": pd.to_datetime([w["end"] for w in windows]),
    })


def run_query(sql, **params):
    """Run one SELECT against MySQL and return the rows as a DataFrame.

    Not conn.query(): it caches results (forever unless ttl=0) and leaves each
    pooled connection for the garbage collector to return. At 5 queries every
    2 s that can exhaust the pool. The with-block hands the connection back at
    once, and nothing is cached, so every refresh reads the current data.
    """
    with conn.engine.connect() as db:
        return pd.read_sql(text(sql), db, params=params)


def load_metrics():
    return run_query("SELECT * FROM metrics_current WHERE id = 1")


def load_curve(n):
    # Only the newest n rows, so the query stays fast however big the table gets.
    # Newest first by id, then put back in time order for the chart.
    df = run_query("SELECT event_time, temperature, rolling_avg, is_anomaly FROM readings "
                   "ORDER BY id DESC LIMIT :n", n=n)
    return df.sort_values("event_time")


def load_recent_flags():
    return run_query("SELECT event_time, temperature, rolling_avg, anomaly_score FROM readings "
                     "WHERE is_anomaly = 1 ORDER BY id DESC LIMIT :n", n=RECENT_FLAGS)


def load_forecast():
    # The newest forecast (12 points). Older forecasts for later target times are
    # replaced as the forecaster refits, so one made_at_event_time = one forecast.
    return run_query("SELECT made_at_event_time, target_time, predicted_temperature, lower, upper FROM forecasts "
                     "WHERE made_at_event_time = (SELECT MAX(made_at_event_time) FROM forecasts) "
                     "ORDER BY target_time")


def load_hourly():
    # The last 48 hours of event time from the processor's tumbling 1-hour windows.
    return run_query("SELECT hour_start, reading_count, anomaly_count FROM hourly_stats "
                     "WHERE hour_start > (SELECT MAX(hour_start) FROM hourly_stats) - INTERVAL 48 HOUR "
                     "ORDER BY hour_start")


def load_flag_times():
    # Every flagged event_time: a few hundred rows at most, even after a full replay.
    return run_query("SELECT event_time FROM readings WHERE is_anomaly = 1")


def show_window_progress(last_event_time):
    """How many labelled windows the stream has reached, and how many of those got a flag."""
    windows = load_windows()
    reached = windows[windows["start"] <= last_event_time]
    flag_times = load_flag_times()["event_time"]
    caught = sum(((flag_times >= w.start) & (flag_times <= w.end)).any() for w in reached.itertuples())
    st.markdown(f"Labelled failure windows with at least one flag: **{caught} of {len(reached)}** reached so far")


def show_status(updated_at):
    # The processor stores updated_at as UTC without a timezone, so compare it
    # with the current UTC time in the same form.
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    age_s = max(0, int((now - updated_at).total_seconds()))
    if age_s > STALE_AFTER_S:
        st.warning(f"No new data for {age_s} s. The dashboard is still refreshing.", icon=":material/pause_circle:")
    else:
        st.success(f"Live, last update {age_s} s ago", icon=":material/sensors:")


def show_kpis(m):
    tiles = st.columns(6)
    tiles[0].metric("Latest temperature", f"{m['last_temperature']:.1f}",
                    delta=f"{m['last_temperature'] - m['rolling_avg']:+.1f}", border=True, height="stretch",
                    help="The newest reading. The delta is its difference from the 1-hour rolling average.")
    tiles[1].metric("1-hour rolling average", f"{m['rolling_avg']:.1f}", border=True, height="stretch",
                    help="Mean of the last 12 readings (one every 5 minutes).")
    tiles[2].metric("Running peak", f"{m['peak_temperature']:.1f}", border=True, height="stretch",
                    help="Highest reading since the stream started.")
    tiles[3].metric("Events processed", f"{int(m['events_processed']):,}", border=True, height="stretch",
                    help="Readings accepted and stored. Repeated timestamps are dropped and not counted here.")
    tiles[4].metric("Anomalies flagged", f"{int(m['anomalies_flagged']):,}", border=True, height="stretch",
                    help="Readings the IsolationForest flagged.")
    lag = "—" if m["pipeline_lag_ms"] is None else f"{int(m['pipeline_lag_ms']):,}"
    tiles[5].metric("Pipeline lag (ms)", lag, border=True, height="stretch",
                    help="Time from the producer sending the newest reading to MySQL storing it.")


def forecast_layers(forecast):
    """The ARIMA forecast: a dashed line with its 95% interval as a faint band.

    clip=True keeps the band inside the fixed 0-115 axis when the interval is
    wide, for example during the collapse on 16 Dec.
    """
    base = alt.Chart(forecast).encode(x="target_time:T")
    band = base.mark_area(color="gray", opacity=0.2, clip=True).encode(
        y=alt.Y("lower:Q").scale(domain=Y_DOMAIN), y2="upper:Q")
    line = base.mark_line(color="gray", strokeDash=[6, 4], clip=True).encode(
        y=alt.Y("predicted_temperature:Q").scale(domain=Y_DOMAIN),
        tooltip=[alt.Tooltip("target_time:T", title="forecast for"),
                 alt.Tooltip("predicted_temperature:Q", format=".1f", title="forecast"),
                 alt.Tooltip("lower:Q", format=".1f"), alt.Tooltip("upper:Q", format=".1f")])
    return [band, line]


def show_hourly(hourly):
    """Anomalies per hour of event time: the tumbling window, next to the sliding one on the curve."""
    bars = alt.Chart(hourly).mark_bar(color="red").encode(
        x=alt.X("hour_start:T", title="Hour of event time"),
        y=alt.Y("anomaly_count:Q", title="Flagged readings").scale(domainMin=0),
        tooltip=[alt.Tooltip("hour_start:T", title="hour"), "anomaly_count:Q", "reading_count:Q"])
    st.altair_chart(bars.properties(height=180),
                    alt="Number of flagged readings in each hour of the last 48 hours of event time")


def show_curve(df, forecast):
    base = alt.Chart(df).encode(x=alt.X("event_time:T", title="Event time"))
    # Two lines from one data set: fold the two columns into (series, value) pairs
    # so Altair draws one coloured line per series, with a legend.
    lines = base.transform_fold(["temperature", "rolling_avg"], as_=["series", "value"]).mark_line().encode(
        y=alt.Y("value:Q", title="Temperature").scale(domain=Y_DOMAIN),
        color=alt.Color("series:N", title=None),
    )
    flags = alt.Chart(df[df["is_anomaly"] == 1]).mark_point(color="red", filled=True, size=60).encode(
        x="event_time:T",
        y=alt.Y("temperature:Q").scale(domain=Y_DOMAIN),
        tooltip=["event_time:T", alt.Tooltip("temperature:Q", format=".1f")],
    )
    layers = [lines, flags]
    if not forecast.empty:
        layers += forecast_layers(forecast)
    if show_windows:
        # Only the windows that overlap the curve, clipped to its time range, so
        # the shading never stretches the x-axis beyond the readings shown.
        first, last = df["event_time"].min(), df["event_time"].max()
        windows = load_windows()
        visible = windows[(windows["end"] >= first) & (windows["start"] <= last)].copy()
        visible["start"] = visible["start"].clip(lower=first)
        visible["end"] = visible["end"].clip(upper=last)
        if not visible.empty:
            shading = alt.Chart(visible).mark_rect(color="orange", opacity=0.15).encode(
                x="start:T", x2="end:T", tooltip=["window:N", "start:T", "end:T"])
            layers.insert(0, shading)   # first layer = drawn behind the lines
    st.altair_chart(alt.layer(*layers), alt="Temperature and 1-hour rolling average by event time, "
                    "with red points on readings flagged as anomalous and labelled failure windows shaded")


@st.fragment(run_every=None if paused else refresh_s)
def live_view():
    """Everything that changes. Only this function reruns on the timer."""
    # A failed query shows one sentence instead of a stack trace. The fragment
    # keeps its timer, so the page recovers by itself once the problem is fixed.
    try:
        draw_live()
    except (OperationalError, ProgrammingError, pd.errors.DatabaseError) as error:
        # pandas wraps an error raised while a query runs in its own
        # DatabaseError; the database's original error is its __cause__.
        cause = error.__cause__ if isinstance(error, pd.errors.DatabaseError) else error
        if isinstance(cause, OperationalError):
            st.error(f"Cannot reach MySQL at {conn.engine.url.host}:{conn.engine.url.port}. "
                     "Start it with: docker compose up -d. This page keeps retrying.")
        elif isinstance(cause, ProgrammingError):
            st.error("The tables do not exist yet. Create them with: python -m src.db --init")
        else:
            raise


def draw_live():
    metrics = load_metrics()
    if metrics.empty:
        st.info("Waiting for data. Start the processor, then the producer.", icon=":material/hourglass_empty:")
        return
    m = metrics.iloc[0]

    show_status(m["updated_at"])
    show_kpis(m)
    show_window_progress(m["last_event_time"])

    with st.container(border=True):
        st.subheader(f"Newest {curve_rows} readings")
        forecast = load_forecast()
        show_curve(load_curve(curve_rows), forecast)
        if forecast.empty:
            st.caption("No forecast yet: the forecaster needs 288 readings (24 h of event time) for its first fit.")
        else:
            st.caption(f"Dashed grey: ARIMA(1,1,1) forecast of the next hour, made at event time "
                       f"{forecast['made_at_event_time'].iloc[0]:%d %b %H:%M}; grey band = 95% interval.")

    with st.container(border=True):
        st.subheader("Anomalies per hour (last 48 hours of event time)")
        hourly = load_hourly()
        if hourly.empty:
            st.caption("No hours stored yet.")
        else:
            show_hourly(hourly)

    with st.container(border=True):
        st.subheader(f"Last {RECENT_FLAGS} flagged readings")
        flags = load_recent_flags()
        if flags.empty:
            st.caption("No anomalies flagged yet.")
        else:
            st.dataframe(flags, hide_index=True, alt="The most recent readings flagged as anomalous")

    # The page's own clock, so a frozen page and frozen data look different.
    st.caption(f"Page refreshed at {datetime.now().strftime('%H:%M:%S')}"
               + (" (refresh paused)" if paused else f", every {refresh_s} s"))


live_view()
