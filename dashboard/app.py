"""Layer 6: the live dashboard.

The page is live for two reasons, and both are needed:
1. New data keeps arriving: the producer and processor keep writing to MySQL.
2. The view re-queries on its own: everything live sits in one fragment that
   Streamlit reruns on a timer, and every query passes ttl=0 so it is never
   answered from Streamlit's cache.
Stop the producer and the numbers freeze while the page keeps refreshing.

    python scripts/make_secrets.py      (once: database settings from .env)
    streamlit run dashboard/app.py
"""
from datetime import datetime, timezone

import altair as alt
import streamlit as st

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

# Database settings come from .streamlit/secrets.toml ([connections.mysql]).
conn = st.connection("mysql", type="sql")


def load_metrics():
    # ttl=0: run the query every time. Without it Streamlit would keep showing
    # the first result forever. show_spinner=False stops a spinner flashing on
    # every refresh.
    return conn.query("SELECT * FROM metrics_current WHERE id = 1", ttl=0, show_spinner=False)


def load_curve(n):
    # Only the newest n rows, so the query stays fast however big the table gets.
    # Newest first by id, then put back in time order for the chart.
    df = conn.query(
        "SELECT event_time, temperature, rolling_avg, is_anomaly FROM readings ORDER BY id DESC LIMIT :n",
        params={"n": n}, ttl=0, show_spinner=False,
    )
    return df.sort_values("event_time")


def load_recent_flags():
    return conn.query(
        "SELECT event_time, temperature, rolling_avg, anomaly_score FROM readings "
        "WHERE is_anomaly = 1 ORDER BY id DESC LIMIT :n",
        params={"n": RECENT_FLAGS}, ttl=0, show_spinner=False,
    )


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


def show_curve(df):
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
    st.altair_chart(alt.layer(lines, flags), alt="Temperature and 1-hour rolling average by event time, "
                    "with red points on readings flagged as anomalous")


@st.fragment(run_every=None if paused else refresh_s)
def live_view():
    """Everything that changes. Only this function reruns on the timer."""
    metrics = load_metrics()
    if metrics.empty:
        st.info("Waiting for data. Start the processor, then the producer.", icon=":material/hourglass_empty:")
        return
    m = metrics.iloc[0]

    show_status(m["updated_at"])
    show_kpis(m)

    with st.container(border=True):
        st.subheader(f"Newest {curve_rows} readings")
        show_curve(load_curve(curve_rows))

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
