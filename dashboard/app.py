"""Layer 6: the live dashboard.

The page is live for two reasons, and both are needed:
1. New data keeps arriving: the producer and processor keep writing to MySQL.
2. The view re-queries on its own: everything live sits in one fragment that
   Streamlit reruns on a timer, and every query goes to MySQL (nothing is
   answered from a cache).
Stop the producer and the numbers freeze while the page keeps refreshing.

Layout, top to bottom: machine state and replay progress, KPI tiles, the
temperature curve (follow live, or browse back through history), five tabs
(model, hourly window, pipeline, readings log, run report) and the last flags.
When the last stream row is stored, the final run report appears at the top.

    python scripts/make_secrets.py      (once: database settings from .env)
    streamlit run dashboard/app.py
"""
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import altair as alt
import joblib
import pandas as pd
import pymysql
import streamlit as st
from kafka import KafkaAdminClient
from kafka.errors import KafkaError
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, ProgrammingError
from streamlit.errors import StreamlitAPIException

# `streamlit run dashboard/app.py` only puts dashboard/ on the import path; add
# the repo root so the page can read its file settings from src/config.py.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dashboard import run_report  # noqa: E402
from dashboard.chart_scale import FULL_SCALE, y_domain  # noqa: E402
from dashboard.history_scrubber import from_step, history_scrubber  # noqa: E402
from dashboard.model_labels import (ALERT_NEED, ALERT_WINDOW, DEFAULT_WATCH_SCORE, alert_episodes,  # noqa: E402
                                    alert_states, flag_label)
from scripts import evaluate, reset_demo  # noqa: E402
from src import config, db  # noqa: E402

STALE_AFTER_S = 10       # older than this, the status line warns that no new data arrives
RECENT_FLAGS = 10        # rows in the table of flagged readings
SPARK_ROWS = 48          # readings in the tile sparklines (4 hours of event time)
# Readings on the curve: 12 hours of event time. Fewer readings than a full day, so each change is wide
# enough to see, and at --rate 10 a 2 s refresh moves the curve by 20 of 144 points (about 14%).
CURVE_ROWS = 144
TIMELINE_MINUTES = 240   # rows in the run timeline (one per wall-clock minute)
FORECAST_CHECK = 288     # newest forecast points compared with the reading that actually arrived
STEP = timedelta(minutes=5)   # one reading every 5 minutes of event time
LOCAL_TZ = datetime.now().astimezone().tzinfo
TABS = ["Model view", "Hourly view", "Pipeline", "Readings log", "Run report"]

# One colour per series, used by every chart (the same palette as .streamlit/config.toml).
COLORS = {"temperature": "#60A5FA", "rolling_avg": "#34D399", "flag": "#F87171", "watch": "#FB923C",
          "forecast": "#94A3B8", "window": "#FBBF24", "normal": "#60A5FA"}
SERIES = alt.Scale(domain=["temperature", "rolling_avg"], range=[COLORS["temperature"], COLORS["rolling_avg"]])
FLAG_COLORS = alt.Scale(domain=["Normal", "Watch", "Flagged"],
                        range=[COLORS["normal"], COLORS["watch"], COLORS["flag"]])
MARKERS = alt.Scale(domain=["Watch", "Flagged"], range=[COLORS["watch"], COLORS["flag"]])
Y_AXIS = {"minExtent": 50, "maxExtent": 50}   # same room for y labels, so the curve and the score strip line up

st.set_page_config(page_title="Machine temperature monitor", page_icon=":material/thermostat:", layout="wide")
st.title("Machine temperature monitor", icon=":material/thermostat:")
st.caption("Live pipeline: CSV replay → Kafka → Python processor → IsolationForest → MySQL → Streamlit")

with st.sidebar:
    refresh_s = st.slider("Refresh every (seconds)", min_value=1, max_value=10, value=2)
    paused = st.toggle("Pause refresh", help="Stops the timed refresh. The data keeps arriving in MySQL.")
    show_windows = st.toggle("Show known failure windows", value=True,
                             help="Shade NAB's four labelled failure windows behind the curve.")
    show_band = st.toggle("Show volatility band", value=True,
                          help="Rolling average ± 2 rolling standard deviations (the last 12 readings).")
    zoom_axis = st.toggle("Zoom temperature axis", value=True,
                          help="Fit the temperature axis to the readings shown, in steps of 10. "
                               f"Off = fixed {FULL_SCALE[0]}–{FULL_SCALE[1]} scale.")

# Widget state used by the history browser, set once per browser session.
st.session_state.setdefault("curve_mode", "Follow live")
st.session_state.setdefault("celebrated", False)


# ---------------------------------------------------------------- clear data and start again

def clear_all_data():
    """The same reset as scripts/reset_demo.py: refuses (ResetError) while anything still runs."""
    admin = KafkaAdminClient(bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS, bootstrap_timeout_ms=5000)
    try:
        with db.connect() as mysql:
            return reset_demo.reset_all(admin, mysql)
    finally:
        admin.close()


@st.dialog("Clear data and start again", icon=":material/restart_alt:")
def clear_data_dialog():
    st.markdown("**Deletes:** every message on the Kafka topic, both consumer groups (processor and forecaster), "
                "all rows in the four MySQL tables, `.producer_state` and the dashboard's cached data.\n\n"
                "**Keeps:** the dataset in `data/`, the failure labels, `db/schema.sql`, the trained model and "
                "the settings.")
    st.caption("Stop the producer, processor and forecaster with Ctrl+C first; the dashboard can stay open. "
               "If anything is still running, nothing is deleted.")
    if st.button("Clear data", type="primary", icon=":material/delete:", width="stretch"):
        try:
            with st.spinner("Checking that nothing runs, then clearing..."):
                clear_all_data()
        except reset_demo.ResetError as error:
            st.error(str(error), icon=":material/block:")
            return
        except (KafkaError, pymysql.err.OperationalError) as error:
            st.error(f"Cannot reach Kafka or MySQL ({error}). Start them with: docker compose up -d",
                     icon=":material/error:")
            return
        # Forget everything cached or remembered from the old run, then redraw the whole page.
        # Widget and component keys (curve_mode, history_scrub) are set, never deleted: a deleted
        # key makes Streamlit run that widget's callback on the next run before the key exists.
        # The scrubber needs no reset; it takes its position from history_end on every run.
        st.cache_data.clear()
        st.session_state.curve_mode = "Follow live"
        st.session_state.celebrated = False
        st.session_state.pop("history_end", None)
        st.session_state.cleared = True
        st.rerun()


with st.sidebar:
    st.divider()
    if st.button("Clear data and start again", icon=":material/restart_alt:", width="stretch",
                 help="Empty Kafka and MySQL so the replay can run again from the start. The dataset is kept."):
        clear_data_dialog()
    if st.session_state.pop("cleared", False):
        # A toast disappears by itself, so it never goes stale once the next run is live.
        st.toast("Data cleared: Kafka topic, consumer groups, tables, .producer_state and caches. "
                 "Start the processor and forecaster, then the producer.", icon=":material/check_circle:")

# Database settings come from .streamlit/secrets.toml ([connections.mysql]).
try:
    conn = st.connection("mysql", type="sql")
except StreamlitAPIException:
    st.error("Database settings not found in .streamlit/secrets.toml. "
             "Create the file with: python scripts/make_secrets.py, then reload this page.")
    st.stop()


# ---------------------------------------------------------------- static files (read once)

@st.cache_data
def load_windows():
    """NAB's labelled failure windows. A static file, so it is read once and cached."""
    windows = evaluate.load_windows()
    return pd.DataFrame({
        "window": [f"Window {i}" for i in range(1, len(windows) + 1)],
        "start": pd.to_datetime([start for start, _ in windows]),
        "end": pd.to_datetime([end for _, end in windows]),
    })


@st.cache_data
def stream_bounds():
    """First and last event time, last seq and row count of the stream part of the CSV.

    The history slider uses these as fixed bounds: a slider whose bounds change
    is reset by Streamlit, so the bounds must not grow with the data.
    """
    rows = evaluate.load_stream()
    times = [event_time for _, event_time, _ in rows]
    return {"first": min(times), "last": max(times), "last_seq": rows[-1][0], "rows": len(rows)}


@st.cache_data
def load_watch_score():
    """The model's own watch limit: the 2nd percentile of its training scores (saved by src/train_model.py)."""
    if not config.MODEL_PATH.exists():
        return DEFAULT_WATCH_SCORE
    return joblib.load(config.MODEL_PATH).get("watch_score", DEFAULT_WATCH_SCORE)


# A score from 0 up to this is "watch": close to the cut-off, as unusual as the next 1% of the training week.
WATCH_SCORE = load_watch_score()


# ---------------------------------------------------------------- queries (nothing cached)

def run_query(sql, **params):
    """Run one SELECT against MySQL and return the rows as a DataFrame.

    Not conn.query(): it caches results (forever unless ttl=0) and leaves each
    pooled connection for the garbage collector to return. At 5 queries every
    2 s that can exhaust the pool. The with-block hands the connection back at
    once, and nothing is cached, so every refresh reads the current data.
    """
    with conn.engine.connect() as db:
        return pd.read_sql(text(sql), db, params=params)


def to_local(times):
    """Wall-clock times are stored as naive UTC; show them in this computer's local time."""
    return pd.to_datetime(times).dt.tz_localize("UTC").dt.tz_convert(LOCAL_TZ).dt.tz_localize(None)


def load_metrics():
    return run_query("SELECT * FROM metrics_current WHERE id = 1")


CURVE_COLUMNS = ("seq, event_time, temperature, rolling_avg, rolling_std, deviation, delta, anomaly_score, "
                 "is_anomaly, stored_at, TIMESTAMPDIFF(MICROSECOND, produced_at, stored_at) / 1000 AS lag_ms")


def load_curve(n, until=None):
    """The newest n readings, or with `until` the n readings up to that event time (history).

    Either way at most n rows (plus 5 earlier ones for the alert rule), so the
    query stays fast however big the table gets. Newest first, then put back in
    time order for the charts.
    """
    # The sustained-alert rule looks at the last 6 readings, so read 5 more than are
    # shown: the first reading on the chart then gets the same answer as everywhere else.
    rows = n + ALERT_WINDOW - 1
    if until is None:
        df = run_query(f"SELECT {CURVE_COLUMNS} FROM readings ORDER BY id DESC LIMIT :n", n=rows)
    else:
        # event_time is indexed, so this reads only the rows it returns.
        df = run_query(f"SELECT {CURVE_COLUMNS} FROM readings WHERE event_time <= :until "
                       "ORDER BY event_time DESC LIMIT :n", n=rows, until=until)
    df["lag_ms"] = df["lag_ms"].astype(float)   # MySQL returns the division as a DECIMAL
    df["stored_at"] = to_local(df["stored_at"])
    # The model's view of each reading: flagged (score below 0), watch (close to the cut-off) or normal.
    df["flag"] = [flag_label(score, flagged, WATCH_SCORE)
                  for score, flagged in zip(df["anomaly_score"], df["is_anomaly"])]
    df["band_low"] = df["rolling_avg"] - 2 * df["rolling_std"]
    df["band_high"] = df["rolling_avg"] + 2 * df["rolling_std"]
    df = df.sort_values("event_time")
    # Sustained alert: at least 3 of the last 6 readings flagged (dashboard/model_labels.py).
    df["alert"] = alert_states([int(f) for f in df["is_anomaly"]])
    return df.tail(n)


def load_recent_flags():
    return run_query("SELECT event_time, temperature, rolling_avg, anomaly_score FROM readings "
                     "WHERE is_anomaly = 1 ORDER BY id DESC LIMIT :n", n=RECENT_FLAGS)


def load_forecast():
    # The newest forecast (12 points). Older forecasts for later target times are
    # replaced as the forecaster refits, so one made_at_event_time = one forecast.
    return run_query("SELECT made_at_event_time, target_time, predicted_temperature, lower, upper FROM forecasts "
                     "WHERE made_at_event_time = (SELECT MAX(made_at_event_time) FROM forecasts) "
                     "ORDER BY target_time")


def load_forecast_check():
    # Forecast points whose target time has arrived, next to the reading that did arrive.
    return run_query("SELECT f.target_time, f.predicted_temperature, f.lower, f.upper, r.temperature AS actual "
                     "FROM forecasts f JOIN readings r ON r.event_time = f.target_time "
                     "ORDER BY f.target_time DESC LIMIT :n", n=FORECAST_CHECK).sort_values("target_time")


def load_hourly():
    # The last 48 hours of event time from the processor's tumbling 1-hour windows.
    return run_query("SELECT hour_start, reading_count, avg_temperature, min_temperature, max_temperature, "
                     "anomaly_count FROM hourly_stats "
                     "WHERE hour_start > (SELECT MAX(hour_start) FROM hourly_stats) - INTERVAL 48 HOUR "
                     "ORDER BY hour_start")


def load_heatmap():
    # Every hour stored so far: at most 1,725 rows after a full replay.
    return run_query("SELECT hour_start, reading_count, anomaly_count FROM hourly_stats")


def load_timeline():
    """One row per wall-clock minute in which readings arrived: what the run produced, minute by minute."""
    df = run_query("SELECT DATE_FORMAT(stored_at, '%Y-%m-%d %H:%i:00') AS arrived_minute, COUNT(*) AS readings, "
                   "MIN(event_time) AS event_from, MAX(event_time) AS event_to, SUM(is_anomaly) AS flags, "
                   "MIN(temperature) AS min_temperature, MAX(temperature) AS max_temperature "
                   "FROM readings GROUP BY arrived_minute ORDER BY arrived_minute DESC LIMIT :n", n=TIMELINE_MINUTES)
    df["arrived_minute"] = to_local(df["arrived_minute"])
    df["flags"] = df["flags"].astype(int)   # SUM() comes back as a DECIMAL
    return df


def load_flag_times():
    # Every flagged event_time: a few hundred rows at most, even after a full replay.
    return run_query("SELECT event_time FROM readings WHERE is_anomaly = 1")


def load_stored_range():
    # Oldest and newest stored event time (event_time is indexed, so this is quick).
    row = run_query("SELECT MIN(event_time) AS oldest, MAX(event_time) AS newest FROM readings").iloc[0]
    return row["oldest"].to_pydatetime(), row["newest"].to_pydatetime()


@st.cache_data(max_entries=2, show_spinner=False)
def load_run_data(version):
    """Every stored reading and every forecast point that can be checked, for the run report.

    The only cached query: `version` is (events processed, updated_at), which
    changes with every processor batch, so a new batch always means a fresh read.
    The cache only stops the same full read repeating every 2 s once the run
    has finished or is paused.
    """
    readings = run_query("SELECT event_time, temperature, is_anomaly, produced_at, stored_at FROM readings ORDER BY id")
    forecasts = run_query("SELECT f.predicted_temperature, f.lower, f.upper, r.temperature AS actual "
                          "FROM forecasts f JOIN readings r ON r.event_time = f.target_time")
    n_forecasts = int(run_query("SELECT COUNT(DISTINCT made_at_event_time) AS n FROM forecasts")["n"].iloc[0])
    return readings, forecasts, n_forecasts


# ---------------------------------------------------------------- history browser callbacks

def enter_browse():
    """Switching to "Browse history" starts at the newest reading, so nothing jumps."""
    if st.session_state.curve_mode == "Browse history":
        st.session_state.history_end = st.session_state.latest_event_time


def shift_history(readings):
    """Move the browsed range by a number of readings (negative = earlier), inside the stream's bounds."""
    bounds = stream_bounds()
    moved = st.session_state.history_end + readings * STEP
    st.session_state.history_end = min(max(moved, bounds["first"]), bounds["last"])


def back_to_live():
    st.session_state.curve_mode = "Follow live"


def scrubbed():
    """The history scrubber moved (also while it is being dragged): show readings up to its new position."""
    bounds = stream_bounds()
    st.session_state.history_end = from_step(st.session_state.history_scrub["end"], bounds["first"], bounds["last"])


# ---------------------------------------------------------------- status, tiles

def windows_caught(last_event_time):
    """How many labelled windows the stream has reached, and how many of those got a flag."""
    windows = load_windows()
    reached = windows[windows["start"] <= last_event_time]
    flag_times = load_flag_times()["event_time"]
    caught = sum(((flag_times >= w.start) & (flag_times <= w.end)).any() for w in reached.itertuples())
    return caught, len(reached)


def machine_state(newest):
    """Anomaly / Flagged reading / Watch / Normal for the newest reading, from the model's own output."""
    if newest["alert"] == 1:
        return "Anomaly", "red", ":material/error:"
    label = flag_label(newest["anomaly_score"], newest["is_anomaly"], WATCH_SCORE)
    if label == "Flagged":
        return "Flagged reading", "orange", ":material/report:"
    if label == "Watch":
        return "Watch", "yellow", ":material/visibility:"
    return "Normal", "green", ":material/check_circle:"


def show_status(updated_at, complete):
    # The processor stores updated_at as UTC without a timezone, so compare it
    # with the current UTC time in the same form.
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    age_s = max(0, int((now - updated_at).total_seconds()))
    if complete:
        st.success(f"Every stream row has been sent and stored; the last one arrived "
                   f"{run_report.format_duration(age_s)} ago. The final report is below.",
                   title="Replay complete", icon=":material/flag:")
    elif age_s > STALE_AFTER_S:
        st.warning(f"No new data for {age_s} s. The dashboard is still refreshing.", icon=":material/pause_circle:")
    else:
        st.success(f"Live, last update {age_s} s ago", icon=":material/sensors:")


def show_header(m, newest, bounds, complete):
    label, color, icon = machine_state(newest)
    with st.container(horizontal=True, vertical_alignment="center"):
        st.badge(f"Machine state: {label}", icon=icon, color=color,
                 help=f"Anomaly: a sustained alert ({ALERT_NEED} of the last {ALERT_WINDOW} readings flagged). "
                      "Flagged reading: the newest reading is flagged but its neighbours mostly are not. "
                      f"Watch: its anomaly score is between 0 and {WATCH_SCORE:.4f} (close to the cut-off at 0). "
                      "Normal: otherwise.")
        st.caption(f"Newest reading {newest['event_time']:%d %b %Y %H:%M} (event time), "
                   f"anomaly score {newest['anomaly_score']:+.3f}, arrived {newest['stored_at']:%H:%M:%S}")
    show_status(m["updated_at"], complete)
    position = int(newest["seq"]) - config.STREAM_START_SEQ + 1
    st.progress(min(position / bounds["rows"], 1.0),
                text=f"Replay position: row {position:,} of {bounds['rows']:,} stream rows "
                     f"({position / bounds['rows']:.0%}), event time {newest['event_time']:%d %b %Y %H:%M}")


def show_kpis(m, live):
    spark = live.tail(SPARK_ROWS)
    caught, reached = windows_caught(m["last_event_time"])
    rate = 100 * m["anomalies_flagged"] / m["events_processed"] if m["events_processed"] else 0
    lag = "—" if m["pipeline_lag_ms"] is None else f"{int(m['pipeline_lag_ms']):,}"
    tile = {"border": True, "height": "stretch"}

    row = st.columns(5)
    row[0].metric("Latest temperature", f"{m['last_temperature']:.1f}",
                  delta=f"{m['last_temperature'] - m['rolling_avg']:+.1f}",
                  chart_data=spark["temperature"].round(2).tolist(), chart_type="area", **tile,
                  help="The newest reading. The delta is its difference from the 1-hour rolling average; "
                       f"the sparkline is the last {SPARK_ROWS} readings.")
    row[1].metric("1-hour rolling average", f"{m['rolling_avg']:.1f}", **tile,
                  help="Mean of the last 12 readings (one every 5 minutes).")
    row[2].metric("Running peak", f"{m['peak_temperature']:.1f}", **tile,
                  help="Highest reading since the stream started.")
    row[3].metric("Running minimum", f"{m['min_temperature']:.1f}", **tile,
                  help="Lowest reading since the stream started.")
    row[4].metric("Failure windows caught", f"{caught} of {reached}", **tile,
                  help="NAB's labelled failure windows the stream has reached, and how many of them "
                       "contain at least one flagged reading.")

    row = st.columns(5)
    row[0].metric("Events processed", f"{int(m['events_processed']):,}", **tile,
                  help="Readings accepted and stored. Repeated timestamps are dropped and not counted here.")
    row[1].metric("Anomalies flagged", f"{int(m['anomalies_flagged']):,}", **tile,
                  help="Readings the IsolationForest flagged.")
    row[2].metric("Anomaly rate", f"{rate:.2f}%", **tile,
                  help="Flagged readings as a share of readings processed. The model flags about 1% of its training "
                       "week; the stream has more unusual stretches (2.06% on a full replay).")
    row[3].metric("Late events dropped", f"{int(m['late_events_dropped']):,}", **tile,
                  help="Readings whose event time was not newer than the last accepted one (12 in a full "
                       "replay: the clock steps back on 7 Jan 2014).")
    row[4].metric("Pipeline lag (ms)", lag, chart_data=spark["lag_ms"].round(1).tolist(), chart_type="bar", **tile,
                  help="Time from the producer sending the newest reading to MySQL storing it; "
                       f"the sparkline is the last {SPARK_ROWS} readings.")


# ---------------------------------------------------------------- main curve

def forecast_layers(forecast, domain):
    """The ARIMA forecast: a dashed line with its 95% interval as a faint band.

    clip=True keeps the band inside the axis when the interval is wide, for
    example during the collapse on 16 Dec, instead of stretching the axis.
    """
    base = alt.Chart(forecast).encode(x="target_time:T")
    band = base.mark_area(color=COLORS["forecast"], opacity=0.2, clip=True).encode(
        y=alt.Y("lower:Q").scale(domain=domain), y2="upper:Q")
    line = base.mark_line(color=COLORS["forecast"], strokeDash=[6, 4], clip=True).encode(
        y=alt.Y("predicted_temperature:Q").scale(domain=domain),
        tooltip=[alt.Tooltip("target_time:T", title="forecast for", format="%d %b %H:%M"),
                 alt.Tooltip("predicted_temperature:Q", format=".1f", title="forecast"),
                 alt.Tooltip("lower:Q", format=".1f"), alt.Tooltip("upper:Q", format=".1f")])
    return [band, line]


def window_shading(df):
    """The labelled windows that overlap the curve, clipped to its time range, so
    the shading never stretches the x-axis beyond the readings shown."""
    first, last = df["event_time"].min(), df["event_time"].max()
    windows = load_windows()
    visible = windows[(windows["end"] >= first) & (windows["start"] <= last)].copy()
    visible["start"] = visible["start"].clip(lower=first)
    visible["end"] = visible["end"].clip(upper=last)
    if visible.empty:
        return []
    return [alt.Chart(visible).mark_rect(color=COLORS["window"], opacity=0.12).encode(
        x="start:T", x2="end:T", tooltip=["window:N", "start:T", "end:T"])]


def alert_bands(df):
    """Sustained alerts on the chart as red bands, half a reading wider on each side, so a
    short alert is still visible and neighbouring bands meet instead of leaving a gap."""
    half = STEP / 2
    episodes = alert_episodes(list(df["event_time"]), list(df["alert"]))
    if not episodes:
        return []
    bands = pd.DataFrame({"start": [a - half for a, _ in episodes], "end": [b + half for _, b in episodes],
                          "from": [a for a, _ in episodes], "to": [b for _, b in episodes]})
    return [alt.Chart(bands).mark_rect(color=COLORS["flag"], opacity=0.22, clip=True).encode(
        x="start:T", x2="end:T",
        tooltip=[alt.Tooltip("from:T", title="sustained alert from", format="%d %b %Y %H:%M"),
                 alt.Tooltip("to:T", title="to", format="%d %b %Y %H:%M")])]


def vl_time(t):
    """A pandas time as a Vega-Lite date, read as local time like the chart data."""
    return alt.DateTime(year=t.year, month=t.month, date=t.day, hours=t.hour, minutes=t.minute)


def show_curve(df, forecast, x_domain):
    """Draw the main chart. Returns the temperature axis it used, for the caption."""
    # From the readings only: the bands are clipped, so a wide forecast interval cannot stretch the axis.
    domain = list(y_domain(df["temperature"], zoom_axis))
    base = alt.Chart(df).encode(x=alt.X("event_time:T", title=None).scale(domain=x_domain))
    layers = window_shading(df) if show_windows else []   # first layers = drawn behind the lines
    layers += alert_bands(df)
    if show_band:
        layers.append(base.mark_area(color=COLORS["rolling_avg"], opacity=0.15, clip=True).encode(
            y=alt.Y("band_low:Q").scale(domain=domain), y2="band_high:Q"))
    # Two lines from one data set: fold the two columns into (series, value) pairs
    # so Altair draws one coloured line per series, with a legend.
    layers.append(base.transform_fold(["temperature", "rolling_avg"], as_=["series", "value"]).mark_line().encode(
        y=alt.Y("value:Q", title="Temperature").scale(domain=domain).axis(**Y_AXIS),
        color=alt.Color("series:N", title=None).scale(SERIES).legend(orient="top")))
    # Red points = flagged readings: large inside a sustained alert, small when on their own.
    # Watch readings are shown in the score strip below, not here, so a long stretch
    # close to the cut-off does not cover the curve in markers.
    flagged = df[df["flag"] == "Flagged"].assign(
        mark=lambda d: d["alert"].map({1: "Flagged, sustained alert", 0: "Flagged, single reading"}))
    layers.append(alt.Chart(flagged).mark_point(filled=True, opacity=1, color=COLORS["flag"]).encode(
        x="event_time:T", y=alt.Y("temperature:Q").scale(domain=domain),
        size=alt.Size("mark:N", title=None).scale(domain=["Flagged, sustained alert", "Flagged, single reading"],
                                                  range=[90, 25]).legend(orient="top")))
    if not forecast.empty:
        layers += forecast_layers(forecast, domain)
    # Hover anywhere: the nearest reading gets a vertical rule and a tooltip that
    # also says when it arrived (wall clock), not only when the sensor took it.
    hover = alt.selection_point(fields=["event_time"], nearest=True, on="pointerover", empty=False)
    layers.append(base.mark_rule(color=COLORS["forecast"]).encode(
        opacity=alt.when(hover).then(alt.value(0.6)).otherwise(alt.value(0)),
        tooltip=[alt.Tooltip("event_time:T", title="event time", format="%d %b %Y %H:%M"),
                 alt.Tooltip("temperature:Q", format=".2f"),
                 alt.Tooltip("rolling_avg:Q", format=".2f", title="rolling avg"),
                 alt.Tooltip("anomaly_score:Q", format="+.3f", title="anomaly score"),
                 alt.Tooltip("flag:N", title="model"),
                 alt.Tooltip("stored_at:T", format="%H:%M:%S", title="arrived at")]).add_params(hover))
    # Two colour legends (lines, model markers): keep their scales apart.
    st.altair_chart(alt.layer(*layers).resolve_scale(color="independent").properties(height=420),
                    alt="Temperature and 1-hour rolling average by event time, with a volatility band, red points on "
                        "readings flagged as anomalous, labelled failure windows shaded and the ARIMA forecast")
    return domain


def show_score_strip(df, x_domain):
    """The model's anomaly score under the curve, on the same time axis.

    Below 0 the reading is flagged; the orange band (0 to WATCH_SCORE, the
    model's own limit) is close to the cut-off. The red band marks sustained
    alerts, as on the curve.
    """
    base = alt.Chart(df).encode(x=alt.X("event_time:T", title="Event time").scale(domain=x_domain))
    bands = alert_bands(df)
    watch = alt.Chart(pd.DataFrame({"low": [0.0], "high": [WATCH_SCORE]})).mark_rect(
        color=COLORS["watch"], opacity=0.18).encode(y="low:Q", y2="high:Q")
    line = base.mark_line(color=COLORS["temperature"], strokeWidth=1.5).encode(
        y=alt.Y("anomaly_score:Q", title="Score").axis(**Y_AXIS, tickCount=4))
    cut = alt.Chart(pd.DataFrame({"score": [0.0]})).mark_rule(color=COLORS["flag"], strokeDash=[4, 4]).encode(y="score:Q")
    points = alt.Chart(df[df["flag"] != "Normal"]).mark_point(filled=True, opacity=1, size=50).encode(
        x="event_time:T", y="anomaly_score:Q", color=alt.Color("flag:N").scale(MARKERS).legend(None),
        tooltip=[alt.Tooltip("event_time:T", format="%d %b %Y %H:%M"), alt.Tooltip("anomaly_score:Q", format="+.3f"),
                 alt.Tooltip("temperature:Q", format=".2f"), alt.Tooltip("flag:N", title="model")])
    st.altair_chart(alt.layer(*bands, watch, line, cut, points).properties(height=130),
                    alt="IsolationForest anomaly score under the temperature curve, cut-off at zero, "
                        f"watch band from 0 to {WATCH_SCORE:.3f}, flagged readings in red, sustained alerts shaded")


MODEL_NOTE = (f"Red band = sustained alert: at least {ALERT_NEED} of the last {ALERT_WINDOW} readings flagged. "
              "Red points = flagged readings (anomaly score below 0); small ones stand alone. In a slow decline the "
              "score can sit right at the cut-off, so single flags come and go from one reading to the next; the "
              f"band shows where flags cluster. Score strip: orange = watch, score from 0 to {WATCH_SCORE:.4f} "
              "(as unusual as the lowest 2% of the model's training week). ")


def axis_note(domain):
    if zoom_axis:
        return (f"Axis zoomed to {domain[0]}–{domain[1]} to show the changes; turn zoom off in the sidebar for the "
                f"full {FULL_SCALE[0]}–{FULL_SCALE[1]} scale.")
    return f"Fixed {FULL_SCALE[0]}–{FULL_SCALE[1]} axis."


def history_controls():
    """Scrubber and buttons to scroll back through every stored reading. Returns the chosen end time.

    The scrubber spans the whole stream (fixed bounds from the CSV), shades the
    part already stored and marks every flagged reading in red. It reports its
    position while it is dragged, so the chart below follows the hand.
    """
    bounds = stream_bounds()
    st.session_state.setdefault("history_end", st.session_state.latest_event_time)
    half = CURVE_ROWS // 2
    history_scrubber(st.session_state.history_end, bounds["first"], bounds["last"],
                     load_flag_times()["event_time"].dt.to_pydatetime(), load_stored_range(),
                     key="history_scrub", on_change=scrubbed)
    with st.container(horizontal=True, vertical_alignment="center"):
        st.button("Earlier", icon=":material/chevron_left:", on_click=shift_history, args=(-half,),
                  help=f"Move back {half} readings ({run_report.format_duration(half * 300)} of event time).")
        st.button("Later", icon=":material/chevron_right:", on_click=shift_history, args=(half,),
                  help=f"Move forward {half} readings.")
        st.button("Back to live", icon=":material/sensors:", type="primary", on_click=back_to_live)
    return st.session_state.history_end


def show_curve_card(live):
    """The main chart. Returns the rows it shows, so the tabs below describe the same readings."""
    with st.container(border=True):
        with st.container(horizontal=True, vertical_alignment="center"):
            st.subheader("Temperature", icon=":material/show_chart:")
            mode = st.segmented_control("Curve mode", ["Follow live", "Browse history"], required=True,
                                        key="curve_mode", on_change=enter_browse, label_visibility="collapsed")
        if mode == "Browse history":
            until = history_controls()
            view = load_curve(CURVE_ROWS, until)
            if view.empty:
                st.caption("No readings stored at or before this time yet.")
                return view
            # The tiles above keep updating; this chart only moves when you move the slider.
            x_domain = [vl_time(view["event_time"].min()), vl_time(view["event_time"].max())]
            domain = show_curve(view, pd.DataFrame(), x_domain)
            show_score_strip(view, x_domain)
            st.caption(f"Browsing history: {len(view):,} readings from {view['event_time'].min():%d %b %H:%M} "
                       f"to {view['event_time'].max():%d %b %Y %H:%M} (event time), which arrived between "
                       f"{view['stored_at'].min():%H:%M:%S} and {view['stored_at'].max():%H:%M:%S}. "
                       f"New data keeps arriving: newest reading {live['event_time'].max():%d %b %Y %H:%M}. "
                       + MODEL_NOTE + axis_note(domain))
            return view
        forecast = load_forecast()
        # Both charts end at the last forecast point (or the newest reading), so they line up.
        last = live["event_time"].max() if forecast.empty else max(live["event_time"].max(), forecast["target_time"].max())
        x_domain = [vl_time(live["event_time"].min()), vl_time(last)]
        domain = show_curve(live, forecast, x_domain)
        show_score_strip(live, x_domain)
        if forecast.empty:
            st.caption(f"Newest {len(live):,} readings. No forecast yet: the forecaster needs 288 readings "
                       "(24 h of event time) for its first fit. " + MODEL_NOTE + axis_note(domain))
        else:
            st.caption(f"Newest {len(live):,} readings. Dashed grey: ARIMA(1,1,1) forecast of the next hour, made at "
                       f"event time {forecast['made_at_event_time'].iloc[0]:%d %b %H:%M}; grey band = 95% interval. "
                       "Green band = rolling average ± 2 standard deviations. Hover for details. "
                       + MODEL_NOTE + axis_note(domain))
        return live


# ---------------------------------------------------------------- tab: model view (layer 4)

def show_score_histogram(df):
    bars = alt.Chart(df).mark_bar(opacity=0.85).encode(
        x=alt.X("anomaly_score:Q", title="Anomaly score").bin(maxbins=40),
        y=alt.Y("count():Q", title="Readings").stack(True),
        color=alt.Color("flag:N", title=None).scale(FLAG_COLORS).legend(orient="top"),
        tooltip=["flag:N", "count():Q"])
    cut = alt.Chart(pd.DataFrame({"score": [0.0]})).mark_rule(color=COLORS["flag"], strokeDash=[4, 4]).encode(x="score:Q")
    st.altair_chart(alt.layer(bars, cut).properties(height=260),
                    alt="Histogram of anomaly scores, normal and flagged readings in two colours, cut-off at zero")


def show_feature_scatter(df):
    """Two of the six model features against each other: flagged readings sit at the edges."""
    points = alt.Chart(df).mark_circle(size=40, opacity=0.7).encode(
        x=alt.X("deviation:Q", title="Deviation from 1-hour mean"),
        y=alt.Y("delta:Q", title="Change since previous reading"),
        color=alt.Color("flag:N", title=None).scale(FLAG_COLORS).legend(orient="top"),
        tooltip=[alt.Tooltip("event_time:T", format="%d %b %Y %H:%M"), alt.Tooltip("temperature:Q", format=".2f"),
                 alt.Tooltip("deviation:Q", format="+.2f"), alt.Tooltip("delta:Q", format="+.2f"), "flag:N"])
    st.altair_chart(points.properties(height=260),
                    alt="Scatter of deviation against delta for each reading, flagged readings in red")


def show_feature(df, column, title, mark="line"):
    """One model feature over event time, small."""
    chart = alt.Chart(df).encode(x=alt.X("event_time:T", title=None), y=alt.Y(f"{column}:Q", title=None),
                                 tooltip=[alt.Tooltip("event_time:T", format="%d %b %H:%M"),
                                          alt.Tooltip(f"{column}:Q", format="+.2f")])
    chart = chart.mark_area(color=COLORS["rolling_avg"], opacity=0.6) if mark == "area" else \
        chart.mark_line(color=COLORS["temperature"], strokeWidth=1.2)
    st.markdown(f"**{title}**")
    st.altair_chart(chart.properties(height=140), alt=f"{title} by event time")


def show_model_tab(view):
    st.caption("The IsolationForest scores six features per reading: temperature, deviation from the 1-hour mean, "
               "1-hour standard deviation, change since the previous reading, and two 24-hour features (deviation "
               "from the 24-hour mean and 24-hour standard deviation) that catch slow drifts. The charts below show "
               "the four stored 1-hour features. A score below 0 is flagged; the score over time is drawn under the "
               "temperature curve.")
    left, right = st.columns(2)
    with left:
        show_score_histogram(view)
    with right:
        show_feature_scatter(view)
    cols = st.columns(3)
    with cols[0]:
        show_feature(view, "rolling_std", "Rolling standard deviation (volatility)", mark="area")
    with cols[1]:
        show_feature(view, "deviation", "Deviation from the 1-hour mean")
    with cols[2]:
        show_feature(view, "delta", "Change since previous reading")


# ---------------------------------------------------------------- tab: hourly view (tumbling window)

def show_hourly_band(hourly):
    """Each hour's lowest to highest reading as a band, with the hourly mean as a line."""
    domain = list(y_domain(list(hourly["min_temperature"]) + list(hourly["max_temperature"]), zoom_axis))
    base = alt.Chart(hourly).encode(x=alt.X("hour_start:T", title="Hour of event time"))
    band = base.mark_area(color=COLORS["temperature"], opacity=0.25).encode(
        y=alt.Y("min_temperature:Q", title="Temperature").scale(domain=domain), y2="max_temperature:Q")
    line = base.mark_line(color=COLORS["temperature"], point=True).encode(
        y="avg_temperature:Q",
        tooltip=[alt.Tooltip("hour_start:T", title="hour", format="%d %b %H:00"),
                 alt.Tooltip("avg_temperature:Q", format=".1f", title="mean"),
                 alt.Tooltip("min_temperature:Q", format=".1f", title="min"),
                 alt.Tooltip("max_temperature:Q", format=".1f", title="max"), "reading_count:Q", "anomaly_count:Q"])
    st.altair_chart(alt.layer(band, line).properties(height=240),
                    alt="Minimum to maximum temperature per hour as a band, with the hourly mean as a line")


def show_hourly(hourly):
    """Anomalies per hour of event time: the tumbling window, next to the sliding one on the curve."""
    bars = alt.Chart(hourly).mark_bar(color=COLORS["flag"]).encode(
        x=alt.X("hour_start:T", title="Hour of event time"),
        # At least 0-1 with whole-number ticks, so 48 quiet hours still give a readable axis.
        y=alt.Y("anomaly_count:Q", title="Flagged readings").scale(domain=[0, max(1, int(hourly["anomaly_count"].max()))])
        .axis(format="d", tickMinStep=1),
        tooltip=[alt.Tooltip("hour_start:T", title="hour", format="%d %b %H:00"), "anomaly_count:Q", "reading_count:Q"])
    st.altair_chart(bars.properties(height=180),
                    alt="Number of flagged readings in each hour of the last 48 hours of event time")


def show_heatmap(hours):
    """Every stored hour as one cell: day across, hour of day down, colour = flagged readings."""
    cells = alt.Chart(hours).mark_rect().encode(
        x=alt.X("yearmonthdate(hour_start):O", title="Day (event time)").axis(format="%d %b", labelAngle=-90),
        y=alt.Y("hours(hour_start):O", title="Hour of day"),
        color=alt.Color("anomaly_count:Q", title="Flags").scale(range=["#1E293B", COLORS["flag"]]),
        tooltip=[alt.Tooltip("hour_start:T", title="hour", format="%d %b %Y %H:00"), "anomaly_count:Q",
                 "reading_count:Q"])
    st.altair_chart(cells.properties(height=320),
                    alt="Heatmap of flagged readings per hour, one column per day and one row per hour of the day")


def show_hourly_tab():
    st.caption("The processor also groups readings into fixed 1-hour windows of event time (a tumbling window) "
               "and stores one row per hour in hourly_stats.")
    hourly = load_hourly()
    if hourly.empty:
        st.caption("No hours stored yet.")
        return
    st.markdown("**Temperature range per hour (last 48 hours)**")
    show_hourly_band(hourly)
    st.markdown("**Flagged readings per hour (last 48 hours)**")
    show_hourly(hourly)
    st.markdown("**Flagged readings per hour, whole run so far**")
    show_heatmap(load_heatmap())


# ---------------------------------------------------------------- tab: pipeline (layers 2 to 5)

def show_lag_chart(df):
    """Per-reading lag (producer send to MySQL store) by the wall-clock time it arrived."""
    line = alt.Chart(df).mark_line(color=COLORS["rolling_avg"], point=alt.OverlayMarkDef(size=12)).encode(
        x=alt.X("stored_at:T", title="Arrival time (local)", axis=alt.Axis(format="%H:%M:%S")),
        y=alt.Y("lag_ms:Q", title="Pipeline lag (ms)").scale(domainMin=0),
        tooltip=[alt.Tooltip("stored_at:T", format="%H:%M:%S", title="arrived"),
                 alt.Tooltip("event_time:T", format="%d %b %H:%M", title="event time"),
                 alt.Tooltip("lag_ms:Q", format=".0f", title="lag (ms)")])
    st.altair_chart(line.properties(height=220), alt="Pipeline lag in milliseconds for each reading by arrival time")


def show_forecast_check(check):
    """Forecasts next to what actually happened: the interval band, the forecast and the real reading."""
    errors = (check["predicted_temperature"] - check["actual"]).abs()
    inside = ((check["actual"] >= check["lower"]) & (check["actual"] <= check["upper"])).mean() * 100
    cols = st.columns(3)
    cols[0].metric("Forecast MAE", f"{errors.mean():.2f}", border=True,
                   help="Mean absolute error of the forecast points shown, 5 to 60 minutes ahead.")
    cols[1].metric("Inside 95% interval", f"{inside:.1f}%", border=True,
                   help="Share of actual readings that fell inside the forecast's 95% interval.")
    cols[2].metric("Points compared", f"{len(check):,}", border=True)
    long = check.rename(columns={"predicted_temperature": "forecast"}).melt(
        id_vars=["target_time"], value_vars=["forecast", "actual"], var_name="series", value_name="value")
    domain = list(y_domain(long["value"], zoom_axis))
    base = alt.Chart(check).encode(x=alt.X("target_time:T", title="Event time"))
    band = base.mark_area(color=COLORS["forecast"], opacity=0.2, clip=True).encode(
        y=alt.Y("lower:Q", title="Temperature").scale(domain=domain), y2="upper:Q")
    lines = alt.Chart(long).mark_line(clip=True).encode(
        x="target_time:T", y=alt.Y("value:Q").scale(domain=domain),
        color=alt.Color("series:N", title=None).scale(
            domain=["actual", "forecast"], range=[COLORS["temperature"], COLORS["forecast"]]
        ).legend(orient="top"),
        strokeDash=alt.StrokeDash("series:N").scale(domain=["actual", "forecast"],
                                                    range=[[1, 0], [6, 4]]).legend(None),
        tooltip=[alt.Tooltip("target_time:T", format="%d %b %H:%M"), "series:N", alt.Tooltip("value:Q", format=".2f")])
    st.altair_chart(alt.layer(band, lines).properties(height=240),
                    alt="ARIMA forecasts against the readings that arrived, with the 95% interval as a band")


def show_timeline(timeline):
    """What the run produced in each wall-clock minute: a bar per minute and a scrollable table."""
    # One labelled bar per minute (an ordinal axis), oldest on the left.
    per_minute = timeline.assign(minute=timeline["arrived_minute"].dt.strftime("%H:%M")).sort_values("arrived_minute")
    x = alt.X("minute:O", title="Arrival minute (local)", sort=None).axis(labelAngle=-45)
    bars = alt.Chart(per_minute).mark_bar(color=COLORS["temperature"]).encode(
        x=x, y=alt.Y("readings:Q", title="Readings stored"),
        tooltip=[alt.Tooltip("minute:O", title="minute"), "readings:Q", "flags:Q"])
    flags = alt.Chart(per_minute).mark_bar(color=COLORS["flag"]).encode(x=x, y="flags:Q")
    st.altair_chart(alt.layer(bars, flags).properties(height=180),
                    alt="Readings stored per wall-clock minute, flagged readings in red")
    st.dataframe(timeline, hide_index=True, height=320, alt="Readings stored per wall-clock minute of the run",
                 column_config={
                     "arrived_minute": st.column_config.DatetimeColumn("Arrived (minute)", format="YYYY-MM-DD HH:mm"),
                     "readings": st.column_config.NumberColumn("Readings"),
                     "event_from": st.column_config.DatetimeColumn("Event time from", format="D MMM YYYY HH:mm"),
                     "event_to": st.column_config.DatetimeColumn("Event time to", format="D MMM YYYY HH:mm"),
                     "flags": st.column_config.NumberColumn("Flags"),
                     "min_temperature": st.column_config.NumberColumn("Min temp", format="%.1f"),
                     "max_temperature": st.column_config.NumberColumn("Max temp", format="%.1f"),
                 })


def show_pipeline_tab(view):
    st.markdown("**Pipeline lag per reading** (producer send → MySQL store)")
    lag = view["lag_ms"]
    st.caption(f"Readings on the curve: average {lag.mean():.1f} ms, 95th percentile {lag.quantile(0.95):.0f} ms, "
               f"maximum {lag.max():.0f} ms.")
    show_lag_chart(view)
    st.markdown("**Forecast accuracy** (second consumer group, ARIMA)")
    check = load_forecast_check()
    if check.empty:
        st.caption("No forecast has reached its target time yet.")
    else:
        show_forecast_check(check)
    st.markdown(f"**Run timeline** (readings that arrived per wall-clock minute, newest first, "
                f"last {TIMELINE_MINUTES} minutes)")
    show_timeline(load_timeline())


# ---------------------------------------------------------------- tab: readings log

def show_readings_log(view):
    st.caption(f"The {len(view):,} readings on the curve above, newest first. Scroll the table, or use "
               "Browse history on the curve to load an earlier stretch.")
    log = view.sort_values("event_time", ascending=False)[
        ["seq", "event_time", "stored_at", "temperature", "rolling_avg", "rolling_std", "deviation", "delta",
         "anomaly_score", "is_anomaly", "lag_ms"]].copy()
    log["is_anomaly"] = log["is_anomaly"].astype(bool)
    st.dataframe(log, hide_index=True, height=420, alt="Stored readings with their features and model output",
                 column_config={
                     "seq": st.column_config.NumberColumn("Row", format="%d"),
                     "event_time": st.column_config.DatetimeColumn("Event time", format="D MMM YYYY HH:mm"),
                     "stored_at": st.column_config.DatetimeColumn("Arrived", format="HH:mm:ss"),
                     "temperature": st.column_config.NumberColumn("Temperature", format="%.2f"),
                     "rolling_avg": st.column_config.NumberColumn("Rolling avg", format="%.2f"),
                     "rolling_std": st.column_config.NumberColumn("Rolling std", format="%.2f"),
                     "deviation": st.column_config.NumberColumn("Deviation", format="%+.2f"),
                     "delta": st.column_config.NumberColumn("Delta", format="%+.2f"),
                     "anomaly_score": st.column_config.NumberColumn("Score", format="%+.3f"),
                     "is_anomaly": st.column_config.CheckboxColumn("Flagged"),
                     "lag_ms": st.column_config.NumberColumn("Lag (ms)", format="%.0f"),
                 })


# ---------------------------------------------------------------- run report

def show_run_report(m, complete, key):
    """The whole run in numbers: how long it took and what it found."""
    readings, forecasts, n_forecasts = load_run_data((int(m["events_processed"]), str(m["updated_at"])))
    if readings.empty:
        st.caption("No readings stored yet.")
        return
    readings["produced_at"] = to_local(readings["produced_at"])
    readings["stored_at"] = to_local(readings["stored_at"])
    windows = load_windows()
    s = run_report.run_summary(readings, list(zip(windows["start"], windows["end"])), forecasts,
                               late_dropped=int(m["late_events_dropped"]), n_forecasts=n_forecasts)
    duration = run_report.format_duration

    st.caption(f"{'Final report' if complete else 'Run so far'}: replay started {s['started']:%d %b %Y %H:%M:%S}, "
               f"last reading stored {s['finished']:%H:%M:%S} (local time).")
    tile = {"border": True, "height": "stretch"}
    row = st.columns(5)
    row[0].metric("Total time", duration(s["total_s"]), **tile, help="First reading sent to last reading stored.")
    row[1].metric("Active time", duration(s["active_s"]), **tile,
                  help=f"Total time minus pauses: gaps over {run_report.PAUSE_AFTER_S} s between two stored "
                       f"readings ({s['pauses']} in this run).")
    row[2].metric("Machine time replayed", duration(s["event_span_s"]), **tile,
                  help=f"Event time {s['first_event']:%d %b %Y %H:%M} to {s['last_event']:%d %b %Y %H:%M}.")
    row[3].metric("Replay speed (× real time)", f"{s['speedup']:,.0f}×" if s["speedup"] else "—", **tile,
                  help="Event time covered divided by active time: how many times faster than the machine "
                       "produced the readings.")
    row[4].metric("Throughput", f"{s['throughput']:,.1f} /s" if s["throughput"] else "—", **tile,
                  help="Readings stored per second of active time.")
    row = st.columns(5)
    row[0].metric("Readings stored", f"{s['readings']:,}", **tile,
                  help=f"{s['late_dropped']} late events were dropped (repeated timestamps).")
    row[1].metric("Readings flagged", f"{s['flags']:,} ({s['flag_pct']:.2f}%)", **tile)
    row[2].metric("Failure windows caught", f"{s['windows_hit']} of {s['windows_reached']}", **tile,
                  help=f"Flags inside labelled windows: {s['inside']}; outside: {s['outside']} "
                       "(unlabelled, not proven false).")
    row[3].metric("Peak / minimum", f"{s['peak']:.1f} / {s['minimum']:.1f}", **tile,
                  help=f"Peak at {s['peak_time']:%d %b %Y %H:%M}, minimum at {s['min_time']:%d %b %Y %H:%M} "
                       "(event time).")
    row[4].metric("Pipeline lag avg / max", f"{s['lag_avg_ms']:.0f} / {s['lag_max_ms']:.0f} ms", **tile,
                  help=f"95th percentile {s['lag_p95_ms']:.0f} ms.")
    if s["forecast_mae"] is not None:
        st.caption(f"Forecaster: {s['n_forecasts']:,} ARIMA forecasts; {s['forecast_points']:,} forecast points "
                   f"checked against the reading that arrived: MAE {s['forecast_mae']:.2f}, "
                   f"{s['forecast_coverage_pct']:.1f}% inside the 95% interval.")
    if s["window_rows"]:
        table = pd.DataFrame(s["window_rows"])
        table["delay_h"] = table["delay_min"] / 60
        st.dataframe(table[["window", "start", "end", "flags_inside", "first_flag", "delay_h"]], hide_index=True,
                     alt="Labelled failure windows with their flags and detection delay",
                     column_config={
                         "window": st.column_config.NumberColumn("Window"),
                         "start": st.column_config.DatetimeColumn("Opens", format="D MMM YYYY HH:mm"),
                         "end": st.column_config.DatetimeColumn("Closes", format="D MMM YYYY HH:mm"),
                         "flags_inside": st.column_config.NumberColumn("Flags inside"),
                         "first_flag": st.column_config.DatetimeColumn("First flag", format="D MMM YYYY HH:mm"),
                         "delay_h": st.column_config.NumberColumn("Delay (hours)", format="%.1f"),
                     })
    st.download_button("Download report (.md)", run_report.summary_markdown(s, complete),
                       file_name="run_report.md", mime="text/markdown", icon=":material/download:",
                       on_click="ignore", key=f"download_{key}")


def show_final_report(m):
    if not st.session_state.celebrated:
        st.balloons()
        st.session_state.celebrated = True
    with st.container(border=True):
        st.subheader("Final run report", icon=":material/summarize:")
        show_run_report(m, complete=True, key="final")


# ---------------------------------------------------------------- the live part

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
    live = load_curve(CURVE_ROWS)
    if metrics.empty or live.empty:
        st.info("Waiting for data. Start the processor, then the producer.", icon=":material/hourglass_empty:")
        return
    m = metrics.iloc[0]
    newest = live.iloc[-1]
    bounds = stream_bounds()
    # Complete = the CSV's last row has been stored (a --limit run never completes).
    complete = int(newest["seq"]) == bounds["last_seq"]
    st.session_state.latest_event_time = newest["event_time"].to_pydatetime()

    show_header(m, newest, bounds, complete)
    show_kpis(m, live)
    if complete:
        show_final_report(m)
    else:
        st.session_state.celebrated = False   # a new run after a reset celebrates again

    view = show_curve_card(live)

    # Lazy tabs: only the open tab runs its queries. The open tab is kept in the URL (?tab=...).
    tabs = st.tabs(TABS, key="tab", on_change="rerun", bind="query-params")
    model_tab, hourly_tab, pipeline_tab, log_tab, report_tab = tabs
    with model_tab:
        if model_tab.open and not view.empty:
            show_model_tab(view)
    with hourly_tab:
        if hourly_tab.open:
            show_hourly_tab()
    with pipeline_tab:
        if pipeline_tab.open and not view.empty:
            show_pipeline_tab(view)
    with log_tab:
        if log_tab.open and not view.empty:
            show_readings_log(view)
    with report_tab:
        if report_tab.open:
            if complete:
                st.caption("The replay is complete: the final report is shown at the top of the page.")
            else:
                show_run_report(m, complete=False, key="tab")

    with st.container(border=True):
        st.subheader(f"Last {RECENT_FLAGS} flagged readings", icon=":material/report:")
        flags = load_recent_flags()
        if flags.empty:
            st.caption("No anomalies flagged yet.")
        else:
            st.dataframe(flags, hide_index=True, alt="The most recent readings flagged as anomalous",
                         column_config={
                             "event_time": st.column_config.DatetimeColumn("Event time", format="D MMM YYYY HH:mm"),
                             "temperature": st.column_config.NumberColumn("Temperature", format="%.2f"),
                             "rolling_avg": st.column_config.NumberColumn("Rolling avg", format="%.2f"),
                             "anomaly_score": st.column_config.NumberColumn("Score", format="%+.3f"),
                         })

    # The page's own clock, so a frozen page and frozen data look different.
    st.caption(f"Page refreshed at {datetime.now().strftime('%H:%M:%S')}"
               + (" (refresh paused)" if paused else f", every {refresh_s} s"))


live_view()
