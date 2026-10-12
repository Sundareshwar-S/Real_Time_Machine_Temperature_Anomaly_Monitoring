"""The run report: what one replay did, from the rows it left in MySQL.

Plain pandas, no Streamlit, so the numbers can be unit-tested
(tests/test_run_report.py). The dashboard shows the result while the replay runs
("so far") and as the final report once the last stream row is stored.
The window scoring is the same function scripts/evaluate.py and
scripts/collect_results.py use, so all three agree.
"""
from scripts.evaluate import score_against_windows

PAUSE_AFTER_S = 10   # a gap longer than this between two stored readings is a pause (same as the stale warning)


def format_duration(seconds):
    """45 -> "45 s", 125 -> "2 min 05 s", 3725 -> "1 h 02 min", 71.8 days -> "71 d 19 h"."""
    seconds = int(round(seconds))
    if seconds < 60:
        return f"{seconds} s"
    if seconds < 3600:
        return f"{seconds // 60} min {seconds % 60:02d} s"
    if seconds < 86400:
        return f"{seconds // 3600} h {seconds % 3600 // 60:02d} min"
    return f"{seconds // 86400} d {seconds % 86400 // 3600} h"


def active_time(stored_at, pause_after_s=PAUSE_AFTER_S):
    """Seconds the pipeline was busy, and how many pauses it had.

    Stopping the producer with Ctrl+C and continuing with --resume leaves a gap
    between two stored readings. Gaps longer than pause_after_s are left out.
    """
    gaps = stored_at.sort_values().diff().dt.total_seconds().dropna()
    pauses = gaps[gaps > pause_after_s]
    total = gaps.sum()
    return total - pauses.sum(), len(pauses)


def run_summary(readings, windows, forecasts, late_dropped, n_forecasts, pause_after_s=PAUSE_AFTER_S):
    """Everything the report shows, as one dict.

    readings:  event_time, temperature, is_anomaly, produced_at, stored_at (one row per stored reading)
    windows:   [(start, end), ...] labelled failure windows (scripts/evaluate.load_windows)
    forecasts: predicted_temperature, lower, upper, actual (forecast points whose target has a reading)
    """
    started, finished = readings["produced_at"].min(), readings["stored_at"].max()
    total_s = (finished - started).total_seconds()
    # The time before the first store (one reading's lag) counts as busy time too.
    busy_s, pauses = active_time(readings["stored_at"], pause_after_s)
    active_s = busy_s + (readings["stored_at"].min() - started).total_seconds()

    first_event, last_event = readings["event_time"].min(), readings["event_time"].max()
    event_span_s = (last_event - first_event).total_seconds()

    # Only the windows the stream has reached can have been caught so far.
    reached = [(start, end) for start, end in windows if start <= last_event]
    score = score_against_windows(list(readings["event_time"]), list(readings["is_anomaly"]), reached)
    flagged = readings[readings["is_anomaly"] == 1]
    window_rows = []
    for i, ((start, end), delay) in enumerate(zip(reached, score["delays_min"]), start=1):
        inside = flagged[(flagged["event_time"] >= start) & (flagged["event_time"] <= end)]
        window_rows.append({"window": i, "start": start, "end": end, "flags_inside": len(inside),
                            "first_flag": inside["event_time"].min() if len(inside) else None,
                            "delay_min": delay})

    peak = readings.loc[readings["temperature"].idxmax()]
    low = readings.loc[readings["temperature"].idxmin()]
    lag_ms = (readings["stored_at"] - readings["produced_at"]).dt.total_seconds() * 1000

    mae = coverage = None
    if len(forecasts):
        mae = (forecasts["predicted_temperature"] - forecasts["actual"]).abs().mean()
        coverage = 100 * ((forecasts["actual"] >= forecasts["lower"]) & (forecasts["actual"] <= forecasts["upper"])).mean()

    return {
        "started": started, "finished": finished, "total_s": total_s, "active_s": active_s, "pauses": pauses,
        "readings": len(readings), "late_dropped": late_dropped,
        "first_event": first_event, "last_event": last_event, "event_span_s": event_span_s,
        "speedup": event_span_s / active_s if active_s > 0 else None,
        "throughput": len(readings) / active_s if active_s > 0 else None,
        "flags": int(score["flags"]), "flag_pct": score["flagged_pct"],
        "windows_hit": score["windows_hit"], "windows_reached": len(reached),
        "inside": score["inside"], "outside": score["outside"], "window_rows": window_rows,
        "peak": peak["temperature"], "peak_time": peak["event_time"],
        "minimum": low["temperature"], "min_time": low["event_time"],
        "lag_avg_ms": lag_ms.mean(), "lag_p95_ms": lag_ms.quantile(0.95), "lag_max_ms": lag_ms.max(),
        "n_forecasts": n_forecasts, "forecast_points": len(forecasts),
        "forecast_mae": mae, "forecast_coverage_pct": coverage,
    }


def summary_markdown(s, complete):
    """The report as a Markdown file for the download button."""
    title = "final run report" if complete else "run report so far"
    fmt = "%Y-%m-%d %H:%M"
    lines = [
        f"# Machine temperature replay: {title}",
        "",
        "Pipeline: CSV replay -> Kafka -> Python processor -> IsolationForest -> MySQL -> Streamlit.",
        "Wall-clock times are the local time of the computer running the dashboard.",
        "",
        "| Measure | Value |",
        "| --- | --- |",
        f"| Replay started | {s['started']:%Y-%m-%d %H:%M:%S} |",
        f"| Last reading stored | {s['finished']:%Y-%m-%d %H:%M:%S} |",
        f"| Total time | {format_duration(s['total_s'])} |",
        f"| Active time (pauses left out) | {format_duration(s['active_s'])} ({s['pauses']} pauses) |",
        f"| Event time covered | {s['first_event']:{fmt}} to {s['last_event']:{fmt}} "
        f"({format_duration(s['event_span_s'])}) |",
        f"| Replay speed | {s['speedup']:,.0f}x real time, {s['throughput']:,.1f} readings/s |"
        if s["speedup"] else "| Replay speed | - |",
        f"| Readings stored | {s['readings']:,} |",
        f"| Late events dropped | {s['late_dropped']:,} |",
        f"| Readings flagged | {s['flags']:,} ({s['flag_pct']:.2f}%) |",
        f"| Labelled failure windows caught | {s['windows_hit']} of {s['windows_reached']} reached |",
        f"| Flags inside / outside windows | {s['inside']} / {s['outside']} |",
        f"| Peak temperature | {s['peak']:.2f} at {s['peak_time']:{fmt}} |",
        f"| Minimum temperature | {s['minimum']:.2f} at {s['min_time']:{fmt}} |",
        f"| Pipeline lag avg / p95 / max | {s['lag_avg_ms']:.1f} / {s['lag_p95_ms']:.0f} / {s['lag_max_ms']:.0f} ms |",
        f"| ARIMA forecasts made | {s['n_forecasts']:,} |",
    ]
    if s["forecast_mae"] is not None:
        lines.append(f"| Forecast MAE / inside 95% interval | {s['forecast_mae']:.2f} / {s['forecast_coverage_pct']:.1f}% |")
    if s["window_rows"]:
        lines += ["", "| Window | Labelled period | Flags inside | First flag | Delay (min) |",
                  "| --- | --- | --- | --- | --- |"]
        for w in s["window_rows"]:
            first = f"{w['first_flag']:{fmt}}" if w["first_flag"] is not None else "none"
            delay = w["delay_min"] if w["delay_min"] is not None else "-"
            lines.append(f"| {w['window']} | {w['start']:{fmt}} to {w['end']:{fmt}} | {w['flags_inside']} | {first} | {delay} |")
    lines += ["", "A flag outside a labelled window is unlabelled, not proven false.", ""]
    return "\n".join(lines)
