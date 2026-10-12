# Dashboard improvement ideas: what else can we chart?

## Why the page looks plain

Today the dashboard (`dashboard/app.py`) shows:

- 6 KPI tiles (latest temperature, rolling average, peak, events, anomalies, pipeline lag)
- one main curve: temperature + rolling average + red flags + failure-window shading + ARIMA forecast
- one bar chart: flagged readings per hour
- one table: last 10 flagged readings

It is correct and live, but it only tells one story: "temperature over time". The pipeline stores much more
than the page shows. The processor already writes `rolling_std`, `deviation`, `delta` and `anomaly_score` for
every reading, `hourly_stats` holds min / avg / max per hour, and `forecasts` holds past predictions. None of
that is drawn yet. The ideas below use **only data that already exists**, so no new table, column, library or
processor change is needed (CLAUDE.md: "Do not change the event contract, table columns ... without asking").

## Ground rules for every new chart

- Query through `run_query()` (no cache), inside the `live_view()` fragment, so it refreshes with the page.
- Bound every query: `ORDER BY id DESC LIMIT :n` or the last 48 hours. Never read all 20,683 rows every 2 s.
- Altair only, one small `show_*` function per chart, so each can be explained line by line in the viva.
- Fixed axes where it makes sense (like `Y_DOMAIN`), so charts do not jump between refreshes.
- Add `alt=` text to each chart (the existing charts do).

## Ideas, ranked by value for effort

| # | Chart | Data (already stored) | Type | What it shows / why it matters | Effort |
| --- | --- | --- | --- | --- | --- |
| 1 | **Anomaly score over time** | `readings.anomaly_score`, `is_anomaly` | line + rule at 0, flagged points red | Shows *why* a reading is flagged: the model's score drops below 0. Makes the ML layer visible instead of only red dots. | Easy |
| 2 | **Volatility band** | `rolling_avg`, `rolling_std` | band `avg ± 2·std` behind the main curve, or a separate `rolling_std` line | One of the 4 model features. Anomalies sit where the band suddenly widens. | Easy |
| 3 | **Deviation and delta panel** | `deviation`, `delta` | two small bar/area charts | The other two model features. Supports the report finding that the features "describe sudden change". | Easy |
| 4 | **Hourly min / avg / max band** | `hourly_stats` | area band (min to max) + avg line | Uses the tumbling window already stored; shows the normal operating range per hour and when it breaks. | Easy |
| 5 | **Anomaly score histogram** | `anomaly_score`, `is_anomaly` | histogram, flagged vs normal colours, line at 0 | Shows how `contamination=0.01` sets the cut-off. Good for discussing the trade-off the guide asks about. | Easy |
| 6 | **Pipeline lag chart** | `stored_at - produced_at` (ms) | line, newest N readings | Proves the "real-time" claim with a number per reading. Strong viva point. | Easy |
| 7 | **Temperature distribution** | `temperature`, `is_anomaly` | histogram, two colours | Normal vs flagged readings; shows anomalies live in the tails (and the collapse near 2). | Easy |
| 8 | **Feature scatter** | `temperature` vs `deviation` (or `rolling_std` vs `delta`) | scatter, flagged red, newest N points | Shows the 2-D region the IsolationForest isolates. The most "ML-looking" chart. | Medium |
| 9 | **Anomaly heatmap** | `hourly_stats.anomaly_count` | day (rows) x hour (columns) heatmap | One look shows which days had failures across the whole replay (16 Dec, 27-29 Jan, 7-9 Feb). | Medium |
| 10 | **Forecast accuracy** | `forecasts` joined to `readings` on time | forecast vs actual line, error bars | Backs the 1-hour MAE 1.50 and 91.2% interval coverage in `report/results.md` with a live picture. | Medium |
| 11 | **More KPI tiles** | `metrics_current`: `min_temperature`, `late_events_dropped`, and flagged / processed | `st.metric` | Minimum temperature, late events dropped (the 12 repeated timestamps), anomaly rate %. Cheap and adds detail. | Trivial |
| 12 | **State badge** | latest `is_anomaly`, `anomaly_score` | coloured badge: Normal / Watch / Anomaly | A big at-a-glance machine status, which suits a "predictive maintenance monitor". | Trivial |

## Suggested layout

Keep the page short by using tabs inside the fragment:

1. Status line + **9 KPI tiles** (add min temperature, late events, anomaly rate) and the state badge.
2. **Main curve** (as now, optionally with the volatility band).
3. Tabs:
   - **Model view**: anomaly score over time, deviation / delta panel, score histogram, feature scatter.
   - **Hourly view**: min/avg/max band, anomalies-per-hour bars, anomaly heatmap.
   - **Pipeline**: lag chart, forecast accuracy.
4. Last 10 flagged readings (table, as now).

## Minimum set worth doing

If time is short, do these five (about five small functions plus two tiles). They make the dashboard clearly
richer and each one ties to a layer you must explain:

1. Anomaly score over time (layer 4, ML)
2. Volatility band / `rolling_std` (layer 3, windowing)
3. Hourly min / avg / max band (layer 3, tumbling window)
4. Pipeline lag chart (layers 2 to 5, "real time")
5. Extra KPI tiles (min temperature, late events dropped, anomaly rate)

## What not to add

- Anything that needs a new table, new column or processor change (ask first, see CLAUDE.md).
- Queries over the whole `readings` table on every refresh.
- `conn.query()`: it caches and leaks pooled connections (see Phase 4/6 decisions). Use `run_query()`.
- New libraries (Plotly, Grafana, etc.). Altair covers everything above.

## Implementation notes

- Charts 1, 2, 3, 6, 7, 8 can read from one extra query:
  `SELECT event_time, temperature, rolling_avg, rolling_std, deviation, delta, anomaly_score, is_anomaly,
  TIMESTAMPDIFF(MICROSECOND, produced_at, stored_at) / 1000 AS lag_ms FROM readings ORDER BY id DESC LIMIT :n`.
  Change `load_curve()` to select these columns instead of adding six queries.
- Charts 4 and 9 read `hourly_stats` (extend `load_hourly()` with `min_temperature`, `max_temperature`,
  `avg_temperature`).
- Chart 10 is a join between `forecasts` and `readings` on `target_time = event_time`, limited to the newest
  few hundred targets.
- Keep the 2 s refresh in mind: each added query runs every refresh. About 8 queries total is fine.
- Update the dashboard section of `CLAUDE.md` and the screenshots list in `report/` afterwards.

## How to verify when implemented

1. Run the dashboard against a live replay and watch each new chart update with no clicks.
2. AppTest from the repo root against real MySQL (as in Phase 4): no exceptions in waiting, stale and live states.
3. For each chart, compare its data with a direct SQL recompute of the same rows.
4. `pytest` still passes (29 tests).
5. Pause the producer: charts freeze while the page clock keeps moving.
6. Take new screenshots for `report/screenshots/` and mention the added views in the report's Dashboard section.
