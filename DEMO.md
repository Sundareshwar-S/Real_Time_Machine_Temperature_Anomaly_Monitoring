# Live demo runbook

A step-by-step script for presenting the pipeline live (Ubuntu, bash). Every command was run in a timed rehearsal on
2026-10-11; the times and outputs below are the real ones from that run (see "Rehearsal record" at the end).

The story in one sentence: a replayed sensor stream flows through Kafka, a Python processor scores every reading with an
IsolationForest, MySQL serves the results, and a dashboard that re-queries every 2 s shows the machine's failure on
16 Dec 2013 as it happens; stopping the producer freezes the numbers while the page keeps refreshing.

## 0. The day before
- Both Docker images are on the machine (`docker images | grep -E "apache/kafka|mysql"` shows `apache/kafka:4.3.1` and
  `mysql:8.4`), so the demo needs no internet.
- Record the screen during one full rehearsal (steps 2 to 5). That recording is the fallback.

## 1. Before presenting (about 2 minutes)
Open **four terminals**, each in the project folder with the virtual environment active:
```bash
cd ~/Desktop/Real-Time-Machine-Temperature-Anomaly-Monitoring && source .venv/bin/activate
```
Arrange them: T1 processor, T2 forecaster, T3 producer, T4 proof commands. Put the browser next to them.

In T4:
```bash
docker compose up -d --wait            # both containers "Healthy" (about 15 s from stopped, under 1 s if already up)
docker compose ps                      # broker and mysql: Up ... (healthy)
python scripts/reset_demo.py           # ends with "Reset complete." (or the dashboard's "Clear data and start again" button)
ls models/anomaly_model.joblib         # if missing: python -m src.train_model
python scripts/make_secrets.py         # "Wrote .streamlit/secrets.toml ..."
streamlit run dashboard/app.py --server.address localhost
```
Streamlit prints `URL: http://localhost:8501`. Open it. Expected: the title, the sidebar, and **"Waiting for data. Start the processor, then the
producer."** (Leave Streamlit running in T4's tab, or start it in a fifth tab and keep T4 free for proof commands.)

## 2. Start the pipeline
| Terminal | Command | You should see |
| --- | --- | --- |
| T1 | `python -m src.processor` | `Cold start: no stored readings yet.` then `Consuming machine-temperature as group 'scoring'.` |
| T2 | `python -m src.forecaster` | `Consuming machine-temperature as group 'forecast'. First forecast after 288 readings.` |
| T3 | `python -m src.producer --start-at "2013-12-10 00:00" --rate 10` | `Sending to topic machine-temperature from seq 2049 (2013-12-10 00:00:00) at 10.0 events/s` |

Start the clock when you press Enter in T3. One reading is 5 minutes of the machine's time; at 10 readings per second,
one real second is 50 minutes of event time.

### Timeline (seconds after starting the producer, from the data, confirmed in the rehearsal)
| Time | On the dashboard | What to say |
| --- | --- | --- |
| 0-5 s | "Live, last update 0 s ago"; the curve starts drawing; tiles tick over | Data moves producer -> Kafka -> processor -> MySQL; the page re-queries MySQL every 2 s. Nothing is clicked. |
| about 8 s | The orange band of labelled failure window 1 appears (10 Dec 06:25) | NAB's labels: where real failures happened. The model never sees them. |
| **about 27 s** | **First red points** (event time 10 Dec 22:25, inside window 1); "1 of 1" windows flagged | The IsolationForest scores every reading live on 6 features: temperature, deviation from the 1-hour mean, 1-hour std, change since the last reading, and two 24-hour features (deviation from the 24-hour mean, 24-hour std) for slow drifts. |
| **about 29 s** | **First grey dashed forecast** ahead of the curve, with its band | A second consumer group ("forecast") reads the same topic independently and refits ARIMA every hour of event time. |
| ongoing | "Anomalies per hour" bars | The hourly bars are a tumbling window (fixed hours); the rolling average is a sliding window. |
| **about 2 min 48 s** | The temperature starts to fall (15 Dec 20:15, from 95.7) and is **flagged at once** | Window 2: a hard-to-detect change before a failure. Watch the forecast: ARIMA follows trends, it cannot predict a failure. |
| **about 3 min 14 s** | **Minimum 2.1** (16 Dec 17:25), a cluster of red points; "2 of 2" windows flagged | The collapse. The KPI tiles show the running minimum and the anomaly count. |

## 3. The pause (the guide's teaching moment)
1. Press **Ctrl+C in T3** (the producer). It prints `Stopping (Ctrl+C)...` and `Done: sent N events, last seq S.
   Continue with: python -m src.producer --resume`.
2. Point at the dashboard: about 12 s later the status turns into **"No new data for N s. The dashboard is still
   refreshing."** N keeps counting up and the footer "Page refreshed at HH:MM:SS" keeps changing: the page is still
   re-querying, only the data stopped. **Live depends on data moving, not on the refresh timer.**
3. Restart in T3: `python -m src.producer --resume --rate 10`. The status turns back to "Live" and the curve continues
   from where it stopped (no gap, no duplicates).

## 4. Proof points (T4)
Show that every layer is real, not a picture:
```bash
# Kafka holds the events (5 messages, then exit)
docker exec broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic machine-temperature --from-beginning --max-messages 5 --formatter-property print.key=true

# Two consumer groups on one topic, each with its own offsets; LAG near 0 = keeping up
docker exec broker /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group scoring --group forecast

# MySQL keeps filling: run twice a few seconds apart, the count climbs
docker exec mysql sh -c 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "SELECT COUNT(*) AS rows_stored, SUM(is_anomaly) AS flagged FROM readings"'
```
What you see (from the rehearsal):
```text
machine-1	{"sensor_id": "machine-1", "seq": 2049, "event_time": "2013-12-10 00:00:00", "temperature": 80.14151889, "produced_at": "2026-10-11T07:45:42.608Z"}
...
Processed a total of 5 messages

GROUP    TOPIC               PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
forecast machine-temperature 0          2389            2389            0
scoring  machine-temperature 0          2388            2389            1

rows_stored  flagged        (3 s later)   rows_stored  flagged
2231         60                           2262         60
```
The console consumer also prints one line about "KIP-848" (Kafka information, ignore it), and the MySQL line warns
"Using a password on the command line" (harmless). Use the two `--group` options rather than `--all-groups`: each
console consumer leaves a throwaway `console-consumer-NNNNN` group that `--all-groups` would also list.

Optional, to show the groups are independent: Ctrl+C in T2 (forecaster), run the consumer-groups command twice: the
`forecast` LAG grows while `scoring` stays near 0 and the dashboard keeps updating. Restart T2; it catches up.

## 5. Stop
Ctrl+C in T3 (producer), T2 (forecaster), T1 (processor) and the Streamlit tab. Each stops cleanly with a summary line.
Leave Docker running, or `docker compose down` (keeps the data).

## 6. If something goes wrong
| Problem | What to do |
| --- | --- |
| `docker compose up` fails: "port is already allocated" | Another Kafka/MySQL holds 9092 or 3307: stop it, then `docker compose up -d --wait`. |
| A container is not healthy | `docker compose down`, then `docker compose up -d --wait`, then `python scripts/reset_demo.py`. |
| Processor/forecaster prints "Waiting for Kafka/MySQL" | It retries for 60 s; start Docker meanwhile. After 60 s it stops with one sentence: start it again. |
| "Topic machine-temperature does not exist" | `python scripts/reset_demo.py` (recreates it). |
| "Model file ... not found" | `python -m src.train_model` (about 1 s). |
| Dashboard: "Database settings not found" | `python scripts/make_secrets.py`, reload the page. |
| Dashboard: "Cannot reach MySQL" | Start MySQL; the page recovers by itself. |
| Dashboard looks frozen | Is the producer running (T3)? Is the processor's batch log moving (T1)? |
| **Nothing recovers within 1 minute** | **Switch to the screen recording** and talk over it. |

## 7. Questions to be ready for
- **What does each layer do, and why does it exist?** Source (replay, repeatable), Kafka (decouples producer and
  consumers; buffers; ordered per partition), processor (state: sliding window, peak, counters), ML (scores each event),
  MySQL (serving layer: the processor writes, the dashboard reads, neither waits for the other), Streamlit (re-queries on
  a timer).
- **Why Kafka in the middle?** The producer knows nothing about MySQL; two consumer groups read the same events
  independently; a stopped consumer catches up from its committed offset.
- **Why train on the first 2,000 rows only?** So the model never scores data it was trained on (no leakage); they are
  the week before the first labelled failure.
- **Why features, not raw temperature?** The training week only spans 52.7-94.4, but the machine later runs
  normally up to 108.5: a raw-value model flags 36.5% of the stream. The features describe behaviour (sudden change,
  instability, distance from the day's average), which stays comparable as the level drifts (see report/evaluation.csv).
- **Why six features, not four?** With only the four 1-hour features, the slow decline in window 4 (the catastrophic
  failure) was first flagged about 45 hours after the window opened. Adding the deviation from the 24-hour mean and
  the 24-hour std gives more flags inside the windows (161 instead of 83; 123 vs 82 as sustained alerts) and catches
  the window-2 decline after 155 instead of 1,320 minutes. Honest caveats: they were chosen after seeing the labels,
  and in window 4 they only produce single flags near the cut-off from the start; as a sustained alert (3 of the last
  6 flagged, the red band on the dashboard) window 4 is still about 45 hours late (report/evaluation.csv).
- **Why are some red points small and others in a red band?** A small point is a single flagged reading; a band means
  at least 3 of the last 6 readings were flagged. In a slow decline the score hovers at the cut-off, so single flags
  come and go; monitoring tools wait for several flags close together (debounce) before raising an alert.
- **What is contamination?** The share of training data treated as anomalous; it sets the threshold. 0.01 here.
  Higher catches failures earlier but adds flags outside the labelled windows (0.02: 699 outside vs 265).
- **Why no ZooKeeper?** Kafka 4 runs in KRaft mode: the broker manages its own metadata, one container is the cluster.
- **Why does pausing the producer freeze the numbers but not the refresh?** The fragment keeps re-querying on its
  timer, but no new rows arrive in MySQL.

## Rehearsal record
**Re-timed on 2026-10-15 with the 6-feature model** (`--start-at "2013-12-10 00:00" --rate 10`, measured from the
stored rows): first flag 26.9 s (event time 10 Dec 22:25, 11 flags on 10 Dec); first forecast 28.7 s; decline
starts 168.3 s (15 Dec 20:15, 95.7), and that very reading is the first flag in window 2; minimum 2.08 at
193.7 s (16 Dec 17:25) with 66 flags so far. The table below is the original 2026-10-11 rehearsal with the
4-feature model; its pause/resume steps do not depend on the model.

Timed run of steps 1 to 5 on 2026-10-11 (Ubuntu, `--start-at "2013-12-10 00:00" --rate 10`). Times in seconds after
the producer started; the moments were measured by polling MySQL every 0.2 s.

| Step | Expected (calculated from the data) | Measured |
| --- | --- | --- |
| `docker compose up -d --wait` (already running) | - | 0.6 s |
| `reset_demo.py` | - | 0.3 s, "Reset complete." (both groups deleted, 4 tables emptied) |
| Processor, forecaster and dashboard ready | - | all three printed their start line within a second; page: "Waiting for data" |
| First reading stored | - | 0.1 s |
| First flag (10 Dec 22:40) | 27.2 s | **27.2 s**; page "Live", "1 of 1" windows, 8 flagged |
| First forecast (288th reading) | 28.7 s | **28.6 s**; caption "ARIMA(1,1,1) forecast of the next hour" |
| Decline starts (15 Dec 20:15, 95.7) | 2 min 48 s | **2 min 48 s** (168.2 s) |
| Minimum 2.1 (16 Dec 17:25) | 3 min 14 s | **3 min 14 s** (193.7 s); "2 of 2" windows, 60 flagged |
| Pause: Ctrl+C in T3 | - | `Done: sent 2062 events, last seq 4110` |
| "No new data" warning | after 10 s | **12.3 s** after Ctrl+C ("No new data for 12 s"); page clock kept moving 13:19:10 -> 13:19:23 |
| `--resume --rate 10` | continues | from seq 4111 (17 Dec 03:50); "Live" again; 2,135 rows = 2,135 distinct offsets, seq 2049-4183 with no gap |
| Proof points | - | 5 events printed; both groups listed (LAG 0 and 1); count 2,231 -> 2,262 in 3 s |
| Optional: stop the forecaster | its lag grows | forecast LAG 18 -> 116 in 8 s, scoring stayed at 1 |
| Stop | clean summaries | producer `Done: sent 589 events`; forecaster `182 forecasts, 0 failed fits`; processor `2651 processed, 60 flagged, 0 late dropped` |

What changed in this runbook after the rehearsal: `--formatter-property` instead of the deprecated `--property`
(Kafka 4.3 warns), the two `--group` options instead of `--all-groups`, the warning delay (about 12 s, not "within
10 s") and the `URL:` line Streamlit prints with `--server.address localhost`.
