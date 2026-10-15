"""The history scrubber: a slider over the whole stream with a red mark at every flagged reading.

Streamlit's own st.slider only reports its value when the mouse is released, so
the chart could not follow a drag. This small custom component (Streamlit's
components v2, no extra library) reports the position while it is being
dragged, about 8 times a second, and the history chart redraws as you go.

A position is a whole number of 5-minute steps after the stream's first event
time, so the slider moves one reading at a time and the arrow keys move 5 minutes.
The time <-> step maths is plain Python and unit-tested (tests/test_history_scrubber.py).
"""
from datetime import timedelta, timezone

import streamlit as st

STEP = timedelta(minutes=5)   # one reading every 5 minutes of event time


def steps_between(first, last):
    """Number of 5-minute steps from the first to the last event time."""
    return round((last - first) / STEP)


def to_step(t, first, last):
    """An event time as a slider position, rounded to the nearest step and kept inside the stream."""
    return min(max(round((t - first) / STEP), 0), steps_between(first, last))


def from_step(step, first, last):
    """A slider position back to an event time."""
    return first + min(max(step, 0), steps_between(first, last)) * STEP


def flag_steps(flag_times, first, last):
    """Slider positions of the flagged readings: sorted, one mark per step."""
    return sorted({to_step(t, first, last) for t in flag_times})


def utc_ms(t):
    """A naive event time as milliseconds, so the browser can format it without applying its own timezone."""
    return int(t.replace(tzinfo=timezone.utc).timestamp() * 1000)


HTML = """
<div class="scrubber">
  <div class="label"><span class="value"></span><span class="hint">Red marks: flagged readings</span></div>
  <div class="track">
    <div class="lane"><div class="stored"></div><div class="marks"></div></div>
    <input class="slider" type="range" min="0" step="1" aria-label="Show readings up to (event time)" />
  </div>
  <div class="ends"><span class="first"></span><span class="last"></span></div>
</div>
"""

CSS = """
.scrubber { font-family: var(--st-font); color: var(--st-text-color); font-size: 0.875rem; }
.label { display: flex; justify-content: space-between; margin-bottom: 0.25rem; }
.value { font-weight: 600; }
.hint, .ends { color: var(--st-gray-color, #94A3B8); font-size: 0.75rem; }
.ends { display: flex; justify-content: space-between; }
.track { position: relative; height: 34px; }
/* The lane is inset by half the thumb width (8px), so a mark sits exactly where
   the thumb's centre is when the slider points at that reading. */
.lane { position: absolute; left: 8px; right: 8px; top: 9px; height: 16px; border-radius: 4px;
        background: var(--st-secondary-background-color); border: 1px solid var(--st-border-color); }
.stored { position: absolute; top: 0; bottom: 0; background: var(--st-primary-color); opacity: 0.25; border-radius: 4px; }
.marks span { position: absolute; top: -3px; bottom: -3px; width: 2px; margin-left: -1px; background: #F87171; }
.slider { position: absolute; left: 0; right: 0; top: 0; width: 100%; height: 34px; margin: 0;
          background: transparent; appearance: none; -webkit-appearance: none; cursor: pointer; }
.slider::-webkit-slider-runnable-track { background: transparent; height: 34px; }
.slider::-moz-range-track { background: transparent; }
.slider::-webkit-slider-thumb { -webkit-appearance: none; width: 16px; height: 30px; margin-top: 2px; border-radius: 4px;
        background: var(--st-primary-color); border: 2px solid var(--st-text-color); }
.slider::-moz-range-thumb { width: 12px; height: 26px; border-radius: 4px; background: var(--st-primary-color);
        border: 2px solid var(--st-text-color); }
"""

JS = """
export default function (component) {
  const { data, parentElement, setStateValue } = component
  const slider = parentElement.querySelector(".slider")
  const pad = (n) => String(n).padStart(2, "0")
  const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
  // Event times are sent as UTC milliseconds and shown with getUTC*, so no timezone shifts them.
  const show = (step) => {
    const d = new Date(data.first_ms + step * 300000)
    return d.getUTCDate() + " " + MONTHS[d.getUTCMonth()] + " " + d.getUTCFullYear() + ", " +
      pad(d.getUTCHours()) + ":" + pad(d.getUTCMinutes())
  }
  const percent = (step) => (100 * step / data.max) + "%"

  slider.max = data.max
  parentElement.querySelector(".first").textContent = show(0)
  parentElement.querySelector(".last").textContent = show(data.max)

  // The stretch of the stream already stored, and one red mark per flagged reading.
  const stored = parentElement.querySelector(".stored")
  stored.style.left = percent(data.stored[0])
  stored.style.width = (100 * (data.stored[1] - data.stored[0]) / data.max) + "%"
  const marks = parentElement.querySelector(".marks")
  if (marks.dataset.key !== String(data.flags)) {   // redraw only when the flags changed
    marks.replaceChildren(...data.flags.map((step) => {
      const mark = document.createElement("span")
      mark.style.left = percent(step)
      mark.title = "Flagged: " + show(step)
      return mark
    }))
    marks.dataset.key = String(data.flags)
  }

  // Python's value wins, except while the thumb is held: the 2 s auto-refresh
  // must never pull the thumb away from the user's hand.
  if (slider.dataset.dragging !== "1" && Number(slider.value) !== data.value) {
    slider.value = data.value
  }
  parentElement.querySelector(".value").textContent = "Show readings up to " + show(Number(slider.value))

  // Send the position while dragging, at most every 120 ms, and once more on release.
  let lastSent = Number(slider.dataset.sent || -1)
  let lastTime = 0
  const send = (force) => {
    const step = Number(slider.value)
    parentElement.querySelector(".value").textContent = "Show readings up to " + show(step)
    if (step === lastSent || (!force && Date.now() - lastTime < 120)) return
    lastSent = step
    lastTime = Date.now()
    slider.dataset.sent = String(step)
    setStateValue("end", step)
  }
  slider.onpointerdown = () => { slider.dataset.dragging = "1" }
  slider.oninput = () => send(false)
  slider.onchange = () => { slider.dataset.dragging = "0"; send(true) }
  slider.onpointerup = () => { slider.dataset.dragging = "0"; send(true) }
}
"""

_SCRUBBER = st.components.v2.component("history_scrubber", html=HTML, css=CSS, js=JS)


def history_scrubber(value, first, last, flag_times, stored, key, on_change):
    """Mount the scrubber. `value` is the chosen end time; `stored` is (oldest, newest) stored event time.

    The new position arrives in st.session_state[key]["end"] as a step; on_change runs first.
    """
    return _SCRUBBER(
        key=key,
        data={
            "max": steps_between(first, last),
            "value": to_step(value, first, last),
            "first_ms": utc_ms(first),
            "flags": flag_steps(flag_times, first, last),
            "stored": [to_step(stored[0], first, last), to_step(stored[1], first, last)],
        },
        on_end_change=on_change,
    )
