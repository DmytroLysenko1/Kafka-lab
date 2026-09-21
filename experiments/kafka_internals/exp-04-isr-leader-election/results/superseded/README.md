# Superseded runs

| Run | What is wrong with it |
|---|---|
| `run-2026-09-20-151858.log` | the first instrument's timings. Every interval included the next process starting up, the kill→notice figure also included 300 `acks=all` writes, and the `200ms` printed for the preferred election was startup and nothing else — the election had already finished before the process could look |

The instrument now builds its binary outside the measured window, takes the elapsed time the
moment the wait returns, and prints how much of each interval was spent polling, so a
degenerate row cannot be mistaken for a measurement.
