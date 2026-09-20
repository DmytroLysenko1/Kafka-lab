# Superseded runs

Kept because the journal discusses them, not because they measure anything.

| Run | What is wrong with it |
|---|---|
| `run-2026-09-20-015215.log` | produced on topics that were never reset, so the handling order it measured was taken over a log that already held an earlier run's records |
| `run-2026-09-20-020812.log` | same, and its stamp is three seconds before the run below — a complete run takes far longer than that, so these two overlapped on the same two topics |
| `run-2026-09-20-020815.log` | see above: measured while the previous run was still producing into the same partitions |

Each of these counted only its own events — the run-id filter works, and the keyed control
reads 0 in all three — but the *order* they observed was interleaved with another run's
traffic, and order is what exp-01 measures. `run.sh` now resets both topics before
producing, so a run owns the log it reads.
