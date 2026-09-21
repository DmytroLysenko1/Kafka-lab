# Superseded runs

Kept because the journal's "what the instrument got wrong first" section is about them, not
because they measure anything. Neither is evidence for any published number.

| Run | What is wrong with it |
|---|---|
| `run-2026-09-20-150733.log` | segment-roll markers counted as data: 52 records and 41 live keys, where 41 live plus 10 deleted is 51 keys out of a possible 50 — arithmetically impossible, which is how it was noticed |
| `run-2026-09-20-150839.log` | the run inherited the previous run's log (its retention topic starts at offset 2003, exactly where the run above ended), so it measured two runs at once |

Both were produced by an earlier build than the one in the repository. The authoritative
run is the newest file in the parent directory.
