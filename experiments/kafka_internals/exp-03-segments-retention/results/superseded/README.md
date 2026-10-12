# Superseded runs

The first two are kept because the journal's "what the instrument got wrong first" section is
about them, not because they measure anything; the last two because a result, once committed,
stays in the repository. None is evidence for any published number.

| Run | What is wrong with it |
|---|---|
| `run-2026-09-20-150733.log` | segment-roll markers counted as data: 52 records and 41 live keys, where 41 live plus 10 deleted is 51 keys out of a possible 50 — arithmetically impossible, which is how it was noticed |
| `run-2026-09-20-150839.log` | the run inherited the previous run's log (its retention topic starts at offset 2003, exactly where the run above ended), so it measured two runs at once |
| `run-2026-09-20-150918.log` | nothing wrong in what it shows — the same compaction and retention outcome as the runs after it — but nothing cites it: [`run-2026-09-20-154030.log`](../run-2026-09-20-154030.log), made later the same day, is the run `docs/static/02` quotes |
| `run-2026-09-21-173854.log` | the same: an agreeing repeat that nothing cites, made between the run the README quotes ([`…-20-200653`](../run-2026-09-20-200653.log)) and the repeat it quotes beside it ([`…-26-025322`](../run-2026-09-26-025322.log)) |

Both were produced by an earlier build than the one in the repository. The authoritative
run is the newest file in the parent directory.
