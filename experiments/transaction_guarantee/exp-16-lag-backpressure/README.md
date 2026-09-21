# exp-16 — consumer lag, and backpressure against blocking, through a dependency outage

**Hypothesis.** When the service behind a consumer goes down, lag grows whatever the
handler does. What the handler decides is everything else: a handler that retries inside
its batch holds the rebalance, gets the member removed from the group and handles records
twice; a handler that pauses fetching and lets go keeps the group stable, at the same lag.

```
make exp-16
```

A steady stream of 400 records/s over three partitions for 50 s. From 15 s to 35 s the
dependency every record needs stops answering; at 20 s a second consumer joins the group,
so a rebalance has to happen in the middle of the outage. Both members hold rebalances off
while they handle a batch (`BlockRebalanceOnPoll`, the way the Java consumer rebalances
only inside `poll`), commit by hand only what they handled, and use an 8 s
`RebalanceTimeout` so that a member holding a rebalance is removed within the outage. They
differ in what they do when the dependency fails:

- **block** — retries the dependency every 500 ms, inside the batch, holding it;
- **pause** — commits what it handled, rewinds each partition to its first unhandled record
  (`SetOffsets`), pauses fetching (`PauseFetchPartitions`), releases the rebalance, and waits
  outside any batch until the dependency answers.

The group's lag is read like an operator reads it — log end offsets against committed
offsets — once a second. Every record handled is checked against every record produced,
and every accepted commit is checked against the one before it on the same partition.

| Knob | Default | Why |
|---|---|---|
| `HANDLERS` | `block pause` | `HANDLERS=block` repeats one shape; see the third finding |

## Result — 2026-09-22

Three runs of each: block [1](results/block-2026-09-22-012039.log) ·
[2](results/block-2026-09-22-012322.log) · [3](results/block-2026-09-22-012605.log),
pause [1](results/pause-2026-09-22-012039.log) · [2](results/pause-2026-09-22-012322.log) ·
[3](results/pause-2026-09-22-012605.log).

| | block | pause |
|---|---|---|
| peak lag | 7 982–8 501, at 35–38 s | 7 940–7 972, at 35 s |
| lag back under 100 | 10–13 s after the dependency came back | 11 s |
| the joining member first owned a partition | **8 s after joining** | 2.0–2.1 s after joining |
| commits the group refused | 1 — `UNKNOWN_MEMBER_ID` | 0 |
| accepted commits that moved an offset backwards | **2 in every run, one of 1 726–1 732 records** | none |
| records handled twice | **1 739**, 6, 8 | 0 |
| records never handled | 0 | 0 |

**Lag is the outage, not the handler.** Both shapes built the same backlog at the same rate
— the produce rate, 400 a second for 20 s — and drained it in about the same time. Lag
tells an operator that the consumer is not keeping up; it cannot say whether the group
underneath is healthy, and here one of the two was not.

**Holding the batch holds the group.** The second member joined at 20 s and got nothing
until 28 s: the rebalance waited for the blocked member, and after the 8 s rebalance
timeout the coordinator removed it and gave everything to the newcomer. The removed member
did not know. At 35.4 s, when the dependency answered, it finished its batch and its commit
was refused with `UNKNOWN_MEMBER_ID` — the first it heard of it. The pausing member released
the rebalance, and the newcomer owned a partition two seconds after joining.

**The removed member's next commit rewound the group.** It went on to handle the next few
records it had fetched before the outage, and their commit waited until it rejoined at
37.6 s — then it was accepted, and moved a partition's committed offset back to where this
member had been twenty seconds earlier: 1 726–1 732 records, in every run. Whether that
turns into duplicates depends on who reads the partition next: in run 1 the rewound
partition went to the rejoined member, which started from the rewound offset and handled
1 739 records a second time; in runs 2 and 3 it stayed with the member that was already
past it, and the next commit covered it. It is the failure franz-go's own documentation
warns of under `DisableAutoCommit` — a member that lost a partition "can rewind" the new
owner's commit — measured, and landing in one run in three.

**Backpressure is what the pause shape did.** It chose to stop taking work — pausing
fetches, giving the uncommitted records back to the log — instead of holding work it could
not finish. Nothing was refused, nothing rewound, nothing handled twice.

**Not shown:** a slow handler without an outage, and in particular whether one slow
partition stalls the others — with a single poll loop per member, as here, it would stall
every partition the member owns; that is argued, not run. Nor franz-go's default of not
blocking rebalances on poll, under which, by the `BlockRebalanceOnPoll` documentation, the
group does not wait for a busy handler and a revoked partition's records can still be in
its hands.

## Cleaning up

```
make reset-topic TOPIC=exp16.payments
```
