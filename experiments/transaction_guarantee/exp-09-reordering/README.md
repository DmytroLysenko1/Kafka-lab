# exp-09 — reordering, and where the duplicates actually came from

**Hypothesis.** With idempotence off and more than one request in flight, a retried batch
can land after a later one, so a refund is recorded before the capture it refunds. With
idempotence on, the broker's sequence numbers make that impossible.

```
make exp-09
```

A steady two-minute stream of captures and refunds into one partition of a
`min.insync.replicas=3` topic. A follower is frozen and thawed three times. While it is
frozen but still in the in-sync set, `acks=all` requests are appended by the leader and then
wait for it until the produce timeout (10 s) answers them `REQUEST_TIMED_OUT`; once the
controller drops it from the set — 10 to 30 s in across these runs, and longer than the 9 s
broker session timeout, presumably, when the frozen node was also the active controller
(not checked; one flap in run 3 read 0 s, also not investigated) — new requests are refused `NOT_ENOUGH_REPLICAS` before anything is written. Then
the partition is read back and checked. Each producer counts its own retries by error code,
read from franz-go's per-response debug summary, because the client retries a retriable
error silently at every other log level.

| Knob | Default | Why |
|---|---|---|
| `DURATION` | 120 s | long enough to cover three freezes with traffic on both sides of each |
| `FLAPS` | 3 | three refusal windows: three chances, not one |

## Result — 2026-09-21, and 2026-09-26

| Producer | Produced | Read back | Out of order | Duplicated | Log |
|---|---|---|---|---|---|
| idempotence off, 5 in flight | 217 840 | 218 131 | **0** | **291** | [run 1](results/plain-2026-09-21-184103.log) |
| idempotence off, 5 in flight | 248 680 | 248 760 | **0** | **80** | [run 2](results/plain-2026-09-21-194744.log) |
| idempotence off, 5 in flight | 338 440 | 338 520 | **0** | **80** | [run 3, retries counted](results/plain-2026-09-22-000803.log) |
| idempotence off, 5 in flight | 297 880 | 297 960 | **0** | **80** | [run 4, retries counted](results/plain-2026-09-26-031033.log) |
| idempotence off, 5 in flight | 176 660 | 176 851 | **0** | **191** | [run 5, retries counted](results/plain-2026-09-26-031449.log) |
| idempotent | 266 980 / 217 200 / 207 420 / 144 780 / 147 200 | the same | 0 | 0 | [1](results/idempotent-2026-09-21-184103.log) · [2](results/idempotent-2026-09-21-194744.log) · [3](results/idempotent-2026-09-22-000803.log) · [4](results/idempotent-2026-09-26-031033.log) · [5](results/idempotent-2026-09-26-031449.log) |

What the producers retried in the three runs that counted it, by the error that caused the retry:

| Run | Producer | `REQUEST_TIMED_OUT` — appended, then refused | `NOT_ENOUGH_REPLICAS` — refused before the append | resent by a rewind after a success | Duplicated |
|---|---|---|---|---|---|
| 3 | idempotence off | 2 batches, **80 records** | 8 batches, 262 records | 0 | **80** |
| 4 | idempotence off | 2 batches, **80 records** | 9 batches, 260 records | 0 | **80** |
| 5 | idempotence off | 2 batches, **191 records** | 17 batches, 2 118 records | 0 | **191** |
| 3 | idempotent | 3 batches, 100 records | 13 batches, 328 records | 0 | 0 |
| 4 | idempotent | 4 batches, 251 records | 15 batches, 1 388 records | 0 | 0 |
| 5 | idempotent | 3 batches, 100 records | 13 batches, 340 records | 0 | 0 |

**The duplicates were the timeout, not the client.** In each of the three runs that counted
retries, the plain producer duplicated exactly as many records as it had retried after the
leader appended them and answered `REQUEST_TIMED_OUT` — 80, 80 and 191. None came from franz-go rewinding past a batch that had succeeded. Any
producer without idempotence, Java included, writes those records twice: the broker has
them, the client was told the request failed, and a retry cannot be told apart from a new
write. The idempotent producer met the same failure — 100 appended records retried — and the
broker dropped every retry by its sequence number, so it read back exactly what it produced.

**Nothing was reordered in any run — the hypothesis did not hold on this client.** Across
runs 3–5, 260 to 2 118 records refused with `NOT_ENOUGH_REPLICAS` were retried per run, and
not one landed behind a later record. The likely reason is franz-go's own:
after an error on a partition it keeps one request in flight until a request succeeds
(`okOnSink` in `sink.go`), so nothing later is on the wire to overtake the retry. That is
read from the source, not isolated by this run.

**Duplicates vary with the timing, and the order does not.** 291, 80, 80, 80 and 191 across
five runs: how many records are appended inside a timeout window depends on where the freeze
lands against the stream. The runs agree on the conclusion, not on the number.

**Not shown:** that franz-go never reorders — its documentation says it "may", and a batch
refused before the append while the next one is already accepted, from a healthy pipeline
of five, is the case this failure does not produce. Not shown either: the same run with the
Java client. The first two runs had no retry counter, so their 291 and 80 are attributed by
analogy with the three that did. The reasoning is in the [journal](../../../docs/00-journal.md).

## Cleaning up

```
make reset-topic TOPIC=exp09.payments
```

The run thaws every broker and restores preferred leadership on any exit.
