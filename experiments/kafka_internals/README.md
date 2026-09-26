# kafka_internals — KR1, fundamentals and internals

Four experiments about what Kafka does with a record once it has it: which partition it
lands on and what that costs in ordering, what happens when the keys are skewed, what the
cleaner keeps, and what a dead broker does to the ISR.

| Run | Question | Answer |
|---|---|---|
| [exp-01](exp-01-partition-keys/) | does the key decide the order a payment is handled in? | keyless: 6 827 – 8 721 ordering violations per 10 000 events over seven runs, every payment split across partitions. Keyed: zero, every time |
| [exp-02](exp-02-hot-partition/) | a hot key — does adding consumers help? | 85% of records on one partition caps any group at 1.17× one consumer, and six members got 1.12–1.16×; with even keys the same six got 5.2–5.8× |
| [exp-03](exp-03-segments-retention/) | what does the cleaner keep? | compaction turned 2 010 records into 50 and kept all 10 tombstones; retention dropped every closed segment |
| [exp-04](exp-04-isr-leader-election/) | what does a killed broker cost? | noticed in 10–11 s, 15.3 s when the controller died with it; writes never stopped with zero margin left and 600 of 600 were readable after; leadership stayed with the replacements until asked (auto rebalance is off here) |
| exp-04c | is that ten seconds the session timeout or the lag timer? | raise `broker.session.timeout.ms` to 20 s and the reaction moves to 20.3 s, while the lag timer sits at 30 s throughout |

The reasoning behind every number is in the [journal](../../docs/00-journal.md); the case
files that quote them are [`docs/static/01`–`04`](../../docs/static/).

```
make exp-01   make exp-02   make exp-03   make exp-04   make exp-04c
```
