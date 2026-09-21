# kafka_internals — KR1, fundamentals and internals

Four experiments about what Kafka does with a record once it has it: which partition it
lands on and what that costs in ordering, what happens when the keys are skewed, what the
cleaner keeps, and what a dead broker does to the ISR.

| Run | Question | Answer |
|---|---|---|
| [exp-01](exp-01-partition-keys/) | does the key decide the order a payment is handled in? | keyless: 7 964 – 8 721 ordering violations per 10 000 events, every payment split across partitions. Keyed: zero, every time |
| [exp-02](exp-02-hot-partition/) | a hot key — does adding consumers help? | 85% of records on one partition; seven consumers drained it 15% faster than one, and the seventh sat idle |
| [exp-03](exp-03-segments-retention/) | what does the cleaner keep? | compaction turned 2 010 records into 50 and kept all 10 tombstones; retention dropped every closed segment |
| [exp-04](exp-04-isr-leader-election/) | what does a killed broker cost? | noticed in about ten seconds, writes never stopped with zero margin left, and leadership never came back on its own |
| exp-04c | is that ten seconds the session timeout or the lag timer? | raise `broker.session.timeout.ms` to 20 s and the reaction moves to 20.3 s, while the lag timer sits at 30 s throughout |

The reasoning behind every number is in the [journal](../../docs/00-journal.md); the case
files that quote them are [`docs/static/01`–`04`](../../docs/static/).

```
make exp-01   make exp-02   make exp-03   make exp-04   make exp-04c
```
