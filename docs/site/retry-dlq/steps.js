Walkthrough.register({
  id: "retry-dlq",
  eyebrow: "Kafka-lab · KR3 · retry chain and DLQ",
  title: "Retry chain and dead letters",
  hint: "The backoff schedule lives in the topic list, not in a sleep call.",
  lanes: [
    { id: "main",  name: "payments.main" },
    { id: "cons",  name: "payments-consumer" },
    { id: "retry", name: "payments.retry.*" },
    { id: "dlq",   name: "payments.dlq" }
  ],
  steps: [
    { kind: "msg", from: "main", to: "cons", label: "record payment 7f3a",
      t: "A record that will not process",
      d: "The payment provider times out. The record is valid and the consumer is healthy — the failure is transient and lives somewhere else entirely." },

    { kind: "note", at: "cons", warn: true, lines: ["do NOT sleep here"],
      t: "The tempting wrong fix",
      d: "Waiting inside the consumer blocks the whole partition. One stuck payment stops every other payment that happens to hash to the same one.",
      r: "Head-of-line blocking is how a single failing merchant destroys throughput for everyone sharing its partition." },

    { kind: "msg", from: "cons", to: "retry", label: "produce → retry.5s, attempt=1",
      t: "Move the wait somewhere else",
      d: "The record is republished to a dedicated retry topic with the attempt count in the headers. The failure has been turned into data." },

    { kind: "msg", from: "cons", to: "main", label: "CommitOffsets",
      t: "The main partition keeps flowing",
      d: "The offset advances immediately. As far as the main topic is concerned this record is handled — its fate now belongs to the retry topic." },

    { kind: "msg", from: "retry", to: "cons", label: "redelivered after 5s", reply: true,
      t: "Blocking is fine in here",
      d: "Kafka has no delayed delivery, so the retry consumer reads the record timestamp and waits until the delay has elapsed. That is acceptable precisely because every record in this topic carries the same delay and they arrive in time order.",
      r: "This is why the tiers are separate topics. One shared retry topic with mixed delays reintroduces the head-of-line blocking the pattern was meant to remove." },

    { kind: "note", at: "cons", warn: true, lines: ["attempt=2 → retry.1m", "attempt=3 → retry.10m"],
      t: "Backoff as topology",
      d: "Each failure promotes the record to the next tier. The backoff schedule is visible in the topic list instead of being buried inside a sleep somewhere in the consumer.",
      r: "Retries reorder events by design: a record sent to retry.10m comes back long after its neighbours. Handlers must guard the state transition in the database, not assume arrival order." },

    { kind: "msg", from: "cons", to: "dlq", label: "attempts exhausted → DLQ",
      t: "Dead letter, with the evidence attached",
      d: "Headers carry the original topic, partition and offset, the error class, the attempt count and the trace id — enough to reconstruct the failure without digging through consumer logs.",
      r: "A DLQ you cannot replay from is a trash can. dlq-replayer exists so that fixing the bug and reprocessing is one command rather than a manual export." },

    { kind: "note", at: "dlq", warn: true, lines: ["poison pill lands here", "on the first failure"],
      t: "Poison pills skip the queue",
      d: "Bytes that will not deserialize are not a transient failure — retrying them a hundred times changes nothing. They go straight to the DLQ and the offset is committed.",
      r: "Retrying deserialization forever stops the partition permanently (exp-11). Broken bytes are a support problem, not a business-logic problem." }
  ]
});
