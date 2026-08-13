Walkthrough.register({
  id: "retry-dlq",
  eyebrow: "Kafka-lab · KR3 · retry chain and DLQ",
  title: "Retry chain and dead letters",
  hint: "The backoff schedule lives in the topic list, not in a sleep call.",
  lanes: [
    { id: "main",  name: "payments.main" },
    { id: "cons",  name: "payments-consumer" },
    { id: "retry", name: "payments.retry.*" },
    { id: "rcons", name: "retry-consumer" },
    { id: "dlq",   name: "payments.dlq" }
  ],
  steps: [
    { kind: "msg", from: "main", to: "cons", label: "record payment 7f3a",
      t: "A record that will not process",
      d: "The payment provider times out. The record is valid and the consumer is healthy — the failure is transient and lives somewhere else entirely." },

    { kind: "note", at: "cons", warn: true, lines: ["do NOT sleep here"],
      t: "The tempting wrong fix",
      d: "Waiting inside the main consumer blocks the whole partition. One stuck payment stops every other payment that happens to hash to the same one.",
      r: "Head-of-line blocking is how a single failing merchant destroys throughput for everyone sharing its partition — and it is silent: throughput drops, lag rises on one partition only, and the handler logs look healthy because the record eventually succeeds." },

    { kind: "msg", from: "cons", to: "dlq", label: "poison bytes → DLQ on the first failure", warn: true, ghost: true,
      t: "Classify the error before choosing a route",
      d: "Bytes that will not deserialize never enter the retry chain at all — they take this arrow, straight to the DLQ, on the very first failure. Drawn faded because in this run the record is valid and the transient path below is the one taken.",
      r: "Retrying deserialization forever stops the partition permanently: the consumer never advances past the offset and the topic looks broken rather than the message (exp-11). Broken bytes are a support problem; business-logic failure is a retry problem. Classifying the error at the boundary is what makes the difference visible in code." },

    { kind: "msg", from: "cons", to: "retry", label: "produce → retry.5s, attempt=1",
      t: "Move the wait somewhere else",
      d: "The record is republished to a dedicated retry topic with the attempt count in the headers. The failure has been turned into data." },

    { kind: "msg", from: "cons", to: "main", label: "CommitOffsets",
      t: "The main partition keeps flowing",
      d: "The offset advances immediately. As far as the main topic is concerned this record is handled — its fate now belongs to the retry topic.",
      r: "Produce first, commit after the ack: these two arrows are themselves a small dual write, and this order is the safe one. A crash in between redelivers the record and it lands in the retry topic twice — harmless only because the consumer has an inbox (exp-07). Without one, this pattern manufactures duplicates by design." },

    { kind: "msg", from: "retry", to: "rcons", label: "redelivered after 5s", reply: true,
      t: "A different consumer — and here blocking is fine",
      d: "Kafka has no delayed delivery, so this consumer reads the record timestamp and pauses the partition until the delay has elapsed. Waiting is safe here because every record in this topic carries the same delay and they arrive in time order; pausing is not sleeping either — the client keeps polling and heartbeating, so the group does not consider the member dead.",
      r: "The delay is computed from the record timestamp, so watch the timestamp type: with message.timestamp.type=LogAppendTime the broker overwrites the producer's timestamp and every delay calculation is wrong. Retry topics stay on CreateTime, or the deadline travels in a header." },

    { kind: "note", at: "rcons", warn: true, lines: ["attempt=2 → retry.1m", "attempt=3 → retry.10m"],
      t: "Backoff as topology",
      d: "Each failure promotes the record to the next tier. The backoff schedule is visible in the topic list instead of being buried inside a sleep somewhere in the consumer, and one shared retry topic with mixed delays would reintroduce exactly the head-of-line blocking the pattern removes.",
      r: "Retries reorder events by design: a record sent to retry.10m comes back long after its neighbours. Handlers must guard the state transition in the database — UPDATE ... WHERE status = $expected — so a stale event finds the row in the wrong state and changes nothing." },

    { kind: "msg", from: "rcons", to: "dlq", label: "attempts exhausted → DLQ",
      t: "Dead letter, with the evidence attached",
      d: "Headers carry the original topic, partition and offset, the error class, the attempt count, the timestamp of the first failure and the trace id — enough to reconstruct what happened without digging through consumer logs that have since rotated." },

    { kind: "note", at: "dlq", lines: ["dlq-replayer", "→ back to payments.main"],
      t: "A DLQ you cannot replay from is a rubbish bin",
      d: "Dead letters are not an archive. dlq-replayer exists so that fixing the bug and reprocessing is one command, and so that the person doing it at 3 a.m. is not writing an ad-hoc export script.",
      r: "Replayed records re-enter the main topic and are processed again by the ordinary consumer — which is safe for exactly one reason: the inbox makes a second delivery a no-op." }
  ]
});
