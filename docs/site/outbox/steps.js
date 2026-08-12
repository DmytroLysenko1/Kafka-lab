Walkthrough.register({
  id: "outbox",
  eyebrow: "Kafka-lab · KR3 · transactional outbox",
  title: "Outbox: where atomicity ends",
  hint: "The event is written to a table, not to a broker — and that is the entire trick.",
  lanes: [
    { id: "api",   name: "payments-api" },
    { id: "db",    name: "Postgres" },
    { id: "relay", name: "outbox-relay" },
    { id: "k",     name: "Kafka" }
  ],
  steps: [
    { kind: "msg", from: "api", to: "db", label: "BEGIN",
      t: "One transaction, two writes",
      d: "The API is about to change state and to announce that change. The pattern exists because those two facts must never be able to disagree." },

    { kind: "msg", from: "api", to: "db", label: "INSERT payment + INSERT outbox",
      t: "State and event, same transaction",
      d: "The event is not sent anywhere. It is inserted into an ordinary table, in the same database, inside the same transaction as the payment itself." },

    { kind: "msg", from: "api", to: "db", label: "COMMIT",
      t: "Atomic by construction",
      d: "Either the payment and its event both exist, or neither does. No code path can produce one without the other, because no second system is involved yet.",
      r: "Calling the broker from the use case instead is the dual-write bug: the transaction commits, the publish fails, and the rest of the company never learns the payment happened." },

    { kind: "note", at: "db", lines: ["outbox row waits:", "published_at IS NULL"],
      t: "A queue that is just a table",
      d: "The row sits there marked unpublished. Nothing has been delivered yet, and the API has already returned success to its caller." },

    { kind: "msg", from: "relay", to: "db", label: "SELECT ... FOR UPDATE SKIP LOCKED",
      t: "The relay picks up work",
      d: "A separate process polls for unpublished rows. SKIP LOCKED lets several relay instances take disjoint batches without blocking one another.",
      r: "Per-aggregate ordering is not free. Fetch batches grouped by payment id, or two relays will publish two events of the same payment out of order." },

    { kind: "msg", from: "relay", to: "k", label: "Produce event",
      t: "Only now does the broker enter the picture",
      d: "Delivery has been separated from the business transaction. The API committed long ago and never had to know whether Kafka was reachable." },

    { kind: "msg", from: "k", to: "relay", label: "ack", reply: true,
      t: "Delivery is a retry loop, not a requirement",
      d: "If the broker is down this call simply fails and the relay tries again later. Meanwhile the business keeps accepting payments and events accumulate in the table." },

    { kind: "msg", from: "relay", to: "db", label: "UPDATE outbox SET published_at",
      t: "Mark only after the ack",
      d: "The row is marked published once — and only once — the broker has confirmed the write.",
      r: "Mark before the ack and a crash loses the event silently. Mark after and a crash between ack and update republishes it, which is harmless because the consumer has an inbox (exp-07)." },

    { kind: "note", at: "k", lines: ["broker down =", "backlog, not data loss"],
      t: "What the pattern actually buys",
      d: "An unavailable Kafka degrades delivery latency, not correctness. Nothing is lost, nothing is invented, and no business request ever failed because of a broker.",
      r: "The price is real: an extra table, a relay process to operate, and events that arrive later than the state change. Do not reach for it where a dropped notification is acceptable." }
  ]
});
