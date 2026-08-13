Walkthrough.register({
  id: "outbox",
  eyebrow: "Kafka-lab · KR3+KR5 · transactional outbox",
  title: "Outbox: where atomicity ends",
  hint: "The event is written to a table, not to a broker — and that is the entire trick.",
  lanes: [
    { id: "api",   name: "payments-api" },
    { id: "db",    name: "Postgres" },
    { id: "relay", name: "outbox-relay" },
    { id: "k",     name: "Kafka" }
  ],
  steps: [
    { kind: "span", at: ["api", "db"], covers: 3, label: "one transaction — the only atomic part of this diagram",
      t: "Everything atomic happens inside this frame",
      d: "The API is about to change state and to announce that change. The pattern exists because those two facts must never be able to disagree — so both of them happen inside one database transaction, and nothing else does.",
      r: "Every boundary drawn after this frame is retryable delivery. Knowing exactly where the atomic part stops is the point of the diagram." },

    { kind: "msg", from: "api", to: "db", label: "BEGIN",
      t: "One transaction, two writes",
      d: "A single connection, a single transaction, and no second system involved yet." },

    { kind: "msg", from: "api", to: "db", label: "INSERT payment + INSERT outbox (published_at IS NULL)",
      t: "State and event, same transaction",
      d: "The event is not sent anywhere. It is inserted into an ordinary table, in the same database, inside the same transaction as the payment itself." },

    { kind: "msg", from: "api", to: "db", label: "COMMIT",
      t: "Atomic by construction",
      d: "Either the payment and its event both exist, or neither does. No code path can produce one without the other, because no broker call exists in this path at all.",
      r: "Calling the broker from the use case instead is the dual-write bug: the transaction commits, the publish fails, and the rest of the company never learns the payment happened. Swapping the order does not help — publish first and a failed commit announces a payment that does not exist." },

    { kind: "note", at: "db", lines: ["outbox row waits:", "published_at IS NULL"],
      t: "A queue that is just a table",
      d: "The row sits there marked unpublished. Nothing has been delivered yet, and the API has already returned success to its caller." },

    { kind: "msg", from: "relay", to: "db", label: "SELECT unpublished FOR UPDATE SKIP LOCKED",
      t: "The relay picks up work",
      d: "A separate process polls for unpublished rows. SKIP LOCKED means competing workers never block each other on the same row.",
      r: "SKIP LOCKED prevents two relays fighting over a row — it does not preserve per-aggregate order. Run one relay behind a lease or an advisory lock, or fetch batches grouped by payment_id; two relays publishing two events of the same payment will otherwise reorder them." },

    { kind: "msg", from: "relay", to: "k", label: "Produce event, key = payment_id",
      t: "Only now does the broker enter the picture",
      d: "Delivery has been separated from the business transaction. The API committed long ago and never had to know whether Kafka was reachable. The key is the aggregate id, because ordering in Kafka is per partition.",
      r: "Do not hold the row lock across this network call for long: FOR UPDATE keeps a transaction and a connection open while a slow broker takes its time, and that turns into pool exhaustion. Keep batches small and put a hard timeout on the produce, or claim rows with claimed_by/claimed_at instead of holding the transaction." },

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
      r: "The price is real: an extra table, a relay process to operate, and events that arrive later than the state change. Relay lag becomes a metric someone has to watch. Do not reach for it where a dropped notification is acceptable." }
  ]
});
