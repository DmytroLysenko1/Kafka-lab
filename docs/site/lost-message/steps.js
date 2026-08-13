Walkthrough.register({
  id: "lost-message",
  eyebrow: "Kafka-lab · KR2 · delivery semantics",
  title: "Where the message is lost",
  hint: "One record, three endings — the commit moves, everything else stays the same.",
  lanes: [
    { id: "k",  name: "Kafka" },
    { id: "c",  name: "payments-consumer" },
    { id: "db", name: "Postgres" }
  ],
  steps: [
    { kind: "msg", from: "k", to: "c", label: "record offset=42, payment 7f3a",
      t: "One record, three possible endings",
      d: "The same delivery replayed three times, with the commit in a different place each time. Only the third version is safe." },

    { kind: "msg", from: "c", to: "k", label: "CommitOffsets(43)", warn: true,
      t: "Act I — commit first",
      d: "The consumer tells Kafka it is finished before it has done anything. This is what enable.auto.commit does by default: on a timer, in the background, without asking." },

    { kind: "note", at: ["c", "db"], warn: true, lines: ["kill -9 in this window"],
      t: "The loss window",
      d: "The process dies between the commit and the database write. On restart the group resumes at offset 43, record 42 is never delivered again, and payment 7f3a exists nowhere.",
      r: "exp-05 will measure how many of 1000 payments disappear. Expect a small number — which is worse than a large one, because small silent losses do not look like an incident." },

    { kind: "msg", from: "c", to: "db", label: "INSERT payment 7f3a", ghost: true,
      t: "The write that never happened",
      d: "Drawn faded because in the crashed run this arrow never fired. Kafka is convinced the record was handled, because \"handled\" only ever meant \"committed\"." },

    { kind: "msg", from: "c", to: "db", label: "INSERT payment 7f3a",
      t: "Act II — process first",
      d: "Same record, opposite order. Write to the database, and only then tell Kafka about it." },

    { kind: "msg", from: "c", to: "k", label: "CommitOffsets(43)", warn: true,
      t: "Commit after the work",
      d: "Nothing can be lost now. A crash before this arrow simply means the record will be delivered again." },

    { kind: "note", at: ["c", "k"], warn: true, lines: ["kill -9 here →", "record redelivered"],
      t: "The duplicate window",
      d: "Die after the INSERT but before the commit and the record comes back. The consumer inserts payment 7f3a a second time — for a payment, that is charging the customer twice.",
      r: "This is not an exotic edge case. Rebalances, deploys and OOM kills all land in this window routinely. At-least-once always means duplicates eventually." },

    { kind: "msg", from: "c", to: "db", label: "BEGIN; INSERT inbox ON CONFLICT DO NOTHING RETURNING",
      t: "Act III — let the database decide",
      d: "The event id becomes the primary key of an inbox table. The first delivery inserts a row and gets it back; a redelivery hits the conflict and gets zero rows back.",
      r: "RETURNING is the whole mechanism, not decoration. ON CONFLICT DO NOTHING raises no error, so the row count is the only signal that this event has been seen before. Ignore it and the business write below still runs on every redelivery — that is Act II again, with an extra table." },

    { kind: "msg", from: "c", to: "db", label: "1 row → INSERT payment; COMMIT",
      t: "First delivery: state and inbox commit together",
      d: "A row came back, so this event is new. The business write and the inbox row are one transaction — either both exist or neither does.",
      r: "Splitting these into two transactions recreates exactly the bug the pattern was introduced to remove." },

    { kind: "note", at: "db", lines: ["conflict → 0 rows", "→ skip the write"],
      t: "The redelivery takes the other branch",
      d: "The record may arrive five times. Deliveries two through five find the event id already present; the insert changes nothing and returns nothing, and that empty result is what the consumer branches on.",
      r: "Notice what did not change: Kafka still delivered at-least-once. The broker behaves identically — it is the effect that became exactly-once, not the delivery." },

    { kind: "msg", from: "c", to: "db", label: "0 rows → skip INSERT; COMMIT", ghost: true,
      t: "The write that must not happen twice",
      d: "Drawn faded because it belongs to the redelivery run, not the first one. Zero rows means the payment is already recorded, so the consumer skips the business write entirely and goes straight to the offset.",
      r: "This is the branch exp-07 has to prove: 1000 payments, redelivered under kill -9, still 1000 rows in the database and zero duplicates." },

    { kind: "msg", from: "c", to: "k", label: "CommitOffsets(43)",
      t: "Exactly-once effect, at-least-once delivery",
      d: "This distinction is worth stating precisely in the write-up. Kafka transactions give exactly-once inside Kafka; they do not extend to your database.",
      r: "For \"Kafka plus Postgres\" the answer is an inbox on the way in and an outbox on the way out — not EOS (exp-07, exp-10)." }
  ]
});
