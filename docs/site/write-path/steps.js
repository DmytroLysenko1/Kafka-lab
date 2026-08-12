Walkthrough.register({
  id: "write-path",
  eyebrow: "Kafka-lab · KR1 · write path",
  title: "Write path",
  hint: "Six arrows, three pauses — from the service call to the acknowledgement.",
  lanes: [
    { id: "app",    name: "payments-api" },
    { id: "prod",   name: "franz-go producer" },
    { id: "leader", name: "Leader p3" },
    { id: "foll",   name: "Followers ISR" }
  ],
  steps: [
    { kind: "msg", from: "app", to: "prod", label: "Produce key=payment_id",
      t: "The service hands over the payment",
      d: "Still an in-memory function call. Nothing has touched the network, and nothing is durable yet." },

    { kind: "note", at: "prod", lines: ["batch fills until", "linger.ms / batch.size"],
      t: "The message waits in a batch",
      d: "The producer does not send one record at a time. It accumulates a batch until linger.ms elapses or batch.size fills up.",
      r: "This is the throughput-versus-latency dial. \"Kafka is slow\" almost always means linger.ms was never touched." },

    { kind: "msg", from: "prod", to: "leader", label: "ProduceRequest acks=all, seq=N",
      t: "The client already chose the partition",
      d: "hash(key) % partitions is computed on the client. The request goes straight to the leader of that partition — the broker is never asked where to put it.",
      r: "Same key means same partition means ordering preserved. No key means round-robin, and ordering is gone (exp-01)." },

    { kind: "note", at: "leader", lines: ["append to active segment", "page cache, not fsync"],
      t: "Written — but not to disk",
      d: "The leader appends the record to the end of the log. That append lands in the operating system page cache; Kafka does not fsync on every record.",
      r: "Durability comes from replication, not from the disk. \"Acked\" is not the same as \"survives a power cut on all three nodes\"." },

    { kind: "msg", from: "foll", to: "leader", label: "FetchRequest",
      t: "Replicas come and ask",
      d: "Followers pull from the leader on their own schedule. The leader never pushes anything to them — replication in Kafka is a fetch, exactly like a consumer read." },

    { kind: "msg", from: "leader", to: "foll", label: "records", reply: true,
      t: "The leader answers with the records",
      d: "A follower that keeps up stays in the ISR — the in-sync replica set. One that falls behind is dropped out of it and stops counting towards acknowledgements." },

    { kind: "note", at: ["leader", "foll"], lines: ["high watermark advances", "once every ISR caught up"],
      t: "Now the record counts as replicated",
      d: "Once every member of the ISR holds the record, the high watermark moves forward. Consumers can only see data below the high watermark.",
      r: "This is why a consumer never reads a record that could still be lost to a leader failure." },

    { kind: "msg", from: "leader", to: "prod", label: "ProduceResponse ok", reply: true,
      t: "The leader acknowledges",
      d: "With acks=all this happens only after every in-sync replica holds the record.",
      r: "\"Every in-sync replica\" is not \"every replica\". If the ISR has shrunk to one, acks=all guards against nothing at all — that is exactly what min.insync.replicas=2 is for (exp-08)." },

    { kind: "msg", from: "prod", to: "app", label: "ack", reply: true,
      t: "Only now does the payment exist",
      d: "Everything before this point was invisible to the application.",
      r: "A timeout here is an unknown outcome, not a failure — the write may well have been accepted. Recording it as failed and moving on is how money gets lost." }
  ]
});
