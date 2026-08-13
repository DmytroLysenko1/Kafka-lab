Walkthrough.register({
  id: "write-path",
  eyebrow: "Kafka-lab · KR1 · write path",
  title: "Write path",
  hint: "Two of these steps are the ones nobody draws: the metadata lookup, and the branch that has to fail.",
  lanes: [
    { id: "app",    name: "payments-api" },
    { id: "prod",   name: "franz-go producer" },
    { id: "any",    name: "Any broker" },
    { id: "leader", name: "Leader p3" },
    { id: "foll",   name: "Followers ISR" }
  ],
  steps: [
    { kind: "msg", from: "app", to: "prod", label: "Produce key=payment_id",
      t: "The service hands over the payment",
      d: "Still an in-memory function call. Nothing has touched the network, and nothing is durable yet.",
      r: "In franz-go the event that matters is the promise callback, not the return of Produce. Treating the call itself as durability is the first mistake on this path." },

    { kind: "msg", from: "prod", to: "any", label: "Metadata topics=payments.main",
      t: "The client asks where the leaders are",
      d: "Any broker can answer this. The reply lists, for every partition, which broker currently leads it and at which epoch — and the client caches it." },

    { kind: "msg", from: "any", to: "prod", label: "partition leaders and epochs", reply: true,
      t: "Placement is a client-side decision",
      d: "No broker is ever asked where to put a record. The client now knows the topology and decides for itself.",
      r: "After leadership moves, the old leader answers NOT_LEADER_OR_FOLLOWER. A client that does not refresh its metadata retries into a void — which is why metadata.max.age.ms exists." },

    { kind: "note", at: "prod", lines: ["partition = murmur2(key) % 6", "batch fills until linger or max bytes"],
      t: "The partition is chosen, then the message waits in a batch",
      d: "murmur2(key) % partitions is computed on the client, the same way the Java client computes it, so a Go producer and a Java producer put the same key on the same partition. The record then accumulates in a batch until the linger interval elapses or the batch hits its size limit — in franz-go those are ProducerLinger, 10 ms by default where Java's linger.ms is 0, and ProducerBatchMaxBytes, about 1 MB where Java's batch.size is 16 KB.",
      r: "Same key, same partition, ordering preserved. Without a key franz-go does not round-robin per record: its default UniformBytesPartitioner (KIP-794) holds one partition until 64 KiB have been produced to it and only then re-picks, adaptively favouring the least backed-up broker. Ordering survives by accident at low volume and collapses once traffic crosses that threshold — exp-01 has to push well past it." },

    { kind: "msg", from: "prod", to: "leader", label: "ProduceRequest acks=all, seq=N",
      t: "Straight to the leader of that partition",
      d: "The batch, not the record, is the unit of transfer, and compression happens here in the producer. The request carries the producer id, its epoch and a per-partition sequence number.",
      r: "This is the throughput-versus-latency dial. \"Kafka is slow\" almost always means linger.ms was never touched." },

    { kind: "msg", from: "leader", to: "prod", label: "NOT_ENOUGH_REPLICAS", reply: true, warn: true, ghost: true,
      t: "The branch that must fail loudly",
      d: "Before appending anything, the leader compares the size of the ISR against min.insync.replicas. Too few in-sync replicas and the record is rejected outright — nothing is written. Drawn faded because in this run the ISR is healthy and this arrow never fires.",
      r: "With min.insync.replicas=1 this branch never fires either, and that is the danger: the cluster degrades in silence while acks=all keeps returning success from a single surviving machine (exp-08)." },

    { kind: "note", at: "leader", lines: ["append to active segment", "page cache, not fsync"],
      t: "Written — but not to disk",
      d: "The leader appends the record to the end of the log and the LEO advances. That append lands in the operating system page cache; Kafka does not fsync on every record.",
      r: "Durability comes from replication, not from the disk. \"Acked\" means \"in the page cache of every in-sync replica\", which is stronger than one disk and weaker than three." },

    { kind: "msg", from: "foll", to: "leader", label: "FetchRequest from their own LEO",
      t: "Replicas come and ask",
      d: "Followers pull from the leader on their own schedule. The leader never pushes anything to them — replication in Kafka is a fetch, exactly like a consumer read.",
      r: "The leader does not notice a dead follower instantly either: it waits replica.lag.time.max.ms, 30 seconds by default, before shrinking the ISR." },

    { kind: "msg", from: "leader", to: "foll", label: "records", reply: true,
      t: "The leader answers with the records",
      d: "A follower that keeps up stays in the ISR — the in-sync replica set. One that falls behind is dropped out of it and stops counting towards acknowledgements." },

    { kind: "note", at: ["leader", "foll"], lines: ["high watermark = lowest LEO in the ISR", "moves once every ISR member holds it"],
      t: "Now the record counts as replicated",
      d: "The high watermark is the lowest LEO across the in-sync replicas. Consumers can only see data below it.",
      r: "This is why a consumer never reads a record that could still be lost to a leader failure." },

    { kind: "msg", from: "leader", to: "prod", label: "ProduceResponse ok, base offset 1043", reply: true,
      t: "The leader acknowledges",
      d: "With acks=all the response is released on the same condition that moves the high watermark: every in-sync replica holds the record.",
      r: "\"Every in-sync replica\" is not \"every replica\". If the ISR has shrunk to one, acks=all guards against nothing at all — that is exactly what min.insync.replicas=2 is for (exp-08)." },

    { kind: "msg", from: "prod", to: "app", label: "ack", reply: true,
      t: "Only now does the payment exist",
      d: "Everything before this point was invisible to the rest of the system.",
      r: "A timeout here is an unknown outcome, not a failure — the write may well have been accepted. Recording it as failed and moving on is how money gets lost." }
  ]
});
