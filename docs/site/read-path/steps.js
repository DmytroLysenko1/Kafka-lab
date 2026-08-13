Walkthrough.register({
  id: "read-path",
  eyebrow: "Kafka-lab · KR1 · read path",
  title: "Read path",
  hint: "Join the group, find your place, pull, commit — in that order.",
  lanes: [
    { id: "cons",   name: "payments-consumer" },
    { id: "coord",  name: "Group Coordinator" },
    { id: "leader", name: "Leader p3" }
  ],
  steps: [
    { kind: "msg", from: "cons", to: "coord", label: "JoinGroup payments-consumer",
      t: "A consumer group is negotiated, not configured",
      d: "The consumer asks to join the group. The coordinator is one specific broker, picked by hashing the group id — every member talks to that same broker about membership. In its reply the coordinator also names one member the group leader." },

    { kind: "msg", from: "coord", to: "cons", label: "SyncGroup assign p0, p3", reply: true,
      t: "The group leader assigns; the coordinator only delivers",
      d: "The assignor runs inside the elected member, not on the broker — the coordinator receives the finished plan and hands each member its share. Every partition goes to exactly one consumer, and that is the whole parallelism model: there is nothing finer-grained than a partition.",
      r: "More consumers than partitions means the extra ones sit idle forever — six partitions is a hard ceiling of six workers (exp-02). Note this is the classic group protocol; KIP-848 (GA in Kafka 4.0) moves assignment onto the broker and removes this JoinGroup/SyncGroup round trip, so check which protocol the client actually negotiates before quoting either version." },

    { kind: "msg", from: "cons", to: "coord", label: "OffsetFetch",
      t: "Where did we stop last time?",
      d: "Committed offsets are not kept in the consumer. They live in __consumer_offsets, an internal compacted topic keyed by group, topic and partition.",
      r: "No committed offset at all? Then auto.offset.reset decides: earliest replays the whole history, latest silently skips everything produced before the consumer showed up." },

    { kind: "msg", from: "cons", to: "leader", label: "FetchRequest offset=1043",
      t: "The consumer pulls; nobody pushes",
      d: "Kafka has no delivery mechanism of its own. The consumer asks a partition leader for records from an offset and waits up to fetch.max.wait for at least fetch.min.bytes to accumulate." },

    { kind: "note", at: "leader", lines: ["serves only up to", "the high watermark"],
      t: "Half-replicated records are invisible",
      d: "The leader will not serve anything above the high watermark. Records that exist on the leader but not yet on every in-sync replica simply do not appear to consumers.",
      r: "This is the guarantee that a consumer never processes a record a leader failure could still erase." },

    { kind: "msg", from: "leader", to: "cons", label: "records 1043..1092", reply: true,
      t: "The batch is sent as it lies on disk",
      d: "The broker does not decompress, deserialize or inspect anything — it ships the stored batch straight from the page cache. Compression is end-to-end between producer and consumer.",
      r: "The batch, not the record, is the unit of transfer. A fetch sized for throughput can hand a consumer far more bytes at once than expected." },

    { kind: "note", at: "cons", lines: ["deserialize, process,", "write to Postgres"],
      t: "This is where the time budget is spent",
      d: "Everything slow lives here: deserialization, business logic, database writes, calls to providers.",
      r: "Take too long here and the group moves on without you. Heartbeats keep flowing in the background, so it is not the session timeout that fires — it is the rebalance timeout: a member that cannot rejoin in time is fenced and its partitions reassigned. Java calls this max.poll.interval.ms; franz-go has no such setting, which is exactly why the tuning checklist has to map the two (exp-16)." },

    { kind: "msg", from: "cons", to: "coord", label: "OffsetCommit 1093",
      t: "Commit after processing, never before",
      d: "A committed offset is a promise: everything below it is done. The entire delivery-semantics question is just where this one arrow goes.",
      r: "Commit before processing and a crash loses records (exp-05). Commit after and a crash duplicates them (exp-06). There is no third option — only an inbox makes duplicates harmless (exp-07)." },

    { kind: "note", at: "coord", lines: ["stored in __consumer_offsets", "compacted, latest wins"],
      t: "A commit is just another produce",
      d: "The coordinator writes the offset into a compacted internal topic. Compaction keeps only the latest offset per group, topic and partition, so it never grows without bound.",
      r: "Consumer group progress is data in Kafka, not state in the process. Restarting the consumer changes nothing; deleting the group changes everything." }
  ]
});
