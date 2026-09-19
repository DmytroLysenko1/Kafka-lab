Walkthrough.register({
  id: "read-path",
  eyebrow: "Kafka-lab · KR1 · read path",
  title: "Read path",
  hint: "Find the coordinator, join, let a member do the assigning, pull, commit — in that order.",
  lanes: [
    { id: "cons",   name: "payments-consumer" },
    { id: "coord",  name: "Group Coordinator" },
    { id: "gl",     name: "Group leader (a member)" },
    { id: "leader", name: "Leader p3" }
  ],
  steps: [
    { kind: "msg", from: "cons", to: "coord", label: "FindCoordinator group=payments-consumer",
      t: "First, find the one broker that owns this group",
      d: "Group membership is not handled by whichever broker the consumer happens to be connected to. One specific broker coordinates the group, and every member has to find it first." },

    { kind: "note", at: "coord", lines: ["coordinator = leader of", "__consumer_offsets partition", "abs(hashCode(group)) % 50"],
      t: "The coordinator is chosen by hashing the group id",
      d: "The group id is hashed onto the 50 partitions of __consumer_offsets, and the leader of that partition is the coordinator. Every member of the group talks to that same broker about membership.",
      r: "This hash is Java's String.hashCode, not the murmur2 used to partition records by key. Two different hashes in two different places — worth keeping straight before someone asks. Killing \"some broker\" in a failure drill may kill a coordinator and cause a far larger outage than the one being tested (exp-13)." },

    { kind: "msg", from: "cons", to: "coord", label: "JoinGroup subscription=payments.main",
      t: "A consumer group is negotiated, not configured",
      d: "The consumer asks to join. The coordinator collects everyone who joins within the rebalance timeout, closes the generation, and picks one of the members to be the group leader." },

    { kind: "msg", from: "coord", to: "gl", label: "member list and subscriptions", reply: true,
      t: "One member is handed the whole picture",
      d: "The elected member — an ordinary consumer process, not a broker — receives the list of every member and what each of them subscribed to." },

    { kind: "note", at: "gl", lines: ["the assignor runs inside", "a member, not on the broker"],
      t: "The broker assigns nothing",
      d: "range, roundrobin, cooperative-sticky: all of them are client-side code, executing inside one elected consumer.",
      r: "A bad custom assignor is a client-side bug, and every member must be configured with the same assignor or the group cannot form at all." },

    { kind: "msg", from: "gl", to: "coord", label: "SyncGroup with the assignment for everyone",
      t: "The finished plan goes back to the coordinator",
      d: "The group leader computed shares for every member at once and hands the whole plan over. The coordinator's job is delivery, not decision." },

    { kind: "msg", from: "coord", to: "cons", label: "SyncGroup: your share is p0, p3", reply: true,
      t: "Each member learns only its own share",
      d: "Every partition goes to exactly one consumer in the group, and that is the whole parallelism model: there is nothing finer-grained than a partition.",
      r: "More consumers than partitions means the extra ones sit idle forever — six partitions is a hard ceiling of six workers (exp-02). Note this is the classic protocol; KIP-848 (GA in Kafka 4.0) moves assignment onto the broker and removes this JoinGroup/SyncGroup round trip, so confirm which protocol the pinned client negotiates before quoting either version." },

    { kind: "msg", from: "cons", to: "coord", label: "OffsetFetch p0, p3",
      t: "Where did we stop last time?",
      d: "Committed offsets are not kept in the consumer process. They live in __consumer_offsets, keyed by group, topic and partition." },

    { kind: "msg", from: "coord", to: "cons", label: "1043, or nothing at all", reply: true,
      t: "A group with no history gets nothing back",
      d: "For a brand-new group there is no committed offset to return, and the consumer has to decide where to start on its own.",
      r: "That decision is auto.offset.reset: earliest replays the whole history, latest silently skips everything produced before the consumer showed up. A misconfigured latest looks exactly like \"the messages never arrived\" — and the clients disagree on the default: Java starts a new group at latest, franz-go starts it at the beginning (ConsumeStartOffset = AtStart). Resetting a committed offset that fell out of retention is a different option again — ConsumeResetOffset, defaulting to RewindOffset(1m), which Kafka has no equivalent for." },

    { kind: "msg", from: "cons", to: "leader", label: "FetchRequest p3 from offset 1043",
      t: "The consumer pulls; nobody pushes",
      d: "Kafka has no delivery mechanism of its own. The consumer asks a partition leader for records from an offset and waits up to fetch.max.wait.ms for at least fetch.min.bytes to accumulate." },

    { kind: "note", at: "leader", lines: ["serves up to the high watermark", "(up to the LSO if read_committed)"],
      t: "Half-replicated records are invisible",
      d: "The leader will not serve anything above the high watermark. For a read_committed consumer the ceiling is lower still: the LSO, the first offset of the oldest open transaction.",
      r: "A read_committed consumer stalling while read_uncommitted consumers are fine is one open transaction pinning the LSO. Lag is visible, records are not." },

    { kind: "msg", from: "leader", to: "cons", label: "records 1043..1092", reply: true,
      t: "The batch is sent as it lies on disk",
      d: "The broker does not decompress, deserialize or inspect anything — it ships the stored batch straight from the page cache. Compression is end-to-end between producer and consumer.",
      r: "True on the fast path only: TLS disables sendfile zero-copy because the bytes have to be encrypted in user space, the broker recompresses if the topic's compression.type differs from the producer's, and down-converting for an old client message format costs CPU and heap. A broker at 100% CPU \"with no traffic\" is usually one of those three." },

    { kind: "note", at: "cons", lines: ["deserialize, process,", "write to Postgres"],
      t: "This is where the time budget is spent",
      d: "Everything slow lives here: deserialization, business logic, database writes, calls to providers. The batch is the unit of transfer, so a fetch sized for throughput can hand a consumer far more work at once than expected.",
      r: "Take too long and the group moves on without you. Heartbeats keep flowing in the background, so it is not the session timeout that fires — it is the rebalance timeout. Java's trigger is max.poll.interval.ms, 300 s by default; franz-go has no poll watchdog at all and its RebalanceTimeout defaults to 60 s, so a slow handler goes unnoticed until a rebalance actually happens — and then it has a fifth of the Java budget to finish (exp-16)." },

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
