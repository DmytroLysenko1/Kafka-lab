Walkthrough.register({
  id: "rebalance",
  eyebrow: "Kafka-lab · KR4 · rebalance strategies",
  title: "Rebalance: eager vs cooperative",
  hint: "The number that matters is downtime, not how often the group rebalances.",
  lanes: [
    { id: "a",     name: "consumer A" },
    { id: "coord", name: "Group Coordinator" },
    { id: "b",     name: "consumer B (new)" }
  ],
  steps: [
    { kind: "msg", from: "b", to: "coord", label: "JoinGroup",
      t: "A second consumer appears",
      d: "A deploy, a scale-up, or a restarted pod. Membership changed, so the partition assignment has to change with it." },

    { kind: "note", at: "coord", lines: ["rebalance triggered:", "new generation"],
      t: "The group has to agree again",
      d: "The coordinator cannot simply hand a partition to B — A might still be processing it. Every member must acknowledge the new generation first." },

    { kind: "msg", from: "coord", to: "a", label: "heartbeat: REBALANCE_IN_PROGRESS", reply: true,
      t: "A finds out on its next heartbeat",
      d: "There is no push channel to a consumer. The coordinator answers A's regular heartbeat with a flag saying the group is reorganising." },

    { kind: "note", at: "a", warn: true, lines: ["eager: revoke ALL partitions", "stop processing entirely"],
      t: "Eager: stop-the-world",
      d: "With the range or roundrobin assignors, A gives up every partition it holds — including the ones it is about to receive straight back — and stops processing completely.",
      r: "Downtime scales with the size of the group, not with how much is actually moving. Adding one consumer to a group of ten stops all ten." },

    { kind: "msg", from: "a", to: "coord", label: "JoinGroup (rejoin)",
      t: "Everyone re-enters the group",
      d: "A rejoins with no assignment and waits. During this window nothing in the group is being processed at all." },

    { kind: "msg", from: "coord", to: "a", label: "SyncGroup: p0, p1", reply: true,
      t: "The new assignment arrives",
      d: "Only now can processing resume. The gap between the revoke and this arrow is precisely what exp-14 measures." },

    { kind: "note", at: "a", lines: ["cooperative: revoke only", "partitions actually moving"],
      t: "Cooperative: revoke the minimum",
      d: "With cooperative-sticky, A keeps processing everything it is not losing. Only the partitions genuinely being transferred are revoked, in a second short round.",
      r: "The trade is one extra rebalance round for near-zero downtime. It is almost always worth taking — switching assignors on a running group needs a rolling upgrade, not a config flip." },

    { kind: "note", at: ["a", "b"], lines: ["measure the gap,", "not the rebalance count"],
      t: "What to actually measure",
      d: "Rebalance frequency on its own says nothing. The number worth reporting is processing downtime — how long the group made no progress. exp-14 records both assignors under identical load.",
      r: "A group that rebalances often but never stalls is healthier than one that rebalances rarely and freezes for thirty seconds when it does." }
  ]
});
