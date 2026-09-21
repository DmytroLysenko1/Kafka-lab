Walkthrough.register({
  id: "rebalance",
  eyebrow: "Kafka-lab · KR2+KR4 · rebalance strategies",
  title: "Rebalance: eager vs cooperative",
  hint: "Two runs of the same event. The number that matters is downtime, not how often the group rebalances.",
  lanes: [
    { id: "a",     name: "consumer A (p0..p3)" },
    { id: "coord", name: "Group Coordinator" },
    { id: "b",     name: "consumer B (new)" }
  ],
  steps: [
    { kind: "msg", from: "b", to: "coord", label: "JoinGroup",
      t: "A second consumer appears",
      d: "A deploy, a scale-up, or a restarted pod. Membership changed, so the partition assignment has to change with it. A holds p0 to p3; B holds nothing yet.",
      r: "Everything below is the classic protocol — JoinGroup, SyncGroup, generations, assignment computed by the group leader. KIP-848 (GA in Kafka 4.0) replaces it with incremental broker-side assignment over ConsumerGroupHeartbeat, which changes the mechanics of both runs though not the metric that matters." },

    { kind: "note", at: "coord", lines: ["rebalance triggered:", "new generation"],
      t: "The group has to agree again",
      d: "The coordinator cannot simply hand a partition to B — A might still be processing it. Every member must acknowledge the new generation first." },

    { kind: "msg", from: "coord", to: "a", label: "heartbeat: REBALANCE_IN_PROGRESS", reply: true,
      t: "A finds out on its next heartbeat",
      d: "There is no push channel to a consumer. The coordinator answers A's regular heartbeat with a flag saying the group is reorganising." },

    { kind: "span", at: ["a", "b"], covers: 4, warn: true, label: "run 1, eager: nothing in the group is processed",
      t: "Eager: stop-the-world",
      d: "With the range or roundrobin assignors the whole group goes quiet for the entire width of this frame. Everything inside it is overhead — no record is handled by anybody until the last arrow lands.",
      r: "Downtime scales with the size of the group, not with how much is actually moving. Adding one consumer to a group of ten stops all ten." },

    { kind: "note", at: "a", warn: true, lines: ["revoke ALL four partitions", "stop processing entirely"],
      t: "A gives up everything, including what it keeps",
      d: "A revokes p0 to p3 — and it is about to receive p0 and p1 straight back. Those two revocations bought nothing at all, and in any group larger than two they are the majority of the movement." },

    { kind: "msg", from: "a", to: "coord", label: "JoinGroup with no assignment",
      t: "Everyone re-enters the group",
      d: "A rejoins holding nothing and waits for the new plan. This is the widest part of the outage." },

    { kind: "msg", from: "coord", to: "a", label: "SyncGroup: p0, p1", reply: true,
      t: "A gets half of what it just gave up",
      d: "Only now can A resume, and only on two of the four partitions it was already serving a moment ago." },

    { kind: "msg", from: "coord", to: "b", label: "SyncGroup: p2, p3", reply: true,
      t: "B finally starts working",
      d: "The group is whole again. exp-14 measures the gap from the revocation to the first record handled after this arrow: with two members on a local network it was 45–61 ms, no longer than an ordinary poll cycle." },

    { kind: "msg", from: "a", to: "coord", label: "JoinGroup keeping the current assignment",
      t: "Run 2, cooperative — same event, different protocol",
      d: "Rewind to the same heartbeat, this time with cooperative-sticky. A rejoins while still processing p0 to p3: joining a rebalance no longer means giving anything up.",
      r: "Switching assignors on a running group is a rolling upgrade, not a config flip: deploy once with both the old and the new assignor configured, then again with the old one removed. Flipping straight across a live group leaves members unable to agree on a protocol." },

    { kind: "msg", from: "coord", to: "a", label: "SyncGroup: p0, p1 only", reply: true,
      t: "The plan arrives while work continues",
      d: "A learns its new share is p0 and p1. Nothing has stopped yet — A is still serving all four partitions and now knows which two it has to release." },

    { kind: "span", at: ["a", "b"], covers: 3, warn: true, label: "run 2, cooperative: only p2 and p3 are idle",
      t: "The downtime that is left",
      d: "This frame is the same kind of outage as the one above, but it covers two partitions instead of four, and A never stopped serving p0 and p1 at any point inside it.",
      r: "The trade is one extra rebalance round for downtime that tracks the partitions actually transferred instead of the size of the group. It is almost always worth taking." },

    { kind: "note", at: "a", warn: true, lines: ["revoke p2 and p3 only", "p0 and p1 never stop"],
      t: "Revoke the minimum",
      d: "A releases exactly what is moving and keeps processing the rest. This is the entire difference between the two runs." },

    { kind: "msg", from: "a", to: "coord", label: "JoinGroup, triggering round two",
      t: "The second round is the price",
      d: "Revoked partitions can only be handed over in a following rebalance, so cooperative-sticky pays one extra round trip to avoid stopping the group." },

    { kind: "msg", from: "coord", to: "b", label: "SyncGroup: p2, p3", reply: true,
      t: "B starts, and the group never went idle",
      d: "Same end state as run 1: A on p0 and p1, B on p2 and p3. Only the size of the red frame differs — and that frame is the deliverable." },

    { kind: "note", at: ["a", "b"], lines: ["measure the gap,", "not the rebalance count"],
      t: "What to actually measure",
      d: "Rebalance frequency on its own says nothing. The number worth reporting is processing downtime — how long each partition went unhandled. exp-14 measured it under identical load: eager stopped all six partitions for 45–61 ms, cooperative stopped only the three that moved, for 0.51–0.68 s, and KIP-848 stopped those three for 5.0–6.5 s, its heartbeat interval.",
      r: "A group that rebalances often but never stalls is healthier than one that rebalances rarely and freezes for thirty seconds when it does. Static membership (group.instance.id) plus a session timeout longer than a pod restart removes most rebalances from a rolling deploy entirely." }
  ]
});
