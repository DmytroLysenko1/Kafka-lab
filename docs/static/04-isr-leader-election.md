# 04 — ISR, leader election, and what "acknowledged" survives

**The question:** what sequence of events makes a record that Kafka already
acknowledged disappear?

KR1 · no interactive twin yet — this case is the one that justifies every default in
[01](01-write-path.md).

```mermaid
sequenceDiagram
    autonumber
    participant CT as Active controller, KRaft


    participant B1 as Broker 1, leader of p3
    participant B2 as Broker 2, follower
    participant B3 as Broker 3, follower

    Note over B1,B3: ISR is 1, 2, 3 — acks=all waits for all three
    B3--xB1: stops fetching, GC pause or a slow disk
    Note over B1: after replica.lag.time.max.ms, 30s by default
    B1->>CT: AlterPartition, shrink ISR to 1, 2
    Note over CT: the new ISR is a record in __cluster_metadata,<br/>brokers learn it by fetching metadata
    Note over B1: acks=all now means two machines, and nothing said so

    B1--xCT: broker 1 dies
    CT->>B2: you lead p3 now, leader epoch incremented
    Note over CT: the new leader comes from the ISR only, because<br/>unclean.leader.election.enable is false
    Note over B2: promotion truncates nothing:<br/>broker 2 keeps its log and serves from the LEO it already had

    B1->>B2: broker 1 returns as a follower, OffsetsForLeaderEpoch
    B2-->>B1: the offset at which the previous epoch ended
    Note over B1: broker 1 truncates to that offset, deleting what it held above it<br/>with a healthy ISR those records were never acknowledged<br/>after case 2 or case 3 below, they were
```

*Fig. 4 — the ISR shrinks quietly and the guarantee shrinks with it; the only thing
between a lagging replica and lost acknowledged writes is
`unclean.leader.election.enable=false` (exp-04).*

## Who decides what

| Decision | Who makes it | Where it is recorded |
|---|---|---|
| a follower is too slow, shrink the ISR | the partition **leader**, via `AlterPartition` | `__cluster_metadata` |
| a broker is gone, elect a new leader | the **active controller** | `__cluster_metadata` |
| which replica may become leader | the ISR, plus `unclean.leader.election.enable` | topic and broker config |
| where a returning replica truncates | the returning replica, using `leader-epoch-checkpoint` | the partition directory |

In KRaft there is no ZooKeeper. Controller nodes run a Raft quorum, one of them is
active, and cluster metadata is itself a replicated log. Brokers are not told things so
much as they fetch metadata and act on it — which is why a leadership change is visible
to clients as a `NOT_LEADER_OR_FOLLOWER` error plus a metadata refresh, not as a push.

## Why the leader epoch exists

Every leadership change increments the partition's leader epoch. A replica that comes
back compares epochs, finds the offset at which its log diverged, and truncates to it.
Before leader epochs (KIP-101), a returning replica could keep records that were never
committed, and two replicas of the same partition could disagree forever while both
believed they were correct. The `leader-epoch-checkpoint` file in
[02](02-log-segments-retention.md) is that mechanism on disk.

## The three ways an acknowledged record dies

**1. `acks=1` and a leader crash.** The leader answers after its own append. If it dies
before any follower fetches, the record was acknowledged and exists nowhere. Nothing in
the cluster reports this — the offset simply never existed for the new leader.

**2. `acks=all` with a collapsed ISR.** The ISR shrank to one, `acks=all` kept
succeeding, and that one machine died. Identical outcome, reached through a
configuration that reads as safe. `min.insync.replicas=2` is what prevents it: the
producer gets `NOT_ENOUGH_REPLICAS` and the payment fails loudly instead of vanishing
quietly.

**3. Unclean leader election.** Every ISR member is dead and an out-of-sync replica is
promoted. It becomes the source of truth while missing acknowledged records, and the
records are not "lost in transit" — they are deleted from the log when the old leader
returns and truncates to the new leader's epoch.

With `unclean.leader.election.enable=false` the partition stays **offline** instead.
Availability is sacrificed on purpose: a payment system that cannot write is an
incident, a payment system that silently deletes captures is a disaster. Writing that
trade-off down explicitly is more valuable than the setting itself.

## How to reproduce it honestly

`docker compose kill`, never `stop`. A graceful stop migrates leadership cleanly and
demonstrates nothing — the interesting behaviour only appears when the process dies
without warning.

## To be measured

| Run | What it shows | Status |
|---|---|---|
| exp-04 | leader killed under load: under-replicated partitions, new leader, ISR recovery time | TBD |
| exp-04b | `unclean.leader.election.enable=true`: acknowledged records missing after promotion, counted | TBD |
| exp-08 | `acks=1` vs `acks=all` with `min.insync.replicas=2` under the same kill | TBD |
