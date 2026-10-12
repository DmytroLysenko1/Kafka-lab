package kafka

import (
	"testing"
	"time"

	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/postgres"
	"github.com/DmytroLysenko1/Kafka-lab/pkg/telemetry"
)

// The timeouts are set in four files; raising any one of them past this sum turns a slow
// record into a member thrown out of its group, which no other test would notice. After its
// handling a record either commits its transaction, or rolls it back and takes a detour —
// never both — each on a bound handlingTimeout does not cover; the slower of the two paths is
// what the batch has to fit.
//
// The bound holds for a broker that is gone, which is what exp-04 and exp-13 measured: its
// connection breaks and every in-flight request fails at once. A broker that hangs instead
// holds an idempotent produce request it has already been sent until the request's own
// deadline — ProduceRequestTimeout plus RequestTimeoutOverhead, 20 s by franz-go's defaults —
// which a detour cannot cut short; in that case a slow batch can overrun the rebalance timeout,
// the member is removed, and the inbox absorbs what it handled twice.
func TestTheSlowestBatchStillFinishesInsideTheRebalanceTimeout(t *testing.T) {
	afterHandling := max(postgres.CommitTimeout, postgres.RollbackTimeout+detourTimeout)
	slowest := batchBudget + handlingTimeout + afterHandling + commitTimeout
	if slowest >= rebalanceTimeout {
		t.Errorf("batch budget %s + handling %s + max(commit %s, rollback %s + detour %s) + offset commit %s = %s, must stay under the %s rebalance timeout",
			batchBudget, handlingTimeout, postgres.CommitTimeout, postgres.RollbackTimeout, detourTimeout, commitTimeout, slowest, rebalanceTimeout)
	}
}

// podGracePeriod is the consumer's terminationGracePeriodSeconds (deploy/helm/payments/values.yaml):
// past it the kubelet sends SIGKILL, and whatever the stop had not finished is lost.
const podGracePeriod = 30 * time.Second

// On SIGTERM the record in flight settles — its rollback, then a detour to a retry tier — the
// handled part of the batch commits its offsets, and the last spans are flushed. A stop that
// overran the grace period would be killed between the detour and the offset commit, and the
// records it had already handled would be handled again.
func TestAStoppingConsumerFinishesInsideItsGracePeriod(t *testing.T) {
	stop := postgres.RollbackTimeout + detourTimeout + commitTimeout + telemetry.FlushTimeout
	if stop >= podGracePeriod {
		t.Errorf("rollback %s + detour %s + offset commit %s + trace flush %s = %s, must stay under the pod's %s grace period",
			postgres.RollbackTimeout, detourTimeout, commitTimeout, telemetry.FlushTimeout, stop, podGracePeriod)
	}
}
