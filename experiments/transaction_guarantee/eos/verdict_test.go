package eos

import (
	"testing"

	"go.uber.org/goleak"
)

// The package builds a transactional session, kgo clients and a pgxpool, whose goroutines
// are reaped only if Close runs to completion. No test here drives one yet, so this
// currently guards the next test that does.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestJudgeRequiresBothHalvesOfEachClaim(t *testing.T) {
	exact := view{Rows: 1000, Distinct: 1000}
	doubled := view{Rows: 1050, Distinct: 1000}

	tests := []struct {
		name  string
		claim Claim
		got   views
		want  bool
	}{
		{
			name:  "10a: read_committed exactly once",
			claim: KafkaExactlyOnce,
			got:   views{Produced: 1000, Committed: exact, Uncommitted: doubled},
			want:  true,
		},
		{
			// The gap the audit found: with no aborted batch in the log, the crash never
			// landed inside a transaction, and an exact output proves nothing about rollback.
			name:  "10a: an exact output with nothing aborted proves nothing",
			claim: KafkaExactlyOnce,
			got:   views{Produced: 1000, Committed: exact, Uncommitted: exact},
			want:  false,
		},
		{
			name:  "10a: a duplicate in the committed output breaks exactly-once",
			claim: KafkaExactlyOnce,
			got:   views{Produced: 1000, Committed: doubled},
			want:  false,
		},
		{
			name:  "10a: a payment missing from the committed output breaks it too",
			claim: KafkaExactlyOnce,
			got:   views{Produced: 1000, Committed: view{Rows: 999, Distinct: 999}},
			want:  false,
		},
		{
			name:  "10b: Kafka exact and the database doubled — the transaction stopped at the boundary",
			claim: DatabaseNotCovered,
			got:   views{Produced: 1000, Committed: exact, Database: doubled},
			want:  true,
		},
		{
			// The run that would read as a success and show nothing: if the crash never
			// landed mid-transaction, the database is clean too and the claim is unproven.
			name:  "10b: a clean database proves nothing about where the boundary is",
			claim: DatabaseNotCovered,
			got:   views{Produced: 1000, Committed: exact, Database: exact},
			want:  false,
		},
		{
			name:  "10b: a doubled database with a broken Kafka output is a different failure",
			claim: DatabaseNotCovered,
			got:   views{Produced: 1000, Committed: doubled, Database: doubled},
			want:  false,
		},
		{
			name:  "10c: read_uncommitted handed the aborted records while read_committed stayed exact",
			claim: AbortedVisibleUncommitted,
			got:   views{Produced: 1000, Committed: exact, Uncommitted: doubled},
			want:  true,
		},
		{
			name:  "10c: nothing aborted means nothing to be handed",
			claim: AbortedVisibleUncommitted,
			got:   views{Produced: 1000, Committed: exact, Uncommitted: exact},
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := judge(tt.claim, tt.got); got != tt.want {
				t.Errorf("judge() = %v, want %v", got, tt.want)
			}
		})
	}
}
