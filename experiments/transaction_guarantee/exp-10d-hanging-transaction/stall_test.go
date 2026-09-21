package main

import (
	"testing"
	"time"

	"go.uber.org/goleak"
)

// The package builds kgo clients, whose goroutines are reaped only if Close runs to
// completion. No test here drives one yet, so this currently guards the next test that does.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestStallHeldOnlyWhenTheOpenTransactionBlockedTheCommittedReader(t *testing.T) {
	timeout := 20 * time.Second

	tests := []struct {
		name string
		s    stall
		want bool
	}{
		{
			name: "committed reader waited out the timeout while the uncommitted one did not",
			s:    stall{Stable: 0, Newest: 101, Uncommitted: 200 * time.Millisecond, Committed: 19 * time.Second},
			want: true,
		},
		{
			// If the stable offset caught up with the watermark there was nothing open to
			// block behind, whatever the timings say.
			name: "no gap between the stable offset and the watermark means no open transaction",
			s:    stall{Stable: 101, Newest: 101, Uncommitted: 200 * time.Millisecond, Committed: 19 * time.Second},
			want: false,
		},
		{
			name: "both readers saw the records at once",
			s:    stall{Stable: 0, Newest: 101, Uncommitted: 200 * time.Millisecond, Committed: 300 * time.Millisecond},
			want: false,
		},
		{
			// A slow cluster can make both readers slow; only the difference is the stall.
			name: "both readers slow by the same amount is latency, not the transaction",
			s:    stall{Stable: 0, Newest: 101, Uncommitted: 15 * time.Second, Committed: 16 * time.Second},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.held(timeout); got != tt.want {
				t.Errorf("held() = %v, want %v", got, tt.want)
			}
		})
	}
}
