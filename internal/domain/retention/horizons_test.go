package retention_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/retention"
)

const day = 24 * time.Hour

// An inbox that forgets an event id while the event can still be delivered again counts that
// payment twice. The shortest safe horizon is a letter's life in the dead letter topic plus a
// record's life in a retry tier, each its retention plus one daily segment — 31 + 8 days — and
// a day more for the clocks of the API, the consumer and prune not agreeing.
func TestAnInboxHorizonShorterThanARedeliveryIsRefused(t *testing.T) {
	type args struct {
		inbox, outbox time.Duration
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{name: "the longest redelivery plus a day for clocks that disagree", args: args{inbox: 40 * day, outbox: 7 * day}},
		{name: "exactly the longest redelivery, no room for clock skew", args: args{inbox: 39 * day, outbox: 7 * day}, wantErr: retention.ErrInboxHorizonTooShort},
		{name: "the retentions without their segments", args: args{inbox: 37 * day, outbox: 7 * day}, wantErr: retention.ErrInboxHorizonTooShort},
		{name: "an outbox kept for no time", args: args{inbox: 40 * day, outbox: 0}, wantErr: retention.ErrOutboxHorizon},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := retention.NewHorizons(tt.args.inbox, tt.args.outbox)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// The cutoffs are the horizons counted back from now: a row older than the cutoff is past its
// horizon.
func TestTheCutoffsCountTheHorizonsBackFromNow(t *testing.T) {
	horizons, err := retention.NewHorizons(40*day, 7*day)
	if err != nil {
		t.Fatalf("horizons: %v", err)
	}
	now := time.Date(2026, time.September, 27, 12, 0, 0, 0, time.UTC)

	got := []time.Time{horizons.InboxCutoff(now), horizons.OutboxCutoff(now)}

	if diff := cmp.Diff([]time.Time{now.Add(-40 * day), now.Add(-7 * day)}, got); diff != "" {
		t.Errorf("cutoffs (-want +got):\n%s", diff)
	}
}

// An event is counted no sooner than it happened, and its inbox row lives at least 39 days
// after it was counted: so an event younger than 39 days is remembered if it was ever
// counted, and one older may not be.
func TestAnEventCanBeDeduplicatedOnlyWhileTheInboxIsSureToRememberIt(t *testing.T) {
	now := time.Date(2026, time.September, 27, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		args time.Duration
		want bool
	}{
		{name: "just happened", args: 0, want: true},
		{name: "a second inside 39 days", args: 39*day - time.Second, want: true},
		{name: "exactly 39 days", args: 39 * day, want: false},
		{name: "never stamped", args: now.Sub(time.Time{}), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, retention.CanBeDeduplicated(now.Add(-tt.args), now)); diff != "" {
				t.Errorf("age %s (-want +got):\n%s", tt.args, diff)
			}
		})
	}
}
