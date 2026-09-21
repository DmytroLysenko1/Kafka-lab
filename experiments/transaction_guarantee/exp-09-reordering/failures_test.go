package main

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestFailuresSeparatesTheTwoWaysARecordIsWrittenTwice(t *testing.T) {
	type entry struct {
		msg     string
		keyvals []any
	}
	const clean = "written, success ignored, resent\t0 records — rewound behind a failed batch"

	tests := []struct {
		name string
		args []entry
		want []string
	}{
		{
			name: "a clean run says nothing was retried, not an empty section",
			args: []entry{{msgProduced, []any{"broker", "1", keyProduced, "exp09.payments[0{0=>40}]"}}},
			want: []string{clean, "retried batches\tnone"},
		},
		{
			// The distinction the audit asked for: a timeout is answered after the append,
			// so its retry is a duplicate; NOT_ENOUGH_REPLICAS is answered before it.
			name: "retries are counted by the error that caused them, in batches and records",
			args: []entry{
				{msgProduced, []any{keyProduced, "exp09.payments[0{retrying@100,40(REQUEST_TIMED_OUT: The request timed out.)}]"}},
				{msgProduced, []any{keyProduced, "exp09.payments[0{retrying@140,10(REQUEST_TIMED_OUT: The request timed out.)}]"}},
				{msgProduced, []any{keyProduced, "exp09.payments[0{retrying@-1,30(NOT_ENOUGH_REPLICAS: Messages are rejected since there are fewer in-sync replicas than required.)}]"}},
			},
			want: []string{
				clean,
				"retried, NOT_ENOUGH_REPLICAS\t1 batches, 30 records — refused before the append",
				"retried, REQUEST_TIMED_OUT\t2 batches, 50 records — appended before the refusal: the retry duplicates it unless the producer is idempotent",
			},
		},
		{
			name: "a success ignored behind a rewind is counted as records written that will be resent",
			args: []entry{{msgProduced, []any{keyProduced, "exp09.payments[0{skipped@200=>240}, 1{skipped@7=>9}]"}}},
			want: []string{
				"written, success ignored, resent\t42 records — rewound behind a failed batch",
				"retried batches\tnone",
			},
		},
		{
			// A failed batch behind an earlier failure is resent like the first one, and a
			// timed-out one may already be in the log: it is counted by its error, not dropped.
			name: "a failed batch behind an earlier failure is counted by its error",
			args: []entry{{msgProduced, []any{keyProduced, "exp09.payments[0{skipped@-1,40(REQUEST_TIMED_OUT: x)}]"}}},
			want: []string{clean, "retried, REQUEST_TIMED_OUT\t1 batches, 40 records — appended before the refusal: the retry duplicates it unless the producer is idempotent"},
		},
		{
			name: "a request with no response is counted apart from batch errors",
			args: []entry{
				{requestFailed[0], []any{"broker", "1", "err", errors.Join(errors.New("read"), kerr.RequestTimedOut)}},
				{requestFailed[1], []any{"err", errors.New("connection reset")}},
			},
			want: []string{
				clean,
				"request failed, REQUEST_TIMED_OUT\t1 — no response, so whether it landed is unknown",
				"request failed, connection reset\t1 — no response, so whether it landed is unknown",
			},
		},
		{
			name: "other client messages and a missing key are ignored",
			args: []entry{
				{"rewinding produce sequence to resend pending batches", []any{"err", kerr.RequestTimedOut}},
				{msgProduced, []any{"broker", "1"}},
			},
			want: []string{clean, "retried batches\tnone"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seen := newFailures()
			for _, e := range tt.args {
				seen.Log(kgo.LogLevelDebug, e.msg, e.keyvals...)
			}
			if diff := cmp.Diff(tt.want, seen.lines()); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
