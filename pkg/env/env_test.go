package env_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/Kafka-lab/pkg/env"
)

const variable = "KAFKA_LAB_ENV_TEST"

// A list that is only separators is a deployment typo, not an empty cluster: a service that
// started with no brokers would fail on its first send, far from the manifest that caused it.
func TestAListIsTrimmedAndAnEmptyOneIsMissing(t *testing.T) {
	type args struct {
		value string
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr error
	}{
		{name: "three brokers with spaces", args: args{value: " a:1, b:2 ,c:3"}, want: []string{"a:1", "b:2", "c:3"}},
		{name: "a trailing comma", args: args{value: "a:1,"}, want: []string{"a:1"}},
		{name: "only commas", args: args{value: " , ,"}, wantErr: env.ErrMissing},
		{name: "unset", args: args{value: ""}, wantErr: env.ErrMissing},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(variable, tt.args.value)
			var read env.Reader
			got := read.List(variable)
			if err := read.Err(); !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("list (-want +got):\n%s", diff)
			}
		})
	}
}

// A batch of zero would make the relay sweep nothing forever and report success each time.
func TestANumberThatIsNotPositiveIsRefusedAndAnUnsetOneTakesTheFallback(t *testing.T) {
	type args struct {
		value string
	}
	tests := []struct {
		name    string
		args    args
		want    int
		wantErr error
	}{
		{name: "unset: the fallback", args: args{value: ""}, want: 100},
		{name: "a positive number", args: args{value: "25"}, want: 25},
		{name: "zero", args: args{value: "0"}, wantErr: env.ErrInvalid},
		{name: "negative", args: args{value: "-5"}, wantErr: env.ErrInvalid},
		{name: "not a number", args: args{value: "lots"}, wantErr: env.ErrInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(variable, tt.args.value)
			var read env.Reader
			got := read.PositiveInt(variable, 100)
			if err := read.Err(); !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("value (-want +got):\n%s", diff)
			}
		})
	}
}

// A zero interval makes a ticker panic; a bare number is a typo for a unit nobody wrote.
func TestADurationThatIsNotPositiveIsRefusedAndAnUnsetOneTakesTheFallback(t *testing.T) {
	type args struct {
		value string
	}
	tests := []struct {
		name    string
		args    args
		want    time.Duration
		wantErr error
	}{
		{name: "unset: the fallback", args: args{value: ""}, want: time.Second},
		{name: "half a second", args: args{value: "500ms"}, want: 500 * time.Millisecond},
		{name: "zero", args: args{value: "0s"}, wantErr: env.ErrInvalid},
		{name: "a number without a unit", args: args{value: "5"}, wantErr: env.ErrInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(variable, tt.args.value)
			var read env.Reader
			got := read.PositiveDuration(variable, time.Second)
			if err := read.Err(); !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("value (-want +got):\n%s", diff)
			}
		})
	}
}

// A manifest with two mistakes must say both: stopping at the first sends the operator
// through one failed deploy per mistake.
func TestEveryProblemInTheEnvironmentIsReportedAtOnce(t *testing.T) {
	t.Setenv("KAFKA_LAB_ENV_TEST_URL", "")
	t.Setenv("KAFKA_LAB_ENV_TEST_BATCH", "zero")

	var read env.Reader
	read.Required("KAFKA_LAB_ENV_TEST_URL")
	read.PositiveInt("KAFKA_LAB_ENV_TEST_BATCH", 100)

	err := read.Err()
	want := "env: required variable is not set: KAFKA_LAB_ENV_TEST_URL\n" +
		`env: variable is set to something unusable: KAFKA_LAB_ENV_TEST_BATCH="zero", want a positive whole number`
	if diff := cmp.Diff(want, err.Error()); diff != "" {
		t.Errorf("message (-want +got):\n%s", diff)
	}
	if !errors.Is(err, env.ErrMissing) || !errors.Is(err, env.ErrInvalid) {
		t.Errorf("err = %v, want both %v and %v inside it", err, env.ErrMissing, env.ErrInvalid)
	}
}
