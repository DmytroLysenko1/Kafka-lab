package main

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/kafka"
)

func TestTopicSpecRejectsWhatWouldSilentlyCreateTheWrongTopic(t *testing.T) {
	type args struct {
		name        string
		partitions  int
		replication int
		settings    configFlag
	}

	tests := []struct {
		name    string
		args    args
		want    kafka.TopicSpec
		wantErr error
	}{
		{
			name: "an experiment topic carries its own shape and configs",
			args: args{
				name:        "exp03.compaction",
				partitions:  1,
				replication: 3,
				settings:    configFlag{"cleanup.policy": "compact", "segment.bytes": "1048576"},
			},
			want: kafka.TopicSpec{
				Name:              "exp03.compaction",
				Partitions:        1,
				ReplicationFactor: 3,
				Config:            map[string]string{"cleanup.policy": "compact", "segment.bytes": "1048576"},
			},
		},
		{
			name: "no configs means no configs, not an empty override set",
			args: args{name: "exp01.keys", partitions: 6, replication: 3, settings: configFlag{}},
			want: kafka.TopicSpec{Name: "exp01.keys", Partitions: 6, ReplicationFactor: 3},
		},
		{
			name:    "a missing name would create a topic called the empty string",
			args:    args{name: "", partitions: 1, replication: 3},
			wantErr: errTopicName,
		},
		{
			name:    "zero partitions would silently fall back to the broker default",
			args:    args{name: "exp03.compaction", partitions: 0, replication: 3},
			wantErr: errTopicShape,
		},
		{
			name:    "a replication factor below one is rejected here, not halfway through an experiment",
			args:    args{name: "exp03.compaction", partitions: 1, replication: 0},
			wantErr: errTopicShape,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := topicSpec(tt.args.name, tt.args.partitions, tt.args.replication, tt.args.settings)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("spec mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestConfigFlagKeepsValuesThatContainCommas(t *testing.T) {
	tests := []struct {
		name    string
		raw     []string
		want    configFlag
		wantErr error
	}{
		{
			name: "a compaction policy with two modes survives as one value",
			raw:  []string{"cleanup.policy=compact,delete"},
			want: configFlag{"cleanup.policy": "compact,delete"},
		},
		{
			name: "repeating the flag collects every setting",
			raw:  []string{"cleanup.policy=compact", "segment.bytes=1048576"},
			want: configFlag{"cleanup.policy": "compact", "segment.bytes": "1048576"},
		},
		{
			name:    "a setting without a value is a typo, not an empty override",
			raw:     []string{"cleanup.policy"},
			want:    configFlag{},
			wantErr: errTopicConfig,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := configFlag{}
			var err error
			for _, raw := range tt.raw {
				if setErr := got.Set(raw); setErr != nil {
					err = setErr
					break
				}
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("config mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestParseBrokersRejectsAListWithAHoleInIt(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    []string
		wantErr error
	}{
		{
			name: "whitespace around entries is a copy-paste artefact, not part of the address",
			raw:  " localhost:19092, localhost:29092 ,localhost:39092\n",
			want: []string{"localhost:19092", "localhost:29092", "localhost:39092"},
		},
		{
			name:    "a trailing comma would hand the client an empty seed broker",
			raw:     "localhost:19092,",
			wantErr: errBrokerList,
		},
		{
			name:    "a doubled comma is the same hole in the middle",
			raw:     "localhost:19092,,localhost:39092",
			wantErr: errBrokerList,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseBrokers(tt.raw)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("seeds mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
