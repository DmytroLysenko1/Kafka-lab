package main

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

func dir(broker int32, topic string, sizes map[int32]int64) kadm.DescribedLogDir {
	partitions := make(map[int32]kadm.DescribedLogDirPartition, len(sizes))
	for partition, size := range sizes {
		partitions[partition] = kadm.DescribedLogDirPartition{
			Broker: broker, Topic: topic, Partition: partition, Size: size,
		}
	}
	return kadm.DescribedLogDir{
		Broker: broker,
		Dir:    "/var/lib/kafka/data",
		Topics: kadm.DescribedLogDirTopics{topic: partitions},
	}
}

func TestAnsweredEveryBrokerRefusesADescriptionThatWouldUndercountTheMove(t *testing.T) {
	const topic = "exp15.move"

	tests := []struct {
		name    string
		dirs    kadm.DescribedAllLogDirs
		wantErr error
	}{
		{
			name: "three brokers that answered are a description worth measuring",
			dirs: kadm.DescribedAllLogDirs{
				1: {"/d": dir(1, topic, map[int32]int64{0: 20 << 20})},
				2: {"/d": dir(2, topic, map[int32]int64{0: 20 << 20})},
				3: {"/d": dir(3, topic, map[int32]int64{0: 20 << 20})},
			},
		},
		{
			// The broker that failed is the interesting one: during a reassignment it is
			// the one being written to, so its bytes are exactly the bytes being measured.
			name: "a broker that could not describe its dirs refuses the whole reading",
			dirs: kadm.DescribedAllLogDirs{
				1: {"/d": dir(1, topic, map[int32]int64{0: 20 << 20})},
				2: {"/d": {Broker: 2, Dir: "/d", Err: kerr.KafkaStorageError}},
			},
			wantErr: errReassign,
		},
		{
			// DescribeAllLogDirs answering nothing at all sums to zero bytes, and zero
			// bytes copied is a move that finished instantly — the shape of the very
			// result this experiment reports.
			name:    "a description nobody answered is not a topic of size zero",
			dirs:    kadm.DescribedAllLogDirs{},
			wantErr: errReassign,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := answeredEveryBroker(tt.dirs); !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestStoredBytesSumsOnlyTheTopicUnderTest(t *testing.T) {
	const topic = "exp15.move"

	dirs := kadm.DescribedAllLogDirs{
		1: {"/d": dir(1, topic, map[int32]int64{0: 10, 1: 20})},
		2: {"/d": dir(2, topic, map[int32]int64{0: 10, 1: 20})},
		3: {"/d": dir(3, "payments.main", map[int32]int64{0: 9_999})},
	}

	const want = 60
	if got := storedBytes(dirs, topic); got != want {
		t.Errorf("stored = %d, want %d", got, want)
	}
}
