package main

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// The package builds kgo clients, whose goroutines are reaped only if Close runs to
// completion. No test here drives one yet, so this currently guards the next test that does.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestPercentileReportsALatencySomeRecordActuallyHad(t *testing.T) {
	ms := func(n ...int) latencies {
		out := make(latencies, 0, len(n))
		for _, v := range n {
			out = append(out, time.Duration(v)*time.Millisecond)
		}
		return out
	}

	tests := []struct {
		name string
		l    latencies
		p    float64
		want time.Duration
	}{
		{name: "no samples", l: nil, p: 0.99, want: 0},
		{name: "one sample is every percentile", l: ms(7), p: 0.99, want: 7 * time.Millisecond},
		{name: "median of an odd sample", l: ms(5, 1, 3), p: 0.5, want: 3 * time.Millisecond},
		{
			// With a hundred samples the 99th percentile is the 99th smallest, not an
			// interpolation towards the maximum.
			name: "p99 of a hundred is the ninety-ninth, not the worst",
			l:    append(ms(make([]int, 98)...), ms(50, 400)...),
			p:    0.99,
			want: 50 * time.Millisecond,
		},
		{name: "unsorted input is not assumed sorted", l: ms(9, 1, 8, 2), p: 1, want: 9 * time.Millisecond},
		{name: "p0 is the fastest", l: ms(9, 1, 8, 2), p: 0, want: 1 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.l.percentile(tt.p); got != tt.want {
				t.Errorf("percentile(%v) = %v, want %v", tt.p, got, tt.want)
			}
		})
	}
}

func TestPercentileDoesNotReorderTheCallersSample(t *testing.T) {
	l := latencies{3, 1, 2}
	_ = l.percentile(0.5)
	if l[0] != 3 || l[1] != 1 || l[2] != 2 {
		t.Errorf("percentile sorted its receiver in place: %v", l)
	}
}

func TestWireRatiosSurviveEmptyRuns(t *testing.T) {
	var w wire
	if w.ratio() != 0 || w.recordsPerBatch() != 0 {
		t.Errorf("an empty run reported ratio %v and %v records per batch", w.ratio(), w.recordsPerBatch())
	}
	w = wire{Batches: 4, Records: 100, Uncompressed: 3000, Compressed: 1000}
	if w.ratio() != 3 || w.recordsPerBatch() != 25 {
		t.Errorf("got ratio %v and %v records per batch, want 3 and 25", w.ratio(), w.recordsPerBatch())
	}
}

// The compression numbers are only worth publishing if the payload is not trivially
// compressible, and only comparable across codecs if every codec saw the same bytes.
func TestPayloadsAreReproducibleAndNotPadding(t *testing.T) {
	first, second := newPayloads(17), newPayloads(17)
	var previous []byte
	for range 50 {
		k1, v1 := first.next()
		k2, v2 := second.next()
		if !bytes.Equal(k1, k2) || !bytes.Equal(v1, v2) {
			t.Fatalf("the same seed produced different payloads: %s vs %s", v1, v2)
		}
		if bytes.Equal(v1, previous) {
			t.Fatalf("two consecutive payloads were identical: %s", v1)
		}
		var decoded map[string]any
		if err := json.Unmarshal(v1, &decoded); err != nil {
			t.Fatalf("payload is not JSON: %v: %s", err, v1)
		}
		previous = v1
	}
}

func TestGridCoversEveryCombinationOnce(t *testing.T) {
	cells := grid()
	if want := len(lingers) * len(batches) * len(codecs); len(cells) != want {
		t.Fatalf("grid has %d cells, want %d", len(cells), want)
	}
	seen := make(map[string]bool, len(cells))
	for _, c := range cells {
		key := c.linger.String() + "/" + batchLabel(c.batch) + "/" + c.codec.name
		if seen[key] {
			t.Errorf("cell %s appears twice", key)
		}
		seen[key] = true
	}
}
