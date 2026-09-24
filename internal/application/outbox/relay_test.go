package outbox

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

var (
	errBroker = errors.New("the broker is not answering")
	errCommit = errors.New("the transaction could not be committed")
)

// fakeOutbox is the store and the transaction manager at once, as Postgres is: what is
// written inside WithinTx lands only when it commits, so a test can tell a record that was
// published-and-forgotten from one that was never published.
type fakeOutbox struct {
	mu         sync.Mutex
	records    []Record
	published  map[int64]time.Time
	staged     []func()
	failCommit error
}

func newFakeOutbox(records ...Record) *fakeOutbox {
	return &fakeOutbox{records: records, published: make(map[int64]time.Time)}
}

func (f *fakeOutbox) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.staged = nil
	if err := fn(ctx); err != nil {
		f.staged = nil
		return err
	}
	if f.failCommit != nil {
		f.staged = nil
		return f.failCommit
	}
	for _, apply := range f.staged {
		apply()
	}
	f.staged = nil
	return nil
}

func (f *fakeOutbox) Claim(_ context.Context, limit int) ([]Record, error) {
	claimed := make([]Record, 0, limit)
	for _, record := range f.records {
		if _, gone := f.published[record.ID]; gone {
			continue
		}
		if len(claimed) == limit {
			break
		}
		claimed = append(claimed, record)
	}
	return claimed, nil
}

func (f *fakeOutbox) MarkPublished(_ context.Context, ids []int64, at time.Time) error {
	f.staged = append(f.staged, func() {
		for _, id := range ids {
			if _, already := f.published[id]; already {
				continue
			}
			f.published[id] = at
		}
	})
	return nil
}

func (f *fakeOutbox) unpublished() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.records) - len(f.published)
}

func (f *fakeOutbox) breakCommit(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failCommit = err
}

type fakePublisher struct {
	mu      sync.Mutex
	batches [][]int64
	fail    error
}

func (p *fakePublisher) breaks(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fail = err
}

func (p *fakePublisher) published() [][]int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.batches)
}

func (p *fakePublisher) Publish(_ context.Context, records []Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	sent := make([]int64, 0, len(records))
	for _, record := range records {
		sent = append(sent, record.ID)
	}
	p.batches = append(p.batches, sent)
	if p.fail != nil {
		return p.fail
	}
	return nil
}

func authorized(id int64) Record {
	return Record{
		ID:          id,
		AggregateID: "11111111-1111-4111-8111-11111111111" + string(rune('0'+id)),
		EventType:   EventTypeAuthorized,
		Payload:     []byte(`{"amount_minor":1999}`),
		OccurredAt:  time.Date(2026, time.September, 25, 9, 0, 0, 0, time.UTC),
	}
}

func harness(t *testing.T, outbox *fakeOutbox, publisher *fakePublisher, batch int) *Relay {
	t.Helper()
	swept := time.Date(2026, time.September, 25, 12, 0, 0, 0, time.UTC)
	return NewRelay(outbox, publisher, outbox, func() time.Time { return swept }, batch, slog.New(slog.DiscardHandler))
}

func TestSweepPublishesTheClaimedRecordsAndMarksThem(t *testing.T) {
	outbox := newFakeOutbox(authorized(1), authorized(2), authorized(3))
	publisher := &fakePublisher{}
	relay := harness(t, outbox, publisher, 10)

	published, err := relay.Sweep(t.Context())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}

	if diff := cmp.Diff(3, published); diff != "" {
		t.Errorf("records published (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([][]int64{{1, 2, 3}}, publisher.published()); diff != "" {
		t.Errorf("published batches (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(0, outbox.unpublished()); diff != "" {
		t.Errorf("records still waiting (-want +got):\n%s", diff)
	}
}

func TestSweepTakesNoMoreThanOneBatch(t *testing.T) {
	outbox := newFakeOutbox(authorized(1), authorized(2), authorized(3))
	publisher := &fakePublisher{}
	relay := harness(t, outbox, publisher, 2)

	published, err := relay.Sweep(t.Context())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}

	if diff := cmp.Diff(2, published); diff != "" {
		t.Errorf("records published (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(1, outbox.unpublished()); diff != "" {
		t.Errorf("records left for the next sweep (-want +got):\n%s", diff)
	}
}

// Marking a record published that the broker refused would lose the event for good, so the
// failure must leave the row exactly as it found it — and the next sweep must pick it up.
func TestSweepWhenTheBrokerRefusesKeepsTheRecordsForTheNextSweep(t *testing.T) {
	outbox := newFakeOutbox(authorized(1), authorized(2))
	publisher := &fakePublisher{fail: errBroker}
	relay := harness(t, outbox, publisher, 10)

	published, err := relay.Sweep(t.Context())
	if !errors.Is(err, errBroker) {
		t.Fatalf("err = %v, want %v", err, errBroker)
	}
	if diff := cmp.Diff(0, published); diff != "" {
		t.Errorf("records reported published (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(2, outbox.unpublished()); diff != "" {
		t.Errorf("records still waiting (-want +got):\n%s", diff)
	}

	publisher.breaks(nil)
	if _, err := relay.Sweep(t.Context()); err != nil {
		t.Fatalf("second sweep: %v", err)
	}
	if diff := cmp.Diff(0, outbox.unpublished()); diff != "" {
		t.Errorf("records still waiting after the broker returned (-want +got):\n%s", diff)
	}
}

// The window this relay is built around: the broker took the records and the transaction
// that would have recorded that did not commit. The consequence must be a duplicate — the
// same records published again — and never a record nobody ever hears about.
func TestSweepWhenTheCommitFailsPublishesTheSameRecordsAgain(t *testing.T) {
	outbox := newFakeOutbox(authorized(1), authorized(2))
	outbox.breakCommit(errCommit)
	publisher := &fakePublisher{}
	relay := harness(t, outbox, publisher, 10)

	if _, err := relay.Sweep(t.Context()); !errors.Is(err, errCommit) {
		t.Fatalf("err = %v, want %v", err, errCommit)
	}
	if diff := cmp.Diff(2, outbox.unpublished()); diff != "" {
		t.Errorf("records marked despite the failed commit (-want +got):\n%s", diff)
	}

	outbox.breakCommit(nil)
	published, err := relay.Sweep(t.Context())
	if err != nil {
		t.Fatalf("second sweep: %v", err)
	}

	if diff := cmp.Diff(2, published); diff != "" {
		t.Errorf("records published by the second sweep (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([][]int64{{1, 2}, {1, 2}}, publisher.published()); diff != "" {
		t.Errorf("the same records must go out twice, not disappear (-want +got):\n%s", diff)
	}
}

func TestSweepWithNothingToPublishTouchesTheBrokerNotAtAll(t *testing.T) {
	outbox := newFakeOutbox()
	publisher := &fakePublisher{}
	relay := harness(t, outbox, publisher, 10)

	published, err := relay.Sweep(t.Context())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}

	if diff := cmp.Diff(0, published); diff != "" {
		t.Errorf("records published (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(0, len(publisher.published())); diff != "" {
		t.Errorf("calls to the broker (-want +got):\n%s", diff)
	}
}

func TestRunRefusesAnIntervalThatWouldSpin(t *testing.T) {
	relay := harness(t, newFakeOutbox(), &fakePublisher{}, 10)

	if err := relay.Run(t.Context(), 0); !errors.Is(err, ErrInterval) {
		t.Fatalf("err = %v, want %v", err, ErrInterval)
	}
}

// A relay that outlives its context is a goroutine nobody can stop; goleak in TestMain is
// what turns that from a code review opinion into a failing test.
func TestRunStopsWhenItsContextIsCancelled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		outbox := newFakeOutbox(authorized(1))
		publisher := &fakePublisher{}
		relay := harness(t, outbox, publisher, 10)

		ctx, cancel := context.WithCancel(t.Context())
		stopped := make(chan error, 1)
		go func() { stopped <- relay.Run(ctx, time.Second) }()

		synctest.Wait()
		cancel()

		select {
		case err := <-stopped:
			if err != nil {
				t.Fatalf("run: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the relay did not stop when its context was cancelled")
		}
	})
}

// A broker outage must not end the relay: the records are still in the table, and the next
// tick is what gets them out once the broker is back.
func TestRunKeepsSweepingAfterAFailedSweep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		outbox := newFakeOutbox(authorized(1))
		publisher := &fakePublisher{fail: errBroker}
		relay := harness(t, outbox, publisher, 10)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		stopped := make(chan error, 1)
		go func() { stopped <- relay.Run(ctx, time.Second) }()

		synctest.Wait()
		if diff := cmp.Diff(1, outbox.unpublished()); diff != "" {
			t.Fatalf("records waiting after the first failed sweep (-want +got):\n%s", diff)
		}

		publisher.breaks(nil)
		time.Sleep(time.Second)
		synctest.Wait()

		if diff := cmp.Diff(0, outbox.unpublished()); diff != "" {
			t.Errorf("records still waiting after the broker returned (-want +got):\n%s", diff)
		}

		cancel()
		if err := <-stopped; err != nil {
			t.Fatalf("run: %v", err)
		}
	})
}
