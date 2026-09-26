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
	errStore  = errors.New("the database is not answering")
	errBroker = errors.New("the broker is not answering")
	errCommit = errors.New("the transaction could not be committed")
)

// fakeOutbox is the store and the transaction manager at once, as Postgres is: what is
// written inside WithinTx lands only when it commits, so a test can tell a record that was
// published-and-forgotten from one that was never published. Like Postgres, it does not
// hold the whole table while a transaction is open — a backlog count is answered while a
// sweep is still waiting on the broker, as `SELECT count(*)` is beside rows claimed with
// FOR UPDATE SKIP LOCKED.
type fakeOutbox struct {
	mu           sync.Mutex
	records      []Record
	published    map[int64]time.Time
	staged       []func()
	failCommit   error
	failBacklog  error
	stallBacklog bool
}

func newFakeOutbox(records ...Record) *fakeOutbox {
	return &fakeOutbox{records: records, published: make(map[int64]time.Time)}
}

// WithinTx runs one transaction at a time, as the relay does; the staged writes are applied
// under the lock only once fn has returned and the commit has not failed.
func (f *fakeOutbox) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	f.mu.Lock()
	f.staged = nil
	f.mu.Unlock()

	err := fn(ctx)

	f.mu.Lock()
	defer f.mu.Unlock()
	staged := f.staged
	f.staged = nil
	switch {
	case err != nil:
		return err
	case f.failCommit != nil:
		return f.failCommit
	}
	for _, apply := range staged {
		apply()
	}
	return nil
}

func (f *fakeOutbox) Claim(_ context.Context, limit int) ([]Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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
	f.mu.Lock()
	defer f.mu.Unlock()
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

// accept is the API taking payments while the relay runs: new records in the table.
func (f *fakeOutbox) accept(records ...Record) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = append(f.records, records...)
}

func (f *fakeOutbox) Backlog(ctx context.Context) (int, error) {
	f.mu.Lock()
	stall := f.stallBacklog
	failure := f.failBacklog
	waiting := len(f.records) - len(f.published)
	f.mu.Unlock()

	if stall {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	if failure != nil {
		return 0, failure
	}
	return waiting, nil
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

// watcher is what the relay reports to. It records rather than counts: a test that only
// knew how many times the relay reported could not tell a backlog of 3 from a backlog of 0.
type watcher struct {
	mu       sync.Mutex
	sweeps   []sweep
	backlogs []int
}

type sweep struct {
	published int
	failed    bool
}

func (w *watcher) Swept(published int, _ time.Duration, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sweeps = append(w.sweeps, sweep{published: published, failed: err != nil})
}

func (w *watcher) Backlog(records int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.backlogs = append(w.backlogs, records)
}

func (w *watcher) reported() ([]sweep, []int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.sweeps), slices.Clone(w.backlogs)
}

type fakePublisher struct {
	mu      sync.Mutex
	batches [][]int64
	fail    error
	// unanswered is a cluster that takes the request and never replies: Publish waits for
	// its context, as a produce to a cluster without a quorum does until the sweep times out.
	unanswered bool
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

func (p *fakePublisher) Publish(ctx context.Context, records []Record) error {
	p.mu.Lock()
	sent := make([]int64, 0, len(records))
	for _, record := range records {
		sent = append(sent, record.ID)
	}
	p.batches = append(p.batches, sent)
	fail, unanswered := p.fail, p.unanswered
	p.mu.Unlock()

	if unanswered {
		<-ctx.Done()
		return ctx.Err()
	}
	return fail
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
	return watched(t, outbox, publisher, batch, &watcher{})
}

func watched(t *testing.T, outbox *fakeOutbox, publisher *fakePublisher, batch int, watch *watcher) *Relay {
	t.Helper()
	swept := time.Date(2026, time.September, 25, 12, 0, 0, 0, time.UTC)
	return NewRelay(outbox, publisher, outbox, func() time.Time { return swept }, batch, slog.New(slog.DiscardHandler), watch)
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

func TestEachSweepIsReportedWithWhatItPublished(t *testing.T) {
	outbox := newFakeOutbox(authorized(1), authorized(2), authorized(3))
	watch := &watcher{}
	relay := watched(t, outbox, &fakePublisher{}, 2, watch)

	relay.sweepOnce(t.Context())
	relay.sweepOnce(t.Context())

	sweeps, _ := watch.reported()
	if diff := cmp.Diff([]sweep{{published: 2}, {published: 1}}, sweeps, cmp.AllowUnexported(sweep{})); diff != "" {
		t.Errorf("sweeps reported (-want +got):\n%s", diff)
	}
}

// A failed sweep must still be counted as one — an outage that silenced the counter would
// leave the graph looking like a service with nothing to do.
func TestAFailedSweepIsStillReported(t *testing.T) {
	outbox := newFakeOutbox(authorized(1))
	watch := &watcher{}
	relay := watched(t, outbox, &fakePublisher{fail: errBroker}, 10, watch)

	relay.sweepOnce(t.Context())

	sweeps, _ := watch.reported()
	if diff := cmp.Diff([]sweep{{published: 0, failed: true}}, sweeps, cmp.AllowUnexported(sweep{})); diff != "" {
		t.Errorf("sweeps reported (-want +got):\n%s", diff)
	}
}

// The failure exp-13 found on the dashboard: with the cluster unable to answer, no sweep
// ends, and a backlog reported at the end of a sweep sat at its last value while payments
// kept arriving. An operator must see the backlog grow while the sweep is stuck.
func TestTheBacklogKeepsBeingReportedWhileASweepIsStuckOnTheBroker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		outbox := newFakeOutbox(authorized(1))
		watch := &watcher{}
		relay := watched(t, outbox, &fakePublisher{unanswered: true}, 10, watch)

		ctx, cancel := context.WithCancel(t.Context())
		stopped := make(chan error, 1)
		go func() { stopped <- relay.Run(ctx, time.Second) }()

		for id := int64(2); id <= 4; id++ {
			time.Sleep(time.Second)
			outbox.accept(authorized(id))
			synctest.Wait()
		}
		time.Sleep(time.Second)
		synctest.Wait()

		sweeps, backlogs := watch.reported()
		if diff := cmp.Diff(0, len(sweeps)); diff != "" {
			t.Errorf("sweeps ended although the broker never answered (-want +got):\n%s", diff)
		}
		if len(backlogs) == 0 {
			t.Fatal("no backlog was reported while the sweep was stuck")
		}
		if diff := cmp.Diff(4, backlogs[len(backlogs)-1]); diff != "" {
			t.Errorf("latest backlog reported while the sweep was stuck (-want +got):\n%s", diff)
		}

		cancel()
		if err := <-stopped; err != nil {
			t.Fatalf("run: %v", err)
		}
	})
}

// A backlog nobody could read is not a backlog of zero: reporting it as one would tell an
// operator that everything had drained at the exact moment the database stopped answering.
func TestABacklogThatCannotBeReadIsNotReportedAsZero(t *testing.T) {
	outbox := newFakeOutbox(authorized(1))
	outbox.failBacklog = errStore
	watch := &watcher{}
	relay := watched(t, outbox, &fakePublisher{}, 10, watch)

	relay.reportBacklog(t.Context())

	_, backlogs := watch.reported()
	if diff := cmp.Diff(0, len(backlogs)); diff != "" {
		t.Errorf("backlogs reported despite the failure (-want +got):\n%s", diff)
	}
}

// A database too slow to count the backlog must not hold up the sweeps: the number is for
// a dashboard, the sweeps are the work.
func TestABacklogThatNeverAnswersDoesNotHoldUpTheSweeps(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		outbox := newFakeOutbox(authorized(1))
		outbox.stallBacklog = true
		watch := &watcher{}
		relay := watched(t, outbox, &fakePublisher{}, 10, watch)

		ctx, cancel := context.WithCancel(t.Context())
		stopped := make(chan error, 1)
		go func() { stopped <- relay.Run(ctx, time.Second) }()
		synctest.Wait()

		outbox.accept(authorized(2))
		time.Sleep(time.Second)
		synctest.Wait()

		sweeps, backlogs := watch.reported()
		if diff := cmp.Diff([]sweep{{published: 1}, {published: 1}}, sweeps, cmp.AllowUnexported(sweep{})); diff != "" {
			t.Errorf("sweeps while the backlog count hung (-want +got):\n%s", diff)
		}
		if diff := cmp.Diff(0, len(backlogs)); diff != "" {
			t.Errorf("a backlog was reported although the count never answered (-want +got):\n%s", diff)
		}

		cancel()
		if err := <-stopped; err != nil {
			t.Fatalf("run: %v", err)
		}
	})
}
