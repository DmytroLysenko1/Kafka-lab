package housekeeping_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/housekeeping"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/retention"
)

var (
	errDatabase = errors.New("the database is not answering")
	now         = time.Date(2026, time.September, 27, 12, 0, 0, 0, time.UTC)
	day         = 24 * time.Hour
)

// fakeTables keeps rows as Postgres would: a DELETE removes at most limit rows past the cutoff,
// and an outbox row that was never published has no publication time to be past.
type fakeTables struct {
	inbox       []time.Time
	outbox      []*time.Time
	failInboxAt int
	batches     []int
}

func (f *fakeTables) PruneInbox(_ context.Context, before time.Time, limit int) (int, error) {
	if f.failInboxAt > 0 && len(f.batches) == f.failInboxAt {
		return 0, errDatabase
	}
	kept := make([]time.Time, 0, len(f.inbox))
	deleted := 0
	for _, consumed := range f.inbox {
		if deleted < limit && consumed.Before(before) {
			deleted++
			continue
		}
		kept = append(kept, consumed)
	}
	f.inbox = kept
	f.batches = append(f.batches, deleted)
	return deleted, nil
}

func (f *fakeTables) PruneOutbox(_ context.Context, before time.Time, limit int) (int, error) {
	kept := make([]*time.Time, 0, len(f.outbox))
	deleted := 0
	for _, published := range f.outbox {
		if deleted < limit && published != nil && published.Before(before) {
			deleted++
			continue
		}
		kept = append(kept, published)
	}
	f.outbox = kept
	return deleted, nil
}

func ago(d time.Duration) time.Time { return now.Add(-d) }

func agoPtr(d time.Duration) *time.Time { return new(now.Add(-d)) }

func pruner(t *testing.T, tables *fakeTables, batch int) *housekeeping.Prune {
	t.Helper()
	horizons, err := retention.NewHorizons(40*day, 7*day)
	if err != nil {
		t.Fatalf("horizons: %v", err)
	}
	prune, err := housekeeping.NewPrune(tables, func() time.Time { return now }, horizons, batch)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	return prune
}

// What stays is what can still matter: an inbox id younger than the horizon can still meet its
// event again, and an outbox row that was never published is an event still owed to Kafka,
// however old it is.
func TestOnlyRowsPastTheirHorizonAreDeleted(t *testing.T) {
	tables := &fakeTables{
		inbox:  []time.Time{ago(41 * day), ago(39 * day), ago(time.Hour)},
		outbox: []*time.Time{agoPtr(8 * day), agoPtr(6 * day), nil},
	}

	got, err := pruner(t, tables, 100).Execute(t.Context())
	if err != nil {
		t.Fatalf("prune: %v", err)
	}

	if diff := cmp.Diff(housekeeping.Result{Inbox: 1, Outbox: 1}, got); diff != "" {
		t.Errorf("rows deleted (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]time.Time{ago(39 * day), ago(time.Hour)}, tables.inbox); diff != "" {
		t.Errorf("inbox left (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]*time.Time{agoPtr(6 * day), nil}, tables.outbox); diff != "" {
		t.Errorf("outbox left (-want +got):\n%s", diff)
	}
}

// A month of rows goes in short statements, one batch at a time, until a batch comes back
// short — never as one DELETE holding its locks for the whole month.
func TestABacklogIsDeletedInBatchesUntilOneComesBackShort(t *testing.T) {
	old := make([]time.Time, 12)
	for i := range old {
		old[i] = ago(50 * day)
	}
	tables := &fakeTables{inbox: old}

	got, err := pruner(t, tables, 5).Execute(t.Context())
	if err != nil {
		t.Fatalf("prune: %v", err)
	}

	if diff := cmp.Diff(12, got.Inbox); diff != "" {
		t.Errorf("inbox rows deleted (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]int{5, 5, 2}, tables.batches); diff != "" {
		t.Errorf("batches (-want +got):\n%s", diff)
	}
}

// A database that fails half-way leaves the next run to finish, and says how far this one
// got: the outbox is not touched after the inbox failed, so the next run sees both whole.
func TestAFailureHalfWayReportsWhatWasDoneAndStops(t *testing.T) {
	old := make([]time.Time, 12)
	for i := range old {
		old[i] = ago(50 * day)
	}
	tables := &fakeTables{inbox: old, outbox: []*time.Time{agoPtr(30 * day)}, failInboxAt: 1}

	got, err := pruner(t, tables, 5).Execute(t.Context())
	if !errors.Is(err, errDatabase) {
		t.Fatalf("err = %v, want %v", err, errDatabase)
	}
	if diff := cmp.Diff(housekeeping.Result{Inbox: 5}, got); diff != "" {
		t.Errorf("rows deleted before the failure (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(1, len(tables.outbox)); diff != "" {
		t.Errorf("outbox rows after a failed inbox pass (-want +got):\n%s", diff)
	}
}

func TestABatchOfNothingIsRefused(t *testing.T) {
	horizons, err := retention.NewHorizons(40*day, 7*day)
	if err != nil {
		t.Fatalf("horizons: %v", err)
	}
	if _, err := housekeeping.NewPrune(&fakeTables{}, func() time.Time { return now }, horizons, 0); !errors.Is(err, housekeeping.ErrBatch) {
		t.Fatalf("err = %v, want %v", err, housekeeping.ErrBatch)
	}
}
