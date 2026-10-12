// Package retention decides how long the service's bookkeeping must remember what it has
// done. Forgetting too early is not a storage question: an inbox that forgets an event id
// while the event can still arrive again counts that payment twice.
package retention

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrInboxHorizonTooShort = errors.New("retention: the inbox would forget events that can still be delivered again")
	ErrOutboxHorizon        = errors.New("retention: the outbox horizon must be positive")
	ErrTooOldToDeduplicate  = errors.New("retention: the event is older than the inbox is sure to remember")
	ErrStorage              = errors.New("retention: storage failure")
)

// A record outlives its topic's retention by up to one segment: Kafka deletes whole segments,
// and only once the newest record in a segment is past retention. The dead letter topic and
// the retry tiers roll a segment a day (segment.ms in deploy/topics/), so a letter is readable
// for up to 30+1 days and a record in a tier for up to 7+1.
const (
	day                = 24 * time.Hour
	segmentRoll        = day
	deadLetterLifetime = 30*day + segmentRoll
	retryTierLifetime  = 7*day + segmentRoll
)

// minInboxHorizon is the longest an event can come back after it was counted: a letter is
// replayed at the last moment of its life in the dead letter topic and then waits out a retry
// tier. The minutes the event spent in the tiers before it died are inside the margin of the
// default horizon, not in this bound.
const minInboxHorizon = deadLetterLifetime + retryTierLifetime

// clockSkewAllowance is what the inbox horizon keeps on top of minInboxHorizon for clocks that
// disagree. Three clocks meet here: the API stamps occurred_at, the consumer stamps consumed_at
// and judges an event's age, and prune cuts by its own now. Kept at exactly minInboxHorizon,
// a prune clock or an API clock a second ahead of the consumer's would open a window in which
// an event old enough to be forgotten is still young enough to be counted. A day is far more
// than NTP lets clocks drift, and costs a day of inbox rows.
const clockSkewAllowance = day

// CanBeDeduplicated is whether an event that happened at occurredAt can still be told apart
// from its own earlier delivery. The inbox forgets an event no sooner than minInboxHorizon plus
// clockSkewAllowance after it was counted (NewHorizons refuses anything shorter), and an event
// is counted no sooner than it happened, so one younger than minInboxHorizon is remembered if it
// was ever counted — the allowance covering the three clocks that stamp and judge those times.
// An older one is not safe to count: minInboxHorizon covers one trip through the dead letter
// topic, but a letter replayed and archived again starts that topic's clock over, and after
// enough trips it could arrive after the inbox forgot it — and be counted twice.
func CanBeDeduplicated(occurredAt, now time.Time) bool {
	return now.Sub(occurredAt) < minInboxHorizon
}

// Horizons are how long each table keeps a row after it stopped mattering: the inbox after an
// event was counted, the outbox after it was published.
type Horizons struct {
	inbox  time.Duration
	outbox time.Duration
}

func NewHorizons(inbox, outbox time.Duration) (Horizons, error) {
	switch {
	case inbox < minInboxHorizon+clockSkewAllowance:
		return Horizons{}, fmt.Errorf("%w: %s, need at least %s", ErrInboxHorizonTooShort, inbox, minInboxHorizon+clockSkewAllowance)
	case outbox <= 0:
		return Horizons{}, ErrOutboxHorizon
	}
	return Horizons{
		inbox:  inbox,
		outbox: outbox,
	}, nil
}

// InboxCutoff is the consumption time before which an inbox row may be forgotten.
func (h Horizons) InboxCutoff(now time.Time) time.Time {
	return now.Add(-h.inbox)
}

// OutboxCutoff is the publication time before which an outbox row may be deleted.
func (h Horizons) OutboxCutoff(now time.Time) time.Time {
	return now.Add(-h.outbox)
}
