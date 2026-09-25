package labkit

import "time"

// Unwatched stands in for the metrics the services publish: an experiment counts what
// reached the database and the topics, not what a scrape would have shown.
type Unwatched struct{}

func (Unwatched) Handled(bool, time.Duration) {}
func (Unwatched) Retried(string)              {}
func (Unwatched) DeadLettered(string)         {}
func (Unwatched) Failed(string)               {}
