package labkit

import (
	"context"
	"time"
)

// restartBackoff is the pause between a crash and the next start, the way an orchestrator
// waits before bringing a pod back.
const restartBackoff = 200 * time.Millisecond

// donePoll is how often a running consumer is checked against the experiment's goal.
const donePoll = 100 * time.Millisecond

// Runner is one consumer life: Run until it stops or dies, then Close.
type Runner interface {
	Run(ctx context.Context) error
	Close()
}

// Supervise runs a consumer the way an orchestrator does: when it dies, a new one comes up,
// reads from the last committed offset, and meets the same record again. A restart is a
// new client, not a second Run on the old one — franz-go keeps its own fetch position, and
// reusing the client would walk past the record that killed it and measure the harness
// instead of the service. Counting the restarts is how a stall shows up as a number.
func Supervise(ctx context.Context, start func() (Runner, error), done func() bool) (restarts int, drained bool, err error) {
	for ctx.Err() == nil {
		if done() {
			return restarts, true, nil
		}

		runner, err := start()
		if err != nil {
			return restarts, false, err
		}
		crashed := runUntil(ctx, runner, done)
		runner.Close()

		if crashed {
			restarts++
			// A sleep that ignores the budget keeps the run going past it, and the overshoot
			// lands in the number the experiment publishes.
			select {
			case <-ctx.Done():
				return restarts, done(), nil
			case <-time.After(restartBackoff):
			}
		}
	}
	return restarts, done(), nil
}

// runUntil returns when the consumer dies, when the work is done, or when the budget runs
// out — and in every case it joins the consumer before handing it back to be closed.
func runUntil(ctx context.Context, runner Runner, done func() bool) (crashed bool) {
	running, stop := context.WithCancel(ctx)
	defer stop()

	finished := make(chan error, 1)
	go func() { finished <- runner.Run(running) }()

	watching := time.NewTicker(donePoll)
	defer watching.Stop()

	for {
		select {
		case err := <-finished:
			return err != nil
		case <-watching.C:
			if done() {
				stop()
				<-finished
				return false
			}
		case <-ctx.Done():
			stop()
			<-finished
			return false
		}
	}
}
