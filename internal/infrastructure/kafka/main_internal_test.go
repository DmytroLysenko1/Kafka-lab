package kafka

import (
	"testing"

	"go.uber.org/goleak"
)

// The consumer, the publisher and the detours each own a franz-go client, and a client runs
// goroutines until it is closed: a test that forgot a Close would leave them running for the
// rest of the binary, and only a check after the last test can see it.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
