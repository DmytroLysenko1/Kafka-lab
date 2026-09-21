package stand

import (
	"testing"

	"go.uber.org/goleak"
)

// The package builds kgo clients and a pgxpool for all three experiments, and their
// goroutines are reaped only if Close runs to completion. No test here drives one yet, so
// this currently guards the next test that does.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
