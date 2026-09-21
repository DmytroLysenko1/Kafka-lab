package main

import (
	"testing"

	"go.uber.org/goleak"
)

// The package builds kgo clients and a pgxpool, whose goroutines are reaped only if their
// Close runs to completion. No test here drives one yet, so this currently guards the next
// test that does.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
