package main

import (
	"testing"

	"go.uber.org/goleak"
)

// The package builds kgo clients whose goroutines are reaped only if Close runs to
// completion. No test here drives one yet, so this guards the next test that does.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
