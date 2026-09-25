// Package env reads a process's configuration from its environment. Every read names the
// variable in its error, because the person reading that error is looking at a deployment
// manifest, not at this code.
package env

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	ErrMissing = errors.New("env: required variable is not set")
	ErrInvalid = errors.New("env: variable is set to something unusable")
)

// Or is the variable, or the fallback when it is unset or empty. It cannot fail, so it is
// not a Reader method.
func Or(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

// Reader reads several variables and keeps every problem it meets instead of stopping at
// the first: a manifest with three mistakes is fixed in one pass, not three deploys. Each
// read returns the zero value when it fails; Err says whether any did.
type Reader struct {
	problems []error
}

func (r *Reader) Err() error { return errors.Join(r.problems...) }

func (r *Reader) Required(name string) string {
	value := os.Getenv(name)
	if value == "" {
		r.missing(name)
	}
	return value
}

// List splits a comma-separated variable, dropping blanks, and counts one that leaves
// nothing as missing: a list of brokers that is only commas is as missing as an unset one.
func (r *Reader) List(name string) []string {
	var items []string
	for item := range strings.SplitSeq(os.Getenv(name), ",") {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}
	if len(items) == 0 {
		r.missing(name)
	}
	return items
}

// PositiveInt is the variable as a number above zero, or the fallback when it is unset.
func (r *Reader) PositiveInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		r.invalid(name, raw, "a positive whole number")
		return 0
	}
	return value
}

// PositiveDuration is the variable as a duration above zero, or the fallback when unset.
func (r *Reader) PositiveDuration(name string, fallback time.Duration) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		r.invalid(name, raw, "a positive duration such as 1s")
		return 0
	}
	return value
}

func (r *Reader) missing(name string) {
	r.problems = append(r.problems, fmt.Errorf("%w: %s", ErrMissing, name))
}

func (r *Reader) invalid(name, raw, want string) {
	r.problems = append(r.problems, fmt.Errorf("%w: %s=%q, want %s", ErrInvalid, name, raw, want))
}
