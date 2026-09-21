package main

import (
	"errors"
	"testing"
)

func TestValidateBoundsTheWorkTheRunWillDo(t *testing.T) {
	sane := settings{keys: 50, updates: 40, tombstones: 10, records: 2000}

	with := func(change func(*settings)) settings {
		copied := sane
		change(&copied)
		return copied
	}

	tests := []struct {
		name    string
		args    settings
		wantErr error
	}{
		{name: "the defaults the run ships with", args: sane},
		{name: "one key, one update, no deletes", args: with(func(c *settings) { c.keys, c.updates, c.tombstones = 1, 1, 0 })},
		{name: "every key deleted", args: with(func(c *settings) { c.tombstones = c.keys })},
		{name: "keys at the ceiling", args: with(func(c *settings) { c.keys, c.updates = maxKeys, 1 })},

		{name: "no keys", args: with(func(c *settings) { c.keys = 0 }), wantErr: errShape},
		{name: "negative keys", args: with(func(c *settings) { c.keys = -1 }), wantErr: errShape},
		{name: "more keys than the ceiling", args: with(func(c *settings) { c.keys = maxKeys + 1 }), wantErr: errShape},
		{name: "no updates", args: with(func(c *settings) { c.updates = 0 }), wantErr: errShape},
		{name: "more deletes than keys", args: with(func(c *settings) { c.tombstones = c.keys + 1 }), wantErr: errShape},
		{name: "negative deletes", args: with(func(c *settings) { c.tombstones = -1 }), wantErr: errShape},
		{name: "no records for the retention topic", args: with(func(c *settings) { c.records = 0 }), wantErr: errShape},

		// The bound on updates is written as a division precisely so these two cannot pass:
		// computed as keys*updates it would wrap, and a wrapped product compares as small.
		{
			name:    "updates chosen so that keys*updates wraps to exactly zero",
			args:    with(func(c *settings) { c.keys, c.updates = maxKeys, 1<<59 }),
			wantErr: errShape,
		},
		{
			name:    "updates at the top of the range",
			args:    with(func(c *settings) { c.keys, c.updates = 2, 1<<62 }),
			wantErr: errShape,
		},
		{
			name:    "keys times updates just over the work ceiling",
			args:    with(func(c *settings) { c.keys, c.updates = 1000, maxKeys*10/1000+1 }),
			wantErr: errShape,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.args
			err := cfg.validate()
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("validate() err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
