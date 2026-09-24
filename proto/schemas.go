// Package proto carries the schema sources at runtime: the producer registers them with
// the schema registry, so the text it publishes under and the text the generated code came
// from are the same file, not two copies that can drift.
package proto

import _ "embed"

//go:embed payment/v1/events.proto
var PaymentEventsV1 string
