-- +goose Up
-- The trace an event was raised in, as W3C trace context (traceparent, tracestate), written by
-- the transaction that wrote the event. The relay publishes the row long after that request
-- has answered, from a process of its own; this column is how the record on the topic, and the
-- consumer that counts it, still belong to the request's trace. NULL for rows written before
-- it existed and for work that ran in no trace — those are published without one.
ALTER TABLE outbox ADD COLUMN trace_context jsonb;

-- +goose Down
ALTER TABLE outbox DROP COLUMN trace_context;
