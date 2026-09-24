-- +goose Up
-- The inbox is what turns at-least-once delivery into an effect that happens once: the
-- event id is the primary key, so a redelivered event loses the race against its own row.
-- Both keys here come off a topic, so both are bounded in the schema as well as in the
-- code: a header is whatever the producer put in it, and this is the last line of defence
-- against one that is a megabyte wide.
CREATE TABLE inbox (
    event_id    varchar(64) PRIMARY KEY,
    consumed_at timestamptz NOT NULL
);

-- A projection, not a ledger: it is derived from the events and can be rebuilt from them.
-- The CHECK is still here because a bug that made it negative would be invisible otherwise.
CREATE TABLE merchant_totals (
    merchant_id      varchar(64) PRIMARY KEY,
    authorized_minor bigint      NOT NULL CHECK (authorized_minor >= 0),
    updated_at       timestamptz NOT NULL
);

-- +goose Down
DROP TABLE merchant_totals;
DROP TABLE inbox;
