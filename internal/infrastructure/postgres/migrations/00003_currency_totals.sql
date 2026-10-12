-- +goose Up
-- One migration for everything this release changes in the schema. The expand and the
-- contract of the totals are in one step because no running version still reads the old
-- table: the stands are rebuilt from this chain, and an image from before it would now fail
-- loudly on the missing merchant_totals instead of counting into a table nobody reads.

-- The prune job (cmd/prune) deletes by age: without these it would scan both tables whole on
-- every batch. The outbox index covers only published rows — the ones prune may remove — and
-- sits next to outbox_unpublished, which serves the relay. Plain CREATE INDEX, not
-- CONCURRENTLY: migrations run in a transaction, and these tables are small enough in the lab
-- that the lock lasts moments; a large production table would take the CONCURRENTLY path in a
-- migration of its own, outside a transaction.
CREATE INDEX inbox_consumed_at ON inbox (consumed_at);
CREATE INDEX outbox_published_at ON outbox (published_at) WHERE published_at IS NOT NULL;

-- The old table is locked against writes before anything reads it, so the backfill below and
-- the DROP see one final state. Helm runs this as a pre-upgrade hook while the old consumers
-- are still up. Locks are taken in the order a consumer takes them — the inbox (the indexes
-- above) before the totals — so the two never wait on each other in a cycle. A consumer that
-- wrote its total before the lock is waited for and its event backfilled; one that comes
-- after waits behind the lock until its own lock_timeout (2 s, postgres/totals.go) or this
-- migration's commit, then fails and rolls its inbox row back with it — so the event is
-- counted again by the new consumer, never lost and never twice. Reads (EXCLUSIVE, not ACCESS
-- EXCLUSIVE) still go through until the DROP.
LOCK TABLE merchant_totals IN EXCLUSIVE MODE;

-- Totals per merchant AND currency: merchant_totals added every currency into one number, so
-- 1999 EUR and 1999 JPY made a total of 3998 of nothing.
CREATE TABLE merchant_currency_totals (
    merchant_id      varchar(64) NOT NULL,
    currency         char(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    authorized_minor bigint      NOT NULL CHECK (authorized_minor >= 0),
    updated_at       timestamptz NOT NULL,
    PRIMARY KEY (merchant_id, currency)
);

-- The backfill counts exactly what the consumer counted: an event is in a total if and only if
-- its id is in the inbox, and the relay sets that id to the outbox row's own id (the event_id
-- header). The old table cannot be split by currency — it never knew one — so the totals are
-- rebuilt from the rows that do; the lock above is what keeps an old consumer from counting an
-- event into the dropped table after this INSERT has read the inbox.
INSERT INTO merchant_currency_totals (merchant_id, currency, authorized_minor, updated_at)
SELECT o.payload ->> 'merchant_id',
       o.payload ->> 'currency',
       sum((o.payload ->> 'amount_minor')::bigint),
       max(i.consumed_at)
FROM outbox o
JOIN inbox i ON i.event_id = o.id::text
WHERE o.event_type = 'payment.authorized'
GROUP BY 1, 2;

DROP TABLE merchant_totals;

-- The same bound the payments domain puts on an amount (payment.maxAuthorizedMinor, 2^50), kept
-- here as the last line: a total grows by amounts this size, and an unbounded one would reach
-- bigint's limit after a handful of payments.
ALTER TABLE payments ADD CONSTRAINT payments_amount_bound CHECK (amount_minor <= 1125899906842624);

-- +goose Down
DROP INDEX outbox_published_at;
DROP INDEX inbox_consumed_at;
ALTER TABLE payments DROP CONSTRAINT payments_amount_bound;

CREATE TABLE merchant_totals (
    merchant_id      varchar(64) PRIMARY KEY,
    authorized_minor bigint      NOT NULL CHECK (authorized_minor >= 0),
    updated_at       timestamptz NOT NULL
);
INSERT INTO merchant_totals (merchant_id, authorized_minor, updated_at)
SELECT merchant_id, sum(authorized_minor), max(updated_at)
FROM merchant_currency_totals
GROUP BY merchant_id;

DROP TABLE merchant_currency_totals;
