-- +goose Up
-- Expand only: new columns are nullable or defaulted and every constraint holds for the rows
-- already there, so the image before this release keeps working against this schema — it
-- writes authorized payments and adds to authorized_minor, both still valid.

-- When each status was reached. NULL until it is.
ALTER TABLE payments
    ADD COLUMN captured_at timestamptz,
    ADD COLUMN refunded_at timestamptz;

-- The domain's rules repeated as the last line, so that a bug in the code cannot store what the
-- domain refuses (payment.ParseStatus, payment.Capture, payment.Refund): a status is one a
-- payment can be in, and its times say how it got there — captured only after authorised,
-- refunded only after captured. The lengths are the ones the code refuses past: a merchant id
-- of 64 bytes (payment.ParseMerchantID), an idempotency key of 255 (payments.ParseIdempotencyKey).
-- Plain ADD CONSTRAINT, validated in this migration's transaction: on the lab's tables the scan
-- takes moments. A large production table would add them NOT VALID here and VALIDATE in a
-- migration of its own, which holds a lighter lock while it scans.
ALTER TABLE payments
    ADD CONSTRAINT payments_status_known
        CHECK (status IN ('authorized', 'captured', 'refunded')),
    ADD CONSTRAINT payments_status_history CHECK (
        (status = 'authorized' AND captured_at IS NULL AND refunded_at IS NULL)
        OR (status = 'captured' AND captured_at IS NOT NULL AND refunded_at IS NULL)
        OR (status = 'refunded' AND captured_at IS NOT NULL AND refunded_at IS NOT NULL)
    ),
    ADD CONSTRAINT payments_merchant_id_length CHECK (octet_length(merchant_id) <= 64),
    ADD CONSTRAINT payments_idempotency_key_length CHECK (octet_length(idempotency_key) <= 255);

-- The projection counts every movement, each in its own column. No CHECK ties them together
-- (refunded <= captured <= authorized): the retry tiers can hand the consumer a refund before
-- the capture it follows, and the totals are allowed to pass through that order on the way to
-- the same sums. Each is still never negative.
-- authorized_minor gets the same default: a capture or refund can be the first movement of a
-- merchant in a currency the consumer meets, and the row it creates must start every other sum
-- at zero rather than fail on a NOT NULL with nothing to put in it.
ALTER TABLE merchant_currency_totals
    ALTER COLUMN authorized_minor SET DEFAULT 0,
    ADD COLUMN captured_minor bigint NOT NULL DEFAULT 0 CHECK (captured_minor >= 0),
    ADD COLUMN refunded_minor bigint NOT NULL DEFAULT 0 CHECK (refunded_minor >= 0);

-- +goose Down
ALTER TABLE merchant_currency_totals
    DROP COLUMN refunded_minor,
    DROP COLUMN captured_minor,
    ALTER COLUMN authorized_minor DROP DEFAULT;

ALTER TABLE payments
    DROP CONSTRAINT payments_idempotency_key_length,
    DROP CONSTRAINT payments_merchant_id_length,
    DROP CONSTRAINT payments_status_history,
    DROP CONSTRAINT payments_status_known,
    DROP COLUMN refunded_at,
    DROP COLUMN captured_at;
