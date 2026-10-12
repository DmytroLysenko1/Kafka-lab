package payments

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
	"github.com/DmytroLysenko1/Kafka-lab/pkg/telemetry"
)

// paymentChanger reads a payment to change it and writes the change back, in one transaction.
// The read is scoped to the merchant and locks the row, so a payment of another merchant is
// not found and two changes of one payment cannot both decide from the same status; the write
// applies only if the status is still the one the change was decided from, and stores the
// events the change raised beside it.
type paymentChanger interface {
	FindForMerchantForUpdate(ctx context.Context, id payment.ID, merchant payment.MerchantID) (*payment.Payment, error)
	Save(ctx context.Context, changed *payment.Payment, from payment.Status) error
}

type ChangeCommand struct {
	MerchantID string
	PaymentID  string
}

type ChangeResult struct {
	PaymentID string
	Status    string
}

// change is what capturing and refunding have in common: find the payment, let it decide
// whether the move is allowed, store what it decided. Which move is the only difference, and
// the payment is the one that knows what each move means.
type change struct {
	name     string
	payments paymentChanger
	tx       txManager
	now      Clock
}

func (c *change) execute(ctx context.Context, cmd ChangeCommand, move func(*payment.Payment, time.Time) error) (ChangeResult, error) {
	ctx, end := telemetry.Start(ctx, c.name)
	result, err := c.apply(ctx, cmd, move)
	end(err)
	return result, err
}

func (c *change) apply(ctx context.Context, cmd ChangeCommand, move func(*payment.Payment, time.Time) error) (ChangeResult, error) {
	id, merchant, err := parseChange(cmd)
	if err != nil {
		return ChangeResult{}, err
	}

	var result ChangeResult
	err = c.tx.WithinTx(ctx, func(ctx context.Context) error {
		changed, err := c.payments.FindForMerchantForUpdate(ctx, id, merchant)
		if err != nil {
			return err
		}
		from := changed.Status()
		if err := move(changed, c.now()); err != nil {
			return err
		}
		result = ChangeResult{
			PaymentID: changed.ID().String(),
			Status:    changed.Status().String(),
		}
		return c.payments.Save(ctx, changed, from)
	})
	if err != nil {
		return ChangeResult{}, fmt.Errorf("payments: change payment %s of merchant %s: %w", cmd.PaymentID, cmd.MerchantID, err)
	}
	return result, nil
}

// parseChange refuses an id that cannot name a payment as not found: no payment has it, and
// telling the caller it is malformed would only tell it which ids are worth guessing.
func parseChange(cmd ChangeCommand) (payment.ID, payment.MerchantID, error) {
	merchant, err := payment.ParseMerchantID(cmd.MerchantID)
	if err != nil {
		return payment.ID{}, payment.MerchantID{}, err
	}
	id, err := payment.ParseID(cmd.PaymentID)
	if err != nil {
		return payment.ID{}, payment.MerchantID{}, errors.Join(payment.ErrNotFound, err)
	}
	return id, merchant, nil
}

type CapturePayment struct {
	change change
}

func NewCapturePayment(payments paymentChanger, tx txManager, now Clock) *CapturePayment {
	return &CapturePayment{
		change: change{
			name:     "payments.CapturePayment",
			payments: payments,
			tx:       tx,
			now:      now,
		},
	}
}

// Execute captures the whole authorised amount. Capturing a captured payment again answers
// with the same payment and stores nothing new, so a caller may repeat a capture whose answer
// it lost without an idempotency key of its own.
func (uc *CapturePayment) Execute(ctx context.Context, cmd ChangeCommand) (ChangeResult, error) {
	return uc.change.execute(ctx, cmd, (*payment.Payment).Capture)
}

type RefundPayment struct {
	change change
}

func NewRefundPayment(payments paymentChanger, tx txManager, now Clock) *RefundPayment {
	return &RefundPayment{
		change: change{
			name:     "payments.RefundPayment",
			payments: payments,
			tx:       tx,
			now:      now,
		},
	}
}

// Execute refunds the whole captured amount, and is safe to repeat for the same reason a
// capture is.
func (uc *RefundPayment) Execute(ctx context.Context, cmd ChangeCommand) (ChangeResult, error) {
	return uc.change.execute(ctx, cmd, (*payment.Payment).Refund)
}
