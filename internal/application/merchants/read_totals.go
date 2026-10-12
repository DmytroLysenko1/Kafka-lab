package merchants

import (
	"context"
	"fmt"

	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/merchant"

	"github.com/DmytroLysenko1/Kafka-lab/pkg/telemetry"
)

type totalsReader interface {
	Totals(ctx context.Context, id merchant.ID) ([]merchant.Total, error)
}

type ReadTotals struct {
	totals totalsReader
}

func NewReadTotals(totals totalsReader) *ReadTotals {
	return &ReadTotals{
		totals: totals,
	}
}

// Execute answers with the projection as it stands, one total per currency. A merchant
// nobody has paid yet has no totals — an empty list rather than an error: the caller asked
// what has been authorised, and "nothing, in no currency" is an answer.
func (uc *ReadTotals) Execute(ctx context.Context, merchantID string) ([]merchant.Total, error) {
	ctx, end := telemetry.Start(ctx, "merchants.ReadTotals")
	totals, err := uc.read(ctx, merchantID)
	end(err)
	return totals, err
}

func (uc *ReadTotals) read(ctx context.Context, merchantID string) ([]merchant.Total, error) {
	id, err := merchant.ParseID(merchantID)
	if err != nil {
		return nil, err
	}

	totals, err := uc.totals.Totals(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("merchants: read the totals of %s: %w", id, err)
	}
	return totals, nil
}
