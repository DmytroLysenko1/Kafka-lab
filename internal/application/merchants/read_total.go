package merchants

import (
	"context"
	"fmt"

	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/merchant"
)

type totalsReader interface {
	Total(ctx context.Context, id merchant.ID) (int64, error)
}

type ReadTotal struct {
	totals totalsReader
}

func NewReadTotal(totals totalsReader) *ReadTotal {
	return &ReadTotal{
		totals: totals,
	}
}

// Execute answers with the projection as it stands. A merchant nobody has paid yet has a
// total of zero rather than a missing row: the caller asked what has been authorised, and
// the answer to that is a number, not an absence.
func (uc *ReadTotal) Execute(ctx context.Context, merchantID string) (int64, error) {
	id, err := merchant.ParseID(merchantID)
	if err != nil {
		return 0, err
	}

	total, err := uc.totals.Total(ctx, id)
	if err != nil {
		return 0, fmt.Errorf("merchants: read the total of %s: %w", id, err)
	}
	return total, nil
}
