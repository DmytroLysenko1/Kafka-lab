package main

import (
	"fmt"
	"math/rand/v2"
)

var (
	currencies = []string{"EUR", "USD", "GBP", "UAH", "PLN"}
	statuses   = []string{"authorised", "captured", "refunded", "settled"}
	schemes    = []string{"visa", "mastercard", "amex"}
)

// payloads generates payment events that compress like payment events. The first version
// of exp-08 padded its records with one repeated character, and 8 MB of that compressed to
// 616 KB — a ratio no real topic ever sees. A compression sweep built on that padding would
// publish codec numbers about the padding. These vary where real payments vary: ids,
// amounts, merchants, timestamps; and repeat where they repeat: field names and enums.
//
// Seeded, so the same run produces the same bytes and codecs are compared on one input.
type payloads struct {
	source *rand.Rand
	seq    int
}

func newPayloads(seed uint64) *payloads {
	// Deliberately not crypto/rand: these are test payloads, and what they need is the
	// opposite of unpredictability — the same seed must give the same bytes, so every codec
	// compresses the identical input. crypto/rand cannot be seeded.
	return &payloads{source: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))} //nolint:gosec // seeded test data, not security
}

func (p *payloads) next() (key, value []byte) {
	p.seq++
	r := p.source
	id := fmt.Sprintf("pay-%08d", p.seq)
	value = fmt.Appendf(nil,
		`{"payment_id":%q,"merchant_id":"m-%05d","amount_minor":%d,"currency":%q,`+
			`"status":%q,"scheme":%q,"card_last4":"%04d","customer_id":"c-%07d",`+
			`"created_at":"2026-09-21T%02d:%02d:%02d.%03dZ","attempt":%d}`,
		id, r.IntN(20_000), 100+r.IntN(5_000_000),
		currencies[r.IntN(len(currencies))], statuses[r.IntN(len(statuses))],
		schemes[r.IntN(len(schemes))], r.IntN(10_000), r.IntN(10_000_000),
		r.IntN(24), r.IntN(60), r.IntN(60), r.IntN(1000), 1+r.IntN(3))
	return []byte(id), value
}
