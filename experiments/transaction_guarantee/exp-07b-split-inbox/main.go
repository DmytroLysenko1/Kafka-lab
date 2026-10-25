package main

import "github.com/DmytroLysenko1/Kafka-lab/experiments/transaction_guarantee/stand"

// exp-07 with one boundary moved: the claim commits on its own and the payment is written in
// a second transaction, so the crash can land between them.
func main() {
	stand.Main(stand.Experiment{
		Name:  "exp-07b",
		Mode:  stand.InboxSplit,
		Topic: "exp07b.payments",
	})
}
