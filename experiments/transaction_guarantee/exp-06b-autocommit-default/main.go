package main

import "github.com/DmytroLysenko1/Kafka-lab/experiments/transaction_guarantee/stand"

// The whole program: the shared stand, with the offset committed by franz-go's own timer.
func main() {
	stand.Main(stand.Experiment{
		Name:  "exp-06b",
		Mode:  stand.AutoCommit,
		Topic: "exp06b.payments",
	})
}
