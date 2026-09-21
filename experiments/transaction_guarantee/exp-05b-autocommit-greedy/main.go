package main

import "github.com/DmytroLysenko1/Kafka-lab/experiments/transaction_guarantee/stand"

// The whole program: the shared stand, with the offset committed by franz-go's own timer.
func main() {
	stand.Main(stand.Experiment{
		Name:  "exp-05b",
		Mode:  stand.AutoCommitGreedy,
		Topic: "exp05b.payments",
	})
}
