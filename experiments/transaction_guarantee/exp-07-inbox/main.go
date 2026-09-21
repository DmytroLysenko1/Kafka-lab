package main

import "github.com/DmytroLysenko1/Kafka-lab/experiments/transaction_guarantee/stand"

// The whole program. Everything this experiment does is the shared stand; what makes it
// this experiment rather than one of its two siblings is the mode, and nothing else.
func main() {
	stand.Main(stand.Experiment{
		Name:  "exp-07",
		Mode:  stand.Inbox,
		Topic: "exp07.payments",
	})
}
