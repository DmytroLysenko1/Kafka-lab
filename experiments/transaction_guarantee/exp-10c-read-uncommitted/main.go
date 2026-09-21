package main

import "github.com/DmytroLysenko1/Kafka-lab/experiments/transaction_guarantee/eos"

// The whole program: the shared transactional loop, the topics this experiment owns, and
// the claim it is held to.
func main() {
	eos.Main(eos.Experiment{
		Name:    "exp-10c",
		Input:   "exp10c.input",
		Output:  "exp10c.output",
		WriteDB: false,
		Claim:   eos.AbortedVisibleUncommitted,
	})
}
