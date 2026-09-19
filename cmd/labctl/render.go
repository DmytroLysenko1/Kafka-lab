package main

import (
	"cmp"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/samber/lo"

	"github.com/DmytroLysenko1/Kafka-lab/internal/infrastructure/kafka"
)

const unknown = "?"

func renderTopics(out io.Writer, statuses []kafka.TopicStatus) error {
	if len(statuses) == 0 {
		return nil
	}

	table := newTable(out, "TOPIC\tSTATE\tDRIFT")
	for _, status := range statuses {
		table.row("%s\t%s\t%s", status.Name, status.State, driftSummary(status.Drift))
	}
	return table.flush()
}

func renderPartitions(out io.Writer, states []kafka.PartitionState) error {
	table := newTable(out, "TOPIC\tPARTITION\tLEADER\tREPLICAS\tISR\tURP\tSTATUS")
	for i := range states {
		state := &states[i]
		table.row("%s\t%d\t%s\t%s\t%s\t%s\t%s",
			state.Topic,
			state.Partition,
			leaderOf(state),
			brokerList(state.Replicas),
			brokerList(state.ISR),
			underReplicated(state),
			cmp.Or(state.Status(), "-"),
		)
	}
	return table.flush()
}

func renderLag(out io.Writer, lags []kafka.GroupPartitionLag) error {
	table := newTable(out, "GROUP\tTOPIC\tPARTITION\tCOMMITTED\tEND\tLAG\tSTATUS")
	for i := range lags {
		lag := &lags[i]
		table.row("%s\t%s\t%d\t%d\t%d\t%s\t%s",
			lag.Group,
			lag.Topic,
			lag.Partition,
			lag.Committed,
			lag.End,
			lagValue(lag),
			statusOf(lag.Err),
		)
	}
	return table.flush()
}

func driftSummary(drift []kafka.TopicDrift) string {
	if len(drift) == 0 {
		return "-"
	}
	return strings.Join(lo.Map(drift, func(entry kafka.TopicDrift, _ int) string {
		return fmt.Sprintf(
			"%s: want %s, got %s",
			entry.Setting,
			entry.Want,
			cmp.Or(entry.Got, "unset"))
	}), "; ")
}

func leaderOf(state *kafka.PartitionState) string {
	if state.Leader < 0 {
		return "none"
	}
	return strconv.Itoa(int(state.Leader))
}

// underReplicated distinguishes "not under-replicated" from "the broker never told us",
// because a partition that failed to load carries no replicas at all and would otherwise
// print as healthy in exactly the run where it is not.
func underReplicated(state *kafka.PartitionState) string {
	switch {
	case !state.Known():
		return unknown
	case state.UnderReplicated():
		return "yes"
	default:
		return "-"
	}
}

func lagValue(lag *kafka.GroupPartitionLag) string {
	if lag.Err != nil || lag.Lag < 0 {
		return unknown
	}
	return strconv.FormatInt(lag.Lag, 10)
}

func statusOf(err error) string {
	if err == nil {
		return "-"
	}
	return err.Error()
}

func brokerList(ids []int32) string {
	if len(ids) == 0 {
		return "-"
	}
	return strings.Join(lo.Map(ids, func(id int32, _ int) string {
		return strconv.Itoa(int(id))
	}), ",")
}

type table struct {
	writer *tabwriter.Writer
	err    error
}

func newTable(out io.Writer, header string) *table {
	t := &table{
		writer: tabwriter.NewWriter(
			out,
			0,
			0,
			2,
			' ',
			0,
		),
	}
	t.row("%s", header)
	return t
}

func (t *table) row(format string, args ...any) {
	if t.err != nil {
		return
	}
	_, t.err = fmt.Fprintf(t.writer, format+"\n", args...)
}

func (t *table) flush() error {
	if t.err != nil {
		return t.err
	}
	return t.writer.Flush()
}
