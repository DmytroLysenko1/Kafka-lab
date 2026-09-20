package main

// record is one entry as it came back off the log: a tombstone is a record whose value was
// deleted, which is how a compacted topic says "this key is gone".
type record struct {
	Key       string
	Tombstone bool
}

// rollKey marks the records the experiment writes purely to close the active segment —
// nothing is ever cleaned while it is open. They are counted apart from the data so the
// instrument does not appear in its own measurement.
const rollKey = "__roll"

// summary is what a reader sees when it reads the whole log from the start — the only
// vantage point that answers "what is still there".
type summary struct {
	Records    int
	LiveKeys   int
	Tombstones int
	Rolls      int
}

func summarise(records []record) summary {
	live := make(map[string]bool, len(records))

	tombstones, rolls := 0, 0
	for _, entry := range records {
		if entry.Key == rollKey {
			rolls++
			continue
		}
		if entry.Tombstone {
			tombstones++
		}
		// A later record for the same key replaces what the reader knew about it, so the
		// last one decides whether the key is alive — exactly what compaction keeps.
		live[entry.Key] = !entry.Tombstone
	}

	alive := 0
	for _, isLive := range live {
		if isLive {
			alive++
		}
	}
	return summary{Records: len(records), LiveKeys: alive, Tombstones: tombstones, Rolls: rolls}
}
