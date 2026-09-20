# Superseded runs

| Run | What is wrong with it |
|---|---|
| `run-2026-09-20-021443.log` | the first instrument, which never committed an offset: a partition that moved between members during a join would have been handled twice |
| `run-2026-09-20-053233.log` | correct instrument, but the topic was never reset, so every drain also fetched and decoded the records of every previous run — the absolute seconds are not reproducible |

The ratio between group sizes was sound in the second file, because all five groups read the
same log within one run. The absolute drain times were not. `run.sh` now resets the topic.
