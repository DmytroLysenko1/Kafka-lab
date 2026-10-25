# Which AI tool for which task — what this repository taught

A write-up for the team, and the script of a 20-minute talk, from two uses of AI in this
repository: Claude Code as the tool the lab was built and audited with, and the Claude API
inside [`askdocs/`](../askdocs/), a Go command that answers questions about the lab from its
own documentation. Every claim points at a file, a commit or a log; the few from a working
session with no log say so.

## The short version

| Task | Tool that fit | Why, with the evidence |
|---|---|---|
| Writing code whose behaviour a test can pin | Claude Code | fast, and wrong in ways a test catches — as long as the tests are checked too (below) |
| Deciding *whether* to write the code | a person, before the tool | the tool builds what it is asked for: labctl was 1 570 lines with tests — 675 in `cmd/labctl`, 895 in `internal/infrastructure/kafka` (`68eb646`) — and was replaced the next day by topicctl, a maintained tool that already did it (`b9d1663`) |
| Checking a repository against its own claims | Claude Code agents, read-only, one question each | `.claude/agents/` holds eight: an evidence auditor that holds every number to its log, a failure-mode analyst, a skeptic of constants, a newcomer who follows the README from zero, and others. Two independent read-throughs of exp-05…17 found six claims stronger than their evidence; each was rerun and rewritten (`4662613`) |
| Reviewing code the same session just wrote | a second agent with a fresh context, given only the diff | the author's review shares the author's blind spots; see *askdocs* below |
| Grounded answers over our own documents | the Claude API with citations | `askdocs`: the API returns which section each claim rests on as data, not as text it was asked to write |
| Finding the documents to answer from | not an LLM | Anthropic's API has no embeddings endpoint; BM25 in process finds the sections, and a question that matches nothing never reaches the model |

## Lessons from building with Claude Code

**Ask what exists before asking for code.** The tool is good at producing a tool. labctl —
declarative topics, drift checks, lag, describe — was written and tested before anyone asked
whether `topicctl` already did all of it. It did, plus checks the
lab lacked. The rule this repository now follows: name the candidate tools and check them
against the running cluster first; write code only where the code is the subject.

**A green test suite written by the same hand proves less than it looks.** The tests are
only as good as the cases they were written to pass. What tells a test from a decoration is a
mutation: break the code on purpose and see whether a test fails. In `askdocs`, eight such
changes each fail a test now — retries cut from three to two, a 4xx called "try later", the
5xx status check dropped, the overload type check dropped, a dropped connection not
recognised, a stream with no stop reason accepted, `x-should-retry` ignored, the heading's
weight in ranking set to one ([`askdocs/README.md`](../askdocs/README.md#tests)). Two
mutations first looked like survivors and were not: they broke compilation, which a count of
failing tests reads as zero. Check that the mutant builds.

**The numbers are where it goes wrong quietly.** Four runs in the Kafka half were wrong the
first time in the same way — absence of data rendered as zero
([talk.md](talk.md#the-part-that-is-not-about-kafka)) — and each was caught by a number that
could not be true. An agent told to hold every number to the log it came from finds the
claims a person stops seeing after the third read.

**Review in a fresh context.** An agent asked to review what it has just written reviews its
own intent. The review that found the most serious defect in `askdocs` — an answer whose
stream closed early was printed as complete — was a separate agent given only the code,
after the author's own review had passed it (from the review session; not in a log).

## Lessons from integrating the Claude API

From [`askdocs/README.md`](../askdocs/README.md#decisions), where each one has its reason and
its cost:

- **Let the API do the citing.** Sending the sections as `search_result` blocks with
  citations on returns each citation as data — which result, which text. A citation is
  matched to its section by its position in the request, never by the strings that come
  back, and one that points past what was sent is dropped.
- **Retries belong to the SDK; their meaning belongs to you.** The SDK already retries 408,
  409, 429, 5xx and connection errors with backoff and `retry-after`. What it cannot know is
  how long the caller will wait and what a failure means to them: one budget for the whole
  question, and every failure classified as "try later" or "fix the setup", with an exit code
  each.
- **A 200 is not an answer.** An overload can arrive inside a stream that has already
  started, under its 200; only the error's type says so. A refusal and an answer cut off at
  the token limit are successful responses too. Accept only a turn the model finished, and
  treat a stream that closes before saying why it stopped as broken.
- **An always-thinking model needs a budget, not a per-request timeout.** A review of the
  first version showed its per-attempt timeout would cut a slow but healthy answer off and
  pay for it again — read from the SDK's code, not observed in a run; the answer is now
  streamed and bounded by one budget sized from the output cap.
- **Retrieval is the cheap half; measure it alone.** `askdocs -retrieve-only` shows what a
  question would be answered from at no cost, and `make ask-eval` runs thirteen hand-labelled
  questions through it: 12 of 13 found their document among six sections of the 331 indexed,
  and a question off the docs' subject retrieved nothing and was never sent
  ([`askdocs/results/retrieval.log`](../askdocs/results/retrieval.log)).
- **A document about the corpus is part of the corpus.** The first version of this write-up
  named the off-topic test question, and the question started retrieving the write-up. Index
  what the tool should answer from, and choose test questions nothing in it mentions.

What is not measured yet, and says so: no answer from the model has been logged, so latency,
tokens and answer quality have no numbers here; `make ask-log` is how they get one. The
six-section cap is a chosen value, and thirteen questions labelled by one person are a smoke
test of retrieval, not a benchmark.

## Where the 20 minutes go

| Part | Planned |
|---|---|
| The short version — which tool for which task | 4 min |
| Building with Claude Code: labctl, mutations, numbers, fresh-context review | 7 min |
| The Claude API in `askdocs`: citations, retries, a 200 that is not an answer, the budget | 6 min |
| A live `make ask-sources` on a question from the room | 1 min |
| Questions | 2 min |

A plan, not a timing; the first time it is given, its real timings replace this table.

## If the audience remembers three sentences

1. Ask what already exists before asking the tool for code.
2. A test the same hand wrote is proven by breaking the code, not by its colour.
3. With an LLM API, a successful response is not yet an answer: check why it stopped.
