# askdocs — ask the lab, get the page it came from

A Go command that answers questions about Kafka-lab from the lab's own documentation, with
Claude, and lists the sections each answer rests on. It is retrieval-augmented generation
(RAG) over internal docs: find the sections that match the question, send only those to the
model, and make the model cite them.

```
$ make ask Q="What happens to the outbox while Kafka is down?"
<the answer, a few paragraphs>

- <file> — <heading of a section the answer cites>
    > <the sentence it is quoted for>
- <file> — <heading of another>
    > <...>
```

It is the deliverable for the AI block's KR *"Build a Go service that integrates an LLM API"*:
*"Working Go demo app with README; code showing API integration, prompt handling, and
error/retry logic."* What "working" covers so far: the command, its retrieval and its
failure handling run and are tested without a key; **no answer from the model has been
logged yet** — `make ask-log` produces that log (see *What is measured and what is not*).

| The KR asks for | Where it is |
|---|---|
| API integration | [`internal/infrastructure/claude/answerer.go`](internal/infrastructure/claude/answerer.go) — the official Go SDK, the Messages API streamed, the evidence sent as `search_result` blocks with citations on |
| prompt handling | [`prompt.go`](internal/infrastructure/claude/prompt.go) — the system prompt and why each rule is in it; [`docs/question.go`](internal/domain/docs/question.go) — what counts as a question; [`docs/evidence.go`](internal/domain/docs/evidence.go) — what an answer may rest on: never nothing, never more than six sections, and a citation only of what was sent |
| error/retry logic | `answerer.go` — the retry policy and the budget; [`response.go`](internal/infrastructure/claude/response.go) — what each failure means; [`answerer_test.go`](internal/infrastructure/claude/answerer_test.go) — all of it against a scripted API, no key needed |
| what it taught (the AI block's share-learnings KR) | [`docs/ai-learnings.md`](../docs/ai-learnings.md) |
| a practical use case | RAG over the 321 sections of this repository's README, `docs/`, experiment READMEs and this README |

## Running it

Needs Go 1.26 and, for answers, an Anthropic API key. The key is read by the SDK, from
`ANTHROPIC_API_KEY` (or an `ant auth login` profile); this code never sees it.

```sh
export ANTHROPIC_API_KEY=...                       # from console.anthropic.com
make ask Q="How do I roll back a Helm release on the kind cluster?"

make ask-sources Q="How do I roll back a Helm release on the kind cluster?"   # no key, no cost
```

`ask-sources` stops after retrieval and prints the sections the model would be given and
their size. It is how retrieval is checked without paying for a model.

```sh
make ask-eval        # every question in eval/questions.tsv through retrieval: results/retrieval.log
make ask-log         # the same questions answered by the model: results/answers-<time>.log
```

`eval/questions.tsv` pairs each question with the one document a correct answer cannot do
without — labelled by hand, so it measures retrieval against that judgement, nothing more —
and one question the docs do not cover at all. `ask-eval` reports for each whether the
labelled document was in the evidence, and recall@6 over the set; it needs no key, and it is
where the numbers below come from. `ask-log` needs `ANTHROPIC_API_KEY` in the environment of
the terminal that runs it; only the SDK reads it, it never reaches the log, and the log can
be committed as it is.

Both targets build `askdocs/bin/askdocs` and run it; the binary takes `-root <checkout>`,
`-retrieve-only`, and the question. Logs (what was indexed, tokens used, why there was no
answer) go to stderr; the answer goes to stdout.

The binary's exit code says what to do next, so a script can tell them apart (sysexits(3)).
Run the binary itself for that: `make` turns any failure into its own 2.

| Code | Meaning | What to do |
|---|---|---|
| 0 | answered, or `-h` | — |
| 64 | an unknown flag, or the question is blank or longer than 2 000 characters — checked before any documentation is read | ask a question |
| 66 | no section matches, so the model was not called; or `-root` does not exist or holds no docs | rephrase in the docs' words, or fix `-root` |
| 75 | the API stayed overloaded or rate-limited through every retry, the stream broke off or closed before the model finished, or the budget (4 min 15 s) ran out | try again later |
| 78 | the API refused the request itself — bad key, bad request, model not available | fix the setup; retrying will not help |
| 1 | the model declined, its answer hit the token cap, or it answered with no text | rephrase, or read the evidence with `-retrieve-only` |
| 130 | stopped with Ctrl-C (or SIGTERM) | — |

## How it works

```
question ──▶ docs.Question (blank / > 2 000 chars refused, before anything is read)
                │
README.md, docs/, experiments/ ──▶ markdown: one section per heading (a "#" inside a code
                │                   fence is code, not a heading), superseded runs skipped
                ▼
             bm25: sections that share a term with the question, best first
                ▼
             docs.Evidence: the best 6; none ──▶ exit 66, model not called
                ▼
             claude: Messages API streamed, evidence as search_result blocks, citations on
                │      SDK retries 408/409/429/5xx with backoff and retry-after
                ▼
             docs.Answer: text + citations, each bound to a section of the evidence
                ▼
             cli: answer, cited sections, the quotes ──▶ stdout
```

`cmd/askdocs/main.go` wires it, `internal/` is layered like the payments service. The domain,
`docs`, holds the rules: what counts as a question, a section and an answer; that evidence is
never empty and never more than six sections; that a citation can only point at a section the
model was given. `ask` is the use case, the adapters are `markdown`, `bm25` and `claude`, and
`interfaces/cli` is what the terminal shows. The use case sees two small interfaces it declares
itself; the SDK's types never leave `internal/infrastructure/claude`.

## Decisions

| Decision | Why | What it costs |
|---|---|---|
| A module of its own, `askdocs/go.mod` | the Anthropic SDK's module requires the AWS SDK, an MCP SDK and testify; none of that belongs in the payments service's module graph | `make verify` reaches it through a separate `verify-askdocs` target |
| BM25 in process, not embeddings | Anthropic's API has no embeddings endpoint (its documentation, October 2026, points to a partner provider), so embeddings mean a second provider and a second key. The corpus is English prose whose questions use its own words — ISR, outbox, rebalance — which is where a word match works | no synonyms and no other languages: a question matches only the words the docs use. The index holds English stems, so a question in another language matches only through the English terms it happens to contain, and one without any finds nothing (exit 66, nothing spent). That is the first thing embeddings would fix |
| six sections per question | the 13 labelled questions in [`results/retrieval.log`](results/retrieval.log) sent 6 421–24 886 bytes of section text each, against the 546 310 bytes of all 321 sections, and 12 of the 13 found their labelled document among the six. Six is a chosen cap, not a measured optimum: nothing here compares it with four or eight | a question whose answer is spread over more than six sections gets a partial one |
| `search_result` blocks with citations, not "please cite your sources" in the prompt | the API returns each citation as structured data — which search result, and the cited text — so the sections printed are the ones the model actually used. A citation is matched to the section by its position in the request, never by the strings that come back, and one that points past the evidence is dropped | the search-result citation format is specific to Claude; another provider means another adapter (which is what the adapter is for) |
| the SDK's retries, not a loop of our own | the SDK already retries what is retryable — 408, 409, 429, 5xx including 529 *overloaded*, and connection errors, unless an `x-should-retry` header says otherwise — with exponential backoff, jitter and `retry-after`. What it cannot know is how long the caller will wait and what a failure means to them; that is the code here: three retries, one budget for the whole question, and a classification into "try later" and "fix the setup" | three retries is a guess about how long an overload lasts; it is one constant |
| the answer is streamed, bounded by one budget | an always-thinking model can take minutes on a hard question, and a timeout per attempt would cut a slow but healthy answer off and pay for it again. The budget, 4 min 15 s, is the SDK's own estimate for 8 000 output tokens (an hour per 128 000) plus 30 s for the retries. The adapter applies it itself, so no caller can forget it | the SDK caps neither the number of seconds a `retry-after` asks for nor their sum, so long ones eat into the time left for the answer. The SDK retries only until the first byte: an overload inside a stream that has started is reported as "try later", not re-asked. There is no idle timeout, because the SDK swallows the stream's keep-alive pings, so a quiet stream during thinking cannot be told from a dead one; the budget is what ends a dead one |
| failures judged the way the SDK judges a retry | `x-should-retry` first, then the error's type — an error inside a stream arrives under the stream's 200, and only its type says `overloaded_error` — then the status | the status list is a copy of the SDK's (`shouldRetry` in `internal/requestconfig/requestconfig.go`) and could drift from it; the tests pin the header, the types and the statuses |
| every failure the SDK retries is safe to retry | asking is read-only: a retried question costs its tokens again and changes nothing, so there is no idempotency key and no "unknown outcome" to handle, unlike the payments service | a retried question is paid for twice |
| `claude-opus-5-5`, effort `medium` | the current Opus. `medium` is its default per Anthropic's model documentation (October 2026; the SDK does not say); it is set anyway so the request says what it runs at, and stays there if the default moves | not measured against `low` or a Sonnet yet — that needs an eval set and real runs |
| server-side fallback (`fallbacks: "default"`) | per Anthropic's documentation (October 2026), the model's safety classifiers can decline a request, and a question about security settings or failure injection is the kind that might trip one; the fallback serves it inside the same call instead of failing it. Not observed here: no run has been declined | beta header `server-side-fallback-2026-07-01` |
| the answer is printed when it is complete | the citations are known only at the end, and the answer is shown with them | nothing appears until the whole answer is ready |
| no prompt caching | the only fixed prefix is the system prompt, 903 bytes, below the minimum cacheable prefix Anthropic's prompt-caching documentation gives for current models (512–4 096 tokens, by model; October 2026); the sections change with every question | each question pays for its input in full |
| a span per use case, no exporter | each use case starts an OpenTelemetry span, as the payments service's do; the command installs no tracer provider, so the spans go nowhere until askdocs runs inside something that installs one | a terminal run shows nothing of them; tokens and the stop reason are logged for every answer |

## What is measured and what is not

Measured, in [`results/retrieval.log`](results/retrieval.log), by `make ask-eval`: 321 sections,
546 310 bytes of text, indexed from the four roots. **recall@6 is 12 of 13**: every labelled
question found its document among the six sections but one — *What happens when a broker
goes down?* gets the journal's exp-13 entry and the ISR case file, not exp-04's README, where
the broker kill is measured. The question the docs do not cover retrieves nothing, and the
binary exits 66 without a call. Thirteen questions labelled by one person are a smoke test of
retrieval, not a benchmark of it.

Not measured yet: an answer. No API key was available when this was written, so latency,
tokens per question, cost and answer quality have no numbers here. The adapter logs
`input_tokens` and `output_tokens` for every answer; the first runs with a key go into
`results/` the same way.

## Tests

`make verify-askdocs` — build, vet, the repository's golangci-lint config, and the tests with
`-race`. None of them needs a key or a network.

- `claude`: a scripted HTTP server plays the API, streams included. An overloaded API (529) is
  ridden out on the second attempt; an overload, a rate limit or a proxy's bare 502 that never
  lifts gives "try later" after exactly one try and three retries; an overload or an API error
  inside a stream that has already started is "try later" though it arrives under a 200; so are
  a connection dropped mid-answer and a stream that closes before the model says why it
  stopped; a 529 marked `x-should-retry: false` is tried once; 400 and 401 are tried once and
  give "fix the setup"; a refusal, an answer cut off at the token limit or the context window,
  and an answer with no text are not shown as answers; a citation of a search result that was
  never sent is dropped; a `retry-after` of a minute is not slept through when the caller has
  200 ms left, and the error still carries the last status; a stream that goes quiet ends at
  the caller's deadline; an address nothing listens on ends as "try later" after the SDK's own
  backoff (about three seconds). The request itself is checked: model, effort, fallback,
  streaming, beta header, and each section sent as a citable search result, one block per
  paragraph. Each of these changes fails a test: `maxRetries` 2; a 4xx as "try later"; no
  status check for 5xx; no type check for an overload or an API error; no
  `io.ErrUnexpectedEOF`; accepting a stream with no stop reason; ignoring `x-should-retry`.
- `docs`: evidence is never empty and never past six sections; an answer cites only sections
  of its evidence, drops any other position, and is refused with no text; cited sections are
  listed once, with every quote; the question limit at exactly 2 000 and 2 001.
- `ask`: with the real BM25 index, the model receives the capped evidence; a question nothing
  matches never reaches it; its failure reaches the caller as the same kind of failure.
- `bm25`, `markdown`: ranking, stemming, the heading's weight (setting it to 1 fails), the
  no-match case and a limit of zero or less; headings, code fences, skipped superseded runs, a
  missing root, a cancelled load.
- `cli`, `cmd/askdocs`: what the terminal shows, including the unsupported-answer notice; the
  whole command short of the model — `-h`, a bad flag, a blank question refused before any
  documentation is read, a missing root, a question nothing matches, `-retrieve-only` — and
  every classified failure on its exit code.
