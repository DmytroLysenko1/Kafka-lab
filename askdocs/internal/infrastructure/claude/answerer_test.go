package claude_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/samber/lo"

	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/domain/docs"
	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/infrastructure/claude"
)

// reply is one scripted HTTP response: a JSON error with a status, or, with stream set, a 200
// carrying those server-sent events. retryAfter goes out as retry-after-ms, the header the SDK
// prefers, so a retried test waits a millisecond instead of the SDK's half-second backoff.
// hang keeps a stream open after its events until the client gives up; cut promises more
// body than it sends and drops the connection, the way a network failure ends a stream.
// shouldRetry goes out as x-should-retry.
type reply struct {
	status      int
	retryAfter  string
	shouldRetry string
	body        string
	stream      []string
	hang        bool
	cut         bool
}

// scriptedAPI plays the Messages API: it answers attempt n with replies[n], repeating the last
// reply once the script runs out, and records every request it was sent.
type scriptedAPI struct {
	mu       sync.Mutex
	replies  []reply
	requests [][]byte
	betas    []string
}

func (s *scriptedAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.requests = append(s.requests, body)
	s.betas = append(s.betas, r.Header.Get("anthropic-beta"))
	scripted := s.replies[min(len(s.requests), len(s.replies))-1]
	s.mu.Unlock()

	if scripted.retryAfter != "" {
		w.Header().Set("retry-after-ms", scripted.retryAfter)
	}
	if scripted.shouldRetry != "" {
		w.Header().Set("x-should-retry", scripted.shouldRetry)
	}
	if scripted.stream == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(scripted.status)
		// A write the client stops reading shows up as the client's error, which the test
		// asserts on; the server side has nothing to add.
		_, _ = io.WriteString(w, scripted.body)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	if scripted.cut {
		w.Header().Set("Content-Length", "1000000")
	}
	w.WriteHeader(http.StatusOK)
	for _, event := range scripted.stream {
		_, _ = io.WriteString(w, event)
	}
	w.(http.Flusher).Flush()
	switch {
	case scripted.hang:
		<-r.Context().Done()
	case scripted.cut:
		s.dropConnection(w)
	}
}

func (s *scriptedAPI) dropConnection(w http.ResponseWriter) {
	connection, _, err := http.NewResponseController(w).Hijack()
	if err != nil {
		panic(err)
	}
	// Closing is the point; an error closing an already broken connection changes nothing.
	_ = connection.Close()
}

func (s *scriptedAPI) attempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.requests)
}

// sent copies what the handler recorded, under the lock the handler wrote it under.
func (s *scriptedAPI) sent() (requests [][]byte, betas []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.requests), slices.Clone(s.betas)
}

func event(name, data string) string {
	return fmt.Sprintf("event: %s\ndata: %s\n\n", name, data)
}

const messageStart = `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant",` +
	`"model":"claude-opus-5-5","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1200,"output_tokens":0}}}`

// answer is the stream of a text answer that cites the search results at the given positions,
// and ends for the given reason.
func answer(text, stopReason string, cited ...int) []string {
	events := []string{
		event("message_start", messageStart),
		event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":"","citations":null}}`),
		event("content_block_delta", fmt.Sprintf(`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":%q}}`, text)),
	}
	for _, position := range cited {
		events = append(events, event("content_block_delta", fmt.Sprintf(
			`{"type":"content_block_delta","index":0,"delta":{"type":"citations_delta","citation":{"type":"search_result_location",`+
				`"cited_text":"marks them published only after the broker acknowledges","source":"ignored","title":"ignored",`+
				`"search_result_index":%d,"start_block_index":0,"end_block_index":1}}}`, position)))
	}
	return append(events,
		event("content_block_stop", `{"type":"content_block_stop","index":0}`),
		event("message_delta", fmt.Sprintf(`{"type":"message_delta","delta":{"stop_reason":%q,"stop_sequence":null,"stop_details":null},"usage":{"output_tokens":40}}`, stopReason)),
		event("message_stop", `{"type":"message_stop"}`),
	)
}

var (
	refusal = []string{
		event("message_start", messageStart),
		event("message_delta", `{"type":"message_delta","delta":{"stop_reason":"refusal","stop_sequence":null,`+
			`"stop_details":{"type":"refusal","category":"cyber","explanation":"x"}},"usage":{"output_tokens":0}}`),
		event("message_stop", `{"type":"message_stop"}`),
	}
	overloadedMidStream = []string{
		event("message_start", messageStart),
		event("error", `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`),
	}
	apiErrorMidStream = []string{
		event("message_start", messageStart),
		event("error", `{"type":"error","error":{"type":"api_error","message":"Internal server error"}}`),
	}
)

// unfinished is an answer whose stream closes cleanly after its text but before the model
// said why it stopped.
func unfinished(text string) []string {
	return answer(text, "end_turn", 0)[:4]
}

const (
	overloaded   = `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`
	rateLimited  = `{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}`
	badRequest   = `{"type":"error","error":{"type":"invalid_request_error","message":"messages: bad"}}`
	unauthorised = `{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}`
)

func question(t *testing.T) (docs.Question, docs.Evidence) {
	t.Helper()
	q, err := docs.NewQuestion("When does the relay mark a row published?")
	if err != nil {
		t.Fatalf("question: %v", err)
	}
	section, err := docs.NewSection("docs/static/06-transactional-outbox.md", "What the relay must get right",
		"The relay publishes outbox rows.\n\nIt marks them published only after the broker acknowledges.")
	if err != nil {
		t.Fatalf("section: %v", err)
	}
	evidence, err := docs.NewEvidence([]docs.Section{section})
	if err != nil {
		t.Fatalf("evidence: %v", err)
	}
	return q, evidence
}

func answerer(t *testing.T, api *scriptedAPI) *claude.Answerer {
	t.Helper()
	server := httptest.NewServer(api)
	t.Cleanup(server.Close)
	return claude.New(slog.New(slog.DiscardHandler), option.WithBaseURL(server.URL), option.WithAPIKey("test-key"))
}

func TestAnswerRetriesAndClassifies(t *testing.T) {
	const relayAnswer = "The relay keeps the rows until the broker acknowledges."
	type args struct{ replies []reply }
	type want struct {
		Text     string
		Cited    []string
		Attempts int
	}
	tests := []struct {
		name    string
		args    args
		want    want
		wantErr error
	}{
		{
			name: "a cited answer comes back with the section it rests on",
			args: args{replies: []reply{{stream: answer(relayAnswer, "end_turn", 0)}}},
			want: want{Text: relayAnswer, Cited: []string{"docs/static/06-transactional-outbox.md#What the relay must get right"}, Attempts: 1},
		},
		{
			name: "a citation of a search result that was never sent is dropped, not invented",
			args: args{replies: []reply{{stream: answer(relayAnswer, "end_turn", 5)}}},
			want: want{Text: relayAnswer, Attempts: 1},
		},
		{
			name: "an overloaded API is ridden out: the second attempt answers",
			args: args{replies: []reply{{status: 529, retryAfter: "1", body: overloaded}, {stream: answer(relayAnswer, "end_turn", 0)}}},
			want: want{Text: relayAnswer, Cited: []string{"docs/static/06-transactional-outbox.md#What the relay must get right"}, Attempts: 2},
		},
		{
			name:    "an overload that outlasts the retries is 'try later', after one try and three retries",
			args:    args{replies: []reply{{status: 529, retryAfter: "1", body: overloaded}}},
			want:    want{Attempts: 4},
			wantErr: docs.ErrModelUnavailable,
		},
		{
			name:    "a 502 from a proxy, with no API error in the body, is judged by its status: 'try later'",
			args:    args{replies: []reply{{status: 502, retryAfter: "1", body: "<html>Bad Gateway</html>"}}},
			want:    want{Attempts: 4},
			wantErr: docs.ErrModelUnavailable,
		},
		{
			name:    "a rate limit that outlasts the retries is 'try later' too",
			args:    args{replies: []reply{{status: 429, retryAfter: "1", body: rateLimited}}},
			want:    want{Attempts: 4},
			wantErr: docs.ErrModelUnavailable,
		},
		{
			name:    "an overload inside a stream that already started is 'try later', though it arrives under a 200",
			args:    args{replies: []reply{{stream: overloadedMidStream}}},
			want:    want{Attempts: 1},
			wantErr: docs.ErrModelUnavailable,
		},
		{
			name:    "an API error inside a started stream is 'try later', not 'fix the setup'",
			args:    args{replies: []reply{{stream: apiErrorMidStream}}},
			want:    want{Attempts: 1},
			wantErr: docs.ErrModelUnavailable,
		},
		{
			name:    "a connection dropped in the middle of the answer is 'try later'",
			args:    args{replies: []reply{{stream: unfinished(relayAnswer), cut: true}}},
			want:    want{Attempts: 1},
			wantErr: docs.ErrModelUnavailable,
		},
		{
			name:    "a stream that closes before the model says why it stopped is not printed as an answer",
			args:    args{replies: []reply{{stream: unfinished(relayAnswer)}}},
			want:    want{Attempts: 1},
			wantErr: docs.ErrModelUnavailable,
		},
		{
			name:    "an overload the API marks x-should-retry: false is not retried, as the SDK does not retry it",
			args:    args{replies: []reply{{status: 529, shouldRetry: "false", body: overloaded}}},
			want:    want{Attempts: 1},
			wantErr: docs.ErrRequestRejected,
		},
		{
			name:    "a malformed request is not retried: waiting does not fix it",
			args:    args{replies: []reply{{status: 400, body: badRequest}}},
			want:    want{Attempts: 1},
			wantErr: docs.ErrRequestRejected,
		},
		{
			name:    "a bad key is not retried",
			args:    args{replies: []reply{{status: 401, body: unauthorised}}},
			want:    want{Attempts: 1},
			wantErr: docs.ErrRequestRejected,
		},
		{
			name:    "a refusal arrives in a successful stream and is still not an answer",
			args:    args{replies: []reply{{stream: refusal}}},
			want:    want{Attempts: 1},
			wantErr: docs.ErrRefused,
		},
		{
			name:    "an answer cut off at the token limit is not shown as if complete",
			args:    args{replies: []reply{{stream: answer("The relay", "max_tokens")}}},
			want:    want{Attempts: 1},
			wantErr: docs.ErrTruncated,
		},
		{
			name:    "an answer stopped by the context window is not shown as if complete",
			args:    args{replies: []reply{{stream: answer("The relay", "model_context_window_exceeded")}}},
			want:    want{Attempts: 1},
			wantErr: docs.ErrTruncated,
		},
		{
			name:    "a turn that ends with no text is no answer",
			args:    args{replies: []reply{{stream: answer(" ", "end_turn")}}},
			want:    want{Attempts: 1},
			wantErr: docs.ErrEmptyAnswer,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := &scriptedAPI{replies: tt.args.replies}
			q, evidence := question(t)

			answer, err := answerer(t, api).Answer(t.Context(), q, evidence)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			got := want{
				Text:     answer.Text(),
				Cited:    lo.Map(answer.CitedSections(), func(s docs.Section, _ int) string { return s.Document() + "#" + s.Heading() }),
				Attempts: api.attempts(),
			}
			if diff := cmp.Diff(tt.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// The caller's deadline bounds every attempt and every wait together: a retry-after longer
// than the time left is not slept through, and the error still says what the API last said.
func TestAnswerStopsWaitingAtTheCallersDeadline(t *testing.T) {
	const (
		deadline = 200 * time.Millisecond
		// Far below the minute the retry-after asks for, far above any scheduler stall.
		giveUpBy = 5 * time.Second
	)
	api := &scriptedAPI{replies: []reply{{status: 529, retryAfter: "60000", body: overloaded}}}
	q, evidence := question(t)
	ctx, cancel := context.WithTimeout(t.Context(), deadline)
	defer cancel()
	started := time.Now()

	_, err := answerer(t, api).Answer(ctx, q, evidence)

	if !errors.Is(err, docs.ErrModelUnavailable) {
		t.Fatalf("err = %v, want %v", err, docs.ErrModelUnavailable)
	}
	if elapsed := time.Since(started); elapsed > giveUpBy {
		t.Errorf("gave up after %v; the deadline was %v", elapsed, deadline)
	}
	if diff := cmp.Diff(1, api.attempts()); diff != "" {
		t.Errorf("attempts mismatch (-want +got), the minute-long retry-after did not fit the deadline:\n%s", diff)
	}
	// The operator reading this error needs the reason the client was waiting.
	status, ok := errors.AsType[*claude.StatusError](err)
	if !ok {
		t.Fatalf("err = %v, want it to carry the last HTTP status", err)
	}
	if diff := cmp.Diff(529, status.Status); diff != "" {
		t.Errorf("last status mismatch (-want +got):\n%s", diff)
	}
}

// A stream that starts and then goes quiet is bounded by the caller's deadline alone, and ends
// as "try later", not as an answer cut short.
func TestAnswerGivesUpOnAStreamThatGoesQuiet(t *testing.T) {
	api := &scriptedAPI{replies: []reply{{stream: []string{event("message_start", messageStart)}, hang: true}}}
	q, evidence := question(t)
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	_, err := answerer(t, api).Answer(ctx, q, evidence)

	if !errors.Is(err, docs.ErrModelUnavailable) {
		t.Fatalf("err = %v, want %v", err, docs.ErrModelUnavailable)
	}
}

// Nothing listening is a connection error, retried like an overload and, once the retries
// run out, "try later" too. The SDK's own backoff runs here (0.5 s, 1 s, 2 s), so the test
// takes a few seconds.
func TestAnswerTreatsAnUnreachableAPIAsTryLater(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	unreachable := server.URL
	server.Close()
	q, evidence := question(t)
	model := claude.New(slog.New(slog.DiscardHandler), option.WithBaseURL(unreachable), option.WithAPIKey("test-key"))

	_, err := model.Answer(t.Context(), q, evidence)

	if !errors.Is(err, docs.ErrModelUnavailable) {
		t.Fatalf("err = %v, want %v", err, docs.ErrModelUnavailable)
	}
}

// sentRequest is the part of the wire request the answer's quality depends on.
type sentRequest struct {
	Model        string `json:"model"`
	Stream       bool   `json:"stream"`
	Fallbacks    string `json:"fallbacks"`
	OutputConfig struct {
		Effort string `json:"effort"`
	} `json:"output_config"`
	System   []struct{ Text string } `json:"system"`
	Messages []sentMessage           `json:"messages"`
}

type sentMessage struct {
	Role    string        `json:"role"`
	Content []sentContent `json:"content"`
}

type sentContent struct {
	Type      string `json:"type"`
	Source    string `json:"source"`
	Title     string `json:"title"`
	Text      string `json:"text"`
	Citations struct {
		Enabled bool `json:"enabled"`
	} `json:"citations"`
	Content []struct{ Text string } `json:"content"`
}

// sentBlock is one block of the user turn as the test compares it.
type sentBlock struct {
	Type, Source, Title, Text string
	Cited                     bool
	Paragraphs                []string
}

//nolint:gocritic // lo hands each element over by value.
func blockOf(content sentContent, _ int) sentBlock {
	return sentBlock{
		Type:       content.Type,
		Source:     content.Source,
		Title:      content.Title,
		Text:       content.Text,
		Cited:      content.Citations.Enabled,
		Paragraphs: lo.Map(content.Content, func(paragraph struct{ Text string }, _ int) string { return paragraph.Text }),
	}
}

func TestAnswerSendsEvidenceAsCitableSearchResults(t *testing.T) {
	api := &scriptedAPI{replies: []reply{{stream: answer("ok", "end_turn")}}}
	q, evidence := question(t)

	if _, err := answerer(t, api).Answer(t.Context(), q, evidence); err != nil {
		t.Fatalf("err = %v", err)
	}

	requests, betas := api.sent()
	var sent sentRequest
	if err := json.Unmarshal(requests[0], &sent); err != nil {
		t.Fatalf("decode request: %v", err)
	}
	type settings struct {
		Model, Fallbacks, Effort string
		Stream                   bool
		SystemPrompts            int
	}
	gotSettings := settings{Model: sent.Model, Fallbacks: sent.Fallbacks, Effort: sent.OutputConfig.Effort, Stream: sent.Stream, SystemPrompts: len(sent.System)}
	wantSettings := settings{Model: "claude-opus-5-5", Fallbacks: "default", Effort: "medium", Stream: true, SystemPrompts: 1}
	if diff := cmp.Diff(wantSettings, gotSettings); diff != "" {
		t.Errorf("request settings mismatch (-want +got):\n%s", diff)
	}

	got := lo.FlatMap(sent.Messages, func(message sentMessage, _ int) []sentBlock { return lo.Map(message.Content, blockOf) })
	want := []sentBlock{
		{
			Type: "search_result", Source: "docs/static/06-transactional-outbox.md", Title: "What the relay must get right", Cited: true,
			Paragraphs: []string{"The relay publishes outbox rows.", "It marks them published only after the broker acknowledges."},
		},
		{Type: "text", Text: "When does the relay mark a row published?"},
	}
	if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("user turn mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"server-side-fallback-2026-07-01"}, betas); diff != "" {
		t.Errorf("beta header mismatch (-want +got):\n%s", diff)
	}
}
