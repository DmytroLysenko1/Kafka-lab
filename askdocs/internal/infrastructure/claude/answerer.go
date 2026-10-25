// Package claude answers a question from documentation sections with Claude, through the
// Messages API. Everything that belongs to the provider lives here: the model, the prompt,
// the retry policy, and the translation of the API's failures into the docs package's.
package claude

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/anthropics/anthropic-sdk-go/packages/ssestream"
	"github.com/samber/lo"

	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/domain/docs"
)

const (
	model = "claude-opus-5-5"

	// The answer is a few paragraphs, but adaptive thinking — always on for this model, per
	// Anthropic's model documentation (October 2026) — spends from the same cap before the
	// first word of it; the cap is also the most one question can cost in output.
	maxTokens = 8000

	// The SDK retries 408, 409, 429, 5xx (529 "overloaded" included) and connection errors,
	// with exponential backoff and retry-after, until the first byte of the answer; an
	// x-should-retry header overrides the status. Three retries ride out a short overload; a
	// longer one is the caller's to wait out.
	maxRetries = 3

	// retryAllowance is the SDK's own backoff for three retries (0.5 s + 1 s + 2 s) with room
	// for a retry-after of a few seconds on each. The SDK does not cap retry-after, so long
	// ones eat into the time left for the answer itself.
	retryAllowance = 30 * time.Second
)

// budget is how long one answer may take, retries included. The answer is streamed, so no
// HTTP timeout cuts a slow answer short, and the SDK hides the stream's keep-alive pings, so
// a quiet stream cannot be told from a dead one: the budget is the only bound, and Answer
// applies it whatever deadline the caller has. It follows the SDK's own estimate for
// generating maxTokens (an hour per 128 000 output tokens, in CalculateNonStreamingTimeout),
// plus the retries before the first byte.
const budget = time.Duration(maxTokens)*time.Hour/128_000 + retryAllowance

// StatusError carries the status of the last HTTP response. When the budget runs out while
// the SDK waits to retry, the SDK returns only the context's error, and this is what still
// says why it was waiting.
type StatusError struct {
	Status int
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("last HTTP status %d", e.Status)
}

type Answerer struct {
	client anthropic.Client
	logger *slog.Logger
}

// New builds the client with the retry policy above. options come after the policy, so a
// caller can point the client elsewhere (a test server, a proxy) but the policy is this
// package's.
func New(logger *slog.Logger, options ...option.RequestOption) *Answerer {
	policy := []option.RequestOption{option.WithMaxRetries(maxRetries)}
	return &Answerer{
		client: anthropic.NewClient(slices.Concat(policy, options)...),
		logger: logger,
	}
}

// Answer streams the answer to the question from the evidence. Asking is read-only, so every
// failure the SDK retries is safe to retry: a retried question costs its tokens again and
// changes nothing.
func (a *Answerer) Answer(ctx context.Context, question docs.Question, evidence docs.Evidence) (docs.Answer, error) {
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	var lastStatus int
	stream := a.client.Beta.Messages.NewStreaming(ctx, request(question, evidence), recordStatus(&lastStatus))
	message, err := collect(stream)
	if err != nil {
		return docs.Answer{}, fmt.Errorf("ask %s: %w", model, failure(err, lastStatus))
	}
	a.logger.InfoContext(ctx, "claude answered",
		"model", message.Model,
		"stop_reason", message.StopReason,
		"input_tokens", message.Usage.InputTokens,
		"output_tokens", message.Usage.OutputTokens,
	)
	return read(message, evidence)
}

func collect(stream *ssestream.Stream[anthropic.BetaRawMessageStreamEventUnion]) (*anthropic.BetaMessage, error) {
	var message anthropic.BetaMessage
	for stream.Next() {
		if err := message.Accumulate(stream.Current()); err != nil {
			return nil, errors.Join(err, stream.Close())
		}
	}
	if err := errors.Join(stream.Err(), stream.Close()); err != nil {
		return nil, err
	}
	return &message, nil
}

func recordStatus(status *int) option.RequestOption {
	return option.WithMiddleware(func(r *http.Request, next option.MiddlewareNext) (*http.Response, error) {
		response, err := next(r)
		if response != nil {
			*status = response.StatusCode
		}
		return response, err
	})
}

// failure is the one error a failed call returns: what it means, what the SDK said, and the
// last HTTP status when there was one — side by side, not nested.
func failure(err error, status int) error {
	if status == 0 {
		return errors.Join(classify(err), err)
	}
	return errors.Join(classify(err), err, &StatusError{Status: status})
}

func request(question docs.Question, evidence docs.Evidence) anthropic.BetaMessageNewParams {
	content := append(lo.Map(evidence.Sections(), searchResult), anthropic.NewBetaTextBlock(question.String()))
	return anthropic.BetaMessageNewParams{
		Model:     model,
		MaxTokens: maxTokens,
		System: []anthropic.BetaTextBlockParam{{
			Text: systemPrompt,
		}},
		Messages: []anthropic.BetaMessageParam{anthropic.NewBetaUserMessage(content...)},
		OutputConfig: anthropic.BetaOutputConfigParam{
			Effort: anthropic.BetaOutputConfigEffortMedium,
		},
		// Per Anthropic's documentation of server-side fallback (October 2026), a declined
		// request is served again by a fallback model inside the same call, so a classifier
		// false positive on a security-flavoured question can still get an answer.
		Fallbacks: anthropic.BetaFallbacksParamUnion{
			OfDefault: "default",
		},
		Betas: []anthropic.AnthropicBeta{anthropic.AnthropicBetaServerSideFallback2026_07_01},
	}
}

// searchResult hands a section over as a search result, one text block per paragraph, with
// citations on. The API cites a range of those blocks and returns the cited text, so the
// smaller the blocks, the closer a quote lands to the sentence it supports. The position of
// the result in the request is how a citation finds its section again.
func searchResult(section docs.Section, _ int) anthropic.BetaContentBlockParamUnion {
	result := anthropic.BetaSearchResultBlockParam{
		Content: lo.Map(paragraphs(section.Body()), func(paragraph string, _ int) anthropic.BetaTextBlockParam {
			return anthropic.BetaTextBlockParam{
				Text: paragraph,
			}
		}),
		Source: section.Document(),
		Title:  section.Title(),
		Citations: anthropic.BetaCitationsConfigParam{
			Enabled: anthropic.Bool(true),
		},
	}
	return anthropic.BetaContentBlockParamUnion{
		OfSearchResult: &result,
	}
}

func paragraphs(text string) []string {
	return lo.FilterMap(strings.Split(text, "\n\n"), func(paragraph string, _ int) (string, bool) {
		paragraph = strings.TrimSpace(paragraph)
		return paragraph, paragraph != ""
	})
}
