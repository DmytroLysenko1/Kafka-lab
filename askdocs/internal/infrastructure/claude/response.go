package claude

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/samber/lo"

	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/domain/docs"
)

// classify names what a failed call means for the person waiting on it. It runs after the
// SDK's retries, so a transient failure here means the retries or the budget ran out.
func classify(err error) error {
	if apiErr, ok := errors.AsType[*anthropic.Error](err); ok {
		if transient(apiErr) {
			return docs.ErrModelUnavailable
		}
		return docs.ErrRequestRejected
	}
	if _, ok := errors.AsType[net.Error](err); ok ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.ErrUnexpectedEOF) {
		return docs.ErrModelUnavailable
	}
	return docs.ErrModelFailed
}

// transient is judged the way the SDK judges a retry — an x-should-retry header first — and
// then by the error's type before its status: an error that arrives inside a stream that has
// already started carries the stream's 200, and only its type says "overloaded".
func transient(apiErr *anthropic.Error) bool {
	if apiErr.Response != nil {
		switch apiErr.Response.Header.Get("x-should-retry") {
		case "true":
			return true
		case "false":
			return false
		}
	}
	switch apiErr.Type() {
	case anthropic.ErrorTypeOverloadedError, anthropic.ErrorTypeRateLimitError,
		anthropic.ErrorTypeAPIError, anthropic.ErrorTypeTimeoutError:
		return true
	}
	return retryableStatus(apiErr.StatusCode)
}

// retryableStatus is the list of statuses the SDK itself retries (shouldRetry in
// internal/requestconfig/requestconfig.go); anything else is a problem with the request or
// the credentials that no amount of waiting fixes.
func retryableStatus(status int) bool {
	switch status {
	case http.StatusRequestTimeout, http.StatusConflict, http.StatusTooManyRequests:
		return true
	default:
		return status >= http.StatusInternalServerError
	}
}

// read turns the response into an answer, and only a turn the model finished is one. A
// refusal or a cut-off answer arrives as a successful response, and a stream that closes
// before the model said why it stopped is a broken answer, however clean the close.
func read(message *anthropic.BetaMessage, evidence docs.Evidence) (docs.Answer, error) {
	switch message.StopReason {
	case anthropic.BetaStopReasonEndTurn, anthropic.BetaStopReasonStopSequence:
	case anthropic.BetaStopReasonRefusal:
		return docs.Answer{}, fmt.Errorf("category %q: %w", message.StopDetails.Category, docs.ErrRefused)
	case "":
		return docs.Answer{}, fmt.Errorf("the stream ended with no stop reason: %w", docs.ErrModelUnavailable)
	default:
		return docs.Answer{}, fmt.Errorf("stopped for %q after %d output tokens: %w",
			message.StopReason, message.Usage.OutputTokens, docs.ErrTruncated)
	}
	texts := lo.FilterMap(message.Content, textBlock)
	text := strings.Join(lo.Map(texts, func(block anthropic.BetaTextBlock, _ int) string { return block.Text }), "")
	return evidence.Answer(text, lo.FlatMap(texts, cites))
}

// textBlock keeps the answer's text; thinking blocks carry no part of it.
//
//nolint:gocritic // lo hands each element over by value; a response has a handful of blocks.
func textBlock(block anthropic.BetaContentBlockUnion, _ int) (anthropic.BetaTextBlock, bool) {
	text, ok := block.AsAny().(anthropic.BetaTextBlock)
	return text, ok
}

// cites reads the block's citations of search results, by their position among the results
// sent; the evidence decides which positions name a section. A citation of any other kind
// has no section to point at and is dropped.
//
//nolint:gocritic // lo hands each element over by value; a response has a handful of blocks.
func cites(block anthropic.BetaTextBlock, _ int) []docs.Cite {
	return lo.FilterMap(block.Citations, func(citation anthropic.BetaTextCitationUnion, _ int) (docs.Cite, bool) {
		location, ok := citation.AsAny().(anthropic.BetaCitationSearchResultLocation)
		return docs.Cite{Position: int(location.SearchResultIndex), Quote: location.CitedText}, ok
	})
}
