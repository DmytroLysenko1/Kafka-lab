package http_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/merchants"
	"github.com/DmytroLysenko1/Kafka-lab/internal/application/payments"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
	payhttp "github.com/DmytroLysenko1/Kafka-lab/internal/interfaces/http"
)

const apiKey = "lab-key"

type fakeAuthorizer struct {
	result payments.AuthorizeResult
	err    error
	calls  int
	seen   payments.AuthorizeCommand
}

func (f *fakeAuthorizer) Execute(_ context.Context, cmd payments.AuthorizeCommand) (payments.AuthorizeResult, error) {
	f.calls++
	f.seen = cmd
	return f.result, f.err
}

type fakeTotals struct {
	total int64
	err   error
	seen  string
}

func (f *fakeTotals) Execute(_ context.Context, merchantID string) (int64, error) {
	f.seen = merchantID
	return f.total, f.err
}

func harness(t *testing.T, authorize *fakeAuthorizer, totals *fakeTotals) http.Handler {
	t.Helper()
	server, err := payhttp.NewServer(authorize, totals, apiKey, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	return server.Handler()
}

func authorizeRequest(t *testing.T, body string, headers map[string]string) *http.Request {
	t.Helper()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/payments", strings.NewReader(body))
	request.Header.Set("X-API-Key", apiKey)
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	return request
}

func decodeFailure(t *testing.T, response *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode the failure body %q: %v", response.Body.String(), err)
	}
	return body.Code
}

// A service that moves money does not serve callers it cannot name, and there is no flag to
// turn that off: refusing to start is the only setting.
func TestAServerWithoutAnAPIKeyRefusesToStart(t *testing.T) {
	_, err := payhttp.NewServer(&fakeAuthorizer{}, &fakeTotals{}, "", slog.New(slog.DiscardHandler))
	if !errors.Is(err, payhttp.ErrAPIKeyRequired) {
		t.Fatalf("err = %v, want %v", err, payhttp.ErrAPIKeyRequired)
	}
}

func TestAuthorizingAPaymentPassesTheCommandOnAndAnswers201(t *testing.T) {
	authorize := &fakeAuthorizer{result: payments.AuthorizeResult{PaymentID: "11111111-1111-4111-8111-111111111111", Stored: true}}
	handler := harness(t, authorize, &fakeTotals{})

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, authorizeRequest(t,
		`{"merchant_id":"m-42","amount_minor":1999,"currency":"EUR"}`,
		map[string]string{"Idempotency-Key": "key-1"}))

	if diff := cmp.Diff(http.StatusCreated, response.Code); diff != "" {
		t.Errorf("status mismatch (-want +got):\n%s", diff)
	}

	want := payments.AuthorizeCommand{MerchantID: "m-42", AmountMinor: 1999, Currency: "EUR", IdempotencyKey: "key-1"}
	if diff := cmp.Diff(want, authorize.seen); diff != "" {
		t.Errorf("the use case was called with the wrong command (-want +got):\n%s", diff)
	}

	var body struct {
		PaymentID string `json:"payment_id"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode the body: %v", err)
	}
	if diff := cmp.Diff("11111111-1111-4111-8111-111111111111", body.PaymentID); diff != "" {
		t.Errorf("payment id mismatch (-want +got):\n%s", diff)
	}
}

// The distinction a retrying client needs: 201 means this request created the payment, 200
// means an earlier one did and this is that same payment.
func TestAReplayedKeyAnswers200RatherThan201(t *testing.T) {
	authorize := &fakeAuthorizer{result: payments.AuthorizeResult{PaymentID: "11111111-1111-4111-8111-111111111111"}}
	handler := harness(t, authorize, &fakeTotals{})

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, authorizeRequest(t,
		`{"merchant_id":"m-42","amount_minor":1999,"currency":"EUR"}`,
		map[string]string{"Idempotency-Key": "key-1"}))

	if diff := cmp.Diff(http.StatusOK, response.Code); diff != "" {
		t.Errorf("status mismatch (-want +got):\n%s", diff)
	}
}

func TestRefusalsBecomeTheStatusTheCallerCanActOn(t *testing.T) {
	tests := []struct {
		name     string
		args     error
		want     int
		wantCode string
	}{
		{name: "no idempotency key", args: payments.ErrIdempotencyKeyRequired, want: http.StatusBadRequest, wantCode: "idempotency_key_required"},
		{name: "the key bought something else", args: payments.ErrIdempotencyKeyReused, want: http.StatusConflict, wantCode: "idempotency_key_reused"},
		{name: "no merchant", args: payment.ErrMerchantRequired, want: http.StatusBadRequest, wantCode: "merchant_required"},
		{name: "a currency that is not one", args: payment.ErrCurrencyFormat, want: http.StatusBadRequest, wantCode: "currency_invalid"},
		{name: "an amount of zero", args: payment.ErrAmountNotPositive, want: http.StatusBadRequest, wantCode: "amount_invalid"},
		{name: "the database is unreachable", args: payment.ErrStorage, want: http.StatusServiceUnavailable, wantCode: "storage_unavailable"},
		{name: "something nobody classified", args: errors.New("some driver said something"), want: http.StatusInternalServerError, wantCode: "internal"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := harness(t, &fakeAuthorizer{err: tt.args}, &fakeTotals{})

			response := httptest.NewRecorder()
			handler.ServeHTTP(response, authorizeRequest(t,
				`{"merchant_id":"m-42","amount_minor":1999,"currency":"EUR"}`,
				map[string]string{"Idempotency-Key": "key-1"}))

			if diff := cmp.Diff(tt.want, response.Code); diff != "" {
				t.Errorf("status mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.wantCode, decodeFailure(t, response)); diff != "" {
				t.Errorf("failure code mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// Whatever went wrong inside, the caller learns that it went wrong and nothing else: an
// error string carries table names, identifiers and driver text.
func TestAnInternalFailureTellsTheCallerNothingAboutTheInside(t *testing.T) {
	handler := harness(t, &fakeAuthorizer{err: errors.New("pq: relation \"payments\" does not exist on host db-7")}, &fakeTotals{})

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, authorizeRequest(t,
		`{"merchant_id":"m-42","amount_minor":1999,"currency":"EUR"}`,
		map[string]string{"Idempotency-Key": "key-1"}))

	body := response.Body.String()
	for _, leak := range []string{"pq:", "relation", "db-7"} {
		if strings.Contains(body, leak) {
			t.Errorf("the response body carries %q from the underlying error: %s", leak, body)
		}
	}
}

func TestBodiesTheServerWillNotAccept(t *testing.T) {
	tests := []struct {
		name string
		args string
	}{
		{name: "not json at all", args: `{`},
		{name: "a field nobody declared, which is usually a typo in one that matters", args: `{"merchant_id":"m-42","amount_minor":1999,"currency":"EUR","amount":19.99}`},
		{name: "an amount sent as a decimal, which is how rounding bugs arrive", args: `{"merchant_id":"m-42","amount_minor":19.99,"currency":"EUR"}`},
		{name: "a body far larger than any payment", args: `{"merchant_id":"` + strings.Repeat("m", 8<<10) + `"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorize := &fakeAuthorizer{}
			handler := harness(t, authorize, &fakeTotals{})

			response := httptest.NewRecorder()
			handler.ServeHTTP(response, authorizeRequest(t, tt.args, map[string]string{"Idempotency-Key": "key-1"}))

			if diff := cmp.Diff(http.StatusBadRequest, response.Code); diff != "" {
				t.Errorf("status mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(0, authorize.calls); diff != "" {
				t.Errorf("the use case was reached anyway (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCallersWithoutTheKeyNeverReachTheUseCase(t *testing.T) {
	tests := []struct {
		name string
		args map[string]string
	}{
		{name: "no key at all", args: map[string]string{"X-API-Key": ""}},
		{name: "a wrong key", args: map[string]string{"X-API-Key": "not-the-key"}},
		{name: "a prefix of the right key", args: map[string]string{"X-API-Key": apiKey[:3]}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorize := &fakeAuthorizer{}
			handler := harness(t, authorize, &fakeTotals{})

			request := authorizeRequest(t, `{"merchant_id":"m-42","amount_minor":1999,"currency":"EUR"}`, tt.args)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			if diff := cmp.Diff(http.StatusUnauthorized, response.Code); diff != "" {
				t.Errorf("status mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(0, authorize.calls); diff != "" {
				t.Errorf("an unauthenticated call reached the use case (-want +got):\n%s", diff)
			}
		})
	}
}

func TestReadingAMerchantTotal(t *testing.T) {
	totals := &fakeTotals{total: 2499}
	handler := harness(t, &fakeAuthorizer{}, totals)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/merchants/m-42/total", nil)
	request.Header.Set("X-API-Key", apiKey)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if diff := cmp.Diff(http.StatusOK, response.Code); diff != "" {
		t.Errorf("status mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff("m-42", totals.seen); diff != "" {
		t.Errorf("the merchant id from the path was not passed on (-want +got):\n%s", diff)
	}

	var body struct {
		MerchantID      string `json:"merchant_id"`
		AuthorizedMinor int64  `json:"authorized_minor"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode the body: %v", err)
	}
	if diff := cmp.Diff(int64(2499), body.AuthorizedMinor); diff != "" {
		t.Errorf("total mismatch (-want +got):\n%s", diff)
	}
}

func TestAMerchantIdTheProjectionCannotHoldIsRefused(t *testing.T) {
	handler := harness(t, &fakeAuthorizer{}, &fakeTotals{err: merchants.ErrUnprocessable})

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/merchants/"+strings.Repeat("m", 65)+"/total", nil)
	request.Header.Set("X-API-Key", apiKey)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if diff := cmp.Diff(http.StatusBadRequest, response.Code); diff != "" {
		t.Errorf("status mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff("merchant_invalid", decodeFailure(t, response)); diff != "" {
		t.Errorf("failure code mismatch (-want +got):\n%s", diff)
	}
}
