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
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/Kafka-lab/internal/application/payments"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/merchant"
	"github.com/DmytroLysenko1/Kafka-lab/internal/domain/payment"
	payhttp "github.com/DmytroLysenko1/Kafka-lab/internal/interfaces/http"
)

const apiKey = "lab-key"

// fakeAuthorizer answers as the use case would, or — with hangs — waits on a database that
// never replies, which is what the request's own deadline is there to cut short.
type fakeAuthorizer struct {
	result payments.AuthorizeResult
	err    error
	hangs  bool
	calls  int
	seen   payments.AuthorizeCommand
}

func (f *fakeAuthorizer) Execute(ctx context.Context, cmd payments.AuthorizeCommand) (payments.AuthorizeResult, error) {
	f.calls++
	f.seen = cmd
	if f.hangs {
		<-ctx.Done()
		return payments.AuthorizeResult{}, errors.Join(payment.ErrStorage, ctx.Err())
	}
	return f.result, f.err
}

type fakeTotals struct {
	totals []merchant.Total
	err    error
	seen   string
}

func (f *fakeTotals) Execute(_ context.Context, merchantID string) ([]merchant.Total, error) {
	f.seen = merchantID
	return f.totals, f.err
}

func total(t *testing.T, currency string, minor int64) merchant.Total {
	t.Helper()
	made, err := merchant.NewTotal(currency, minor, 0, 0)
	if err != nil {
		t.Fatalf("total %s %d: %v", currency, minor, err)
	}
	return made
}

// fakeDatabase answers a ping the way Postgres would in each state readiness must tell
// apart: up, refusing connections, or accepting them and never replying.
type fakeDatabase struct {
	refuses error
	hangs   bool
}

func (f *fakeDatabase) Ping(ctx context.Context) error {
	if f.hangs {
		<-ctx.Done()
		return ctx.Err()
	}
	return f.refuses
}

// watcher records what the server reported about each request, so a test can check the
// route label is the pattern rather than the path — a label built from paths would grow a
// new series per merchant.
type watcher struct {
	routes   []string
	statuses []string
}

func (w *watcher) Served(route, status string, _ time.Duration) {
	w.routes = append(w.routes, route)
	w.statuses = append(w.statuses, status)
}

func harness(t *testing.T, authorize *fakeAuthorizer, totals *fakeTotals) http.Handler {
	t.Helper()
	return watched(t, authorize, totals, &watcher{})
}

func watched(t *testing.T, authorize *fakeAuthorizer, totals *fakeTotals, watch *watcher) http.Handler {
	t.Helper()
	return serve(t, authorize, totals, &fakeDatabase{}, watch)
}

func serve(t *testing.T, authorize *fakeAuthorizer, totals *fakeTotals, database *fakeDatabase, watch *watcher) http.Handler {
	t.Helper()
	server, err := payhttp.NewServer(payhttp.UseCases{
		Authorize: authorize,
		Capture:   &fakeChanger{},
		Refund:    &fakeChanger{},
		Totals:    totals,
	}, database, apiKey, slog.New(slog.DiscardHandler), watch)
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
	_, err := payhttp.NewServer(payhttp.UseCases{
		Authorize: &fakeAuthorizer{},
		Capture:   &fakeChanger{},
		Refund:    &fakeChanger{},
		Totals:    &fakeTotals{},
	}, &fakeDatabase{}, "", slog.New(slog.DiscardHandler), &watcher{})
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

	key, err := payments.ParseIdempotencyKey("key-1")
	if err != nil {
		t.Fatalf("parse the key: %v", err)
	}
	want := payments.AuthorizeCommand{MerchantID: "m-42", AmountMinor: 1999, Currency: "EUR", IdempotencyKey: key}
	if diff := cmp.Diff(want, authorize.seen, cmp.Comparer(func(a, b payments.IdempotencyKey) bool { return a.String() == b.String() })); diff != "" {
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
		{name: "an amount no total could keep", args: payment.ErrAmountTooLarge, want: http.StatusBadRequest, wantCode: "amount_too_large"},
		{name: "a merchant id with a NUL byte", args: payment.ErrMerchantInvalid, want: http.StatusBadRequest, wantCode: "merchant_invalid"},
		{name: "the database is unreachable", args: payment.ErrStorage, want: http.StatusServiceUnavailable, wantCode: "storage_unavailable"},
		{name: "the commit's outcome never came back", args: errors.Join(payment.ErrOutcomeUnknown, context.DeadlineExceeded), want: http.StatusServiceUnavailable, wantCode: "outcome_unknown"},
		{name: "the request ran out of time outside any store", args: context.DeadlineExceeded, want: http.StatusServiceUnavailable, wantCode: "timeout"},
		{name: "a stored payment this version cannot read back, which no retry changes", args: errors.Join(payment.ErrCorrupt, errors.New("status unknown")), want: http.StatusInternalServerError, wantCode: "internal"},
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
	captured, err := merchant.NewTotal("EUR", 2499, 2000, 500)
	if err != nil {
		t.Fatalf("total: %v", err)
	}
	totals := &fakeTotals{totals: []merchant.Total{captured, total(t, "JPY", 1999)}}
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

	type currencyTotal struct {
		Currency        string `json:"currency"`
		AuthorizedMinor int64  `json:"authorized_minor"`
		CapturedMinor   int64  `json:"captured_minor"`
		RefundedMinor   int64  `json:"refunded_minor"`
	}
	var body struct {
		MerchantID string          `json:"merchant_id"`
		Totals     []currencyTotal `json:"totals"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode the body: %v", err)
	}
	want := []currencyTotal{{Currency: "EUR", AuthorizedMinor: 2499, CapturedMinor: 2000, RefundedMinor: 500}, {Currency: "JPY", AuthorizedMinor: 1999}}
	if diff := cmp.Diff(want, body.Totals); diff != "" {
		t.Errorf("totals mismatch (-want +got):\n%s", diff)
	}
}

// A merchant nobody has paid has no totals, and the body says so with an empty list — not
// null, which a client would have to special-case, and not a zero in some currency.
func TestAMerchantWithNoPaymentsHasAnEmptyListOfTotals(t *testing.T) {
	handler := harness(t, &fakeAuthorizer{}, &fakeTotals{totals: []merchant.Total{}})
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/merchants/m-42/total", nil)
	request.Header.Set("X-API-Key", apiKey)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if diff := cmp.Diff(`{"merchant_id":"m-42","totals":[]}`, strings.TrimSpace(response.Body.String())); diff != "" {
		t.Errorf("body mismatch (-want +got):\n%s", diff)
	}
}

func TestAMerchantIdTheProjectionCannotHoldIsRefused(t *testing.T) {
	handler := harness(t, &fakeAuthorizer{}, &fakeTotals{err: merchant.ErrIDTooLong})

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

// The label must be the route pattern, not the path: one series per merchant would be a
// metric that gets more expensive the more customers the service has.
func TestRequestsAreReportedByRoutePatternAndStatus(t *testing.T) {
	watch := &watcher{}
	handler := watched(t, &fakeAuthorizer{err: payments.ErrIdempotencyKeyReused}, &fakeTotals{totals: []merchant.Total{}}, watch)

	handler.ServeHTTP(httptest.NewRecorder(), authorizeRequest(t,
		`{"merchant_id":"m-42","amount_minor":1999,"currency":"EUR"}`,
		map[string]string{"Idempotency-Key": "key-1"}))

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/merchants/m-42/total", nil)
	request.Header.Set("X-API-Key", apiKey)
	handler.ServeHTTP(httptest.NewRecorder(), request)

	if diff := cmp.Diff([]string{"POST /payments", "GET /merchants/{id}/total"}, watch.routes); diff != "" {
		t.Errorf("routes reported (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"409", "200"}, watch.statuses); diff != "" {
		t.Errorf("statuses reported (-want +got):\n%s", diff)
	}
}

// Readiness decides whether the Service routes callers to this pod. It is asked without an
// API key, because the kubelet has none, and a database that hangs must read as not ready
// within the server's own deadline rather than leave the probe to time out: inside the
// synctest bubble a ping without a deadline would block forever and fail the test as a
// deadlock, and one with a deadline longer than the probe's would show in the fake clock.
func TestReadinessFollowsWhetherTheDatabaseAnswers(t *testing.T) {
	// timeoutSeconds of the readiness probe in deploy/helm/payments/templates/deployment.yaml.
	const probeTimeout = 3 * time.Second

	type args struct {
		database *fakeDatabase
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{name: "database answers", args: args{database: &fakeDatabase{}}, want: http.StatusOK},
		{name: "database refuses connections", args: args{database: &fakeDatabase{refuses: errors.New("connection refused")}}, want: http.StatusServiceUnavailable},
		{name: "database never replies", args: args{database: &fakeDatabase{hangs: true}}, want: http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				handler := serve(t, &fakeAuthorizer{}, &fakeTotals{}, tt.args.database, &watcher{})
				response := httptest.NewRecorder()
				started := time.Now()

				handler.ServeHTTP(response, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/readyz", nil))

				if diff := cmp.Diff(tt.want, response.Code); diff != "" {
					t.Errorf("status mismatch (-want +got):\n%s", diff)
				}
				if took := time.Since(started); took >= probeTimeout {
					t.Errorf("answered after %s, the kubelet gives up at %s and reads a timeout, not an answer", took, probeTimeout)
				}
			})
		})
	}
}

// A key the database index cannot hold is the caller's mistake, and it is refused before
// the use case runs — not stored, not a 500.
func TestAnIdempotencyKeyLongerThanTheHeaderAllowsIsRefused(t *testing.T) {
	type args struct {
		keyLength int
	}
	type want struct {
		status int
		code   string
		calls  int
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{name: "at the limit", args: args{keyLength: 255}, want: want{status: http.StatusCreated, calls: 1}},
		{name: "one byte over", args: args{keyLength: 256}, want: want{status: http.StatusBadRequest, code: "idempotency_key_too_long", calls: 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorize := &fakeAuthorizer{result: payments.AuthorizeResult{PaymentID: "p-1", Stored: true}}
			handler := harness(t, authorize, &fakeTotals{})
			response := httptest.NewRecorder()
			request := authorizeRequest(t, `{"merchant_id":"m-1","amount_minor":100,"currency":"EUR"}`,
				map[string]string{"Idempotency-Key": strings.Repeat("k", tt.args.keyLength)})

			handler.ServeHTTP(response, request)

			got := want{status: response.Code, calls: authorize.calls}
			if response.Code != http.StatusCreated {
				got.code = decodeFailure(t, response)
			}
			if diff := cmp.Diff(tt.want, got, cmp.AllowUnexported(want{})); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// A database that stops answering must not hold a payment request open until the client
// gives up: the request's own deadline cuts it at 5 s, and the caller hears 503 — "repeat with
// the same key" — instead of a connection dropped by the write timeout after 15 s with the
// metric recording whatever status the handler would have written.
func TestARequestWaitingOnAHungDatabaseIsAnswered503AtItsDeadline(t *testing.T) {
	// requestTimeout in internal/interfaces/http/server.go.
	const requestTimeout = 5 * time.Second

	synctest.Test(t, func(t *testing.T) {
		handler := harness(t, &fakeAuthorizer{hangs: true}, &fakeTotals{})
		response := httptest.NewRecorder()
		started := time.Now()

		handler.ServeHTTP(response, authorizeRequest(t,
			`{"merchant_id":"m-42","amount_minor":1999,"currency":"EUR"}`,
			map[string]string{"Idempotency-Key": "key-1"}))

		if diff := cmp.Diff(http.StatusServiceUnavailable, response.Code); diff != "" {
			t.Errorf("status mismatch (-want +got):\n%s", diff)
		}
		if diff := cmp.Diff("storage_unavailable", decodeFailure(t, response)); diff != "" {
			t.Errorf("failure code mismatch (-want +got):\n%s", diff)
		}
		if diff := cmp.Diff(requestTimeout, time.Since(started)); diff != "" {
			t.Errorf("answered after (-want +got):\n%s", diff)
		}
		// Told to repeat, the caller is also told when: at once would hit the database while
		// it is least able to answer.
		wantHeaders := map[string]string{"Retry-After": "10", "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"}
		gotHeaders := map[string]string{}
		for name := range wantHeaders {
			gotHeaders[name] = response.Header().Get(name)
		}
		if diff := cmp.Diff(wantHeaders, gotHeaders); diff != "" {
			t.Errorf("headers (-want +got):\n%s", diff)
		}
	})
}

// A caller that hangs up mid-request cancels the work, and the handler answers a 5xx nobody
// reads. Counted as a 5xx it would look like the service failing; the metric says 499 instead,
// while a caller still waiting when the database fails is counted as the 503 it received.
func TestARequestWhoseCallerHungUpIsReportedAs499(t *testing.T) {
	tests := []struct {
		name    string
		args    bool
		want    []string
		wantErr error
	}{
		{name: "the caller hung up", args: true, want: []string{"499"}},
		{name: "the caller was still waiting", args: false, want: []string{"503"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				watch := &watcher{}
				handler := watched(t, &fakeAuthorizer{hangs: true}, &fakeTotals{}, watch)
				ctx, hangUp := context.WithCancel(t.Context())
				defer hangUp()
				if tt.args {
					time.AfterFunc(time.Second, hangUp)
				}

				request := authorizeRequest(t,
					`{"merchant_id":"m-42","amount_minor":1999,"currency":"EUR"}`,
					map[string]string{"Idempotency-Key": "key-1"})
				handler.ServeHTTP(httptest.NewRecorder(), request.WithContext(ctx))

				if diff := cmp.Diff(tt.want, watch.statuses); diff != "" {
					t.Errorf("statuses reported (-want +got):\n%s", diff)
				}
			})
		})
	}
}

// Reading totals from a database that is down is the same situation as authorising against
// one: 503, which a client retries, not a 500 that says the service is broken.
func TestReadingTotalsFromAnUnreachableDatabaseAnswers503(t *testing.T) {
	handler := harness(t, &fakeAuthorizer{}, &fakeTotals{err: errors.Join(merchant.ErrStorage, errors.New("connection refused"))})
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/merchants/m-42/total", nil)
	request.Header.Set("X-API-Key", apiKey)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if diff := cmp.Diff(http.StatusServiceUnavailable, response.Code); diff != "" {
		t.Errorf("status mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff("storage_unavailable", decodeFailure(t, response)); diff != "" {
		t.Errorf("failure code mismatch (-want +got):\n%s", diff)
	}
}

// A header may carry bytes that are not UTF-8, and Postgres refuses to store them: let
// through, the key would fail in the unique index as a storage error, and the caller would
// be told to retry a request that can never succeed.
func TestAnIdempotencyKeyThatIsNotTextIsRefusedBeforeTheUseCase(t *testing.T) {
	authorize := &fakeAuthorizer{result: payments.AuthorizeResult{PaymentID: "p-1", Stored: true}}
	handler := harness(t, authorize, &fakeTotals{})
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, authorizeRequest(t, `{"merchant_id":"m-1","amount_minor":100,"currency":"EUR"}`,
		map[string]string{"Idempotency-Key": "key-\xff"}))

	if diff := cmp.Diff(http.StatusBadRequest, response.Code); diff != "" {
		t.Errorf("status mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff("idempotency_key_invalid", decodeFailure(t, response)); diff != "" {
		t.Errorf("failure code mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(0, authorize.calls); diff != "" {
		t.Errorf("the use case ran (-want +got):\n%s", diff)
	}
}
