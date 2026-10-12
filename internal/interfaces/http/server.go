package http

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/DmytroLysenko1/Kafka-lab/pkg/httpserve"
	"github.com/DmytroLysenko1/Kafka-lab/pkg/telemetry"
)

var (
	ErrAPIKeyRequired  = errors.New("http: an api key is required; this service moves money and does not serve anonymous callers")
	ErrUseCaseRequired = errors.New("http: every route needs its use case")
)

// maxRequestBytes: a payment request is ~150 bytes of JSON; 4 KiB leaves room for longer
// identifiers and refuses anything that is not a payment before it is parsed.
//
// requestTimeout bounds a request's own work, which writeTimeout does not: that one cuts the
// connection but never cancels the handler's context. A payment is one short transaction
// (p99 under 15 ms at the client in every drill), so 5 s is a database that is not answering,
// or a pool with no free connection. Settling the transaction may run past it — the commit or,
// after a failure, the rollback, never both, each on its own 5 s bound (postgres/storage.go),
// because abandoning either halfway leaves the connection or the outcome unknown: the request's
// own work is therefore ~10 s at most. writeTimeout counts from the end of the headers, body
// included, so a body that trickles in for its full readTimeout could still leave the answer
// cut off — a caller that slow learns the outcome by repeating with its idempotency key.
//
// The other timeouts are slow-client guards: a header that never finishes, a body that
// trickles in, an idle keep-alive. shutdownTimeout plus the pod's preStop pause stays inside
// its terminationGracePeriodSeconds (deploy/helm/payments).
const (
	maxRequestBytes = 4 << 10
	requestTimeout  = 5 * time.Second
	// statusClientClosedRequest is not a status any caller receives; see observed.
	statusClientClosedRequest = 499
	// retryAfterSeconds: readiness takes an unreachable database's pods out of the Service
	// within two failed 5 s probes, so a caller back after about that long reaches a pod that
	// can answer (deploy/helm/payments/templates/deployment.yaml).
	retryAfterSeconds = 10
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 15 * time.Second
	writeTimeout      = 15 * time.Second
	idleTimeout       = 60 * time.Second
	shutdownTimeout   = 20 * time.Second
	readinessTimeout  = 2 * time.Second
)

// observer is told about each request in terms of the route pattern, never the path: a
// path carries merchant ids, and a label made of those grows without limit.
type observer interface {
	Served(route, status string, took time.Duration)
}

// pinger is the database as readiness sees it: whether it answers at all. A pod that cannot
// reach Postgres cannot authorize a payment, and failing readiness takes it out of the
// Service instead of leaving it to answer 500 to every caller routed to it.
type pinger interface {
	Ping(ctx context.Context) error
}

// UseCases are the operations the API exposes, one per route that changes or reads payments.
type UseCases struct {
	Authorize authorizer
	Capture   changer
	Refund    changer
	Totals    totalReader
}

func (u UseCases) complete() bool {
	return u.Authorize != nil && u.Capture != nil && u.Refund != nil && u.Totals != nil
}

type Server struct {
	useCases UseCases
	database pinger
	apiKey   []byte
	logger   *slog.Logger
	observer observer
}

// NewServer refuses to build a server that would answer one of its routes with a panic: a
// missing use case is a wiring mistake, and it is cheaper found at start than by a caller.
func NewServer(useCases UseCases, database pinger, apiKey string, logger *slog.Logger, watch observer) (*Server, error) {
	if apiKey == "" {
		return nil, ErrAPIKeyRequired
	}
	if !useCases.complete() {
		return nil, ErrUseCaseRequired
	}
	return &Server{
		useCases: useCases,
		database: database,
		apiKey:   []byte(apiKey),
		logger:   logger,
		observer: watch,
	}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /readyz", s.ready)
	mux.Handle("POST /payments", s.observed("POST /payments", bounded(s.authenticated(http.HandlerFunc(s.authorizePayment)))))
	mux.Handle("POST /merchants/{merchantID}/payments/{paymentID}/capture", s.observed("POST /merchants/{id}/payments/{id}/capture", bounded(s.authenticated(s.changePayment("capture a payment", s.useCases.Capture)))))
	mux.Handle("POST /merchants/{merchantID}/payments/{paymentID}/refund", s.observed("POST /merchants/{id}/payments/{id}/refund", bounded(s.authenticated(s.changePayment("refund a payment", s.useCases.Refund)))))
	mux.Handle("GET /merchants/{merchantID}/total", s.observed("GET /merchants/{id}/total", bounded(s.authenticated(http.HandlerFunc(s.merchantTotal)))))
	return mux
}

// Listen serves until the context is cancelled, then gives in-flight requests a bounded
// time to finish. A payment that is mid-transaction when the pod is told to stop should be
// allowed to finish rather than be cut off and retried by a client that cannot tell which.
func (s *Server) Listen(ctx context.Context, address string) error {
	return httpserve.Serve(ctx, &http.Server{
		Addr:              address,
		Handler:           s.Handler(),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}, shutdownTimeout)
}

// ready is asked by the kubelet, which holds no API key, so it sits outside authenticated.
// The ping has a deadline of its own, shorter than the probe's: a database that hangs must
// read as not ready, not as a probe that timed out without an answer.
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), readinessTimeout)
	defer cancel()

	if err := s.database.Ping(ctx); err != nil {
		s.logger.WarnContext(ctx, "not ready: the database does not answer", "error", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// bounded gives the request's work a deadline of its own; see requestTimeout.
func bounded(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// authenticated compares in constant time: a comparison that returns early tells a caller
// how much of the key it guessed correctly, one byte per attempt.
func (s *Server) authenticated(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		presented := []byte(r.Header.Get("X-API-Key"))
		if subtle.ConstantTimeCompare(presented, s.apiKey) != 1 {
			s.respond(r.Context(), w, http.StatusUnauthorized, failure{
				Code:    "unauthorized",
				Message: "a valid X-API-Key header is required",
			})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// observed opens the request's span — continuing the caller's trace when it sent one, named
// by the route pattern like the metrics — then times the request and records the status the
// caller actually received, which means wrapping the writer: the status is not knowable any
// other way once the handler has written it. A request that failed because its caller hung up
// is recorded as 499, nginx's "client closed request": nobody received that 5xx, and counted
// as one it would page someone over a caller that gave up.
func (s *Server) observed(route string, next http.Handler) http.Handler {
	return otelhttp.NewHandler(s.timed(route, next), route)
}

func (s *Server) timed(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorder := &statusRecorder{
			ResponseWriter: w,
			status:         http.StatusOK,
		}
		next.ServeHTTP(recorder, r)
		s.observer.Served(route, strconv.Itoa(reportedStatus(r.Context(), recorder.status)), time.Since(started))
	})
}

func reportedStatus(ctx context.Context, written int) int {
	if written >= http.StatusInternalServerError && errors.Is(ctx.Err(), context.Canceled) {
		return statusClientClosedRequest
	}
	return written
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// respond writes every answer the same way. nosniff and no-store because the bodies carry
// payment ids and merchants' totals, which no browser should reinterpret and no cache should
// keep. A 503 says when to come back: every one of them asks the caller to repeat the request,
// and a caller told to repeat without a pause would do it at once, while the database it is
// waiting for is least able to take it. X-Trace-Id names the trace the request ran in, so a
// caller reporting a failure can hand over the one id that finds every hop it touched.
func (s *Server) respond(ctx context.Context, w http.ResponseWriter, status int, body any) {
	header := w.Header()
	if traceID := telemetry.TraceID(ctx); traceID != "" {
		header.Set("X-Trace-Id", traceID)
	}
	header.Set("Content-Type", "application/json")
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("Cache-Control", "no-store")
	if status == http.StatusServiceUnavailable {
		header.Set("Retry-After", strconv.Itoa(retryAfterSeconds))
	}
	w.WriteHeader(status)
	// The status is already on the wire, so what fails here is the connection — almost always
	// a caller that went away — not the service: a warning, not an error to be paged for.
	if err := json.NewEncoder(w).Encode(body); err != nil {
		s.logger.WarnContext(ctx, "failed to write the response", "error", err)
	}
}
