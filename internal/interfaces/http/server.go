package http

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"
)

var ErrAPIKeyRequired = errors.New("http: an api key is required; this service moves money and does not serve anonymous callers")

const (
	maxRequestBytes   = 4 << 10
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 15 * time.Second
	writeTimeout      = 15 * time.Second
	idleTimeout       = 60 * time.Second
	shutdownTimeout   = 20 * time.Second
)

// observer is told about each request in terms of the route pattern, never the path: a
// path carries merchant ids, and a label made of those grows without limit.
type observer interface {
	Served(route, status string, took time.Duration)
}

type Server struct {
	authorize authorizer
	totals    totalReader
	apiKey    []byte
	logger    *slog.Logger
	observer  observer
}

func NewServer(authorize authorizer, totals totalReader, apiKey string, logger *slog.Logger, watch observer) (*Server, error) {
	if apiKey == "" {
		return nil, ErrAPIKeyRequired
	}
	return &Server{authorize: authorize, totals: totals, apiKey: []byte(apiKey), logger: logger, observer: watch}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.Handle("POST /payments", s.observed("POST /payments", s.authenticated(http.HandlerFunc(s.authorizePayment))))
	mux.Handle("GET /merchants/{merchantID}/total", s.observed("GET /merchants/{id}/total", s.authenticated(http.HandlerFunc(s.merchantTotal))))
	return mux
}

// Listen serves until the context is cancelled, then gives in-flight requests a bounded
// time to finish. A payment that is mid-transaction when the pod is told to stop should be
// allowed to finish rather than be cut off and retried by a client that cannot tell which.
func (s *Server) Listen(ctx context.Context, address string) error {
	server := &http.Server{
		Addr:              address,
		Handler:           s.Handler(),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		BaseContext:       func(_ net.Listener) context.Context { return context.WithoutCancel(ctx) },
	}

	served := make(chan error, 1)
	go func() { served <- server.ListenAndServe() }()

	select {
	case err := <-served:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		stopping, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer cancel()
		return server.Shutdown(stopping)
	}
}

// authenticated compares in constant time: a comparison that returns early tells a caller
// how much of the key it guessed correctly, one byte per attempt.
func (s *Server) authenticated(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		presented := []byte(r.Header.Get("X-API-Key"))
		if subtle.ConstantTimeCompare(presented, s.apiKey) != 1 {
			s.respond(r.Context(), w, http.StatusUnauthorized, failure{Code: "unauthorized", Message: "a valid X-API-Key header is required"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// observed times the request and records the status the caller actually received, which
// means wrapping the writer: the status is not knowable any other way once the handler has
// written it.
func (s *Server) observed(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		s.observer.Served(route, strconv.Itoa(recorder.status), time.Since(started))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (s *Server) respond(ctx context.Context, w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		s.logger.ErrorContext(ctx, "failed to write the response", "error", err)
	}
}
