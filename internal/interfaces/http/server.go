package http

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
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

type Server struct {
	authorize authorizer
	totals    totalReader
	apiKey    []byte
	logger    *slog.Logger
}

func NewServer(authorize authorizer, totals totalReader, apiKey string, logger *slog.Logger) (*Server, error) {
	if apiKey == "" {
		return nil, ErrAPIKeyRequired
	}
	return &Server{authorize: authorize, totals: totals, apiKey: []byte(apiKey), logger: logger}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.Handle("POST /payments", s.authenticated(http.HandlerFunc(s.authorizePayment)))
	mux.Handle("GET /merchants/{merchantID}/total", s.authenticated(http.HandlerFunc(s.merchantTotal)))
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

func (s *Server) respond(ctx context.Context, w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		s.logger.ErrorContext(ctx, "failed to write the response", "error", err)
	}
}
