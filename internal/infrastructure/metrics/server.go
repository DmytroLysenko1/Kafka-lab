package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/DmytroLysenko1/Kafka-lab/pkg/httpserve"
)

const (
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 10 * time.Second
	writeTimeout      = 10 * time.Second
	shutdownTimeout   = 5 * time.Second
)

// Serve exposes the metrics on a listener of their own, never beside the business API: a
// scrape must not be able to take a connection a payment needed, and the two are reached
// from different places — one by an operator's Prometheus, one by a caller.
func Serve(ctx context.Context, address string, gatherer prometheus.Gatherer) error {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	return httpserve.Serve(ctx, &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
	}, shutdownTimeout)
}
