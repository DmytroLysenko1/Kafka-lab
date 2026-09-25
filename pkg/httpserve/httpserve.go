// Package httpserve runs an http.Server for as long as a context lives and then shuts it
// down within a bound, so that requests in flight can finish and a stuck one cannot hold
// the process forever.
package httpserve

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

var ErrListen = errors.New("httpserve: cannot listen")

// Serve binds server.Addr and serves on it; see ServeOn.
func Serve(ctx context.Context, server *http.Server, shutdownTimeout time.Duration) error {
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", server.Addr)
	if err != nil {
		return fmt.Errorf("%w on %s: %w", ErrListen, server.Addr, err)
	}
	return ServeOn(ctx, server, listener, shutdownTimeout)
}

// ServeOn serves on a listener that is already bound until ctx ends, then gives requests in
// flight up to shutdownTimeout. The requests themselves run on a context that ctx's end does
// not cancel: stopping the server must not cut a request off half-way, which is what the
// shutdown window is for.
func ServeOn(ctx context.Context, server *http.Server, listener net.Listener, shutdownTimeout time.Duration) error {
	server.BaseContext = func(net.Listener) context.Context {
		return context.WithoutCancel(ctx)
	}

	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()

	select {
	case err := <-served:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		stopping, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer cancel()
		err := server.Shutdown(stopping)
		// Serve returns ErrServerClosed as soon as Shutdown begins; waiting for it here is
		// what joins the serving goroutine before ServeOn returns.
		<-served
		return err
	}
}
