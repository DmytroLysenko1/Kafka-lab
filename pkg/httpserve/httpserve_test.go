package httpserve_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/DmytroLysenko1/Kafka-lab/pkg/httpserve"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return listener
}

// A request that is in flight when the service is told to stop is a payment half-way
// through; it gets to finish, and ServeOn returns only after it has, with nothing left
// running behind it.
func TestARequestInFlightWhenTheServiceStopsIsAllowedToFinish(t *testing.T) {
	listener := listen(t)
	started := make(chan struct{})
	release := make(chan struct{})
	server := &http.Server{
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(started)
			<-release
			w.WriteHeader(http.StatusCreated)
		}),
	}

	ctx, stop := context.WithCancel(t.Context())
	served := make(chan error, 1)
	go func() { served <- httpserve.ServeOn(ctx, server, listener, 5*time.Second) }()

	answered := make(chan int, 1)
	go func() { answered <- statusOf(t, listener.Addr().String()) }()
	<-started
	stop()
	close(release)

	if status := <-answered; status != http.StatusCreated {
		t.Errorf("the request in flight got %d, want %d", status, http.StatusCreated)
	}
	if err := <-served; err != nil {
		t.Errorf("ServeOn returned %v after a clean shutdown, want nil", err)
	}
}

// A port someone else holds is a failure to start, not a clean stop.
func TestAnAddressAlreadyInUseIsReportedRatherThanTreatedAsAStop(t *testing.T) {
	held := listen(t)
	t.Cleanup(func() {
		if err := held.Close(); err != nil {
			t.Errorf("release the held port: %v", err)
		}
	})

	err := httpserve.Serve(t.Context(), &http.Server{Addr: held.Addr().String(), ReadHeaderTimeout: time.Second}, time.Second)
	if !errors.Is(err, httpserve.ErrListen) {
		t.Errorf("Serve returned %v, want %v", err, httpserve.ErrListen)
	}
}

// statusOf needs no retry: the listener is bound before ServeOn starts, so the connection
// is accepted by the kernel even if the server has not reached Serve yet.
func statusOf(t *testing.T, address string) int {
	request, err := http.NewRequestWithContext(context.WithoutCancel(t.Context()), http.MethodGet, "http://"+address+"/", http.NoBody)
	if err != nil {
		t.Errorf("build the request: %v", err)
		return 0
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Errorf("request: %v", err)
		return 0
	}
	if err := response.Body.Close(); err != nil {
		t.Errorf("close the response: %v", err)
	}
	http.DefaultClient.CloseIdleConnections()
	return response.StatusCode
}
