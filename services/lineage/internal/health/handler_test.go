package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type checkerStub struct {
	err error
}

func (c checkerStub) Ready(context.Context) error {
	return c.err
}

func TestHealthHandler_NoChecker_ReturnsOK(t *testing.T) {
	mux := http.NewServeMux()
	NewHandler(nil).Register(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d want=%d", resp.StatusCode, http.StatusOK)
	}

	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("status=%q want=ok", body["status"])
	}
}

func TestHealthHandler_CheckerFailure_ReturnsUnavailable(t *testing.T) {
	mux := http.NewServeMux()
	NewHandler(checkerStub{err: errors.New("sidecar down")}).Register(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want=%d", resp.StatusCode, http.StatusServiceUnavailable)
	}

	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["status"] != "unhealthy" {
		t.Fatalf("status=%q want=unhealthy", body["status"])
	}
}

func TestSidecarChecker_Ready(t *testing.T) {
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Fatalf("path=%q want=/health", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer sidecar.Close()

	checker := NewSidecarChecker(sidecar.URL)
	if err := checker.Ready(context.Background()); err != nil {
		t.Fatalf("Ready() returned error: %v", err)
	}
}

func TestSidecarChecker_NotReadyOnNon200(t *testing.T) {
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer sidecar.Close()

	checker := NewSidecarChecker(sidecar.URL)
	if err := checker.Ready(context.Background()); err == nil {
		t.Fatal("Ready() error=nil, want non-nil")
	}
}
