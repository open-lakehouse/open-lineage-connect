package forwarder

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lineagev1 "github.com/open-lakehouse/open-lineage-service/gen/lineage/v1"
	tablewriterv1 "github.com/open-lakehouse/open-lineage-service/gen/table/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func makeEvent(producer string) *lineagev1.OpenLineageEvent {
	return &lineagev1.OpenLineageEvent{
		Event: &lineagev1.OpenLineageEvent_RunEvent{
			RunEvent: &lineagev1.RunEvent{
				EventType: "START",
				EventTime: timestamppb.Now(),
				Producer:  producer,
				Job: &lineagev1.Job{
					Namespace: "test-ns",
					Name:      "test-job",
				},
			},
		},
	}
}

func TestForwarder_BatchesEvents(t *testing.T) {
	var received atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req tablewriterv1.WriteBatchRequest
		if err := json.Unmarshal(body, &req); err == nil {
			received.Add(int32(len(req.Events)))
		}
		resp := tablewriterv1.WriteBatchResponse{Status: "ok", Written: int32(len(req.Events))}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	fwd := New(srv.URL, WithBatchSize(5), WithFlushMs(50), WithChanSize(100))

	for i := 0; i < 10; i++ {
		fwd.Forward(makeEvent("test"))
	}

	// Wait for flush.
	time.Sleep(200 * time.Millisecond)
	fwd.Close()

	got := received.Load()
	if got != 10 {
		t.Errorf("expected 10 events forwarded, got %d", got)
	}
}

func TestForwarder_FlushOnTimer(t *testing.T) {
	var received atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req tablewriterv1.WriteBatchRequest
		if err := json.Unmarshal(body, &req); err == nil {
			received.Add(int32(len(req.Events)))
		}
		resp := tablewriterv1.WriteBatchResponse{Status: "ok", Written: int32(len(req.Events))}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	fwd := New(srv.URL, WithBatchSize(1000), WithFlushMs(50))

	fwd.Forward(makeEvent("timer-test"))

	time.Sleep(200 * time.Millisecond)
	fwd.Close()

	got := received.Load()
	if got != 1 {
		t.Errorf("expected 1 event flushed by timer, got %d", got)
	}
}

func TestForwarder_GracefulShutdown(t *testing.T) {
	var mu sync.Mutex
	var total int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req tablewriterv1.WriteBatchRequest
		if err := json.Unmarshal(body, &req); err == nil {
			mu.Lock()
			total += int32(len(req.Events))
			mu.Unlock()
		}
		resp := tablewriterv1.WriteBatchResponse{Status: "ok", Written: int32(len(req.Events))}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	fwd := New(srv.URL, WithBatchSize(1000), WithFlushMs(10000))

	for i := 0; i < 5; i++ {
		fwd.Forward(makeEvent("shutdown-test"))
	}

	// Close should drain without waiting for the long flush interval.
	fwd.Close()

	mu.Lock()
	got := total
	mu.Unlock()

	if got != 5 {
		t.Errorf("expected 5 events drained on shutdown, got %d", got)
	}
}

func TestForwarder_DropsWhenFull(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	fwd := New(srv.URL, WithChanSize(2), WithBatchSize(1000), WithFlushMs(10000))

	// Fill the channel (size=2) plus one extra that should be dropped.
	fwd.Forward(makeEvent("a"))
	fwd.Forward(makeEvent("b"))
	fwd.Forward(makeEvent("drop-me"))

	// The third should not block.
	fwd.Close()
}
