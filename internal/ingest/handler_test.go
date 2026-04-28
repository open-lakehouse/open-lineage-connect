package ingest_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/open-lakehouse/open-lineage-service/internal/ingest"
	"github.com/open-lakehouse/open-lineage-service/internal/service"
)

func newTestServer(t *testing.T) (*httptest.Server, *service.LineageService) {
	t.Helper()
	svc := service.NewLineageService()
	mux := http.NewServeMux()
	h := ingest.NewHandler(svc)
	h.Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, svc
}

// ---------- POST /lineage ----------

func TestHandleEvent_RunEvent(t *testing.T) {
	srv, svc := newTestServer(t)

	body := `{
		"eventTime": "2024-06-15T10:30:00Z",
		"producer": "test-producer",
		"schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		"eventType": "START",
		"run": {"runId": "abc-123"},
		"job": {"namespace": "ns", "name": "job1"}
	}`

	resp, err := http.Post(srv.URL+"/lineage", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /lineage failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	events := svc.OpenLineageEvents()
	if len(events) != 1 {
		t.Fatalf("stored events = %d, want 1", len(events))
	}
	if events[0].GetRunEvent() == nil {
		t.Error("expected stored event to be RunEvent")
	}
	if events[0].GetRunEvent().GetRun().GetRunId() != "abc-123" {
		t.Errorf("RunId = %q", events[0].GetRunEvent().GetRun().GetRunId())
	}
}

func TestHandleEvent_JobEvent(t *testing.T) {
	srv, svc := newTestServer(t)

	body := `{
		"eventTime": "2024-06-15T12:00:00Z",
		"producer": "test-producer",
		"schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		"job": {"namespace": "airflow", "name": "dag.task"}
	}`

	resp, err := http.Post(srv.URL+"/lineage", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /lineage failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	events := svc.OpenLineageEvents()
	if len(events) != 1 {
		t.Fatalf("stored events = %d, want 1", len(events))
	}
	if events[0].GetJobEvent() == nil {
		t.Error("expected stored event to be JobEvent")
	}
}

func TestHandleEvent_DatasetEvent(t *testing.T) {
	srv, svc := newTestServer(t)

	body := `{
		"eventTime": "2024-06-15T08:00:00Z",
		"producer": "test-producer",
		"schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		"dataset": {"namespace": "s3://lake", "name": "raw/data"}
	}`

	resp, err := http.Post(srv.URL+"/lineage", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /lineage failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	events := svc.OpenLineageEvents()
	if len(events) != 1 {
		t.Fatalf("stored events = %d, want 1", len(events))
	}
	if events[0].GetDatasetEvent() == nil {
		t.Error("expected stored event to be DatasetEvent")
	}
}

func TestHandleEvent_InvalidJSON(t *testing.T) {
	srv, _ := newTestServer(t)

	resp, err := http.Post(srv.URL+"/lineage", "application/json", bytes.NewBufferString(`{bad json`))
	if err != nil {
		t.Fatalf("POST /lineage failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleEvent_UnclassifiableEvent(t *testing.T) {
	srv, _ := newTestServer(t)

	body := `{"eventTime": "2024-06-15T08:00:00Z", "producer": "p", "schemaURL": "s"}`
	resp, err := http.Post(srv.URL+"/lineage", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /lineage failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

// ---------- POST /lineage/batch ----------

func TestHandleBatch_MixedEvents(t *testing.T) {
	srv, svc := newTestServer(t)

	body := `[
		{
			"eventTime": "2024-06-15T10:30:00Z",
			"producer": "p1", "schemaURL": "s", "eventType": "START",
			"run": {"runId": "r1"},
			"job": {"namespace": "ns", "name": "j1"}
		},
		{
			"eventTime": "2024-06-15T11:00:00Z",
			"producer": "p2", "schemaURL": "s",
			"job": {"namespace": "ns", "name": "j2"}
		},
		{
			"eventTime": "2024-06-15T08:00:00Z",
			"producer": "p3", "schemaURL": "s",
			"dataset": {"namespace": "ns", "name": "ds"}
		}
	]`

	resp, err := http.Post(srv.URL+"/lineage/batch", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /lineage/batch failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if result["status"] != "success" {
		t.Errorf("status = %v, want success", result["status"])
	}

	summary := result["summary"].(map[string]interface{})
	if summary["received"] != float64(3) {
		t.Errorf("received = %v, want 3", summary["received"])
	}
	if summary["successful"] != float64(3) {
		t.Errorf("successful = %v, want 3", summary["successful"])
	}

	events := svc.OpenLineageEvents()
	if len(events) != 3 {
		t.Fatalf("stored events = %d, want 3", len(events))
	}
	if events[0].GetRunEvent() == nil {
		t.Error("events[0] should be RunEvent")
	}
	if events[1].GetJobEvent() == nil {
		t.Error("events[1] should be JobEvent")
	}
	if events[2].GetDatasetEvent() == nil {
		t.Error("events[2] should be DatasetEvent")
	}
}

func TestHandleBatch_PartialFailure(t *testing.T) {
	srv, svc := newTestServer(t)

	body := `[
		{
			"eventTime": "2024-06-15T10:30:00Z",
			"producer": "p1", "schemaURL": "s", "eventType": "START",
			"run": {"runId": "r1"},
			"job": {"namespace": "ns", "name": "j1"}
		},
		{
			"producer": "p2", "schemaURL": "s",
			"run": {"runId": "r2"},
			"job": {"namespace": "ns", "name": "j2"}
		}
	]`

	resp, err := http.Post(srv.URL+"/lineage/batch", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /lineage/batch failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result["status"] != "partial_success" {
		t.Errorf("status = %v, want partial_success", result["status"])
	}

	summary := result["summary"].(map[string]interface{})
	if summary["received"] != float64(2) {
		t.Errorf("received = %v, want 2", summary["received"])
	}
	if summary["successful"] != float64(1) {
		t.Errorf("successful = %v, want 1", summary["successful"])
	}
	if summary["failed"] != float64(1) {
		t.Errorf("failed = %v, want 1", summary["failed"])
	}

	failedEvents := result["failed_events"].([]interface{})
	if len(failedEvents) != 1 {
		t.Fatalf("failed_events length = %d, want 1", len(failedEvents))
	}

	// Only the valid event should be stored
	events := svc.OpenLineageEvents()
	if len(events) != 1 {
		t.Fatalf("stored events = %d, want 1", len(events))
	}
}

func TestHandleBatch_EmptyArray(t *testing.T) {
	srv, _ := newTestServer(t)

	resp, err := http.Post(srv.URL+"/lineage/batch", "application/json", bytes.NewBufferString(`[]`))
	if err != nil {
		t.Fatalf("POST /lineage/batch failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandleBatch_InvalidJSON(t *testing.T) {
	srv, _ := newTestServer(t)

	resp, err := http.Post(srv.URL+"/lineage/batch", "application/json", bytes.NewBufferString(`not json`))
	if err != nil {
		t.Fatalf("POST /lineage/batch failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

// ---------- Example file integration tests ----------

func TestHandleEvent_ExampleRunEvent(t *testing.T) {
	srv, svc := newTestServer(t)
	body := readExampleFile(t, "../../resources/examples/lineage/single/run-event.json")

	resp, err := http.Post(srv.URL+"/lineage", "application/json", bytes.NewBuffer(body))
	if err != nil {
		t.Fatalf("POST /lineage failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	events := svc.OpenLineageEvents()
	if len(events) != 1 {
		t.Fatalf("stored events = %d, want 1", len(events))
	}
	re := events[0].GetRunEvent()
	if re == nil {
		t.Fatal("expected RunEvent")
	}
	if re.EventType != "START" {
		t.Errorf("EventType = %q, want START", re.EventType)
	}
}

func TestHandleEvent_ExampleJobEvent(t *testing.T) {
	srv, svc := newTestServer(t)
	body := readExampleFile(t, "../../resources/examples/lineage/single/job-event.json")

	resp, err := http.Post(srv.URL+"/lineage", "application/json", bytes.NewBuffer(body))
	if err != nil {
		t.Fatalf("POST /lineage failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	events := svc.OpenLineageEvents()
	if len(events) != 1 {
		t.Fatalf("stored events = %d, want 1", len(events))
	}
	if events[0].GetJobEvent() == nil {
		t.Fatal("expected JobEvent")
	}
}

func TestHandleEvent_ExampleDatasetEvent(t *testing.T) {
	srv, svc := newTestServer(t)
	body := readExampleFile(t, "../../resources/examples/lineage/single/dataset-event.json")

	resp, err := http.Post(srv.URL+"/lineage", "application/json", bytes.NewBuffer(body))
	if err != nil {
		t.Fatalf("POST /lineage failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	events := svc.OpenLineageEvents()
	if len(events) != 1 {
		t.Fatalf("stored events = %d, want 1", len(events))
	}
	if events[0].GetDatasetEvent() == nil {
		t.Fatal("expected DatasetEvent")
	}
}

func TestHandleBatch_ExampleMixedBatch(t *testing.T) {
	srv, svc := newTestServer(t)
	body := readExampleFile(t, "../../resources/examples/lineage/batch/mixed-event-batch.json")

	resp, err := http.Post(srv.URL+"/lineage/batch", "application/json", bytes.NewBuffer(body))
	if err != nil {
		t.Fatalf("POST /lineage/batch failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	events := svc.OpenLineageEvents()
	if len(events) != 3 {
		t.Fatalf("stored events = %d, want 3", len(events))
	}
}

func readExampleFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read example file %s: %v", path, err)
	}
	return data
}
