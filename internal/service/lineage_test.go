package service_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	lineagev1 "github.com/open-lakehouse/open-lineage-service/gen/lineage/v1"
	"github.com/open-lakehouse/open-lineage-service/gen/lineage/v1/lineagev1connect"
	"github.com/open-lakehouse/open-lineage-service/internal/interceptor"
	"github.com/open-lakehouse/open-lineage-service/internal/service"
)

// transport enumerates the two Connect content types we exercise.
type transport struct {
	name string
	opts []connect.ClientOption
}

var transports = []transport{
	{name: "proto" /* default binary protobuf */},
	{name: "json", opts: []connect.ClientOption{connect.WithProtoJSON()}},
}

// testEnv holds a running httptest server and a fresh service instance.
type testEnv struct {
	server *httptest.Server
	svc    *service.LineageService
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()

	svc := service.NewLineageService()
	mux := http.NewServeMux()
	path, handler := lineagev1connect.NewLineageServiceHandler(
		svc,
		connect.WithInterceptors(
			interceptor.NewAuthInterceptor(),
			interceptor.NewValidateInterceptor(),
		),
	)
	mux.Handle(path, handler)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return &testEnv{server: srv, svc: svc}
}

func (e *testEnv) client(opts ...connect.ClientOption) lineagev1connect.LineageServiceClient {
	return lineagev1connect.NewLineageServiceClient(e.server.Client(), e.server.URL, opts...)
}

func authedRequest[T any](msg *T) *connect.Request[T] {
	req := connect.NewRequest(msg)
	req.Header().Set("Authorization", "Bearer valid-token")
	return req
}

// ---------- IngestEvent ----------

func TestIngestEvent_HappyPath(t *testing.T) {
	env := newTestEnv(t)

	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			client := env.client(tr.opts...)
			resp, err := client.IngestEvent(context.Background(), authedRequest(&lineagev1.IngestEventRequest{
				Event: &lineagev1.RunEvent{
					EventType: "START",
					EventTime: timestamppb.Now(),
					Producer:  "test",
				},
			}))
			if err != nil {
				t.Fatalf("IngestEvent failed: %v", err)
			}
			if resp.Msg.Status != "ok" {
				t.Errorf("expected status 'ok', got %q", resp.Msg.Status)
			}
		})
	}

	if got := len(env.svc.Events()); got != 2 {
		t.Errorf("expected 2 stored events (one per transport), got %d", got)
	}
}

func TestIngestEvent_NilEvent(t *testing.T) {
	env := newTestEnv(t)

	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			client := env.client(tr.opts...)
			_, err := client.IngestEvent(context.Background(), authedRequest(&lineagev1.IngestEventRequest{}))
			if err == nil {
				t.Fatal("expected error for nil event")
			}
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Errorf("expected CodeInvalidArgument, got %v", connect.CodeOf(err))
			}
		})
	}
}

func TestIngestEvent_AuthFailure(t *testing.T) {
	env := newTestEnv(t)

	tests := []struct {
		name  string
		token string
	}{
		{"missing", ""},
		{"invalid", "Bearer wrong"},
	}

	for _, tr := range transports {
		for _, tt := range tests {
			t.Run(tr.name+"/"+tt.name, func(t *testing.T) {
				client := env.client(tr.opts...)
				req := connect.NewRequest(&lineagev1.IngestEventRequest{
					Event: &lineagev1.RunEvent{
						EventType: "START",
						EventTime: timestamppb.Now(),
						Producer:  "test",
					},
				})
				if tt.token != "" {
					req.Header().Set("Authorization", tt.token)
				}
				_, err := client.IngestEvent(context.Background(), req)
				if err == nil {
					t.Fatal("expected auth error")
				}
				if connect.CodeOf(err) != connect.CodeUnauthenticated {
					t.Errorf("expected CodeUnauthenticated, got %v", connect.CodeOf(err))
				}
			})
		}
	}
}

// ---------- IngestEvent Validation ----------

func TestIngestEvent_MissingEventType(t *testing.T) {
	env := newTestEnv(t)

	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			client := env.client(tr.opts...)
			_, err := client.IngestEvent(context.Background(), authedRequest(&lineagev1.IngestEventRequest{
				Event: &lineagev1.RunEvent{
					EventTime: timestamppb.Now(),
					Producer:  "test",
				},
			}))
			if err == nil {
				t.Fatal("expected validation error for missing event_type")
			}
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Errorf("expected CodeInvalidArgument, got %v", connect.CodeOf(err))
			}
		})
	}
}

func TestIngestEvent_MissingEventTime(t *testing.T) {
	env := newTestEnv(t)

	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			client := env.client(tr.opts...)
			_, err := client.IngestEvent(context.Background(), authedRequest(&lineagev1.IngestEventRequest{
				Event: &lineagev1.RunEvent{
					EventType: "START",
					Producer:  "test",
				},
			}))
			if err == nil {
				t.Fatal("expected validation error for missing event_time")
			}
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Errorf("expected CodeInvalidArgument, got %v", connect.CodeOf(err))
			}
		})
	}
}

func TestIngestEvent_MissingProducer(t *testing.T) {
	env := newTestEnv(t)

	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			client := env.client(tr.opts...)
			_, err := client.IngestEvent(context.Background(), authedRequest(&lineagev1.IngestEventRequest{
				Event: &lineagev1.RunEvent{
					EventType: "START",
					EventTime: timestamppb.Now(),
				},
			}))
			if err == nil {
				t.Fatal("expected validation error for missing producer")
			}
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Errorf("expected CodeInvalidArgument, got %v", connect.CodeOf(err))
			}
		})
	}
}

// ---------- IngestBatch ----------

func TestIngestBatch_MultipleEvents(t *testing.T) {
	env := newTestEnv(t)

	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			client := env.client(tr.opts...)
			now := timestamppb.Now()
			resp, err := client.IngestBatch(context.Background(), authedRequest(&lineagev1.IngestBatchRequest{
				Events: []*lineagev1.RunEvent{
					{EventType: "START", EventTime: now, Producer: "a"},
					{EventType: "COMPLETE", EventTime: now, Producer: "b"},
					{EventType: "FAIL", EventTime: now, Producer: "c"},
				},
			}))
			if err != nil {
				t.Fatalf("IngestBatch failed: %v", err)
			}
			if resp.Msg.Ingested != 3 {
				t.Errorf("expected 3 ingested, got %d", resp.Msg.Ingested)
			}
		})
	}
}

func TestIngestBatch_Empty(t *testing.T) {
	env := newTestEnv(t)

	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			client := env.client(tr.opts...)
			resp, err := client.IngestBatch(context.Background(), authedRequest(&lineagev1.IngestBatchRequest{}))
			if err != nil {
				t.Fatalf("IngestBatch failed: %v", err)
			}
			if resp.Msg.Ingested != 0 {
				t.Errorf("expected 0 ingested, got %d", resp.Msg.Ingested)
			}
		})
	}
}

func TestIngestBatch_AuthFailure(t *testing.T) {
	env := newTestEnv(t)

	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			client := env.client(tr.opts...)
			req := connect.NewRequest(&lineagev1.IngestBatchRequest{})
			_, err := client.IngestBatch(context.Background(), req)
			if err == nil {
				t.Fatal("expected auth error")
			}
			if connect.CodeOf(err) != connect.CodeUnauthenticated {
				t.Errorf("expected CodeUnauthenticated, got %v", connect.CodeOf(err))
			}
		})
	}
}

// ---------- QueryLineage ----------

func TestQueryLineage_NoMatchingJob(t *testing.T) {
	env := newTestEnv(t)

	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			client := env.client(tr.opts...)
			resp, err := client.QueryLineage(context.Background(), authedRequest(&lineagev1.QueryLineageRequest{
				JobNamespace: "ns",
				JobName:      "nonexistent",
				Depth:        1,
			}))
			if err != nil {
				t.Fatalf("QueryLineage failed: %v", err)
			}
			if len(resp.Msg.Nodes) != 0 {
				t.Errorf("expected 0 nodes, got %d", len(resp.Msg.Nodes))
			}
		})
	}
}

func TestQueryLineage_Depth1_JobWithDatasets(t *testing.T) {
	env := newTestEnv(t)
	client := env.client()
	now := timestamppb.Now()

	_, err := client.IngestEvent(context.Background(), authedRequest(&lineagev1.IngestEventRequest{
		Event: &lineagev1.RunEvent{
			EventType: "COMPLETE",
			EventTime: now,
			Producer:  "test",
			Run:       &lineagev1.Run{RunId: "r1"},
			Job:       &lineagev1.Job{Namespace: "scheduler", Name: "etl.transform"},
			Inputs:    []*lineagev1.InputDataset{{Namespace: "pg", Name: "raw.users"}},
			Outputs:   []*lineagev1.OutputDataset{{Namespace: "s3", Name: "curated.users"}},
		},
	}))
	if err != nil {
		t.Fatalf("IngestEvent failed: %v", err)
	}

	resp, err := client.QueryLineage(context.Background(), authedRequest(&lineagev1.QueryLineageRequest{
		JobNamespace: "scheduler",
		JobName:      "etl.transform",
		Depth:        1,
	}))
	if err != nil {
		t.Fatalf("QueryLineage failed: %v", err)
	}

	// depth=1: seed job + its input + output datasets = 3 nodes
	if len(resp.Msg.Nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(resp.Msg.Nodes))
	}

	nodesByType := make(map[string][]*lineagev1.LineageNode)
	for _, n := range resp.Msg.Nodes {
		nodesByType[n.NodeType] = append(nodesByType[n.NodeType], n)
	}

	if len(nodesByType["dataset"]) != 2 {
		t.Errorf("expected 2 dataset nodes, got %d", len(nodesByType["dataset"]))
	}
	if len(nodesByType["job"]) != 1 {
		t.Errorf("expected 1 job node, got %d", len(nodesByType["job"]))
	}

	jobNode := nodesByType["job"][0]
	if len(jobNode.Edges) != 2 {
		t.Fatalf("expected 2 edges on job node, got %d", len(jobNode.Edges))
	}

	hasConsumes := false
	hasProduces := false
	for _, e := range jobNode.Edges {
		if e.Relationship == "consumes" && e.SourceName == "raw.users" {
			hasConsumes = true
		}
		if e.Relationship == "produces" && e.TargetName == "curated.users" {
			hasProduces = true
		}
	}
	if !hasConsumes {
		t.Error("job node missing 'consumes' edge for raw.users")
	}
	if !hasProduces {
		t.Error("job node missing 'produces' edge for curated.users")
	}
}

func TestQueryLineage_Depth2_UpstreamDownstreamJobs(t *testing.T) {
	svc := service.NewLineageService()
	now := timestamppb.Now()

	// Job A produces dataset X
	svc.StoreEvent(&lineagev1.OpenLineageEvent{
		Event: &lineagev1.OpenLineageEvent_RunEvent{RunEvent: &lineagev1.RunEvent{
			EventType: "COMPLETE", EventTime: now, Producer: "test",
			Job:     &lineagev1.Job{Namespace: "ns", Name: "jobA"},
			Run:     &lineagev1.Run{RunId: "r1"},
			Outputs: []*lineagev1.OutputDataset{{Namespace: "warehouse", Name: "datasetX"}},
		}},
	})
	// Job B consumes dataset X and produces dataset Y
	svc.StoreEvent(&lineagev1.OpenLineageEvent{
		Event: &lineagev1.OpenLineageEvent_RunEvent{RunEvent: &lineagev1.RunEvent{
			EventType: "COMPLETE", EventTime: now, Producer: "test",
			Job:     &lineagev1.Job{Namespace: "ns", Name: "jobB"},
			Run:     &lineagev1.Run{RunId: "r2"},
			Inputs:  []*lineagev1.InputDataset{{Namespace: "warehouse", Name: "datasetX"}},
			Outputs: []*lineagev1.OutputDataset{{Namespace: "warehouse", Name: "datasetY"}},
		}},
	})
	// Job C consumes dataset Y
	svc.StoreEvent(&lineagev1.OpenLineageEvent{
		Event: &lineagev1.OpenLineageEvent_RunEvent{RunEvent: &lineagev1.RunEvent{
			EventType: "COMPLETE", EventTime: now, Producer: "test",
			Job:    &lineagev1.Job{Namespace: "ns", Name: "jobC"},
			Run:    &lineagev1.Run{RunId: "r3"},
			Inputs: []*lineagev1.InputDataset{{Namespace: "warehouse", Name: "datasetY"}},
		}},
	})

	// depth=1 from jobB: jobB + datasetX + datasetY
	mux := http.NewServeMux()
	path, handler := lineagev1connect.NewLineageServiceHandler(
		svc,
		connect.WithInterceptors(
			interceptor.NewAuthInterceptor(),
			interceptor.NewValidateInterceptor(),
		),
	)
	mux.Handle(path, handler)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	client := lineagev1connect.NewLineageServiceClient(srv.Client(), srv.URL)

	resp, err := client.QueryLineage(context.Background(), authedRequest(&lineagev1.QueryLineageRequest{
		JobNamespace: "ns",
		JobName:      "jobB",
		Depth:        1,
	}))
	if err != nil {
		t.Fatalf("QueryLineage depth=1 failed: %v", err)
	}
	if len(resp.Msg.Nodes) != 3 {
		t.Errorf("depth=1: expected 3 nodes (jobB + 2 datasets), got %d", len(resp.Msg.Nodes))
	}

	// depth=2 from jobB: should add jobA (upstream producer of datasetX) and jobC (downstream consumer of datasetY)
	resp, err = client.QueryLineage(context.Background(), authedRequest(&lineagev1.QueryLineageRequest{
		JobNamespace: "ns",
		JobName:      "jobB",
		Depth:        2,
	}))
	if err != nil {
		t.Fatalf("QueryLineage depth=2 failed: %v", err)
	}
	if len(resp.Msg.Nodes) != 5 {
		t.Errorf("depth=2: expected 5 nodes (3 jobs + 2 datasets), got %d", len(resp.Msg.Nodes))
	}

	names := make(map[string]bool)
	for _, n := range resp.Msg.Nodes {
		names[n.NodeType+":"+n.Name] = true
	}
	for _, expected := range []string{"job:jobA", "job:jobB", "job:jobC", "dataset:datasetX", "dataset:datasetY"} {
		if !names[expected] {
			t.Errorf("missing expected node %s", expected)
		}
	}
}

func TestQueryLineage_JobEvent_Contributes(t *testing.T) {
	svc := service.NewLineageService()
	now := timestamppb.Now()

	svc.StoreEvent(&lineagev1.OpenLineageEvent{
		Event: &lineagev1.OpenLineageEvent_JobEvent{JobEvent: &lineagev1.JobEvent{
			EventTime: now, Producer: "test",
			Job:     &lineagev1.Job{Namespace: "airflow", Name: "report"},
			Inputs:  []*lineagev1.InputDataset{{Namespace: "bq", Name: "sales"}},
			Outputs: []*lineagev1.OutputDataset{{Namespace: "bq", Name: "summary"}},
		}},
	})

	mux := http.NewServeMux()
	path, handler := lineagev1connect.NewLineageServiceHandler(
		svc,
		connect.WithInterceptors(
			interceptor.NewAuthInterceptor(),
			interceptor.NewValidateInterceptor(),
		),
	)
	mux.Handle(path, handler)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	client := lineagev1connect.NewLineageServiceClient(srv.Client(), srv.URL)

	resp, err := client.QueryLineage(context.Background(), authedRequest(&lineagev1.QueryLineageRequest{
		JobNamespace: "airflow",
		JobName:      "report",
		Depth:        1,
	}))
	if err != nil {
		t.Fatalf("QueryLineage failed: %v", err)
	}
	if len(resp.Msg.Nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(resp.Msg.Nodes))
	}
}

func TestQueryLineage_DefaultDepth(t *testing.T) {
	env := newTestEnv(t)
	client := env.client()
	now := timestamppb.Now()

	_, err := client.IngestEvent(context.Background(), authedRequest(&lineagev1.IngestEventRequest{
		Event: &lineagev1.RunEvent{
			EventType: "START", EventTime: now, Producer: "test",
			Run:    &lineagev1.Run{RunId: "r1"},
			Job:    &lineagev1.Job{Namespace: "ns", Name: "j"},
			Inputs: []*lineagev1.InputDataset{{Namespace: "ds", Name: "t"}},
		},
	}))
	if err != nil {
		t.Fatalf("IngestEvent failed: %v", err)
	}

	// depth=0 should be treated as depth=1
	resp, err := client.QueryLineage(context.Background(), authedRequest(&lineagev1.QueryLineageRequest{
		JobNamespace: "ns", JobName: "j", Depth: 0,
	}))
	if err != nil {
		t.Fatalf("QueryLineage failed: %v", err)
	}
	if len(resp.Msg.Nodes) != 2 {
		t.Errorf("depth=0 (treated as 1): expected 2 nodes, got %d", len(resp.Msg.Nodes))
	}
}

func TestQueryLineage_AuthFailure(t *testing.T) {
	env := newTestEnv(t)

	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			client := env.client(tr.opts...)
			req := connect.NewRequest(&lineagev1.QueryLineageRequest{})
			_, err := client.QueryLineage(context.Background(), req)
			if err == nil {
				t.Fatal("expected auth error")
			}
			if connect.CodeOf(err) != connect.CodeUnauthenticated {
				t.Errorf("expected CodeUnauthenticated, got %v", connect.CodeOf(err))
			}
		})
	}
}

// ---------- OpenLineageEvent storage ----------

func TestStoreEvent_AllTypes(t *testing.T) {
	svc := service.NewLineageService()

	svc.StoreEvent(&lineagev1.OpenLineageEvent{
		Event: &lineagev1.OpenLineageEvent_RunEvent{
			RunEvent: &lineagev1.RunEvent{
				EventType: "START",
				EventTime: timestamppb.Now(),
				Producer:  "test",
			},
		},
	})
	svc.StoreEvent(&lineagev1.OpenLineageEvent{
		Event: &lineagev1.OpenLineageEvent_JobEvent{
			JobEvent: &lineagev1.JobEvent{
				EventTime: timestamppb.Now(),
				Producer:  "test",
				Job:       &lineagev1.Job{Namespace: "ns", Name: "j"},
			},
		},
	})
	svc.StoreEvent(&lineagev1.OpenLineageEvent{
		Event: &lineagev1.OpenLineageEvent_DatasetEvent{
			DatasetEvent: &lineagev1.DatasetEvent{
				EventTime: timestamppb.Now(),
				Producer:  "test",
				Dataset:   &lineagev1.StaticDataset{Namespace: "ns", Name: "ds"},
			},
		},
	})

	all := svc.OpenLineageEvents()
	if len(all) != 3 {
		t.Fatalf("OpenLineageEvents() = %d, want 3", len(all))
	}
	if all[0].GetRunEvent() == nil {
		t.Error("events[0] should be RunEvent")
	}
	if all[1].GetJobEvent() == nil {
		t.Error("events[1] should be JobEvent")
	}
	if all[2].GetDatasetEvent() == nil {
		t.Error("events[2] should be DatasetEvent")
	}

	runEvents := svc.Events()
	if len(runEvents) != 1 {
		t.Errorf("Events() returned %d RunEvents, want 1", len(runEvents))
	}
}

// ---------- IngestOpenLineageBatch RPC ----------

func TestIngestOpenLineageBatch_HappyPath(t *testing.T) {
	env := newTestEnv(t)

	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			client := env.client(tr.opts...)
			now := timestamppb.Now()
			resp, err := client.IngestOpenLineageBatch(context.Background(), authedRequest(&lineagev1.IngestOpenLineageBatchRequest{
				Events: []*lineagev1.OpenLineageEvent{
					{Event: &lineagev1.OpenLineageEvent_RunEvent{
						RunEvent: &lineagev1.RunEvent{EventType: "START", EventTime: now, Producer: "a"},
					}},
					{Event: &lineagev1.OpenLineageEvent_JobEvent{
						JobEvent: &lineagev1.JobEvent{EventTime: now, Producer: "b", Job: &lineagev1.Job{Namespace: "ns", Name: "j"}},
					}},
				},
			}))
			if err != nil {
				t.Fatalf("IngestOpenLineageBatch failed: %v", err)
			}
			if resp.Msg.Status != "success" {
				t.Errorf("status = %q, want success", resp.Msg.Status)
			}
			if resp.Msg.Summary.Received != 2 {
				t.Errorf("received = %d, want 2", resp.Msg.Summary.Received)
			}
			if resp.Msg.Summary.Successful != 2 {
				t.Errorf("successful = %d, want 2", resp.Msg.Summary.Successful)
			}
		})
	}
}

func TestIngestOpenLineageBatch_Empty(t *testing.T) {
	env := newTestEnv(t)

	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			client := env.client(tr.opts...)
			resp, err := client.IngestOpenLineageBatch(context.Background(), authedRequest(&lineagev1.IngestOpenLineageBatchRequest{}))
			if err != nil {
				t.Fatalf("IngestOpenLineageBatch failed: %v", err)
			}
			if resp.Msg.Summary.Received != 0 {
				t.Errorf("received = %d, want 0", resp.Msg.Summary.Received)
			}
		})
	}
}
