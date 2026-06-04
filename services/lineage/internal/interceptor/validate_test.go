package interceptor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	lineagev1 "github.com/open-lakehouse/open-lineage-service/gen/lineage/v1"
	"github.com/open-lakehouse/open-lineage-service/gen/lineage/v1/lineagev1connect"
)

func newValidatedServer(t *testing.T) (lineagev1connect.LineageServiceClient, *httptest.Server) {
	t.Helper()
	mux := http.NewServeMux()
	path, handler := lineagev1connect.NewLineageServiceHandler(
		lineagev1connect.UnimplementedLineageServiceHandler{},
		connect.WithInterceptors(NewValidateInterceptor()),
	)
	mux.Handle(path, handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return lineagev1connect.NewLineageServiceClient(srv.Client(), srv.URL), srv
}

func TestNewValidateInterceptor_ReturnsNonNil(t *testing.T) {
	i := NewValidateInterceptor()
	if i == nil {
		t.Fatal("expected non-nil interceptor")
	}
}

func TestValidateInterceptor_RejectsInvalidEvent(t *testing.T) {
	client, _ := newValidatedServer(t)

	tests := []struct {
		name  string
		event *lineagev1.RunEvent
	}{
		{
			name:  "missing event_type",
			event: &lineagev1.RunEvent{EventTime: timestamppb.Now(), Producer: "p"},
		},
		{
			name:  "missing event_time",
			event: &lineagev1.RunEvent{EventType: "START", Producer: "p"},
		},
		{
			name:  "missing producer",
			event: &lineagev1.RunEvent{EventType: "START", EventTime: timestamppb.Now()},
		},
		{
			name:  "all required fields missing",
			event: &lineagev1.RunEvent{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := connect.NewRequest(&lineagev1.IngestEventRequest{Event: tt.event})
			_, err := client.IngestEvent(context.Background(), req)
			if err == nil {
				t.Fatal("expected validation error")
			}
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Errorf("expected CodeInvalidArgument, got %v", connect.CodeOf(err))
			}
		})
	}
}

func TestValidateInterceptor_AcceptsValidEvent(t *testing.T) {
	client, _ := newValidatedServer(t)

	req := connect.NewRequest(&lineagev1.IngestEventRequest{
		Event: &lineagev1.RunEvent{
			EventType: "START",
			EventTime: timestamppb.Now(),
			Producer:  "test-producer",
		},
	})
	_, err := client.IngestEvent(context.Background(), req)
	if err == nil {
		t.Fatal("expected CodeUnimplemented from stub handler")
	}
	if connect.CodeOf(err) != connect.CodeUnimplemented {
		t.Errorf("expected CodeUnimplemented (validation passed), got %v", connect.CodeOf(err))
	}
}
