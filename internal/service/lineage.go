package service

import (
	"context"
	"sync"

	"connectrpc.com/connect"

	lineagev1 "github.com/open-lakehouse/open-lineage-service/gen/lineage/v1"
	"github.com/open-lakehouse/open-lineage-service/gen/lineage/v1/lineagev1connect"
)

// EventCallback is invoked after every event is stored.
type EventCallback func(*lineagev1.OpenLineageEvent)

type LineageService struct {
	lineagev1connect.UnimplementedLineageServiceHandler

	mu        sync.Mutex
	events    []*lineagev1.OpenLineageEvent
	callbacks []EventCallback
}

func NewLineageService() *LineageService {
	return &LineageService{}
}

// OnEvent registers a callback that fires after every stored event.
// Callbacks are invoked outside the lock and must not block.
func (s *LineageService) OnEvent(cb EventCallback) {
	s.mu.Lock()
	s.callbacks = append(s.callbacks, cb)
	s.mu.Unlock()
}

// StoreEvent appends a single OpenLineageEvent to the in-memory store.
// This is the direct method used by the REST ingestion adapter.
func (s *LineageService) StoreEvent(event *lineagev1.OpenLineageEvent) {
	s.mu.Lock()
	s.events = append(s.events, event)
	cbs := make([]EventCallback, len(s.callbacks))
	copy(cbs, s.callbacks)
	s.mu.Unlock()

	for _, cb := range cbs {
		cb(event)
	}
}

// OpenLineageEvents returns a copy of all stored events.
func (s *LineageService) OpenLineageEvents() []*lineagev1.OpenLineageEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*lineagev1.OpenLineageEvent, len(s.events))
	copy(out, s.events)
	return out
}

// Events returns only the stored RunEvents (backward-compatible accessor).
func (s *LineageService) Events() []*lineagev1.RunEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*lineagev1.RunEvent
	for _, e := range s.events {
		if re := e.GetRunEvent(); re != nil {
			out = append(out, re)
		}
	}
	return out
}

func (s *LineageService) IngestEvent(
	_ context.Context,
	req *connect.Request[lineagev1.IngestEventRequest],
) (*connect.Response[lineagev1.IngestEventResponse], error) {
	if req.Msg.Event == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, nil)
	}

	s.StoreEvent(&lineagev1.OpenLineageEvent{
		Event: &lineagev1.OpenLineageEvent_RunEvent{RunEvent: req.Msg.Event},
	})

	return connect.NewResponse(&lineagev1.IngestEventResponse{
		Status: "ok",
	}), nil
}

func (s *LineageService) IngestBatch(
	_ context.Context,
	req *connect.Request[lineagev1.IngestBatchRequest],
) (*connect.Response[lineagev1.IngestBatchResponse], error) {
	s.mu.Lock()
	for _, e := range req.Msg.Events {
		s.events = append(s.events, &lineagev1.OpenLineageEvent{
			Event: &lineagev1.OpenLineageEvent_RunEvent{RunEvent: e},
		})
	}
	count := int32(len(req.Msg.Events))
	s.mu.Unlock()

	return connect.NewResponse(&lineagev1.IngestBatchResponse{
		Ingested: count,
	}), nil
}

func (s *LineageService) IngestOpenLineageBatch(
	_ context.Context,
	req *connect.Request[lineagev1.IngestOpenLineageBatchRequest],
) (*connect.Response[lineagev1.IngestOpenLineageBatchResponse], error) {
	s.mu.Lock()
	s.events = append(s.events, req.Msg.Events...)
	received := int32(len(req.Msg.Events))
	s.mu.Unlock()

	return connect.NewResponse(&lineagev1.IngestOpenLineageBatchResponse{
		Status: "success",
		Summary: &lineagev1.BatchSummary{
			Received:   received,
			Successful: received,
			Failed:     0,
		},
	}), nil
}

func (s *LineageService) QueryLineage(
	_ context.Context,
	req *connect.Request[lineagev1.QueryLineageRequest],
) (*connect.Response[lineagev1.QueryLineageResponse], error) {
	s.mu.Lock()
	snapshot := make([]*lineagev1.OpenLineageEvent, len(s.events))
	copy(snapshot, s.events)
	s.mu.Unlock()

	depth := req.Msg.Depth
	if depth <= 0 {
		depth = 1
	}

	nodes := buildLineageGraph(snapshot, req.Msg.JobNamespace, req.Msg.JobName, depth)
	return connect.NewResponse(&lineagev1.QueryLineageResponse{Nodes: nodes}), nil
}
