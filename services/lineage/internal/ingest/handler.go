package ingest

import (
	"encoding/json"
	"io"
	"log"
	"net/http"

	lineagev1 "github.com/open-lakehouse/open-lineage-service/gen/lineage/v1"
	"github.com/open-lakehouse/open-lineage-service/internal/service"
)

// Handler serves the OpenLineage REST ingestion endpoints.
type Handler struct {
	svc *service.LineageService
}

func NewHandler(svc *service.LineageService) *Handler {
	return &Handler{svc: svc}
}

// Register adds the OpenLineage REST routes to the given mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /lineage", h.handleEvent)
	mux.HandleFunc("POST /lineage/batch", h.handleBatch)
}

func (h *Handler) handleEvent(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read request body")
		return
	}

	evt, err := ConvertEvent(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	h.svc.StoreEvent(evt)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleBatch(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read request body")
		return
	}

	events, batchErr := convertBatchWithErrors(body)

	received := int32(len(events.all))
	successful := int32(len(events.ok))
	failed := int32(len(events.failures))

	for _, evt := range events.ok {
		h.svc.StoreEvent(evt)
	}

	if batchErr != nil && len(events.all) == 0 {
		writeError(w, http.StatusBadRequest, batchErr.Error())
		return
	}

	status := "success"
	if failed > 0 {
		status = "partial_success"
	}

	resp := batchResponse{
		Status: status,
		Summary: batchSummary{
			Received:   received,
			Successful: successful,
			Failed:     failed,
		},
	}

	for _, f := range events.failures {
		resp.FailedEvents = append(resp.FailedEvents, failedEventInfo{
			Index:     f.index,
			Reason:    f.reason,
			Retriable: false,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("failed to write batch response: %v", err)
	}
}

type batchResult struct {
	all      []json.RawMessage
	ok       []*lineagev1.OpenLineageEvent
	failures []eventFailure
}

type eventFailure struct {
	index  int32
	reason string
}

// convertBatchWithErrors parses a JSON array and converts each element,
// collecting per-event errors instead of failing on the first one.
func convertBatchWithErrors(data []byte) (*batchResult, error) {
	var rawItems []json.RawMessage
	if err := json.Unmarshal(data, &rawItems); err != nil {
		return &batchResult{}, err
	}

	result := &batchResult{all: rawItems}
	for i, item := range rawItems {
		evt, err := ConvertEvent([]byte(item))
		if err != nil {
			result.failures = append(result.failures, eventFailure{
				index:  int32(i),
				reason: err.Error(),
			})
			continue
		}
		result.ok = append(result.ok, evt)
	}
	return result, nil
}

type batchResponse struct {
	Status       string           `json:"status"`
	Summary      batchSummary     `json:"summary"`
	FailedEvents []failedEventInfo `json:"failed_events,omitempty"`
}

type batchSummary struct {
	Received   int32 `json:"received"`
	Successful int32 `json:"successful"`
	Failed     int32 `json:"failed"`
}

type failedEventInfo struct {
	Index     int32  `json:"index"`
	Reason    string `json:"reason"`
	Retriable bool   `json:"retriable"`
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
