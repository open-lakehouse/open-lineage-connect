package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const defaultProbeTimeout = 2 * time.Second

// Checker validates whether dependencies required by this service are ready.
type Checker interface {
	Ready(ctx context.Context) error
}

// Handler serves the /health endpoint.
type Handler struct {
	checker Checker
}

func NewHandler(checker Checker) *Handler {
	return &Handler{checker: checker}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", h.handleHealth)
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if h.checker != nil {
		ctx, cancel := context.WithTimeout(r.Context(), defaultProbeTimeout)
		defer cancel()
		if err := h.checker.Ready(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"status": "unhealthy",
				"error":  err.Error(),
			})
			return
		}
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// SidecarChecker validates that the Rust sidecar health endpoint is reachable.
type SidecarChecker struct {
	healthURL string
	client    *http.Client
}

func NewSidecarChecker(tableServiceURL string) *SidecarChecker {
	baseURL := strings.TrimRight(tableServiceURL, "/")
	return &SidecarChecker{
		healthURL: baseURL + "/health",
		client:    &http.Client{},
	}
}

func (s *SidecarChecker) Ready(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.healthURL, nil)
	if err != nil {
		return fmt.Errorf("build sidecar health request: %w", err)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("probe sidecar health: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("sidecar health returned status %d", resp.StatusCode)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, body map[string]string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}
