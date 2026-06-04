package main

import (
	"log"
	"net/http"
	"os"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/open-lakehouse/open-lineage-service/gen/lineage/v1/lineagev1connect"
	"github.com/open-lakehouse/open-lineage-service/internal/forwarder"
	"github.com/open-lakehouse/open-lineage-service/internal/health"
	"github.com/open-lakehouse/open-lineage-service/internal/ingest"
	"github.com/open-lakehouse/open-lineage-service/internal/interceptor"
	"github.com/open-lakehouse/open-lineage-service/internal/service"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8090"
	}

	svc := service.NewLineageService()

	var healthChecker health.Checker
	if tableURL := os.Getenv("TABLE_SERVICE_URL"); tableURL != "" {
		fwd := forwarder.New(tableURL)
		svc.OnEvent(fwd.Forward)
		defer fwd.Close()
		healthChecker = health.NewSidecarChecker(tableURL)
		log.Printf("event forwarding enabled -> %s", tableURL)
	}

	path, handler := lineagev1connect.NewLineageServiceHandler(
		svc,
		connect.WithInterceptors(
			interceptor.NewAuthInterceptor(),
			interceptor.NewValidateInterceptor(),
		),
	)

	mux := http.NewServeMux()
	mux.Handle(path, handler)

	rest := ingest.NewHandler(svc)
	rest.Register(mux)
	health.NewHandler(healthChecker).Register(mux)

	addr := ":" + port
	log.Printf("lineage service listening on %s", addr)
	if err := http.ListenAndServe(addr, h2c.NewHandler(mux, &http2.Server{})); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
