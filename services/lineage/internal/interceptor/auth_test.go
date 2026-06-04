package interceptor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"

	lineagev1 "github.com/open-lakehouse/open-lineage-service/gen/lineage/v1"
	"github.com/open-lakehouse/open-lineage-service/gen/lineage/v1/lineagev1connect"
)

func TestCheckToken(t *testing.T) {
	tests := []struct {
		name    string
		header  string
		wantErr bool
		code    connect.Code
	}{
		{
			name:    "valid token",
			header:  "Bearer valid-token",
			wantErr: false,
		},
		{
			name:    "missing header",
			header:  "",
			wantErr: true,
			code:    connect.CodeUnauthenticated,
		},
		{
			name:    "wrong token",
			header:  "Bearer wrong-token",
			wantErr: true,
			code:    connect.CodeUnauthenticated,
		},
		{
			name:    "missing bearer prefix",
			header:  "valid-token",
			wantErr: true,
			code:    connect.CodeUnauthenticated,
		},
		{
			name:    "empty bearer value",
			header:  "Bearer ",
			wantErr: true,
			code:    connect.CodeUnauthenticated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkToken(tt.header)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				connectErr, ok := err.(*connect.Error)
				if !ok {
					t.Fatalf("expected *connect.Error, got %T", err)
				}
				if connectErr.Code() != tt.code {
					t.Errorf("expected code %v, got %v", tt.code, connectErr.Code())
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestNewAuthInterceptor_ReturnsNonNil(t *testing.T) {
	interceptor := NewAuthInterceptor()
	if interceptor == nil {
		t.Fatal("expected non-nil interceptor")
	}
}

func TestAuthInterceptor_Integration(t *testing.T) {
	mux := http.NewServeMux()
	path, handler := lineagev1connect.NewLineageServiceHandler(
		lineagev1connect.UnimplementedLineageServiceHandler{},
		connect.WithInterceptors(NewAuthInterceptor()),
	)
	mux.Handle(path, handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := lineagev1connect.NewLineageServiceClient(srv.Client(), srv.URL)

	tests := []struct {
		name    string
		token   string
		wantErr bool
		code    connect.Code
	}{
		{
			name:    "rejected without token",
			token:   "",
			wantErr: true,
			code:    connect.CodeUnauthenticated,
		},
		{
			name:    "rejected with bad token",
			token:   "Bearer bad",
			wantErr: true,
			code:    connect.CodeUnauthenticated,
		},
		{
			name:    "passes with valid token",
			token:   "Bearer valid-token",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := connect.NewRequest(&lineagev1.QueryLineageRequest{})
			if tt.token != "" {
				req.Header().Set("Authorization", tt.token)
			}
			_, err := client.QueryLineage(context.Background(), req)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if connect.CodeOf(err) != tt.code {
					t.Errorf("expected code %v, got %v", tt.code, connect.CodeOf(err))
				}
			} else {
				// Valid token passes the interceptor; the Unimplemented handler
				// returns CodeUnimplemented which is expected.
				if err == nil {
					t.Fatal("expected CodeUnimplemented from stub handler")
				}
				if connect.CodeOf(err) != connect.CodeUnimplemented {
					t.Errorf("expected CodeUnimplemented, got %v", connect.CodeOf(err))
				}
			}
		})
	}
}
