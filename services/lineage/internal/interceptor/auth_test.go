package interceptor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	lineagev1 "github.com/open-lakehouse/open-lineage-service/gen/lineage/v1"
	"github.com/open-lakehouse/open-lineage-service/gen/lineage/v1/lineagev1connect"
)

func staticAuth(token string) *authenticator {
	return &authenticator{mode: AuthStatic, staticToken: token, ttl: ucValidationTTL, cache: map[string]time.Time{}}
}

func TestParseAuthMode(t *testing.T) {
	tests := []struct {
		in   string
		want AuthMode
	}{
		{"uc-jwt", AuthUCJWT},
		{"UC_JWT", AuthUCJWT},
		{"jwt", AuthUCJWT},
		{"disabled", AuthDisabled},
		{"off", AuthDisabled},
		{"static", AuthStatic},
		{"", AuthStatic},
		{"garbage", AuthStatic},
	}
	for _, tt := range tests {
		if got := ParseAuthMode(tt.in); got != tt.want {
			t.Errorf("ParseAuthMode(%q)=%v want %v", tt.in, got, tt.want)
		}
	}
}

func TestBearerToken(t *testing.T) {
	tests := []struct{ in, want string }{
		{"Bearer abc", "abc"},
		{"bearer abc", "abc"},
		{"Bearer  abc  ", "abc"},
		{"abc", "abc"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := bearerToken(tt.in); got != tt.want {
			t.Errorf("bearerToken(%q)=%q want %q", tt.in, got, tt.want)
		}
	}
}

func TestAuthenticate_StaticMode(t *testing.T) {
	a := staticAuth("Bearer valid-token")
	tests := []struct {
		name      string
		header    string
		wantErr   bool
		code      connect.Code
		wantToken string
	}{
		{name: "valid token", header: "Bearer valid-token", wantToken: "valid-token"},
		{name: "missing header", header: "", wantErr: true, code: connect.CodeUnauthenticated},
		{name: "wrong token", header: "Bearer wrong-token", wantErr: true, code: connect.CodeUnauthenticated},
		{name: "missing bearer prefix", header: "valid-token", wantErr: true, code: connect.CodeUnauthenticated},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tok, err := a.authenticate(context.Background(), tt.header)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if connect.CodeOf(err) != tt.code {
					t.Errorf("code=%v want %v", connect.CodeOf(err), tt.code)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tok != tt.wantToken {
				t.Errorf("token=%q want %q", tok, tt.wantToken)
			}
		})
	}
}

func TestAuthenticate_DisabledMode_AlwaysPasses(t *testing.T) {
	a := &authenticator{mode: AuthDisabled, ttl: ucValidationTTL, cache: map[string]time.Time{}}
	for _, header := range []string{"", "Bearer anything", "garbage"} {
		if _, err := a.authenticate(context.Background(), header); err != nil {
			t.Errorf("disabled mode should pass header %q, got %v", header, err)
		}
	}
}

func TestAuthenticate_UCJWTMode(t *testing.T) {
	var calls atomic.Int32
	uc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/api/1.0/unity-control/scim2/Me" {
			t.Errorf("path=%q", r.URL.Path)
		}
		if r.Header.Get("Authorization") == "Bearer good-jwt" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer uc.Close()

	a := &authenticator{
		mode:       AuthUCJWT,
		ucURL:      uc.URL,
		httpClient: uc.Client(),
		ttl:        ucValidationTTL,
		cache:      map[string]time.Time{},
	}

	// Valid token accepted and the raw (de-prefixed) token returned.
	tok, err := a.authenticate(context.Background(), "Bearer good-jwt")
	if err != nil {
		t.Fatalf("expected good-jwt to validate, got %v", err)
	}
	if tok != "good-jwt" {
		t.Fatalf("token=%q want good-jwt", tok)
	}

	// Second call with the same token must hit the cache (no extra UC call).
	if _, err := a.authenticate(context.Background(), "Bearer good-jwt"); err != nil {
		t.Fatalf("cached validation failed: %v", err)
	}
	if calls.Load() != 1 {
		t.Errorf("expected 1 UC call (second served from cache), got %d", calls.Load())
	}

	// Bad token rejected.
	if _, err := a.authenticate(context.Background(), "Bearer bad-jwt"); err == nil {
		t.Fatal("expected bad-jwt to be rejected")
	} else if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("code=%v want Unauthenticated", connect.CodeOf(err))
	}

	// Missing header rejected.
	if _, err := a.authenticate(context.Background(), ""); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("missing header code=%v want Unauthenticated", connect.CodeOf(err))
	}
}

func TestAuthenticate_UCJWTMode_NoURLConfigured(t *testing.T) {
	a := &authenticator{mode: AuthUCJWT, ttl: ucValidationTTL, cache: map[string]time.Time{}}
	_, err := a.authenticate(context.Background(), "Bearer something")
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("code=%v want FailedPrecondition", connect.CodeOf(err))
	}
}

func TestNewAuthInterceptor_ReturnsNonNil(t *testing.T) {
	if NewAuthInterceptor() == nil {
		t.Fatal("expected non-nil interceptor")
	}
}

func TestTokenFromContext(t *testing.T) {
	if got := TokenFromContext(context.Background()); got != "" {
		t.Errorf("empty ctx token=%q want empty", got)
	}
	ctx := withToken(context.Background(), "my-jwt")
	if got := TokenFromContext(ctx); got != "my-jwt" {
		t.Errorf("token=%q want my-jwt", got)
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
		{name: "rejected without token", token: "", wantErr: true, code: connect.CodeUnauthenticated},
		{name: "rejected with bad token", token: "Bearer bad", wantErr: true, code: connect.CodeUnauthenticated},
		{name: "passes with valid token", token: "Bearer valid-token", wantErr: false},
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
