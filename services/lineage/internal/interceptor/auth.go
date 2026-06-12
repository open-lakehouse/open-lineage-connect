package interceptor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
)

// defaultStaticToken is the legacy expected header value, retained so existing
// local/dev setups keep working when LINEAGE_AUTH_TOKEN is unset.
const defaultStaticToken = "Bearer valid-token"

// ucValidationTTL is how long a successful Unity Catalog token validation is
// cached before the next inbound request re-probes UC.
const ucValidationTTL = 60 * time.Second

// AuthMode selects how inbound Authorization headers are validated.
type AuthMode int

const (
	// AuthStatic compares the header against a single shared token.
	AuthStatic AuthMode = iota
	// AuthUCJWT validates the bearer token by forwarding it to Unity Catalog.
	AuthUCJWT
	// AuthDisabled skips authentication entirely (local / UC auth off).
	AuthDisabled
)

// tokenContextKey is the context key under which the validated bearer token
// (without the "Bearer " prefix) is stashed for downstream forwarding.
type tokenContextKey struct{}

// TokenFromContext returns the bearer token captured by the auth interceptor,
// or "" when none was present. Used by the service layer to forward the
// caller's Unity Catalog JWT to the table-service for per-user credential
// vending.
func TokenFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(tokenContextKey{}).(string); ok {
		return v
	}
	return ""
}

// withToken returns a child context carrying the raw bearer token.
func withToken(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, tokenContextKey{}, token)
}

// authenticator validates inbound Authorization headers per the configured
// mode. It is constructed once at startup; the UC validation cache is shared
// across requests.
type authenticator struct {
	mode        AuthMode
	staticToken string
	ucURL       string
	httpClient  *http.Client
	ttl         time.Duration

	mu    sync.Mutex
	cache map[string]time.Time // header value -> validated-until
}

func newAuthenticatorFromEnv() *authenticator {
	staticToken := os.Getenv("LINEAGE_AUTH_TOKEN")
	if staticToken == "" {
		staticToken = defaultStaticToken
	}
	return &authenticator{
		mode:        ParseAuthMode(os.Getenv("LINEAGE_AUTH_MODE")),
		staticToken: staticToken,
		ucURL:       strings.TrimRight(os.Getenv("UNITY_CATALOG_URL"), "/"),
		httpClient:  &http.Client{Timeout: 5 * time.Second},
		ttl:         ucValidationTTL,
		cache:       make(map[string]time.Time),
	}
}

// ParseAuthMode maps the LINEAGE_AUTH_MODE env value to an AuthMode. Unknown
// or empty values default to AuthStatic for backward compatibility.
func ParseAuthMode(s string) AuthMode {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "uc-jwt", "uc_jwt", "ucjwt", "jwt":
		return AuthUCJWT
	case "disabled", "disable", "off", "none":
		return AuthDisabled
	default:
		return AuthStatic
	}
}

// authenticate validates the header and returns the raw bearer token (without
// the "Bearer " prefix) on success.
func (a *authenticator) authenticate(ctx context.Context, header string) (string, error) {
	switch a.mode {
	case AuthDisabled:
		return bearerToken(header), nil
	case AuthUCJWT:
		if header == "" {
			return "", connect.NewError(connect.CodeUnauthenticated, errors.New("missing authorization header"))
		}
		if err := a.validateWithUC(ctx, header); err != nil {
			return "", err
		}
		return bearerToken(header), nil
	default: // AuthStatic
		if header == "" {
			return "", connect.NewError(connect.CodeUnauthenticated, errors.New("missing authorization header"))
		}
		if header != a.staticToken {
			return "", connect.NewError(connect.CodeUnauthenticated, errors.New("invalid token"))
		}
		return bearerToken(header), nil
	}
}

// validateWithUC accepts the token if Unity Catalog accepts it on the SCIM
// /Me endpoint. Successful validations are cached for a.ttl to avoid a UC
// round-trip on every ingest.
func (a *authenticator) validateWithUC(ctx context.Context, header string) error {
	if a.ucURL == "" {
		return connect.NewError(connect.CodeFailedPrecondition,
			errors.New("UNITY_CATALOG_URL not set; required for uc-jwt auth mode"))
	}

	now := time.Now()
	a.mu.Lock()
	if until, ok := a.cache[header]; ok && now.Before(until) {
		a.mu.Unlock()
		return nil
	}
	a.mu.Unlock()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		a.ucURL+"/api/1.0/unity-control/scim2/Me", nil)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	req.Header.Set("Authorization", header)

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return connect.NewError(connect.CodeUnavailable, fmt.Errorf("validate token via unity catalog: %w", err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return connect.NewError(connect.CodeUnauthenticated,
			fmt.Errorf("unity catalog rejected token (status %d)", resp.StatusCode))
	}

	a.mu.Lock()
	a.cache[header] = now.Add(a.ttl)
	a.mu.Unlock()
	return nil
}

// bearerToken strips a leading "Bearer " (case-insensitive) prefix.
func bearerToken(header string) string {
	const prefix = "Bearer "
	if len(header) >= len(prefix) && strings.EqualFold(header[:len(prefix)], prefix) {
		return strings.TrimSpace(header[len(prefix):])
	}
	return strings.TrimSpace(header)
}

// NewAuthInterceptor returns a ConnectRPC interceptor that authenticates
// requests per LINEAGE_AUTH_MODE and stashes the validated bearer token in the
// request context for downstream forwarding.
func NewAuthInterceptor() connect.UnaryInterceptorFunc {
	a := newAuthenticatorFromEnv()
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			token, err := a.authenticate(ctx, req.Header().Get("Authorization"))
			if err != nil {
				return nil, err
			}
			return next(withToken(ctx, token), req)
		}
	}
}
