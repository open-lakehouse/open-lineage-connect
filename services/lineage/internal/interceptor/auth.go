package interceptor

import (
	"context"
	"errors"

	"connectrpc.com/connect"
)

const expectedToken = "Bearer valid-token"

// NewAuthInterceptor returns a ConnectRPC interceptor that rejects requests
// lacking a valid Authorization header.
func NewAuthInterceptor() connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			if err := checkToken(req.Header().Get("Authorization")); err != nil {
				return nil, err
			}
			return next(ctx, req)
		}
	}
}

func checkToken(header string) error {
	if header == "" {
		return connect.NewError(connect.CodeUnauthenticated, errors.New("missing authorization header"))
	}
	if header != expectedToken {
		return connect.NewError(connect.CodeUnauthenticated, errors.New("invalid token"))
	}
	return nil
}
