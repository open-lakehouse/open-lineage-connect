package interceptor

import (
	"context"

	"buf.build/go/protovalidate"
	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
)

// NewValidateInterceptor returns a ConnectRPC interceptor that validates
// incoming request messages against their protovalidate constraints.
func NewValidateInterceptor() connect.UnaryInterceptorFunc {
	validator, err := protovalidate.New()
	if err != nil {
		panic("failed to create protovalidate validator: " + err.Error())
	}
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			if msg, ok := req.Any().(proto.Message); ok {
				if err := validator.Validate(msg); err != nil {
					return nil, connect.NewError(connect.CodeInvalidArgument, err)
				}
			}
			return next(ctx, req)
		}
	}
}
