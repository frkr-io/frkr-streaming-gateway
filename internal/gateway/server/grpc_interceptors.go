package server

import (
	"context"
	"strings"

	"github.com/frkr-io/frkr-common/plugins"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// authContextKey is the key used to store auth result in context
type authContextKey struct{}

// AuthFromContext extracts the AuthResult from the context
func AuthFromContext(ctx context.Context) (*plugins.AuthResult, bool) {
	result, ok := ctx.Value(authContextKey{}).(*plugins.AuthResult)
	return result, ok
}

// UnaryAuthInterceptor creates a unary interceptor that validates authentication
func UnaryAuthInterceptor(authPlugin plugins.AuthPlugin, secretPlugin plugins.SecretPlugin) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		// Skip auth for health checks
		if strings.Contains(info.FullMethod, "grpc.health") {
			return handler(ctx, req)
		}

		authCtx, err := authenticateFromMetadata(ctx, authPlugin, secretPlugin)
		if err != nil {
			return nil, err
		}

		return handler(authCtx, req)
	}
}

// StreamAuthInterceptor creates a stream interceptor that validates authentication
func StreamAuthInterceptor(authPlugin plugins.AuthPlugin, secretPlugin plugins.SecretPlugin) grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		// Skip auth for health checks
		if strings.Contains(info.FullMethod, "grpc.health") {
			return handler(srv, ss)
		}

		authCtx, err := authenticateFromMetadata(ss.Context(), authPlugin, secretPlugin)
		if err != nil {
			return err
		}

		// Wrap the stream with authenticated context
		wrapped := &wrappedServerStream{
			ServerStream: ss,
			ctx:          authCtx,
		}

		return handler(srv, wrapped)
	}
}

// authenticateFromMetadata extracts auth from gRPC metadata and validates it
func authenticateFromMetadata(ctx context.Context, authPlugin plugins.AuthPlugin, secretPlugin plugins.SecretPlugin) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}

	// Extract authorization header from metadata
	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing authorization header")
	}

	authHeader := authHeaders[0]

	// Use the protocol-agnostic ValidateAuthHeader method
	result, err := authPlugin.ValidateAuthHeader(ctx, authHeader, secretPlugin)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "authentication failed: "+err.Error())
	}

	// Store auth result in context
	authCtx := context.WithValue(ctx, authContextKey{}, result)
	return authCtx, nil
}

// wrappedServerStream wraps a grpc.ServerStream with a custom context
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}
