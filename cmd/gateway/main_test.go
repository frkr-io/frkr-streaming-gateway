package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"net"
	"testing"

	dbcommon "github.com/frkr-io/frkr-common/db"
	"github.com/frkr-io/frkr-common/plugins"
	streamingv1 "github.com/frkr-io/frkr-proto/go/streaming/v1"
	"github.com/frkr-io/frkr-streaming-gateway/internal/gateway/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// setupTestUserForGateway creates a test user in the database
func setupTestUserForStreamingGateway(t *testing.T, dbConn *sql.DB, tenantID, username, password string) {
	// Table creation is handled by migrations in SetupTestDB

	passwordHash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	require.NoError(t, err)

	_, err = dbConn.Exec(`
		INSERT INTO users (tenant_id, username, password_hash)
		VALUES ($1, $2, $3)
		ON CONFLICT (tenant_id, username) DO UPDATE SET password_hash = EXCLUDED.password_hash
	`, tenantID, username, string(passwordHash))
	require.NoError(t, err)
}

func startTestGRPCServer(t *testing.T, dbConn *sql.DB, authPlugin plugins.AuthPlugin, secretPlugin plugins.SecretPlugin, brokerURL string) (string, func()) {
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(server.UnaryAuthInterceptor(authPlugin, secretPlugin)),
		grpc.StreamInterceptor(server.StreamAuthInterceptor(authPlugin, secretPlugin)),
	)

	srv := server.NewStreamingGRPCServer(dbConn, brokerURL, authPlugin, secretPlugin)
	streamingv1.RegisterStreamingServiceServer(grpcServer, srv)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			// Expected on stop
		}
	}()

	return lis.Addr().String(), func() {
		grpcServer.Stop()
	}
}

func TestStreamingGateway_AuthenticatedRequest(t *testing.T) {
	testDB, _ := dbcommon.SetupTestDB(t, "../../../frkr-common/migrations")

	// Create tenant and user
	tenant, err := dbcommon.CreateOrGetTenant(testDB, "test-tenant-streaming")
	require.NoError(t, err)

	_, err = dbcommon.CreateUser(testDB, tenant.ID, "streamuser", "streampass123")
	require.NoError(t, err)

	// Create stream
	_, err = dbcommon.CreateStream(testDB, tenant.ID, "test-stream", "Test stream", 7)
	require.NoError(t, err)

	// Initialize plugins
	secretPlugin, err := plugins.NewDatabaseSecretPlugin(testDB)
	require.NoError(t, err)

	authPlugin := plugins.NewBasicAuthPlugin(testDB)

	// Start gRPC server
	addr, cleanup := startTestGRPCServer(t, testDB, authPlugin, secretPlugin, "localhost:9092")
	defer cleanup()

	// Connect client
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := streamingv1.NewStreamingServiceClient(conn)

	t.Run("successful authenticated request", func(t *testing.T) {
		creds := base64.StdEncoding.EncodeToString([]byte("streamuser:streampass123"))
		ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Basic "+creds)

		// ListStreams uses UnaryInterceptor and logic similar to OpenStream for auth
		resp, err := client.ListStreams(ctx, &streamingv1.ListStreamsRequest{})
		require.NoError(t, err)
		require.NotNil(t, resp)
		
		// We expect at least the stream we created
		found := false
		for _, s := range resp.Streams {
			if s.Name == "test-stream" {
				found = true
				break
			}
		}
		assert.True(t, found, "test-stream should be returned")
	})

	t.Run("unauthorized - missing auth header", func(t *testing.T) {
		_, err := client.ListStreams(context.Background(), &streamingv1.ListStreamsRequest{})
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.Unauthenticated, st.Code())
	})

	t.Run("unauthorized - invalid credentials", func(t *testing.T) {
		creds := base64.StdEncoding.EncodeToString([]byte("streamuser:wrongpass"))
		ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Basic "+creds)

		_, err := client.ListStreams(ctx, &streamingv1.ListStreamsRequest{})
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.Unauthenticated, st.Code())
	})

    t.Run("unauthorized - wrong tenant", func(t *testing.T) {
		// Create another tenant and user
		otherTenant, err := dbcommon.CreateOrGetTenant(testDB, "other-tenant-streaming")
		require.NoError(t, err)
        _, err = dbcommon.CreateUser(testDB, otherTenant.ID, "otheruser", "otherpass123")
		require.NoError(t, err)
		
		creds := base64.StdEncoding.EncodeToString([]byte("otheruser:otherpass123"))
		ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Basic "+creds)

		resp, err := client.ListStreams(ctx, &streamingv1.ListStreamsRequest{})
		require.NoError(t, err)
		
        // Other user should NOT see streams from first tenant
		for _, s := range resp.Streams {
			if s.Name == "test-stream" {
				assert.Fail(t, "Should not see stream from another tenant")
			}
		}
	})
}
