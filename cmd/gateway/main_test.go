package main

import (
	"database/sql"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/frkr-io/frkr-streaming-gateway/internal/gateway/server"
	"github.com/frkr-io/frkr-common/db"
	dbcommon "github.com/frkr-io/frkr-common/db"
	"github.com/frkr-io/frkr-common/gateway"
	"github.com/frkr-io/frkr-common/plugins"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

// setupTestUserForGateway creates a test user in the database
func setupTestUserForStreamingGateway(t *testing.T, dbConn *sql.DB, tenantID, username, password string) {
	_, err := dbConn.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
			username STRING(255) NOT NULL,
			password_hash STRING(255) NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			deleted_at TIMESTAMPTZ,
			UNIQUE (tenant_id, username)
		)
	`)
	require.NoError(t, err)

	passwordHash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	require.NoError(t, err)

	_, err = dbConn.Exec(`
		INSERT INTO users (tenant_id, username, password_hash)
		VALUES ($1, $2, $3)
		ON CONFLICT (tenant_id, username) DO UPDATE SET password_hash = EXCLUDED.password_hash
	`, tenantID, username, string(passwordHash))
	require.NoError(t, err)
}

func TestStreamingGateway_AuthenticatedRequest(t *testing.T) {
	testDB, _ := db.SetupTestDB(t, "../../../frkr-common/migrations")

	// Create tenant and user
	tenant, err := dbcommon.CreateOrGetTenant(testDB, "test-tenant-streaming")
	require.NoError(t, err)

	setupTestUserForStreamingGateway(t, testDB, tenant.ID, "streamuser", "streampass123")

	// Create stream
	stream, err := dbcommon.CreateStream(testDB, tenant.ID, "test-stream", "Test stream", 7)
	require.NoError(t, err)

	// Initialize plugins
	secretPlugin, err := plugins.NewDatabaseSecretPlugin(testDB)
	require.NoError(t, err)

	authPlugin := plugins.NewBasicAuthPlugin(testDB)

	healthChecker := gateway.NewHealthChecker("frkr-streaming-gateway", "0.1.0")
	healthChecker.CheckDependencies(testDB, "localhost:9092")

	// Create server and get handler
	srv := server.NewServer(testDB, "localhost:9092", healthChecker, authPlugin, secretPlugin)
	cfg := &gateway.Config{
		HTTPPort: 8081,
		DBURL:    "test",
		BrokerURL: "localhost:9092",
	}
	mux := http.NewServeMux()
	srv.SetupHandlers(mux, cfg)
	handler := mux.ServeHTTP

	t.Run("successful authenticated request - auth passes before Kafka read", func(t *testing.T) {
		t.Skip("Skipping blocking SSE test - requires Kafka testcontainers")
	})

	t.Run("unauthorized - missing auth header", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/stream?stream_id="+stream.Name, nil)
		w := httptest.NewRecorder()

		handler(w, req)

		assert.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("unauthorized - invalid credentials", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/stream?stream_id="+stream.Name, nil)
		credentials := base64.StdEncoding.EncodeToString([]byte("streamuser:wrongpass"))
		req.Header.Set("Authorization", "Basic "+credentials)

		w := httptest.NewRecorder()
		handler(w, req)

		assert.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("missing stream_id parameter", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/stream", nil)
		credentials := base64.StdEncoding.EncodeToString([]byte("streamuser:streampass123"))
		req.Header.Set("Authorization", "Basic "+credentials)

		w := httptest.NewRecorder()
		handler(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("unauthorized - wrong tenant", func(t *testing.T) {
		// Create another tenant and stream
		otherTenant, err := dbcommon.CreateOrGetTenant(testDB, "other-tenant-streaming")
		require.NoError(t, err)

		otherStream, err := dbcommon.CreateStream(testDB, otherTenant.ID, "other-stream", "Other", 7)
		require.NoError(t, err)

		req := httptest.NewRequest("GET", "/stream?stream_id="+otherStream.Name, nil)
		credentials := base64.StdEncoding.EncodeToString([]byte("streamuser:streampass123"))
		req.Header.Set("Authorization", "Basic "+credentials)

		w := httptest.NewRecorder()
		handler(w, req)

		assert.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("method not allowed", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/stream?stream_id="+stream.Name, nil)
		credentials := base64.StdEncoding.EncodeToString([]byte("streamuser:streampass123"))
		req.Header.Set("Authorization", "Basic "+credentials)

		w := httptest.NewRecorder()
		handler(w, req)

		assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
	})
}
