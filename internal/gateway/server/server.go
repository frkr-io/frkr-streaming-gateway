package server

import (
	"database/sql"
	"fmt"
	"net/http"

	"github.com/frkr-io/frkr-common/gateway"
	"github.com/frkr-io/frkr-common/plugins"
)

// StreamingGatewayServer holds the gateway server dependencies
type StreamingGatewayServer struct {
	DB            *sql.DB
	BrokerURL     string
	HealthChecker *gateway.GatewayHealthChecker
	AuthPlugin    plugins.AuthPlugin
	SecretPlugin  plugins.SecretPlugin
}

// NewStreamingGatewayServer creates a new streaming gateway server
func NewStreamingGatewayServer(
	db *sql.DB,
	brokerURL string,
	healthChecker *gateway.GatewayHealthChecker,
	authPlugin plugins.AuthPlugin,
	secretPlugin plugins.SecretPlugin,
) *StreamingGatewayServer {
	return &StreamingGatewayServer{
		DB:            db,
		BrokerURL:     brokerURL,
		HealthChecker: healthChecker,
		AuthPlugin:    authPlugin,
		SecretPlugin:  secretPlugin,
	}
}

// SetupHandlers registers all HTTP handlers on the provided mux
func (s *StreamingGatewayServer) SetupHandlers(mux *http.ServeMux, cfg *gateway.GatewayBaseConfig) {
	// Build URLs for health endpoints
	var dbURL string
	if cfg.DBURL != "" {
		dbURL = cfg.DBURL
	} else {
		port := cfg.DBPort
		if port == "" {
			port = "26257"
		}
		if cfg.DBUser != "" {
			if cfg.DBPassword != "" {
				dbURL = fmt.Sprintf("postgres://%s:%s@%s:%s/%s", cfg.DBUser, cfg.DBPassword, cfg.DBHost, port, cfg.DBName)
			} else {
				dbURL = fmt.Sprintf("postgres://%s@%s:%s/%s", cfg.DBUser, cfg.DBHost, port, cfg.DBName)
			}
		} else {
			dbURL = fmt.Sprintf("postgres://%s:%s/%s", cfg.DBHost, port, cfg.DBName)
		}
	}

	// Register standard health endpoints
	s.HealthChecker.RegisterHealthEndpoints(mux, cfg.HTTPPort, dbURL, s.BrokerURL)

	// Business endpoint
	mux.HandleFunc("/stream", s.StreamHandler())
}
