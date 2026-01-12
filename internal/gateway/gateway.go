package gateway

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/frkr-io/frkr-common/gateway"
	"github.com/frkr-io/frkr-common/plugins"
	streamingv1 "github.com/frkr-io/frkr-proto/go/streaming/v1"
	"github.com/frkr-io/frkr-streaming-gateway/internal/gateway/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

const (
	ServiceName = "frkr-streaming-gateway"
	Version     = "0.2.0"
)

// StreamingGateway is the main gateway server
type StreamingGateway struct {
	authPlugin   plugins.AuthPlugin
	secretPlugin plugins.SecretPlugin
}

// NewStreamingGateway creates a new streaming gateway with injected plugins
func NewStreamingGateway(authPlugin plugins.AuthPlugin, secretPlugin plugins.SecretPlugin) (*StreamingGateway, error) {
	if authPlugin == nil {
		return nil, fmt.Errorf("authPlugin cannot be nil")
	}
	if secretPlugin == nil {
		return nil, fmt.Errorf("secretPlugin cannot be nil")
	}
	return &StreamingGateway{
		authPlugin:   authPlugin,
		secretPlugin: secretPlugin,
	}, nil
}

// Start starts the gRPC gateway server
func (g *StreamingGateway) Start(cfg *gateway.GatewayBaseConfig, db *sql.DB) error {
	// Build broker URL
	var brokerURL string
	if cfg.BrokerURL != "" {
		brokerURL = cfg.BrokerURL
	} else if cfg.BrokerHost != "" {
		port := cfg.BrokerPort
		if port == "" {
			port = "9092"
		}
		brokerURL = fmt.Sprintf("%s:%s", cfg.BrokerHost, port)
	}

	// Create gRPC server with auth interceptors
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(server.UnaryAuthInterceptor(g.authPlugin, g.secretPlugin)),
		grpc.StreamInterceptor(server.StreamAuthInterceptor(g.authPlugin, g.secretPlugin)),
	)

	// Create and register the streaming service
	streamingServer := server.NewStreamingGRPCServer(db, brokerURL, g.authPlugin, g.secretPlugin)
	streamingv1.RegisterStreamingServiceServer(grpcServer, streamingServer)

	// Register gRPC health checking
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus(ServiceName, grpc_health_v1.HealthCheckResponse_SERVING)

	// Enable gRPC reflection for debugging
	reflection.Register(grpcServer)

	// Start background health checking for dependencies
	go g.healthCheckLoop(db, brokerURL, healthServer)

	// Start listening
	addr := fmt.Sprintf(":%d", cfg.HTTPPort) // Reusing HTTPPort config for gRPC
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	go func() {
		log.Printf("Starting %s v%s (gRPC) on port %d", ServiceName, Version, cfg.HTTPPort)
		log.Printf("  Broker: %s", brokerURL)
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")

	// Graceful shutdown
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	// Wait up to 10 seconds for graceful shutdown
	select {
	case <-stopped:
		log.Println("Server stopped gracefully")
	case <-time.After(10 * time.Second):
		log.Println("Forcing shutdown...")
		grpcServer.Stop()
	}

	return nil
}

// healthCheckLoop periodically checks dependencies and updates gRPC health status
func (g *StreamingGateway) healthCheckLoop(db *sql.DB, brokerURL string, healthServer *health.Server) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C

		// Check database
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := db.PingContext(ctx)
		cancel()

		if err != nil {
			log.Printf("Health check: database unhealthy: %v", err)
			healthServer.SetServingStatus(ServiceName, grpc_health_v1.HealthCheckResponse_NOT_SERVING)
			continue
		}

		// TODO: Add Kafka health check if needed

		healthServer.SetServingStatus(ServiceName, grpc_health_v1.HealthCheckResponse_SERVING)
	}
}
