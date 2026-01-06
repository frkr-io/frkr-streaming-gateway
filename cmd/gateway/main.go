package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/frkr-io/frkr-common/auth"
	dbcommon "github.com/frkr-io/frkr-common/db"
	"github.com/frkr-io/frkr-common/gateway"
	"github.com/frkr-io/frkr-common/messages"
	_ "github.com/lib/pq" // PostgreSQL driver registration
	"github.com/segmentio/kafka-go"
)

const (
	ServiceName = "frkr-streaming-gateway"
	Version     = "0.1.0"
)

var (
	httpPort  = flag.Int("http-port", 8081, "HTTP server port")
	dbURL     = flag.String("db-url", "", "Postgres-compatible database connection URL (required)")
	brokerURL = flag.String("broker-url", "", "Broker URL (Kafka Protocol compliant, required)")
)

func main() {
	flag.Parse()

	// Load and validate configuration (12-factor app pattern)
	cfg := &gateway.Config{
		HTTPPort:  *httpPort,
		DBURL:     *dbURL,
		BrokerURL: *brokerURL,
	}
	gateway.MustLoadConfig(cfg)

	// Initialize database connection
	db, err := sql.Open("postgres", cfg.DBURL)
	if err != nil {
		log.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Create health checker and start background health checks
	healthChecker := gateway.NewHealthChecker(ServiceName, Version)
	healthChecker.StartHealthCheckLoop(db, cfg.BrokerURL)

	// Set up HTTP handlers
	mux := http.NewServeMux()

	// Register standard health endpoints from shared package
	healthChecker.RegisterHealthEndpoints(mux, cfg.HTTPPort, cfg.DBURL, cfg.BrokerURL)

	// Business endpoint
	mux.HandleFunc("/stream", makeStreamHandler(db, cfg.BrokerURL, healthChecker))

	// Start server
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler: mux,
	}

	go func() {
		log.Printf("Starting %s v%s on port %d", ServiceName, Version, cfg.HTTPPort)
		log.Printf("  Database: %s", gateway.SanitizeURL(cfg.DBURL))
		log.Printf("  Broker:   %s", cfg.BrokerURL)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}

func makeStreamHandler(db *sql.DB, brokerURL string, hc *gateway.HealthChecker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Check if we're ready
		if !hc.IsReady() {
			http.Error(w, "Service unavailable - dependencies not ready", http.StatusServiceUnavailable)
			return
		}

		// Authenticate
		authHeader := r.Header.Get("Authorization")
		streamID := r.URL.Query().Get("stream_id")
		if !auth.ValidateBasicAuthForStream(authHeader, streamID, db) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		// Get stream topic
		topic, err := dbcommon.GetStreamTopic(db, streamID)
		if err != nil {
			http.Error(w, "Stream not found", http.StatusNotFound)
			return
		}

		// Set up SSE
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		// Create Kafka reader
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{brokerURL},
			Topic:   topic,
			GroupID: fmt.Sprintf("streaming-gateway-%s", streamID),
		})
		defer reader.Close()

		// Stream messages
		ctx := r.Context()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := reader.ReadMessage(ctx)
				if err != nil {
					if err == context.Canceled {
						return
					}
					log.Printf("Error reading message: %v", err)
					time.Sleep(1 * time.Second)
					continue
				}

				// Parse message
				var streamMsg messages.StreamMessage
				if err := json.Unmarshal(msg.Value, &streamMsg); err != nil {
					log.Printf("Error parsing message: %v", err)
					continue
				}

				// Send as SSE
				data, _ := json.Marshal(streamMsg)
				_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			}
		}
	}
}
