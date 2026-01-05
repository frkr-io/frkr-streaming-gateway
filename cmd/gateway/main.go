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
	"strconv"
	"syscall"
	"time"

	"github.com/frkr-io/frkr-common/auth"
	dbcommon "github.com/frkr-io/frkr-common/db"
	"github.com/frkr-io/frkr-common/messages"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

var (
	httpPort    = flag.Int("http-port", 8081, "HTTP server port")
	dbURL       = flag.String("db-url", "", "Postgres-compatible database connection URL")
	brokerURL = flag.String("broker-url", "localhost:19092", "Kafka-compatible broker URL")
)

func main() {
	flag.Parse()

	// Check environment variables if flags not set
	if *dbURL == "" {
		*dbURL = os.Getenv("DB_URL")
		if *dbURL == "" {
			*dbURL = "postgres://root@localhost:26257/frkrdb?sslmode=disable"
		}
	}

	if *brokerURL == "localhost:19092" {
		if envURL := os.Getenv("BROKER_URL"); envURL != "" {
			*brokerURL = envURL
		}
	}

	if *httpPort == 8081 {
		if envPort := os.Getenv("HTTP_PORT"); envPort != "" {
			if port, err := strconv.Atoi(envPort); err == nil {
				*httpPort = port
			}
		}
	}

	// Connect to database
	db, err := sql.Open("postgres", *dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	// HTTP handlers
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	http.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
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
			Brokers: []string{*brokerURL},
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
				fmt.Fprintf(w, "data: %s\n\n", data)
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			}
		}
	})

	// Start server
	server := &http.Server{
		Addr: fmt.Sprintf(":%d", *httpPort),
	}

	go func() {
		log.Printf("Starting Streaming Gateway on port %d", *httpPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	server.Close()
}

