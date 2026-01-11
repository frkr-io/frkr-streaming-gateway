package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	dbcommon "github.com/frkr-io/frkr-common/db"
	"github.com/frkr-io/frkr-common/gateway"
	streamingv1 "github.com/frkr-io/frkr-proto/go/streaming/v1"
	"github.com/segmentio/kafka-go"
)

// StreamHandler handles GET /stream requests (SSE streaming)
func (s *StreamingGatewayServer) StreamHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Check if we're ready
		if !s.HealthChecker.IsReady() {
			http.Error(w, "Service unavailable - dependencies not ready", http.StatusServiceUnavailable)
			return
		}

		// Get stream ID from query parameter
		streamID := r.URL.Query().Get("stream_id")
		if streamID == "" {
			http.Error(w, "stream_id query parameter is required", http.StatusBadRequest)
			return
		}

		// Authenticate and authorize
		ctx := r.Context()
		_, err := gateway.AuthenticateHTTPRequest(ctx, r, s.AuthPlugin, s.SecretPlugin, streamID, "read")
		if err != nil {
			log.Printf("Authentication failed: %v", err)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		// Get stream topic
		topic, err := dbcommon.GetStreamTopic(s.DB, streamID)
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
			Brokers: []string{s.BrokerURL},
			Topic:   topic,
			GroupID: fmt.Sprintf("streaming-gateway-%s", streamID),
		})
		defer reader.Close()

		// Stream messages
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
				var streamMsg streamingv1.StreamMessage
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
