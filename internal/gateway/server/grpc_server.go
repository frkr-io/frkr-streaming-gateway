package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	dbcommon "github.com/frkr-io/frkr-common/db"
	"github.com/frkr-io/frkr-common/metrics"
	"github.com/frkr-io/frkr-common/plugins"
	streamingv1 "github.com/frkr-io/frkr-proto/go/streaming/v1"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// StreamingGRPCServer implements the StreamingServiceServer interface
type StreamingGRPCServer struct {
	streamingv1.UnimplementedStreamingServiceServer
	DB           *sql.DB
	BrokerURL    string
	AuthPlugin   plugins.AuthPlugin
	SecretPlugin plugins.SecretPlugin
}

// NewStreamingGRPCServer creates a new gRPC server for the streaming service
func NewStreamingGRPCServer(
	db *sql.DB,
	brokerURL string,
	authPlugin plugins.AuthPlugin,
	secretPlugin plugins.SecretPlugin,
) *StreamingGRPCServer {
	return &StreamingGRPCServer{
		DB:           db,
		BrokerURL:    brokerURL,
		AuthPlugin:   authPlugin,
		SecretPlugin: secretPlugin,
	}
}

// ListStreams returns all streams the authenticated user has access to
func (s *StreamingGRPCServer) ListStreams(ctx context.Context, req *streamingv1.ListStreamsRequest) (*streamingv1.ListStreamsResponse, error) {
	// Get auth result from context (set by interceptor)
	authResult, ok := AuthFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "not authenticated")
	}

	// Get tenant ID from auth result
	tenantID := authResult.TenantID
	if tenantID == "" {
		return nil, status.Error(codes.FailedPrecondition, "no tenant ID in auth result")
	}

	// Query streams for this tenant
	streams, err := dbcommon.ListStreams(s.DB, tenantID)
	if err != nil {
		log.Printf("Error listing streams: %v", err)
		return nil, status.Error(codes.Internal, "failed to list streams")
	}

	// Convert to proto messages
	var protoStreams []*streamingv1.StreamInfo
	for _, stream := range streams {
		protoStream := &streamingv1.StreamInfo{
			Id:            stream.ID,
			TenantId:      stream.TenantID,
			Name:          stream.Name,
			Description:   stream.Description,
			Status:        stream.Status,
			RetentionDays: int32(stream.RetentionDays),
			Topic:         stream.Topic,
			CreatedAt:     timestamppb.New(stream.CreatedAt),
			UpdatedAt:     timestamppb.New(stream.UpdatedAt),
		}
		if stream.DeletedAt != nil {
			protoStream.DeletedAt = timestamppb.New(*stream.DeletedAt)
		}
		protoStreams = append(protoStreams, protoStream)
	}

	return &streamingv1.ListStreamsResponse{
		Streams: protoStreams,
	}, nil
}

// OpenStream opens a stream and sends messages to the client
func (s *StreamingGRPCServer) OpenStream(req *streamingv1.OpenStreamRequest, stream streamingv1.StreamingService_OpenStreamServer) error {
	ctx := stream.Context()
	startTime := time.Now()

	// Track stream open/close
	metrics.RecordStreamOpened()
	defer func() {
		metrics.RecordStreamClosed()
		metrics.RecordStreamDuration(req.StreamId, time.Since(startTime).Seconds())
	}()

	// Get auth result from context (set by interceptor)
	authResult, ok := AuthFromContext(ctx)
	if !ok {
		metrics.RecordAuthFailure("frkr-streaming-gateway", "missing_context")
		return status.Error(codes.Unauthenticated, "not authenticated")
	}

	streamID := req.StreamId
	if streamID == "" {
		return status.Error(codes.InvalidArgument, "stream_id is required")
	}

	// Check stream access authorization
	allowed, err := s.AuthPlugin.CanAccessStream(ctx, authResult, streamID, "read")
	if err != nil {
		log.Printf("Authorization check failed: %v", err)
		return status.Error(codes.Internal, "authorization check failed")
	}
	if !allowed {
		metrics.RecordAuthFailure("frkr-streaming-gateway", "access_denied")
		return status.Error(codes.PermissionDenied, "access denied to stream")
	}

	// Get stream topic from database
	topic, err := dbcommon.GetStreamTopic(s.DB, streamID)
	if err != nil {
		return status.Error(codes.NotFound, "stream not found")
	}

	log.Printf("Opening stream %s (topic: %s) for user %s", streamID, topic, authResult.UserID)

	// Create Kafka reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{s.BrokerURL},
		Topic:   topic,
		GroupID: fmt.Sprintf("streaming-gateway-%s-%d", streamID, time.Now().UnixNano()),
	})
	defer reader.Close()

	// Stream messages
	for {
		select {
		case <-ctx.Done():
			log.Printf("Stream %s closed: client disconnected", streamID)
			return nil
		default:
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil // Client disconnected
				}
				log.Printf("Error reading message from Kafka: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			// Parse message
			var streamMsg streamingv1.StreamMessage
			if err := json.Unmarshal(msg.Value, &streamMsg); err != nil {
				log.Printf("Error parsing message: %v", err)
				continue
			}

			// Send to client
			if err := stream.Send(&streamMsg); err != nil {
				log.Printf("Error sending message to client: %v", err)
				return err
			}

			// Record message delivered
			metrics.RecordMessageDelivered(streamID)
		}
	}
}

