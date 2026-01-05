# frkr-streaming-gateway

Streaming Gateway service for frkr.

## Overview

The Streaming Gateway streams mirrored HTTP requests from a Kafka-compatible message broker to CLI clients using Server-Sent Events (SSE).

## Features

- Server-Sent Events (SSE) streaming endpoint
- Basic authentication support
- Automatic stream subscription based on stream configuration
- Health check endpoint
- Kafka-compatible message broker integration

## Installation

```bash
go get github.com/frkr-io/frkr-streaming-gateway
```

## Building

```bash
make build
```

The binary will be created in the `bin/` directory as `bin/gateway`.

To clean build artifacts:

```bash
make clean
```

## Configuration

The gateway can be configured via command-line flags or environment variables:

| Flag | Environment Variable | Default | Description |
|------|---------------------|---------|-------------|
| `--http-port` | `HTTP_PORT` | `8081` | HTTP server port |
| `--db-url` | `DB_URL` | `postgres://root@localhost:26257/defaultdb?sslmode=disable` | Database connection URL |
| `--broker-url` | `BROKER_URL` | `localhost:19092` | Kafka-compatible broker URL |

## Usage

### Start the Gateway

```bash
./bin/gateway \
  --http-port=8081 \
  --db-url="postgres://user@localhost/dbname?sslmode=disable" \
  --broker-url="localhost:9092"
```

### Stream Messages

```bash
curl -N -H "Authorization: Basic dXNlcm5hbWU6cGFzc3dvcmQ=" \
  "http://localhost:8081/stream?stream_id=my-api"
```

The response will be a Server-Sent Events stream:

```
data: {"method":"GET","path":"/api/users","headers":{},"body":"","query":{},"timestamp_ns":1234567890,"request_id":"req-123"}

data: {"method":"POST","path":"/api/users","headers":{},"body":"{\"name\":\"Alice\"}","query":{},"timestamp_ns":1234567891,"request_id":"req-124"}

...
```

### Health Check

```bash
curl http://localhost:8081/health
```

## API Reference

### GET /stream

Streams messages from the specified stream.

**Query Parameters:**
- `stream_id` (required) - The stream identifier

**Headers:**
- `Authorization: Basic <base64-encoded-credentials>` (required)

**Response:**
- `200 OK` - SSE stream of messages
- `401 Unauthorized` - Authentication failed
- `404 Not Found` - Stream not found

**Response Format:**
Server-Sent Events (SSE) with JSON message data:

```
data: <json-message>

data: <json-message>

...
```

Each message is a JSON object:
```json
{
  "method": "string",
  "path": "string",
  "headers": {},
  "body": "string",
  "query": {},
  "timestamp_ns": 0,
  "request_id": "string"
}
```

### GET /health

Health check endpoint.

**Response:**
- `200 OK` - Service is healthy

## Requirements

- Go 1.21 or later
- PostgreSQL-compatible database
- Kafka-compatible message broker

## License

Apache 2.0
