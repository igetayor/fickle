# =========================================================================
# Multi-Stage Production Dockerfile for Fickle WebSocket Server
# =========================================================================

# Stage 1: Build & Static Compilation
FROM golang:1.24-alpine AS builder

WORKDIR /build

# Install CA certificates and build essentials
RUN apk update && apk add --no-cache ca-certificates git

# Cache Go dependency layers
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# Copy application source code
COPY . .

# Compile static binary with optimizations
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -trimpath \
    -ldflags="-s -w -extldflags '-static'" \
    -o /build/bin/fickle \
    ./cmd/server

# Stage 2: Minimal Distroless/Alpine Production Runtime
FROM alpine:3.20

# Create non-root execution user
RUN addgroup -S fickle && adduser -S fickleuser -G fickle

# Install runtime dependencies for healthchecks & TLS
RUN apk --no-cache add ca-certificates tzdata wget

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/bin/fickle /app/fickle

# Permissions
RUN chown -R fickleuser:fickle /app

# Run as non-privileged user
USER fickleuser

# Expose HTTP and WebSocket port
EXPOSE 8080

# Environment Defaults
ENV PORT=8080 \
    LOG_LEVEL=INFO \
    REDIS_ADDR=redis:6379 \
    REDIS_CHANNEL=fickle:pubsub:broadcast

# Production Healthcheck Probe
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/healthz || exit 1

ENTRYPOINT ["/app/fickle"]
