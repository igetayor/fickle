package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/igetayor/fickle/internal/api"
	"github.com/igetayor/fickle/internal/config"
	"github.com/igetayor/fickle/internal/hub"
	"github.com/igetayor/fickle/internal/logger"
	"github.com/igetayor/fickle/internal/redis"
	"github.com/igetayor/fickle/web"
)

func main() {
	// 1. Load Configuration
	cfg := config.LoadFromEnv()

	// 2. Initialize Structured JSON Logger
	log := logger.InitLogger(cfg.LogLevel, cfg.NodeID)
	log.Info("Initializing Fickle High-Performance WebSocket Server",
		slog.String("node_id", cfg.NodeID),
		slog.String("port", cfg.Port),
		slog.String("redis_addr", cfg.RedisAddr))

	// 3. Root Context with Graceful Signal Handling
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 4. Connect Redis Pub/Sub Backend
	redisBackend := redis.NewRedisBackend(cfg)
	defer func() {
		_ = redisBackend.Close()
	}()

	// 5. Initialize Connection Hub
	connectionHub := hub.NewHub(cfg, redisBackend)
	go connectionHub.Run(ctx)

	// 6. Start Real-Time Telemetry & Financial Ticker Generator
	api.StartTelemetryStream(ctx, connectionHub, 600*time.Millisecond)

	// 7. Initialize HTTP & WebSocket Server Router
	server := api.NewServer(cfg, connectionHub, web.FS)

	httpServer := &http.Server{
		Addr:              cfg.Port,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// 8. Start HTTP Server in Goroutine
	serverErr := make(chan error, 1)
	go func() {
		log.Info("Server listening for WebSocket and HTTP connections",
			slog.String("addr", cfg.Port),
			slog.String("ui_url", "http://localhost"+cfg.Port))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	// 9. Await Shutdown Signal or Fatal Server Error
	select {
	case <-ctx.Done():
		log.Info("Shutdown signal received; gracefully draining connections...")
	case err := <-serverErr:
		log.Error("Fatal server listener error", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// 10. Graceful Shutdown with Timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Error("Forced shutdown due to timeout", slog.String("error", err.Error()))
	} else {
		log.Info("Server exited cleanly")
	}
}
