// Package controllers houses HTTP and WebSocket transport adapters.
package controllers

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
	"github.com/igetayor/fickle/internal/config"
	"github.com/igetayor/fickle/internal/hub"
	"github.com/igetayor/fickle/internal/logger"
)

// WebSocketController manages WebSocket upgrade and lifecycle handshake.
type WebSocketController struct {
	cfg      *config.Config
	hub      *hub.Hub
	upgrader websocket.Upgrader
}

// NewWebSocketController constructs a controller for WebSocket connections.
func NewWebSocketController(cfg *config.Config, h *hub.Hub, upgrader websocket.Upgrader) *WebSocketController {
	return &WebSocketController{
		cfg:      cfg,
		hub:      h,
		upgrader: upgrader,
	}
}

// HandleUpgrade upgrades incoming HTTP requests to full-duplex WebSocket connections.
func (c *WebSocketController) HandleUpgrade(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	traceID := logger.TraceIDFromContext(ctx)

	conn, err := c.upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.FromContext(ctx).Error("Failed to upgrade connection to WebSocket",
			slog.String("error", err.Error()),
			slog.String("remote_addr", r.RemoteAddr))
		return
	}

	query := r.URL.Query()
	nickname := strings.TrimSpace(query.Get("nickname"))
	initialRoom := strings.TrimSpace(query.Get("room"))
	userAgent := r.UserAgent()

	client := hub.NewClient(c.hub, conn, nickname, r.RemoteAddr, userAgent)
	c.hub.RegisterChannel() <- client

	if initialRoom != "" {
		c.hub.JoinRoom(client, initialRoom)
	}

	logger.FromContext(ctx).Info("WebSocket connection established",
		slog.String("client_id", client.ID()),
		slog.String("room", initialRoom),
		slog.String("trace_id", traceID))

	go client.WritePump()
	client.ReadPump()
}
