package api

import (
	"context"
	"encoding/json"
	"io/fs"
	"log/slog"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/igetayor/fickle/internal/config"
	"github.com/igetayor/fickle/internal/hub"
	"github.com/igetayor/fickle/internal/logger"
	"github.com/igetayor/fickle/internal/models"
)

// Server encapsulates the HTTP and WebSocket API router.
type Server struct {
	cfg      *config.Config
	hub      *hub.Hub
	upgrader websocket.Upgrader
	mux      *http.ServeMux
	webFS    fs.FS
}

// NewServer builds an API server instance.
func NewServer(cfg *config.Config, h *hub.Hub, webFS fs.FS) *Server {
	s := &Server{
		cfg: cfg,
		hub: h,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  cfg.ReadBufferSize,
			WriteBufferSize: cfg.WriteBufferSize,
			CheckOrigin: func(r *http.Request) bool {
				return true // Permissive CORS for cross-origin WebSocket clients
			},
		},
		mux:   http.NewServeMux(),
		webFS: webFS,
	}

	s.registerRoutes()
	return s
}

// Handler returns the HTTP handler with trace ID injection middleware.
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID := r.Header.Get("X-Request-ID")
		if traceID == "" {
			traceID = uuid.New().String()
		}
		w.Header().Set("X-Request-ID", traceID)
		w.Header().Set("X-Node-ID", s.cfg.NodeID)

		ctx := logger.WithTraceID(r.Context(), traceID)
		r = r.WithContext(ctx)

		s.mux.ServeHTTP(w, r)
	})
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/ws", s.handleWebSocket)
	s.mux.HandleFunc("/healthz", s.handleHealthz)
	s.mux.HandleFunc("/api/v1/metrics", s.handleMetrics)
	s.mux.HandleFunc("/api/v1/rooms", s.handleRooms)
	s.mux.HandleFunc("/api/v1/broadcast", s.handleBroadcast)

	// Serve Frontend Web Dashboard & Interactive Documentation
	if s.webFS != nil {
		fileServer := http.FileServer(http.FS(s.webFS))
		s.mux.Handle("/", fileServer)
	}
}

// handleWebSocket upgrades incoming HTTP connections to WebSocket.
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	traceID := logger.TraceIDFromContext(ctx)

	conn, err := s.upgrader.Upgrade(w, r, nil)
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

	client := hub.NewClient(s.hub, conn, nickname, r.RemoteAddr, userAgent)
	s.hub.RegisterChannel() <- client

	// Auto-join requested room if specified in query param
	if initialRoom != "" {
		s.hub.JoinRoom(client, initialRoom)
	}

	logger.FromContext(ctx).Info("WebSocket connection established",
		slog.String("client_id", client.ID()),
		slog.String("room", initialRoom),
		slog.String("trace_id", traceID))

	// Launch write pump in dedicated goroutine
	go client.WritePump()
	// Read pump runs on current goroutine until disconnect
	client.ReadPump()
}

// handleHealthz responds with service health and Redis cluster connectivity.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	metrics := s.hub.GetMetrics()
	status := "healthy"
	httpStatus := http.StatusOK

	response := map[string]any{
		"status":             status,
		"node_id":            s.cfg.NodeID,
		"uptime_seconds":     metrics.UptimeSeconds,
		"active_connections": metrics.ActiveConnections,
		"redis_connected":    metrics.RedisConnected,
		"timestamp":          time.Now().Unix(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	_ = json.NewEncoder(w).Encode(response)
}

// handleMetrics returns live operational metrics for monitoring dashboards.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := s.hub.GetMetrics()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(metrics)
}

// handleRooms lists all active rooms with their subscriber counts.
func (s *Server) handleRooms(w http.ResponseWriter, r *http.Request) {
	rooms := s.hub.GetRoomStats()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"rooms": rooms,
		"count": len(rooms),
	})
}

// handleBroadcast allows external HTTP clients/microservices to broadcast into rooms.
func (s *Server) handleBroadcast(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Room    string `json:"room"`
		Action  string `json:"action"`
		Message string `json:"message"`
		Data    any    `json:"data"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	traceID := logger.TraceIDFromContext(r.Context())
	envelope := models.OutboundEnvelope{
		Type:   models.TypeMessage,
		Action: req.Action,
		Room:   req.Room,
		Sender: &models.SenderInfo{
			ID:        "rest-api",
			Nickname:  "System Webhook",
			SessionID: "rest-" + traceID[:8],
		},
		Data:      req.Data,
		TraceID:   traceID,
		NodeID:    s.cfg.NodeID,
		Timestamp: time.Now().UnixMilli(),
	}

	if envelope.Action == "" {
		envelope.Action = models.ActionBroadcast
	}

	if req.Room != "" {
		s.hub.BroadcastToRoom(r.Context(), req.Room, envelope, nil, true)
	} else {
		s.hub.BroadcastGlobal(r.Context(), envelope, nil, true)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":   "queued",
		"trace_id": traceID,
		"room":     req.Room,
	})
}

// StartTelemetryStream periodically publishes synthetic financial/telemetry ticks
// to room "crypto-ticks" and "telemetry" to demonstrate real-time streaming throughput.
func StartTelemetryStream(ctx context.Context, h *hub.Hub, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		symbols := []string{"BTC/USDT", "ETH/USDT", "SOL/USDT"}
		basePrices := map[string]float64{
			"BTC/USDT": 96500.0,
			"ETH/USDT": 3450.0,
			"SOL/USDT": 195.0,
		}

		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				sym := symbols[rand.Intn(len(symbols))]
				drift := (rand.Float64() - 0.495) * (basePrices[sym] * 0.002)
				basePrices[sym] += drift
				price := basePrices[sym]
				spread := price * 0.0002

				tick := models.TelemetryTick{
					Symbol:    sym,
					Price:     price,
					Change24h: 2.45 + (rand.Float64()-0.5)*0.1,
					Volume:    float64(rand.Intn(500000) + 100000),
					Bid:       price - spread/2,
					Ask:       price + spread/2,
					Timestamp: t.UnixMilli(),
				}

				envelope := models.OutboundEnvelope{
					Type:      models.TypeTick,
					Action:    models.ActionTelemetry,
					Room:      "crypto-ticks",
					Data:      tick,
					TraceID:   uuid.New().String(),
					NodeID:    h.GetMetrics().NodeID,
					Timestamp: t.UnixMilli(),
				}

				// Broadcast to room "crypto-ticks" (local + sync cluster)
				h.BroadcastToRoom(ctx, "crypto-ticks", envelope, nil, true)
			}
		}
	}()
}
