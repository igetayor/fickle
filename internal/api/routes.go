package api

import (
	"io/fs"
	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/igetayor/fickle/internal/api/controllers"
	"github.com/igetayor/fickle/internal/api/services"
	"github.com/igetayor/fickle/internal/config"
	"github.com/igetayor/fickle/internal/hub"
	"github.com/igetayor/fickle/internal/logger"
)

// Server encapsulates the HTTP and WebSocket API router.
// Wires: Controllers (HTTP adapters) -> Services (business logic) -> Hub/Redis (persistence).
type Server struct {
	cfg       *config.Config
	hub       *hub.Hub
	mux       *http.ServeMux
	webFS     fs.FS
	wsCtrl    *controllers.WebSocketController
	roomCtrl  *controllers.RoomController
	statsCtrl *controllers.MetricsController
}

// NewServer builds an API server instance with clean Controller-Service architecture.
func NewServer(cfg *config.Config, h *hub.Hub, webFS fs.FS) *Server {
	upgrader := websocket.Upgrader{
		ReadBufferSize:  cfg.ReadBufferSize,
		WriteBufferSize: cfg.WriteBufferSize,
		CheckOrigin: func(r *http.Request) bool {
			return true // Permissive CORS for cross-origin WebSocket clients
		},
	}

	roomSvc := services.NewRoomService(h)

	s := &Server{
		cfg:       cfg,
		hub:       h,
		mux:       http.NewServeMux(),
		webFS:     webFS,
		wsCtrl:    controllers.NewWebSocketController(cfg, h, upgrader),
		roomCtrl:  controllers.NewRoomController(roomSvc),
		statsCtrl: controllers.NewMetricsController(cfg, h),
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
	// WebSocket Connection Handshake
	s.mux.HandleFunc("/ws", s.wsCtrl.HandleUpgrade)

	// Health and System Telemetry
	s.mux.HandleFunc("/healthz", s.statsCtrl.HandleHealthz)
	s.mux.HandleFunc("/api/v1/metrics", s.statsCtrl.HandleMetrics)

	// Room & Broadcasting APIs
	s.mux.HandleFunc("/api/v1/rooms", s.roomCtrl.HandleRooms)
	s.mux.HandleFunc("/api/v1/broadcast", s.roomCtrl.HandleBroadcast)

	// Serve Frontend Web Dashboard & Interactive Documentation from web/
	if s.webFS != nil {
		fileServer := http.FileServer(http.FS(s.webFS))
		s.mux.Handle("/", fileServer)
	}
}
