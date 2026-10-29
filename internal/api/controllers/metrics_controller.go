package controllers

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/igetayor/fickle/internal/config"
	"github.com/igetayor/fickle/internal/hub"
)

// MetricsController exposes telemetry, health checks, and runtime stats.
type MetricsController struct {
	cfg *config.Config
	hub *hub.Hub
}

// NewMetricsController creates a new metrics controller.
func NewMetricsController(cfg *config.Config, h *hub.Hub) *MetricsController {
	return &MetricsController{cfg: cfg, hub: h}
}

// HandleHealthz responds with health status and cluster info.
func (c *MetricsController) HandleHealthz(w http.ResponseWriter, r *http.Request) {
	metrics := c.hub.GetMetrics()
	response := map[string]any{
		"status":             "healthy",
		"node_id":            c.cfg.NodeID,
		"uptime_seconds":     metrics.UptimeSeconds,
		"active_connections": metrics.ActiveConnections,
		"redis_connected":    metrics.RedisConnected,
		"timestamp":          time.Now().Unix(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(response)
}

// HandleMetrics returns detailed telemetry for monitoring dashboards.
func (c *MetricsController) HandleMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := c.hub.GetMetrics()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(metrics)
}
