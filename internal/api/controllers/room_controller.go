package controllers

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/igetayor/fickle/internal/api/services"
	"github.com/igetayor/fickle/internal/models"
)

// RoomController handles REST interactions for querying rooms and pushing broadcasts.
type RoomController struct {
	roomService *services.RoomService
}

// NewRoomController builds a new RoomController instance.
func NewRoomController(rs *services.RoomService) *RoomController {
	return &RoomController{roomService: rs}
}

// HandleRooms returns the active rooms directory with participant counts.
func (c *RoomController) HandleRooms(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rooms := c.roomService.ListRooms()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"rooms": rooms,
		"count": len(rooms),
	})
}

// HandleBroadcast sends a message to all participants in a target room.
func (c *RoomController) HandleBroadcast(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Room    string `json:"room"`
		Content string `json:"content"`
		Sender  string `json:"sender"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Room == "" || req.Content == "" {
		http.Error(w, "Fields 'room' and 'content' are required", http.StatusBadRequest)
		return
	}

	if req.Sender == "" {
		req.Sender = "system"
	}

	envelope := models.OutboundEnvelope{
		Type:      models.TypeMessage,
		Action:    models.ActionBroadcast,
		Room:      req.Room,
		Sender:    &models.SenderInfo{Nickname: req.Sender, ID: "system"},
		Data:      req.Content,
		Timestamp: time.Now().Unix(),
	}

	c.roomService.Broadcast(r.Context(), req.Room, envelope)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":     "broadcast_queued",
		"message_id": uuid.New().String(),
		"room":       req.Room,
		"timestamp":  envelope.Timestamp,
	})
}
