// Package services contains domain orchestration for rooms, clients, and broadcasting.
package services

import (
	"context"

	"github.com/igetayor/fickle/internal/hub"
	"github.com/igetayor/fickle/internal/models"
)

// RoomService coordinates room state, user presence, and cross-node broadcasting.
type RoomService struct {
	hub *hub.Hub
}

// NewRoomService creates a new RoomService instance.
func NewRoomService(h *hub.Hub) *RoomService {
	return &RoomService{hub: h}
}

// ListRooms retrieves active rooms and their user counts.
func (s *RoomService) ListRooms() map[string]int {
	stats := s.hub.GetRoomStats()
	res := make(map[string]int, len(stats))
	for _, r := range stats {
		res[r.Name] = r.ClientCount
	}
	return res
}

// Broadcast sends a server-initiated announcement or event to a specific room or global.
func (s *RoomService) Broadcast(ctx context.Context, room string, envelope models.OutboundEnvelope) {
	s.hub.BroadcastToRoom(ctx, room, envelope, nil, true)
}

// GetMetrics returns real-time hub statistics.
func (s *RoomService) GetMetrics() models.ServerMetrics {
	return s.hub.GetMetrics()
}
