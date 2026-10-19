package models

import (
	"encoding/json"
	"time"
)

// Standard WebSocket Protocol Actions
const (
	ActionJoin      = "join_room"
	ActionLeave     = "leave_room"
	ActionBroadcast = "broadcast"
	ActionDirect    = "direct_msg"
	ActionPing      = "ping"
	ActionPong      = "pong"
	ActionSystem    = "system"
	ActionTelemetry = "telemetry"
)

// Message types
const (
	TypeAck     = "ack"
	TypeMessage = "message"
	TypeEvent   = "event"
	TypeSystem  = "system"
	TypeError   = "error"
	TypeTick    = "tick"
)

// SenderInfo contains identity information of the client.
type SenderInfo struct {
	ID        string `json:"id"`
	Nickname  string `json:"nickname"`
	SessionID string `json:"session_id"`
}

// InboundPayload represents client-originated WebSocket frames.
type InboundPayload struct {
	Action   string          `json:"action"`              // e.g. "join_room", "broadcast"
	Room     string          `json:"room,omitempty"`      // Target room name
	TargetID string          `json:"target_id,omitempty"` // For direct messages
	Nickname string          `json:"nickname,omitempty"`  // Update client nickname
	Data     json.RawMessage `json:"data,omitempty"`      // Arbitrary JSON payload
	TraceID  string          `json:"trace_id,omitempty"`  // Request tracing correlation ID
}

// OutboundEnvelope represents standardized JSON frames streamed to clients.
type OutboundEnvelope struct {
	Type      string      `json:"type"`                // "ack", "message", "event", "system", "error"
	Action    string      `json:"action,omitempty"`
	Room      string      `json:"room,omitempty"`
	Sender    *SenderInfo `json:"sender,omitempty"`
	Data      any         `json:"data,omitempty"`
	TraceID   string      `json:"trace_id"`
	NodeID    string      `json:"node_id"`
	Timestamp int64       `json:"timestamp"`
}

// ClusterMessage wraps messages dispatched over Redis Pub/Sub between server instances.
type ClusterMessage struct {
	OriginNodeID string           `json:"origin_node_id"`
	Room         string           `json:"room"`
	Payload      OutboundEnvelope `json:"payload"`
	TraceID      string           `json:"trace_id"`
	Timestamp    int64            `json:"timestamp"`
}

// SessionMetadata represents in-memory session metadata for a client.
type SessionMetadata struct {
	ClientID     string    `json:"client_id"`
	SessionID    string    `json:"session_id"`
	Nickname     string    `json:"nickname"`
	RemoteAddr   string    `json:"remote_addr"`
	UserAgent    string    `json:"user_agent"`
	ConnectedAt  time.Time `json:"connected_at"`
	ActiveRooms  []string  `json:"active_rooms"`
	MessagesSent int64     `json:"messages_sent"`
	MessagesRecv int64     `json:"messages_recv"`
}

// RoomInfo provides room statistics.
type RoomInfo struct {
	Name        string `json:"name"`
	ClientCount int    `json:"client_count"`
}

// ServerMetrics holds live server operational telemetry.
type ServerMetrics struct {
	NodeID            string     `json:"node_id"`
	UptimeSeconds     int64      `json:"uptime_seconds"`
	ActiveConnections int        `json:"active_connections"`
	TotalSessions     int64      `json:"total_sessions"`
	ActiveRoomsCount  int        `json:"active_rooms_count"`
	TotalMessagesIn   int64      `json:"total_messages_in"`
	TotalMessagesOut  int64      `json:"total_messages_out"`
	RedisConnected    bool       `json:"redis_connected"`
	Rooms             []RoomInfo `json:"rooms,omitempty"`
}

// TelemetryTick represents simulated high-throughput market tick or sensor telemetry.
type TelemetryTick struct {
	Symbol    string  `json:"symbol"`
	Price     float64 `json:"price"`
	Change24h float64 `json:"change_24h"`
	Volume    float64 `json:"volume"`
	Bid       float64 `json:"bid"`
	Ask       float64 `json:"ask"`
	Timestamp int64   `json:"timestamp"`
}
