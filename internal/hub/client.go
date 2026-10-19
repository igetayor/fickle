package hub

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/igetayor/fickle/internal/logger"
	"github.com/igetayor/fickle/internal/models"
)

// Client represents a single concurrent WebSocket client connection.
type Client struct {
	hub       *Hub
	conn      *websocket.Conn
	send      chan []byte
	session   *models.SessionMetadata
	rooms     map[string]bool
	roomsMu   sync.RWMutex
	closeOnce sync.Once
	closed    bool
}

// NewClient constructs a new Client session.
func NewClient(hub *Hub, conn *websocket.Conn, nickname, remoteAddr, userAgent string) *Client {
	clientID := uuid.New().String()
	sessionID := uuid.New().String()
	if nickname == "" {
		nickname = "user-" + clientID[:8]
	}

	return &Client{
		hub:  hub,
		conn: conn,
		send: make(chan []byte, hub.cfg.SendQueueCap),
		session: &models.SessionMetadata{
			ClientID:    clientID,
			SessionID:   sessionID,
			Nickname:    nickname,
			RemoteAddr:  remoteAddr,
			UserAgent:   userAgent,
			ConnectedAt: time.Now(),
			ActiveRooms: make([]string, 0),
		},
		rooms: make(map[string]bool),
	}
}

// ID returns the unique client identifier.
func (c *Client) ID() string {
	return c.session.ClientID
}

// SessionID returns the unique session ID.
func (c *Client) SessionID() string {
	return c.session.SessionID
}

// Nickname returns the current nickname.
func (c *Client) Nickname() string {
	c.roomsMu.RLock()
	defer c.roomsMu.RUnlock()
	return c.session.Nickname
}

// SetNickname updates the client's display name.
func (c *Client) SetNickname(nick string) {
	c.roomsMu.Lock()
	c.session.Nickname = nick
	c.roomsMu.Unlock()
}

// AddRoom tracks that the client joined a room.
func (c *Client) AddRoom(room string) {
	c.roomsMu.Lock()
	defer c.roomsMu.Unlock()
	c.rooms[room] = true
	c.refreshActiveRoomsList()
}

// RemoveRoom tracks that the client left a room.
func (c *Client) RemoveRoom(room string) {
	c.roomsMu.Lock()
	defer c.roomsMu.Unlock()
	delete(c.rooms, room)
	c.refreshActiveRoomsList()
}

func (c *Client) refreshActiveRoomsList() {
	roomList := make([]string, 0, len(c.rooms))
	for r := range c.rooms {
		roomList = append(roomList, r)
	}
	c.session.ActiveRooms = roomList
}

// IsSubscribed returns true if the client is subscribed to room.
func (c *Client) IsSubscribed(room string) bool {
	c.roomsMu.RLock()
	defer c.roomsMu.RUnlock()
	return c.rooms[room]
}

// SendJSON marshals an OutboundEnvelope to JSON and attempts a non-blocking queueing.
func (c *Client) SendJSON(envelope *models.OutboundEnvelope) bool {
	raw, err := json.Marshal(envelope)
	if err != nil {
		logger.Get().Error("Failed to marshal outbound envelope", slog.String("error", err.Error()))
		return false
	}
	return c.SendRaw(raw)
}

// SendRaw enqueues raw byte message into client send queue.
// If the buffer is full, it drops the message to protect against slow consumers (backpressure).
func (c *Client) SendRaw(msg []byte) bool {
	c.roomsMu.RLock()
	if c.closed {
		c.roomsMu.RUnlock()
		return false
	}
	c.roomsMu.RUnlock()

	select {
	case c.send <- msg:
		return true
	default:
		// Send buffer overflow - client cannot keep up
		logger.Get().Warn("Client send buffer full; dropping frame",
			slog.String("client_id", c.session.ClientID),
			slog.Int("buffer_cap", cap(c.send)))
		return false
	}
}

// ReadFromSendQueueNonBlocking attempts to read a message from the outbound queue without blocking (useful for test assertions).
func (c *Client) ReadFromSendQueueNonBlocking() []byte {
	select {
	case msg := <-c.send:
		return msg
	default:
		return nil
	}
}

// Close gracefully closes the client channel and connection.
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		c.roomsMu.Lock()
		c.closed = true
		c.roomsMu.Unlock()

		close(c.send)
		if c.conn != nil {
			_ = c.conn.Close()
		}
	})
}

// ReadPump continuously pulls incoming frames from the WebSocket connection.
func (c *Client) ReadPump() {
	defer func() {
		c.hub.unregister <- c
		c.Close()
	}()

	c.conn.SetReadLimit(c.hub.cfg.MaxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(c.hub.cfg.PongWait))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(c.hub.cfg.PongWait))
		return nil
	})

	for {
		_, rawMsg, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				logger.Get().Warn("WebSocket read unexpected close",
					slog.String("client_id", c.session.ClientID),
					slog.String("error", err.Error()))
			}
			break
		}

		c.session.MessagesRecv++
		var inbound models.InboundPayload
		if err := json.Unmarshal(rawMsg, &inbound); err != nil {
			// Reply with error frame
			c.SendJSON(&models.OutboundEnvelope{
				Type:      models.TypeError,
				Action:    "parse_error",
				Data:      map[string]string{"error": "Invalid JSON frame payload"},
				TraceID:   uuid.New().String(),
				NodeID:    c.hub.nodeID,
				Timestamp: time.Now().UnixMilli(),
			})
			continue
		}

		if inbound.TraceID == "" {
			inbound.TraceID = uuid.New().String()
		}

		ctx := logger.WithTraceID(context.Background(), inbound.TraceID)
		c.hub.HandleInbound(ctx, c, inbound)
	}
}

// WritePump pumps outbound frames from the hub to the WebSocket connection.
// It uses message batching to coalesce multiple pending frames into network writes.
func (c *Client) WritePump() {
	ticker := time.NewTicker(c.hub.cfg.PingPeriod)
	defer func() {
		ticker.Stop()
		c.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.hub.cfg.WriteWait))
			if !ok {
				// The hub closed the channel
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			if _, err := w.Write(msg); err != nil {
				return
			}
			c.session.MessagesSent++

			// Coalesce queued messages waiting in buffer to reduce network syscalls
			n := len(c.send)
			for i := 0; i < n; i++ {
				nextMsg := <-c.send
				if _, err := w.Write([]byte{'\n'}); err != nil {
					return
				}
				if _, err := w.Write(nextMsg); err != nil {
					return
				}
				c.session.MessagesSent++
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.hub.cfg.WriteWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
