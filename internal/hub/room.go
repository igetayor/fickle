package hub

import (
	"sync"
)

// Room manages a collection of subscribed clients in a named channel.
type Room struct {
	name    string
	clients map[*Client]bool
	mu      sync.RWMutex
}

// NewRoom creates an empty Room.
func NewRoom(name string) *Room {
	return &Room{
		name:    name,
		clients: make(map[*Client]bool),
	}
}

// Name returns the room's identifier.
func (r *Room) Name() string {
	return r.name
}

// Add attaches a client to this room.
func (r *Room) Add(c *Client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clients[c] = true
	c.AddRoom(r.name)
}

// Remove detaches a client and returns true if the room has become empty.
func (r *Room) Remove(c *Client) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.clients, c)
	c.RemoveRoom(r.name)
	return len(r.clients) == 0
}

// Broadcast distributes a raw byte payload to all clients in this room.
// Optional skipClient avoids echoing to the sender.
func (r *Room) Broadcast(rawMessage []byte, skipClient *Client) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	sentCount := 0
	for client := range r.clients {
		if client == skipClient {
			continue
		}
		if client.SendRaw(rawMessage) {
			sentCount++
		}
	}
	return sentCount
}

// ClientCount returns the number of active subscribers.
func (r *Room) ClientCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.clients)
}

// GetClientIDs returns a snapshot list of client IDs in this room.
func (r *Room) GetClientIDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.clients))
	for c := range r.clients {
		ids = append(ids, c.ID())
	}
	return ids
}
