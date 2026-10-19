package hub_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/igetayor/fickle/internal/config"
	"github.com/igetayor/fickle/internal/hub"
	"github.com/igetayor/fickle/internal/models"
	"github.com/igetayor/fickle/internal/redis"
)

func createTestConfig(nodeID string) *config.Config {
	return &config.Config{
		Port:            ":8080",
		NodeID:          nodeID,
		RedisAddr:       "localhost:6379",
		RedisChannel:    "test:pubsub:broadcast",
		LogLevel:        "ERROR",
		WriteWait:       5 * time.Second,
		PongWait:        10 * time.Second,
		PingPeriod:      9 * time.Second,
		MaxMessageSize:  65536,
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		SendQueueCap:    256,
	}
}

// TestHub_ClientRegistration tests adding and removing a client session from the hub.
func TestHub_ClientRegistration(t *testing.T) {
	cfg := createTestConfig("test-node-1")
	memPubSub := redis.NewMemoryPubSub()
	h := hub.NewHub(cfg, memPubSub)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	client := hub.NewClient(h, nil, "Alice", "127.0.0.1:5000", "Go-Test-Agent")
	h.RegisterChannel() <- client

	// Wait for registration
	time.Sleep(50 * time.Millisecond)

	metrics := h.GetMetrics()
	if metrics.ActiveConnections != 1 {
		t.Fatalf("Expected 1 active connection, got %d", metrics.ActiveConnections)
	}

	session, exists := h.GetSession(client.ID())
	if !exists || session == nil {
		t.Fatalf("Expected session for client %s to exist", client.ID())
	}
	if session.Nickname != "Alice" {
		t.Fatalf("Expected nickname 'Alice', got '%s'", session.Nickname)
	}

	// Unregister
	h.UnregisterChannel() <- client
	time.Sleep(50 * time.Millisecond)

	metricsAfter := h.GetMetrics()
	if metricsAfter.ActiveConnections != 0 {
		t.Fatalf("Expected 0 active connections after unregister, got %d", metricsAfter.ActiveConnections)
	}
}

// TestHub_RoomJoinLeave tests thread-safe room subscription tracking.
func TestHub_RoomJoinLeave(t *testing.T) {
	cfg := createTestConfig("test-node-1")
	memPubSub := redis.NewMemoryPubSub()
	h := hub.NewHub(cfg, memPubSub)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	client := hub.NewClient(h, nil, "Bob", "127.0.0.1:5001", "Go-Test-Agent")
	h.RegisterChannel() <- client
	time.Sleep(30 * time.Millisecond)

	// Join room
	h.JoinRoom(client, "crypto-ticks")
	if !client.IsSubscribed("crypto-ticks") {
		t.Fatalf("Expected client to be subscribed to 'crypto-ticks'")
	}

	rooms := h.GetRoomStats()
	if len(rooms) != 1 || rooms[0].Name != "crypto-ticks" || rooms[0].ClientCount != 1 {
		t.Fatalf("Unexpected room stats: %+v", rooms)
	}

	// Leave room
	h.LeaveRoom(client, "crypto-ticks")
	if client.IsSubscribed("crypto-ticks") {
		t.Fatalf("Expected client to no longer be subscribed to 'crypto-ticks'")
	}

	roomsAfter := h.GetRoomStats()
	if len(roomsAfter) != 0 {
		t.Fatalf("Expected room to be automatically pruned when empty, found: %+v", roomsAfter)
	}
}

// TestHub_BroadcastToRoom tests room-isolated message fanout.
func TestHub_BroadcastToRoom(t *testing.T) {
	cfg := createTestConfig("test-node-1")
	memPubSub := redis.NewMemoryPubSub()
	h := hub.NewHub(cfg, memPubSub)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	clientA := hub.NewClient(h, nil, "Sender", "127.0.0.1:5002", "Go-Test-Agent")
	clientB := hub.NewClient(h, nil, "Receiver1", "127.0.0.1:5003", "Go-Test-Agent")
	clientC := hub.NewClient(h, nil, "Receiver2", "127.0.0.1:5004", "Go-Test-Agent")

	h.RegisterChannel() <- clientA
	h.RegisterChannel() <- clientB
	h.RegisterChannel() <- clientC
	time.Sleep(50 * time.Millisecond)

	// Drain initial handshake acks
	drainChannel(clientA)
	drainChannel(clientB)
	drainChannel(clientC)

	// Subscribe clientA and clientB to "general", clientC to "random"
	h.JoinRoom(clientA, "general")
	h.JoinRoom(clientB, "general")
	h.JoinRoom(clientC, "random")

	envelope := models.OutboundEnvelope{
		Type:      models.TypeMessage,
		Action:    models.ActionBroadcast,
		Room:      "general",
		Data:      map[string]string{"text": "Hello general room"},
		TraceID:   uuid.New().String(),
		NodeID:    cfg.NodeID,
		Timestamp: time.Now().UnixMilli(),
	}

	// Broadcast to "general", skipping clientA (the sender)
	h.BroadcastToRoom(ctx, "general", envelope, clientA, false)

	// Client B should receive the message
	receivedB := readWithTimeout(clientB, 500*time.Millisecond)
	if receivedB == nil {
		t.Fatalf("Expected Client B to receive broadcast in 'general' room")
	}

	var envB models.OutboundEnvelope
	if err := json.Unmarshal(receivedB, &envB); err != nil {
		t.Fatalf("Failed to parse envelope: %v", err)
	}
	if envB.Room != "general" {
		t.Fatalf("Expected room 'general', got '%s'", envB.Room)
	}

	// Client A (sender) should NOT receive it
	if receivedA := readWithTimeout(clientA, 100*time.Millisecond); receivedA != nil {
		t.Fatalf("Expected sender Client A to be skipped, but received frame")
	}

	// Client C (in different room) should NOT receive it
	if receivedC := readWithTimeout(clientC, 100*time.Millisecond); receivedC != nil {
		t.Fatalf("Expected Client C in 'random' room to NOT receive frame")
	}
}

// TestHub_DirectMessage tests point-to-point delivery using ClientID / SessionID.
func TestHub_DirectMessage(t *testing.T) {
	cfg := createTestConfig("test-node-1")
	memPubSub := redis.NewMemoryPubSub()
	h := hub.NewHub(cfg, memPubSub)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	clientA := hub.NewClient(h, nil, "Sender", "127.0.0.1:5005", "Go-Test-Agent")
	clientB := hub.NewClient(h, nil, "Target", "127.0.0.1:5006", "Go-Test-Agent")

	h.RegisterChannel() <- clientA
	h.RegisterChannel() <- clientB
	time.Sleep(50 * time.Millisecond)

	drainChannel(clientA)
	drainChannel(clientB)

	envelope := models.OutboundEnvelope{
		Type:      models.TypeMessage,
		Action:    models.ActionDirect,
		Data:      map[string]string{"secret": "direct-payload-42"},
		TraceID:   uuid.New().String(),
		NodeID:    cfg.NodeID,
		Timestamp: time.Now().UnixMilli(),
	}

	// Send to Client B's SessionID
	delivered := h.SendDirect(clientB.SessionID(), envelope)
	if !delivered {
		t.Fatalf("Expected SendDirect to successfully deliver to Client B")
	}

	received := readWithTimeout(clientB, 500*time.Millisecond)
	if received == nil {
		t.Fatalf("Target did not receive direct message")
	}

	// Send to invalid ID
	invalidDelivered := h.SendDirect("invalid-non-existent-id", envelope)
	if invalidDelivered {
		t.Fatalf("Expected delivery to non-existent target to return false")
	}
}

// TestHub_ConcurrentHighThroughput stress tests concurrent broadcasts across 20 goroutines.
func TestHub_ConcurrentHighThroughput(t *testing.T) {
	cfg := createTestConfig("test-node-perf")
	memPubSub := redis.NewMemoryPubSub()
	h := hub.NewHub(cfg, memPubSub)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	const clientCount = 50
	clients := make([]*hub.Client, clientCount)
	for i := 0; i < clientCount; i++ {
		c := hub.NewClient(h, nil, fmt.Sprintf("User-%d", i), "127.0.0.1:0", "Perf-Agent")
		clients[i] = c
		h.RegisterChannel() <- c
		h.JoinRoom(c, "high-throughput-room")
	}

	time.Sleep(100 * time.Millisecond)

	for _, c := range clients {
		drainChannel(c)
	}

	// Launch 20 concurrent publisher goroutines sending 50 messages each (1,000 total broadcasts)
	const publishers = 20
	const msgsPerPublisher = 50
	var wg sync.WaitGroup
	wg.Add(publishers)

	start := time.Now()
	for p := 0; p < publishers; p++ {
		go func(pubID int) {
			defer wg.Done()
			for m := 0; m < msgsPerPublisher; m++ {
				env := models.OutboundEnvelope{
					Type:      models.TypeMessage,
					Action:    models.ActionBroadcast,
					Room:      "high-throughput-room",
					Data:      map[string]int{"publisher": pubID, "seq": m},
					TraceID:   uuid.New().String(),
					NodeID:    cfg.NodeID,
					Timestamp: time.Now().UnixMilli(),
				}
				h.BroadcastToRoom(ctx, "high-throughput-room", env, nil, false)
			}
		}(p)
	}

	wg.Wait()
	duration := time.Since(start)
	t.Logf("Successfully executed %d concurrent broadcasts across %d clients in %v",
		publishers*msgsPerPublisher, clientCount, duration)

	metrics := h.GetMetrics()
	if metrics.TotalMessagesOut == 0 {
		t.Fatalf("Expected TotalMessagesOut to be > 0, got %d", metrics.TotalMessagesOut)
	}
}

// TestMultiNode_RedisPubSubSync tests multi-node horizontal synchronization.
func TestMultiNode_RedisPubSubSync(t *testing.T) {
	sharedPubSub := redis.NewMemoryPubSub()

	cfg1 := createTestConfig("node-alpha")
	h1 := hub.NewHub(cfg1, sharedPubSub)

	cfg2 := createTestConfig("node-beta")
	h2 := hub.NewHub(cfg2, sharedPubSub)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go h1.Run(ctx)
	go h2.Run(ctx)
	time.Sleep(50 * time.Millisecond)

	// Client 1 connects to Node 1 in "cluster-room"
	c1 := hub.NewClient(h1, nil, "Node1-Client", "127.0.0.1:6001", "Agent")
	h1.RegisterChannel() <- c1
	h1.JoinRoom(c1, "cluster-room")

	// Client 2 connects to Node 2 in "cluster-room"
	c2 := hub.NewClient(h2, nil, "Node2-Client", "127.0.0.1:6002", "Agent")
	h2.RegisterChannel() <- c2
	h2.JoinRoom(c2, "cluster-room")

	time.Sleep(50 * time.Millisecond)
	drainChannel(c1)
	drainChannel(c2)

	// Broadcast on Node 1 with syncCluster = true
	traceID := uuid.New().String()
	env := models.OutboundEnvelope{
		Type:      models.TypeMessage,
		Action:    models.ActionBroadcast,
		Room:      "cluster-room",
		Data:      map[string]string{"msg": "Sync across nodes test!"},
		TraceID:   traceID,
		NodeID:    "node-alpha",
		Timestamp: time.Now().UnixMilli(),
	}

	h1.BroadcastToRoom(ctx, "cluster-room", env, c1, true)

	// Client 2 on Node 2 should receive this synced message!
	msg2 := readWithTimeout(c2, 1*time.Second)
	if msg2 == nil {
		t.Fatalf("Node 2 client failed to receive synced cluster message from Node 1")
	}

	var parsed models.OutboundEnvelope
	if err := json.Unmarshal(msg2, &parsed); err != nil {
		t.Fatalf("Failed to parse synced envelope: %v", err)
	}
	if parsed.NodeID != "node-alpha" {
		t.Fatalf("Expected origin NodeID 'node-alpha', got '%s'", parsed.NodeID)
	}
	if parsed.TraceID != traceID {
		t.Fatalf("Expected trace ID '%s', got '%s'", traceID, parsed.TraceID)
	}
}

func drainChannel(c *hub.Client) {
	time.Sleep(10 * time.Millisecond)
	for {
		raw := c.ReadFromSendQueueNonBlocking()
		if raw == nil {
			break
		}
	}
}

func readWithTimeout(c *hub.Client, timeout time.Duration) []byte {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msg := c.ReadFromSendQueueNonBlocking()
		if msg != nil {
			return msg
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil
}
