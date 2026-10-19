package redis

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/igetayor/fickle/internal/config"
	"github.com/igetayor/fickle/internal/logger"
	"github.com/igetayor/fickle/internal/models"
	goredis "github.com/redis/go-redis/v9"
)

// PubSubBackend represents a multi-instance message synchronization interface.
type PubSubBackend interface {
	Publish(ctx context.Context, clusterMsg *models.ClusterMessage) error
	Subscribe(ctx context.Context, onMessage func(clusterMsg *models.ClusterMessage)) error
	IsConnected() bool
	Close() error
}

// RedisBackend implements PubSubBackend over Redis Pub/Sub.
type RedisBackend struct {
	client    *goredis.Client
	cfg       *config.Config
	connected atomic.Bool
	closed    atomic.Bool
	pubsub    *goredis.PubSub
	mu        sync.RWMutex
}

// NewRedisBackend initializes a new Redis Pub/Sub client with automatic reconnect resilience.
func NewRedisBackend(cfg *config.Config) *RedisBackend {
	rdb := goredis.NewClient(&goredis.Options{
		Addr:         cfg.RedisAddr,
		Password:     cfg.RedisPassword,
		DB:           0,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     20,
	})

	rb := &RedisBackend{
		client: rdb,
		cfg:    cfg,
	}

	// Verify initial connection with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		logger.Get().Warn("Redis server not reachable at startup; running in isolated fallback mode",
			slog.String("addr", cfg.RedisAddr),
			slog.String("error", err.Error()))
		rb.connected.Store(false)
	} else {
		logger.Get().Info("Successfully connected to Redis cluster backend",
			slog.String("addr", cfg.RedisAddr))
		rb.connected.Store(true)
	}

	return rb
}

// IsConnected returns whether Redis is currently responsive.
func (r *RedisBackend) IsConnected() bool {
	return r.connected.Load()
}

// Publish distributes a cluster message to other server nodes via Redis.
func (r *RedisBackend) Publish(ctx context.Context, clusterMsg *models.ClusterMessage) error {
	if !r.connected.Load() {
		return errors.New("redis pubsub not connected")
	}

	payload, err := json.Marshal(clusterMsg)
	if err != nil {
		return err
	}

	err = r.client.Publish(ctx, r.cfg.RedisChannel, payload).Err()
	if err != nil {
		logger.FromContext(ctx).Error("Failed to publish to Redis Pub/Sub",
			slog.String("channel", r.cfg.RedisChannel),
			slog.String("error", err.Error()))
		return err
	}

	return nil
}

// Subscribe listens on the Redis Pub/Sub channel and dispatches messages to the hub.
func (r *RedisBackend) Subscribe(ctx context.Context, onMessage func(clusterMsg *models.ClusterMessage)) error {
	go func() {
		for {
			if r.closed.Load() {
				return
			}

			// If not connected, test ping and backoff
			if !r.connected.Load() {
				pingCtx, pingCancel := context.WithTimeout(context.Background(), 2*time.Second)
				if err := r.client.Ping(pingCtx).Err(); err == nil {
					logger.Get().Info("Reconnected to Redis Pub/Sub backend", slog.String("addr", r.cfg.RedisAddr))
					r.connected.Store(true)
				}
				pingCancel()

				if !r.connected.Load() {
					select {
					case <-ctx.Done():
						return
					case <-time.After(3 * time.Second):
						continue
					}
				}
			}

			r.mu.Lock()
			r.pubsub = r.client.Subscribe(ctx, r.cfg.RedisChannel)
			r.mu.Unlock()

			ch := r.pubsub.Channel()
			logger.Get().Info("Listening for cluster synchronization events",
				slog.String("channel", r.cfg.RedisChannel))

			for msg := range ch {
				var clusterMsg models.ClusterMessage
				if err := json.Unmarshal([]byte(msg.Payload), &clusterMsg); err != nil {
					logger.Get().Error("Malformed cluster message received from Redis",
						slog.String("error", err.Error()))
					continue
				}

				onMessage(&clusterMsg)
			}

			// If channel closed, mark disconnected and retry
			r.connected.Store(false)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	}()

	return nil
}

// Close gracefully terminates Redis subscription and client connection.
func (r *RedisBackend) Close() error {
	r.closed.Store(true)
	r.mu.Lock()
	if r.pubsub != nil {
		_ = r.pubsub.Close()
	}
	r.mu.Unlock()
	return r.client.Close()
}

// MemoryPubSub is an in-memory PubSub backend used for testing and local single-node deployments.
type MemoryPubSub struct {
	subscribers []func(clusterMsg *models.ClusterMessage)
	mu          sync.RWMutex
}

// NewMemoryPubSub returns an in-memory implementation of PubSubBackend.
func NewMemoryPubSub() *MemoryPubSub {
	return &MemoryPubSub{
		subscribers: make([]func(clusterMsg *models.ClusterMessage), 0),
	}
}

func (m *MemoryPubSub) Publish(ctx context.Context, clusterMsg *models.ClusterMessage) error {
	m.mu.RLock()
	subs := make([]func(clusterMsg *models.ClusterMessage), len(m.subscribers))
	copy(subs, m.subscribers)
	m.mu.RUnlock()

	for _, sub := range subs {
		go sub(clusterMsg)
	}
	return nil
}

func (m *MemoryPubSub) Subscribe(ctx context.Context, onMessage func(clusterMsg *models.ClusterMessage)) error {
	m.mu.Lock()
	m.subscribers = append(m.subscribers, onMessage)
	m.mu.Unlock()
	return nil
}

func (m *MemoryPubSub) IsConnected() bool {
	return true
}

func (m *MemoryPubSub) Close() error {
	m.mu.Lock()
	m.subscribers = nil
	m.mu.Unlock()
	return nil
}
