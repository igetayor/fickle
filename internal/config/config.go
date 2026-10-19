package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all server configuration loaded from environment or defaults.
type Config struct {
	Port            string        `json:"port"`
	NodeID          string        `json:"node_id"`
	RedisAddr       string        `json:"redis_addr"`
	RedisPassword   string        `json:"redis_password"`
	RedisChannel    string        `json:"redis_channel"`
	LogLevel        string        `json:"log_level"`
	
	// WebSocket tuning
	WriteWait       time.Duration `json:"write_wait"`       // Time allowed to write a message to peer
	PongWait        time.Duration `json:"pong_wait"`        // Time allowed to read next pong message from peer
	PingPeriod      time.Duration `json:"ping_period"`      // Send pings to peer with this period (must be < pong_wait)
	MaxMessageSize  int64         `json:"max_message_size"` // Maximum message size allowed from peer
	ReadBufferSize  int           `json:"read_buffer_size"`
	WriteBufferSize int           `json:"write_buffer_size"`
	SendQueueCap    int           `json:"send_queue_cap"`   // Client outbound channel capacity
}

// LoadFromEnv loads configuration parameters with sensible production defaults.
func LoadFromEnv() *Config {
	port := getEnv("PORT", "8080")
	if port[0] != ':' && port != "" {
		port = ":" + port
	}

	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "node-alpha"
	}
	nodeID := getEnv("NODE_ID", "fickle-"+hostname)

	sendQueueCap := getEnvInt("SEND_QUEUE_CAP", 512)
	readBuf := getEnvInt("READ_BUFFER_SIZE", 4096)
	writeBuf := getEnvInt("WRITE_BUFFER_SIZE", 4096)
	maxMsgSize := int64(getEnvInt("MAX_MESSAGE_SIZE", 65536)) // 64KB

	return &Config{
		Port:            port,
		NodeID:          nodeID,
		RedisAddr:       getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:   getEnv("REDIS_PASSWORD", ""),
		RedisChannel:    getEnv("REDIS_CHANNEL", "fickle:pubsub:broadcast"),
		LogLevel:        getEnv("LOG_LEVEL", "INFO"),
		WriteWait:       10 * time.Second,
		PongWait:        60 * time.Second,
		PingPeriod:      (60 * time.Second * 9) / 10, // 54 seconds
		MaxMessageSize:  maxMsgSize,
		ReadBufferSize:  readBuf,
		WriteBufferSize: writeBuf,
		SendQueueCap:    sendQueueCap,
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}
