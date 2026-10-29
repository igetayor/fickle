package api

import (
	"context"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/igetayor/fickle/internal/hub"
	"github.com/igetayor/fickle/internal/models"
)

// StartTelemetryStream periodically publishes synthetic financial/telemetry ticks
// to room "crypto-ticks" and "telemetry" to demonstrate real-time streaming throughput.
func StartTelemetryStream(ctx context.Context, h *hub.Hub, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		symbols := []string{"BTC/USDT", "ETH/USDT", "SOL/USDT"}
		basePrices := map[string]float64{
			"BTC/USDT": 96500.0,
			"ETH/USDT": 3450.0,
			"SOL/USDT": 195.0,
		}

		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				sym := symbols[rand.Intn(len(symbols))]
				drift := (rand.Float64() - 0.495) * (basePrices[sym] * 0.002)
				basePrices[sym] += drift
				price := basePrices[sym]
				spread := price * 0.0002

				tick := models.TelemetryTick{
					Symbol:    sym,
					Price:     price,
					Change24h: 2.45 + (rand.Float64()-0.5)*0.1,
					Volume:    float64(rand.Intn(500000) + 100000),
					Bid:       price - spread/2,
					Ask:       price + spread/2,
					Timestamp: t.UnixMilli(),
				}

				envelope := models.OutboundEnvelope{
					Type:      models.TypeTick,
					Action:    models.ActionTelemetry,
					Room:      "crypto-ticks",
					Data:      tick,
					TraceID:   uuid.New().String(),
					NodeID:    h.GetMetrics().NodeID,
					Timestamp: t.UnixMilli(),
				}

				// Broadcast to room "crypto-ticks" (local + sync cluster)
				h.BroadcastToRoom(ctx, "crypto-ticks", envelope, nil, true)
			}
		}
	}()
}
