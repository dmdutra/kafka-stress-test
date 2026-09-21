package generator

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	eventTypes = []string{
		"order_created",
		"payment_processed",
		"user_authenticated",
		"item_viewed",
		"cart_updated",
		"order_delivered",
		"refund_requested",
	}

	currencies = []string{"BRL", "USD", "EUR"}
	statuses   = []string{"SUCCESS", "PENDING", "PROCESSING", "FAILED"}
	devices    = []string{"ios", "android", "web", "api"}
)

// RandomEvent represents a structured JSON message for stress testing.
type RandomEvent struct {
	ID        string         `json:"id"`
	Timestamp int64          `json:"timestamp"`
	ISOTime   string         `json:"iso_time"`
	Sequence  int64          `json:"sequence"`
	UserID    string         `json:"user_id"`
	EventType string         `json:"event_type"`
	Amount    float64        `json:"amount"`
	Currency  string         `json:"currency"`
	Status    string         `json:"status"`
	Metadata  EventMetadata  `json:"metadata"`
	Padding   string         `json:"padding,omitempty"`
}

// EventMetadata contains nested attributes for realistic complexity.
type EventMetadata struct {
	Device   string `json:"device"`
	ClientIP string `json:"client_ip"`
	Version  string `json:"version"`
}

// RandomJSONGenerator generates randomized JSON events.
type RandomJSONGenerator struct {
	paddingSize int
}

// NewRandomJSONGenerator creates a new RandomJSONGenerator.
func NewRandomJSONGenerator(paddingSize int) *RandomJSONGenerator {
	return &RandomJSONGenerator{
		paddingSize: paddingSize,
	}
}

// Generate creates a randomized JSON message as a byte slice.
func (g *RandomJSONGenerator) Generate(counter int64) ([]byte, error) {
	now := time.Now().UTC()
	userID := fmt.Sprintf("usr_%05d", rand.IntN(100000))
	eventType := eventTypes[rand.IntN(len(eventTypes))]
	currency := currencies[rand.IntN(len(currencies))]
	status := statuses[rand.IntN(len(statuses))]
	device := devices[rand.IntN(len(devices))]

	amount := float64(rand.IntN(100000)) / 100.0 // e.g. 0.00 to 999.99
	clientIP := fmt.Sprintf("192.168.%d.%d", rand.IntN(255), rand.IntN(255))

	var padding string
	if g.paddingSize > 0 {
		padding = strings.Repeat("x", g.paddingSize)
	}

	event := RandomEvent{
		ID:        uuid.NewString(),
		Timestamp: now.UnixMilli(),
		ISOTime:   now.Format(time.RFC3339Nano),
		Sequence:  counter,
		UserID:    userID,
		EventType: eventType,
		Amount:    amount,
		Currency:  currency,
		Status:    status,
		Metadata: EventMetadata{
			Device:   device,
			ClientIP: clientIP,
			Version:  "1.0.0",
		},
		Padding: padding,
	}

	return json.Marshal(event)
}
