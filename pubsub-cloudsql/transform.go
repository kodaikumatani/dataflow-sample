package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

// Location represents a single location update message.
type Location struct {
	ID        string  `json:"id"`
	Lat       float64 `json:"lat"`
	Lng       float64 `json:"lng"`
	Timestamp int64   `json:"timestamp"`
}

func init() {
	register.DoFn3x0(&parseMessageFn{})
	register.Emitter2[string, Location]()
}

// parseMessageFn parses a JSON message into a Location keyed by ID.
type parseMessageFn struct{}

func (fn *parseMessageFn) ProcessElement(ctx context.Context, msg []byte, emit func(string, Location)) {
	var loc Location
	if err := json.Unmarshal(msg, &loc); err != nil {
		log.Printf("Failed to unmarshal message: %v", err)
		return
	}
	emit(loc.ID, loc)
}
