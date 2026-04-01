package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/google/uuid"
)

// Mutation represents a single Bigtable cell mutation.
type Mutation struct {
	Key             string `json:"key"`
	FamilyName      string `json:"family_name"`
	ColumnQualifier string `json:"column_qualifier"`
	Value           []byte `json:"value"`
	TimestampMicros int64  `json:"timestamp_micros"`
}

func init() {
	register.DoFn3x0(&convertToMutationFn{})
	register.Emitter2[string, Mutation]()
}

// convertToMutationFn converts a JSON message into individual Bigtable mutations.
type convertToMutationFn struct{}

func (fn *convertToMutationFn) ProcessElement(ctx context.Context, msg []byte, emit func(string, Mutation)) {
	var data map[string]any
	if err := json.Unmarshal(msg, &data); err != nil {
		log.Printf("Failed to unmarshal message: %v", err)
		return
	}

	rowKey := uuid.New().String()
	timestampMicros := time.Now().UnixMicro()

	for key, value := range data {
		emit("batch", Mutation{
			Key:             rowKey,
			FamilyName:      "cf1",
			ColumnQualifier: key,
			Value:           fmt.Appendf(nil, "%v", value),
			TimestampMicros: timestampMicros,
		})
	}
}
