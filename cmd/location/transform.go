package main

import (
	"context"
	"log"
	"math"
	"strconv"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/go-playground/validator/v10"
	bt "github.com/kodai-kumatani/dataflow-sample/internal/bigtable"
	pb "github.com/kodai-kumatani/dataflow-sample/pkg/pb/location"
	"google.golang.org/protobuf/proto"
)

func init() {
	register.DoFn3x0(&convertToMutationFn{})
	register.Emitter2[string, bt.Mutation]()
}

type convertToMutationFn struct{}

func (fn *convertToMutationFn) ProcessElement(ctx context.Context, msg []byte, emit func(string, bt.Mutation)) {
	var locs pb.Locations
	if err := proto.Unmarshal(msg, &locs); err != nil {
		log.Printf("proto.Unmarshal Locations: %v", err)
		return
	}

	for _, loc := range locs.GetItems() {
		request := struct {
			UserID    string    `validate:"required"`
			WakingID  string    `validate:"required"`
			Timestamp time.Time `validate:"required"`
		}{
			UserID:    loc.GetUserId(),
			WakingID:  loc.GetWalkingId(),
			Timestamp: loc.GetTimestamp().AsTime(),
		}

		if err := validator.New().Struct(request); err != nil {
			continue
		}

		bytes, err := proto.Marshal(loc)
		if err != nil {
			continue
		}

		rowKey := newRowKey(request.UserID, request.WakingID, request.Timestamp)
		emit(rowKey, bt.Mutation{
			Ops: []bt.Operation{
				{Family: "measurements", Column: "data", Value: bytes},
			},
		})
	}
}

func newRowKey(userID, wakingID string, timestamp time.Time) string {
	reverseTimestamp := math.MaxInt64 - timestamp.Unix()

	return userID + "#" + wakingID + "#" + strconv.FormatInt(reverseTimestamp, 10)
}
