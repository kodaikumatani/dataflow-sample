package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/pubsub"
	pb "github.com/kodai-kumatani/dataflow-sample/pkg/pb/location"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	ctx := context.Background()
	projectID := "apache-beam-test-482413"
	topicID := "location"

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("pubsub.NewClient: %v", err)
	}
	defer client.Close()

	now := time.Now()

	msg := &pb.Locations{
		Items: []*pb.Location{
			{
				UserId:    "user-001",
				WalkingId: "walk-001",
				Timestamp: timestamppb.New(now),
				Lat:       35.681236,
				Lng:       139.767125,
				Altitude:  40.0,
				Accuracy:  5.0,
				Speed:     1.2,
			},
			{
				UserId:    "user-001",
				WalkingId: "walk-001",
				Timestamp: timestamppb.New(now.Add(time.Second)),
				Lat:       35.681300,
				Lng:       139.767200,
				Altitude:  40.5,
				Accuracy:  4.8,
				Speed:     1.3,
			},
		},
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		log.Fatalf("proto.Marshal: %v", err)
	}

	topic := client.Topic(topicID)
	result := topic.Publish(ctx, &pubsub.Message{Data: data})

	id, err := result.Get(ctx)
	if err != nil {
		log.Fatalf("publish: %v", err)
	}

	fmt.Printf("Published message ID: %s (%d bytes, %d locations)\n", id, len(data), len(msg.Items))
}
