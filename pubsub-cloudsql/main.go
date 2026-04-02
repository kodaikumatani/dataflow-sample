package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/pubsubio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	inputSubscription = flag.String("input_subscription", "", "Pub/Sub subscription to read from")
	project           = flag.String("project", "", "GCP project ID")
	cloudsqlInstance  = flag.String("cloudsql_instance", "", "Cloud SQL instance connection name (project:region:instance)")
	dbName            = flag.String("db_name", "", "Database name")
	dbUser            = flag.String("db_user", "", "Database user")
	dbPassword        = flag.String("db_password", "", "Database password")
	windowSize        = flag.Int("window_size", 10, "Fixed window size in seconds")
)

func main() {
	flag.Parse()
	beam.Init()

	p, s := beam.NewPipelineWithRoot()

	// Read from Pub/Sub.
	messages := pubsubio.Read(s, *project, pubsubio.ReadOptions{
		Subscription: *inputSubscription,
	})

	// Parse JSON messages to KV<id, Location>.
	kvLocations := beam.ParDo(s.Scope("ParseMessage"), &parseMessageFn{}, messages)

	// Apply fixed windowing.
	windowed := beam.WindowInto(s.Scope("ApplyWindow"), window.NewFixedWindows(time.Duration(*windowSize)*time.Second), kvLocations)

	// Group locations by ID per window.
	grouped := beam.GroupByKey(s.Scope("GroupByID"), windowed)

	// Calculate distance and upsert to Cloud SQL.
	beam.ParDo0(s.Scope("WriteToCloudSQL"), &batchWriteFn{
		Instance: *cloudsqlInstance,
		DBName:   *dbName,
		User:     *dbUser,
		Password: *dbPassword,
	}, grouped)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute pipeline: %v", err)
	}
}
