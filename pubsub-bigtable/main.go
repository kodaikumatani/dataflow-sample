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
	bigtableProject   = flag.String("bigtable_project", "", "Bigtable project ID")
	bigtableInstance  = flag.String("bigtable_instance", "", "Bigtable instance ID")
	bigtableTable     = flag.String("bigtable_table", "", "Bigtable table name")
	windowSize        = flag.Int("window_size", 10, "Fixed window size in seconds")
)

func main() {
	flag.Parse()
	beam.Init()

	p, s := beam.NewPipelineWithRoot()

	// Read from Pub/Sub.
	messages := pubsubio.Read(s, *bigtableProject, pubsubio.ReadOptions{
		Subscription: *inputSubscription,
	})

	// Convert JSON messages to KV<string, Mutation>.
	kvMutations := beam.ParDo(s.Scope("ConvertToMutation"), &convertToMutationFn{}, messages)

	// Apply fixed windowing.
	windowed := beam.WindowInto(s.Scope("ApplyWindow"), window.NewFixedWindows(time.Duration(*windowSize)*time.Second), kvMutations)

	// Group mutations per window.
	grouped := beam.GroupByKey(s.Scope("GroupMutations"), windowed)

	// Batch write to Bigtable.
	beam.ParDo0(s.Scope("WriteToBigtable"), &batchWriteFn{
		Project:  *bigtableProject,
		Instance: *bigtableInstance,
		Table:    *bigtableTable,
	}, grouped)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute pipeline: %v", err)
	}
}
