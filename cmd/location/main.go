package main

import (
	"context"
	"flag"
	"log"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/pubsubio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/kodai-kumatani/dataflow-sample/internal/bigtable"
)

var (
	inputSubscription = flag.String("input_subscription", "", "Pub/Sub subscription to read from")
	bigtableProject   = flag.String("bigtable_project", "", "Bigtable project ID")
	bigtableInstance  = flag.String("bigtable_instance", "", "Bigtable instance ID")
	bigtableTable     = flag.String("bigtable_table", "", "Bigtable table name")
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

	// Batch write to Bigtable.
	beam.ParDo0(s.Scope("WriteToBigtable"), &bigtable.Writer{
		Project:  *bigtableProject,
		Instance: *bigtableInstance,
		Table:    *bigtableTable,
	}, kvMutations)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute pipeline: %v", err)
	}
}
