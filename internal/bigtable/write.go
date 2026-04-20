package bigtable

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/bigtable"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn3x1(&Writer{})
}

// maxBatchSize is a safety cap to avoid exceeding Bigtable's ApplyBulk limits
// (100,000 mutations / 256MB per request). Not a performance tuning knob:
// in streaming, Beam bundles usually finalize well before this is reached.
const maxBatchSize = 1000

// Writer writes mutations to Bigtable in bulk per bundle.
type Writer struct {
	Project  string
	Instance string
	Table    string

	client *bigtable.Client
	table  *bigtable.Table

	mutations []*bigtable.Mutation
	rowKeys   []string
}

func (fn *Writer) Setup(ctx context.Context) error {
	client, err := bigtable.NewClient(ctx, fn.Project, fn.Instance)
	if err != nil {
		return fmt.Errorf("bigtable.NewClient: %w", err)
	}

	fn.client = client
	fn.table = client.Open(fn.Table)

	return nil
}

func (fn *Writer) StartBundle(ctx context.Context) {
	fn.mutations = make([]*bigtable.Mutation, 0, maxBatchSize)
	fn.rowKeys = make([]string, 0, maxBatchSize)
}

func (fn *Writer) ProcessElement(ctx context.Context, key string, m Mutation) error {
	fn.rowKeys = append(fn.rowKeys, key)
	fn.mutations = append(fn.mutations, m.toBigtableMutation())

	if len(fn.mutations) >= maxBatchSize {
		return fn.flush(ctx)
	}

	return nil
}

func (fn *Writer) FinishBundle(ctx context.Context) error {
	return fn.flush(ctx)
}

func (fn *Writer) flush(ctx context.Context) error {
	if len(fn.mutations) == 0 {
		return nil
	}

	errs, err := fn.table.ApplyBulk(ctx, fn.rowKeys, fn.mutations)
	if err != nil {
		return err
	}

	for i, e := range errs {
		if e != nil {
			log.Printf("ApplyBulk error for row %s: %v", fn.rowKeys[i], e)
		}
	}

	// clear slices but retain capacity for the next batch
	fn.mutations = fn.mutations[:0]
	fn.rowKeys = fn.rowKeys[:0]

	return nil
}

func (fn *Writer) Teardown() error {
	if fn.client != nil {
		return fn.client.Close()
	}

	return nil
}
