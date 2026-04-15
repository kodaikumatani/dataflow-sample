package bigtable

import (
	"context"
	"fmt"
	"log"
	"reflect"

	"cloud.google.com/go/bigtable"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

// Operation represents a single Set operation on a Bigtable row.
type Operation struct {
	Family    string
	Column    string
	Timestamp int64 // microseconds since Unix epoch; 0 uses server time
	Value     []byte
}

// Mutation is a serializable wrapper for bigtable mutations.
// Use this as the value type in PCollection<KV<string, Mutation>>.
type Mutation struct {
	Ops []Operation
}

func (m *Mutation) toBigtableMutation() *bigtable.Mutation {
	mut := bigtable.NewMutation()
	for _, op := range m.Ops {
		ts := bigtable.Timestamp(op.Timestamp)
		if op.Timestamp == 0 {
			ts = bigtable.Now()
		}
		mut.Set(op.Family, op.Column, ts, op.Value)
	}
	return mut
}

func init() {
	register.DoFn3x1(&Writer{})
	beam.RegisterType(reflect.TypeOf((*Mutation)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*Operation)(nil)).Elem())
}

// Writer writes mutations to Bigtable in bulk per window.
type Writer struct {
	Project  string
	Instance string
	Table    string

	client *bigtable.Client
	table  *bigtable.Table
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

func (fn *Writer) ProcessElement(ctx context.Context, key string, iter func(*Mutation) bool) error {
	var keys []string
	var muts []*bigtable.Mutation

	var m Mutation
	for iter(&m) {
		keys = append(keys, key)
		muts = append(muts, m.toBigtableMutation())
	}

	if len(muts) == 0 {
		return nil
	}

	errs, err := fn.table.ApplyBulk(ctx, keys, muts)
	if err != nil {
		return err
	}

	for i, e := range errs {
		if e != nil {
			log.Printf("ApplyBulk error for row %s: %v", keys[i], e)
		}
	}

	return nil
}

func (fn *Writer) Teardown() error {
	if fn.client != nil {
		return fn.client.Close()
	}

	return nil
}
