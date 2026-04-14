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

// Writer writes mutations to Bigtable in bulk per window.
type Writer struct {
	Project  string
	Instance string
	Table    string
	Family   string
	Column   string

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

func (fn *Writer) ProcessElement(ctx context.Context, key string, iter func(*[]byte) bool) error {
	var keys []string
	var muts []*bigtable.Mutation

	var b []byte
	for iter(&b) {
		mut := bigtable.NewMutation()
		mut.Set(fn.Family, fn.Column, bigtable.Now(), b)

		keys = append(keys, key)
		muts = append(muts, mut)
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
