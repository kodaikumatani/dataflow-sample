package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn3x1(&batchWriteFn{})
}

// batchWriteFn writes mutations to Bigtable in bulk per window.
type batchWriteFn struct {
	Project  string
	Instance string
	Table    string

	client *bigtable.Client
	table  *bigtable.Table
}

func (fn *batchWriteFn) Setup(ctx context.Context) error {
	client, err := bigtable.NewClient(ctx, fn.Project, fn.Instance)
	if err != nil {
		return fmt.Errorf("bigtable.NewClient: %w", err)
	}
	fn.client = client
	fn.table = client.Open(fn.Table)
	return nil
}

func (fn *batchWriteFn) ProcessElement(ctx context.Context, _ string, iter func(*Mutation) bool) error {
	var rowKeys []string
	var muts []*bigtable.Mutation

	var m Mutation
	for iter(&m) {
		mut := bigtable.NewMutation()
		mut.Set(m.FamilyName, m.ColumnQualifier, bigtable.Time(time.UnixMicro(m.TimestampMicros)), m.Value)
		rowKeys = append(rowKeys, m.Key)
		muts = append(muts, mut)
	}

	if len(muts) == 0 {
		return nil
	}

	errs, err := fn.table.ApplyBulk(ctx, rowKeys, muts)
	if err != nil {
		return fmt.Errorf("ApplyBulk: %w", err)
	}
	for i, e := range errs {
		if e != nil {
			log.Printf("ApplyBulk error for row %s: %v", rowKeys[i], e)
		}
	}
	return nil
}

func (fn *batchWriteFn) Teardown() error {
	if fn.client != nil {
		return fn.client.Close()
	}
	return nil
}
