package bigtable

import (
	"reflect"

	"cloud.google.com/go/bigtable"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

func init() {
	beam.RegisterType(reflect.TypeFor[Mutation]())
	beam.RegisterType(reflect.TypeFor[Operation]())
}

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
