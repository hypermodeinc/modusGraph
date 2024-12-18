package modusdb

import (
	"context"
	"fmt"
	"reflect"

	"github.com/dgraph-io/dgraph/v24/protos/pb"
	"github.com/dgraph-io/dgraph/v24/query"
	"github.com/dgraph-io/dgraph/v24/worker"
	"github.com/dgraph-io/dgraph/v24/x"
)

func Create[T any](ctx context.Context, n *Namespace, object *T) (uint64, *T, error) {
	n.db.mutex.Lock()
	defer n.db.mutex.Unlock()
	gid, err := n.db.z.nextUID()
	if err != nil {
		return 0, object, err
	}

	dms, sch, err := generateCreateDqlMutationAndSchema(n, object, gid)
	if err != nil {
		return 0, object, err
	}

	edges, err := query.ToDirectedEdges(dms, nil)
	if err != nil {
		return 0, object, err
	}
	ctx = x.AttachNamespace(ctx, n.ID())

	err = n.alterSchemaWithParsed(ctx, sch)
	if err != nil {
		return 0, object, err
	}

	if !n.db.isOpen {
		return 0, object, ErrClosedDB
	}

	startTs, err := n.db.z.nextTs()
	if err != nil {
		return 0, object, err
	}
	commitTs, err := n.db.z.nextTs()
	if err != nil {
		return 0, object, err
	}

	m := &pb.Mutations{
		GroupId: 1,
		StartTs: startTs,
		Edges:   edges,
	}
	m.Edges, err = query.ExpandEdges(ctx, m)
	if err != nil {
		return 0, object, fmt.Errorf("error expanding edges: %w", err)
	}

	p := &pb.Proposal{Mutations: m, StartTs: startTs}
	if err := worker.ApplyMutations(ctx, p); err != nil {
		return 0, object, err
	}

	err = worker.ApplyCommited(ctx, &pb.OracleDelta{
		Txns: []*pb.TxnStatus{{StartTs: startTs, CommitTs: commitTs}},
	})
	if err != nil {
		return 0, object, err
	}

	v := reflect.ValueOf(object).Elem()

	gidField := v.FieldByName("Gid")

	if gidField.IsValid() && gidField.CanSet() && gidField.Kind() == reflect.Uint64 {
		gidField.SetUint(gid)
	}

	return gid, object, nil
}

func Get[T any, R UniqueField](ctx context.Context, n *Namespace, uniqueField R) (*T, error) {
	if uid, ok := any(uniqueField).(uint64); ok {
		return getByUid[T](ctx, n, uid)
	}

	if cf, ok := any(uniqueField).(ConstrainedField); ok {
		return getByConstrainedField[T](ctx, n, cf)
	}

	return nil, fmt.Errorf("invalid unique field type")
}
