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

func Create[T any](db *DB, object *T, ns ...uint64) (uint64, *T, error) {
	if len(ns) > 1 {
		return 0, object, fmt.Errorf("only one namespace is allowed")
	}
	ctx := context.Background()

	db.mutex.Lock()
	defer db.mutex.Unlock()

	gid, err := db.z.nextUID()
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
	ctx = x.AttachNamespace(ctx, ns.ID())

	err = n.alterSchemaWithParsed(ctx, sch)
	if err != nil {
		return 0, object, err
	}

	if !db.isOpen {
		return 0, object, ErrClosedDB
	}

	startTs, err := db.z.nextTS()
	if err != nil {
		return 0, object, err
	}
	commitTs, err := db.z.nextTS()
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

func Get[T any, R UniqueField](db *DB, uniqueField R, ns ...uint64) (uint64, *T, error) {
	ctx := context.Background()
	n, err := getDefaultNamespace(db, ns...)
	if err != nil {
		return 0, nil, err
	}
	if uid, ok := any(uniqueField).(uint64); ok {
		return getByGid[T](ctx, n, uid)
	}

	if cf, ok := any(uniqueField).(ConstrainedField); ok {
		return getByConstrainedField[T](ctx, n, cf)
	}

	return 0, nil, fmt.Errorf("invalid unique field type")
}
