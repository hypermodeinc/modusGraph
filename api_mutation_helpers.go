package modusdb

import (
	"context"
	"fmt"
	"reflect"

	"github.com/dgraph-io/dgraph/v24/dql"
	"github.com/dgraph-io/dgraph/v24/protos/pb"
	"github.com/dgraph-io/dgraph/v24/query"
	"github.com/dgraph-io/dgraph/v24/schema"
	"github.com/dgraph-io/dgraph/v24/worker"
	"github.com/hypermodeinc/modusdb/api/utils"
)

func processStructValue(ctx context.Context, value any, n *Namespace) (any, error) {
	if reflect.TypeOf(value).Kind() == reflect.Struct {
		value = reflect.ValueOf(value).Interface()
		newGid, err := getUidOrMutate(ctx, n.db, n, value)
		if err != nil {
			return nil, err
		}
		return newGid, nil
	}
	return value, nil
}

func processPointerValue(ctx context.Context, value any, n *Namespace) (any, error) {
	reflectValueType := reflect.TypeOf(value)
	if reflectValueType.Kind() == reflect.Pointer {
		reflectValueType = reflectValueType.Elem()
		if reflectValueType.Kind() == reflect.Struct {
			value = reflect.ValueOf(value).Elem().Interface()
			return processStructValue(ctx, value, n)
		}
	}
	return value, nil
}

func getUidOrMutate[T any](ctx context.Context, db *DB, n *Namespace, object T) (uint64, error) {
	gid, cfKeyValue, err := utils.GetUniqueConstraint[T](object)
	if err != nil {
		return 0, err
	}
	var cf *ConstrainedField
	if cfKeyValue != nil {
		cf = &ConstrainedField{Key: cfKeyValue.Key(), Value: cfKeyValue.Value()}
	}

	dms := make([]*dql.Mutation, 0)
	sch := &schema.ParsedSchema{}
	err = generateSetDqlMutationsAndSchema(ctx, n, object, gid, &dms, sch)
	if err != nil {
		return 0, err
	}

	err = n.alterSchemaWithParsed(ctx, sch)
	if err != nil {
		return 0, err
	}
	if gid != 0 || cf != nil {
		gid, err = getExistingObject(ctx, n, gid, cf, object)
		if err != nil && err != utils.ErrNoObjFound {
			return 0, err
		}
		if err == nil {
			return gid, nil
		}
	}

	gid, err = db.z.nextUID()
	if err != nil {
		return 0, err
	}

	dms = make([]*dql.Mutation, 0)
	err = generateSetDqlMutationsAndSchema(ctx, n, object, gid, &dms, sch)
	if err != nil {
		return 0, err
	}

	err = applyDqlMutations(ctx, db, dms)
	if err != nil {
		return 0, err
	}

	return gid, nil
}

func applyDqlMutations(ctx context.Context, db *DB, dms []*dql.Mutation) error {
	edges, err := query.ToDirectedEdges(dms, nil)
	if err != nil {
		return err
	}

	if !db.isOpen {
		return ErrClosedDB
	}

	startTs, err := db.z.nextTs()
	if err != nil {
		return err
	}
	commitTs, err := db.z.nextTs()
	if err != nil {
		return err
	}

	m := &pb.Mutations{
		GroupId: 1,
		StartTs: startTs,
		Edges:   edges,
	}
	m.Edges, err = query.ExpandEdges(ctx, m)
	if err != nil {
		return fmt.Errorf("error expanding edges: %w", err)
	}

	p := &pb.Proposal{Mutations: m, StartTs: startTs}
	if err := worker.ApplyMutations(ctx, p); err != nil {
		return err
	}

	return worker.ApplyCommited(ctx, &pb.OracleDelta{
		Txns: []*pb.TxnStatus{{StartTs: startTs, CommitTs: commitTs}},
	})
}
