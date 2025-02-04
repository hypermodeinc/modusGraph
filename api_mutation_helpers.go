/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusdb

import (
	"context"
	"fmt"
	"reflect"

	"github.com/hypermodeinc/dgraph/v24/dql"
	"github.com/hypermodeinc/dgraph/v24/protos/pb"
	"github.com/hypermodeinc/dgraph/v24/query"
	"github.com/hypermodeinc/dgraph/v24/schema"
	"github.com/hypermodeinc/dgraph/v24/worker"
	"github.com/hypermodeinc/modusdb/api/apiutils"
	"github.com/hypermodeinc/modusdb/api/structreflect"
)

func processStructValue(ctx context.Context, value any, ns *Namespace) (any, error) {
	if reflect.TypeOf(value).Kind() == reflect.Struct {
		value = reflect.ValueOf(value).Interface()
		newGid, err := getUidOrMutate(ctx, ns.engine, ns, value)
		if err != nil {
			return nil, err
		}
		return newGid, nil
	}
	return value, nil
}

func processPointerValue(ctx context.Context, value any, ns *Namespace) (any, error) {
	reflectValueType := reflect.TypeOf(value)
	if reflectValueType.Kind() == reflect.Pointer {
		reflectValueType = reflectValueType.Elem()
		if reflectValueType.Kind() == reflect.Struct {
			value = reflect.ValueOf(value).Elem().Interface()
			return processStructValue(ctx, value, ns)
		}
	}
	return value, nil
}

func getUidOrMutate[T any](ctx context.Context, engine *Engine, ns *Namespace, object T) (uint64, error) {
	gid, cfKeyValue, err := structreflect.GetUniqueConstraint[T](object)
	if err != nil {
		return 0, err
	}
	var cf *ConstrainedField
	if cfKeyValue != nil {
		cf = &ConstrainedField{Key: cfKeyValue.Key(), Value: cfKeyValue.Value()}
	}

	dms := make([]*dql.Mutation, 0)
	sch := &schema.ParsedSchema{}
	err = generateSetDqlMutationsAndSchema(ctx, ns, object, gid, &dms, sch)
	if err != nil {
		return 0, err
	}

	err = engine.alterSchemaWithParsed(ctx, sch)
	if err != nil {
		return 0, err
	}
	if gid != 0 || cf != nil {
		gid, err = getExistingObject(ctx, ns, gid, cf, object)
		if err != nil && err != apiutils.ErrNoObjFound {
			return 0, err
		}
		if err == nil {
			return gid, nil
		}
	}

	gid, err = engine.z.nextUID()
	if err != nil {
		return 0, err
	}

	dms = make([]*dql.Mutation, 0)
	err = generateSetDqlMutationsAndSchema(ctx, ns, object, gid, &dms, sch)
	if err != nil {
		return 0, err
	}

	err = applyDqlMutations(ctx, engine, dms)
	if err != nil {
		return 0, err
	}

	return gid, nil
}

func applyDqlMutations(ctx context.Context, engine *Engine, dms []*dql.Mutation) error {
	edges, err := query.ToDirectedEdges(dms, nil)
	if err != nil {
		return err
	}

	if !engine.isOpen.Load() {
		return ErrClosedEngine
	}

	startTs, err := engine.z.nextTs()
	if err != nil {
		return err
	}
	commitTs, err := engine.z.nextTs()
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
