/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusdb

import (
	"context"
	"fmt"

	"github.com/hypermodeinc/dgraph/v24/dql"
	"github.com/hypermodeinc/dgraph/v24/schema"
	"github.com/hypermodeinc/modusdb/api/apiutils"
	"github.com/hypermodeinc/modusdb/api/structreflect"
)

func Create[T any](ctx context.Context, engine *Engine, object T,
	nsId ...uint64) (uint64, T, error) {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()
	if len(nsId) > 1 {
		return 0, object, fmt.Errorf("only one namespace is allowed")
	}
	ctx, ns, err := getDefaultNamespace(ctx, engine, nsId...)
	if err != nil {
		return 0, object, err
	}

	gid, err := engine.z.nextUID()
	if err != nil {
		return 0, object, err
	}

	dms := make([]*dql.Mutation, 0)
	sch := &schema.ParsedSchema{}
	err = generateSetDqlMutationsAndSchema[T](ctx, ns, object, gid, &dms, sch)
	if err != nil {
		return 0, object, err
	}

	err = engine.alterSchemaWithParsed(ctx, sch)
	if err != nil {
		return 0, object, err
	}

	err = applyDqlMutations(ctx, engine, dms)
	if err != nil {
		return 0, object, err
	}

	return getByGid[T](ctx, ns, gid)
}

func Upsert[T any](ctx context.Context, engine *Engine, object T,
	nsId ...uint64) (uint64, T, bool, error) {

	var wasFound bool
	engine.mutex.Lock()
	defer engine.mutex.Unlock()
	if len(nsId) > 1 {
		return 0, object, false, fmt.Errorf("only one namespace is allowed")
	}

	ctx, ns, err := getDefaultNamespace(ctx, engine, nsId...)
	if err != nil {
		return 0, object, false, err
	}

	gid, cfKeyValue, err := structreflect.GetUniqueConstraint[T](object)
	if err != nil {
		return 0, object, false, err
	}
	var cf *ConstrainedField
	if cfKeyValue != nil {
		cf = &ConstrainedField{
			Key:   cfKeyValue.Key(),
			Value: cfKeyValue.Value(),
		}
	}

	dms := make([]*dql.Mutation, 0)
	sch := &schema.ParsedSchema{}
	err = generateSetDqlMutationsAndSchema[T](ctx, ns, object, gid, &dms, sch)
	if err != nil {
		return 0, object, false, err
	}

	err = ns.engine.alterSchemaWithParsed(ctx, sch)
	if err != nil {
		return 0, object, false, err
	}

	if gid != 0 || cf != nil {
		gid, err = getExistingObject[T](ctx, ns, gid, cf, object)
		if err != nil && err != apiutils.ErrNoObjFound {
			return 0, object, false, err
		}
		wasFound = err == nil
	}

	if gid == 0 {
		gid, err = engine.z.nextUID()
		if err != nil {
			return 0, object, false, err
		}
	}

	dms = make([]*dql.Mutation, 0)
	err = generateSetDqlMutationsAndSchema[T](ctx, ns, object, gid, &dms, sch)
	if err != nil {
		return 0, object, false, err
	}

	err = applyDqlMutations(ctx, engine, dms)
	if err != nil {
		return 0, object, false, err
	}

	gid, object, err = getByGid[T](ctx, ns, gid)
	if err != nil {
		return 0, object, false, err
	}

	return gid, object, wasFound, nil
}

func Get[T any, R UniqueField](ctx context.Context, engine *Engine, uniqueField R,
	nsId ...uint64) (uint64, T, error) {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()
	var obj T
	if len(nsId) > 1 {
		return 0, obj, fmt.Errorf("only one namespace is allowed")
	}
	ctx, ns, err := getDefaultNamespace(ctx, engine, nsId...)
	if err != nil {
		return 0, obj, err
	}
	if uid, ok := any(uniqueField).(uint64); ok {
		return getByGid[T](ctx, ns, uid)
	}

	if cf, ok := any(uniqueField).(ConstrainedField); ok {
		return getByConstrainedField[T](ctx, ns, cf)
	}

	return 0, obj, fmt.Errorf("invalid unique field type")
}

func Query[T any](ctx context.Context, engine *Engine, queryParams QueryParams,
	nsId ...uint64) ([]uint64, []T, error) {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()
	if len(nsId) > 1 {
		return nil, nil, fmt.Errorf("only one namespace is allowed")
	}
	ctx, ns, err := getDefaultNamespace(ctx, engine, nsId...)
	if err != nil {
		return nil, nil, err
	}

	return executeQuery[T](ctx, ns, queryParams, true)
}

func Delete[T any, R UniqueField](ctx context.Context, engine *Engine, uniqueField R,
	nsId ...uint64) (uint64, T, error) {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()
	var zeroObj T
	if len(nsId) > 1 {
		return 0, zeroObj, fmt.Errorf("only one namespace is allowed")
	}
	ctx, ns, err := getDefaultNamespace(ctx, engine, nsId...)
	if err != nil {
		return 0, zeroObj, err
	}
	if uid, ok := any(uniqueField).(uint64); ok {
		uid, obj, err := getByGid[T](ctx, ns, uid)
		if err != nil {
			return 0, zeroObj, err
		}

		dms := generateDeleteDqlMutations(ns, uid)

		err = applyDqlMutations(ctx, engine, dms)
		if err != nil {
			return 0, zeroObj, err
		}

		return uid, obj, nil
	}

	if cf, ok := any(uniqueField).(ConstrainedField); ok {
		uid, obj, err := getByConstrainedField[T](ctx, ns, cf)
		if err != nil {
			return 0, zeroObj, err
		}

		dms := generateDeleteDqlMutations(ns, uid)

		err = applyDqlMutations(ctx, engine, dms)
		if err != nil {
			return 0, zeroObj, err
		}

		return uid, obj, nil
	}

	return 0, zeroObj, fmt.Errorf("invalid unique field type")
}
