/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusgraph

import (
	"context"
	"errors"
	"reflect"

	"github.com/hypermodeinc/dgraph/v25/dql"
	"github.com/hypermodeinc/dgraph/v25/schema"
	"github.com/hypermodeinc/modusgraph/api/apiutils"
	"github.com/hypermodeinc/modusgraph/api/structreflect"
)

// Deprecated: Use NewClient and client.Insert instead.
func Create[T any](ctx context.Context, engine *Engine, object T,
	nsId ...uint64) (uint64, T, error) {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()
	if len(nsId) > 1 {
		return 0, object, errors.New("only one namespace is allowed")
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

// Deprecated
func Upsert[T any](ctx context.Context, engine *Engine, object T,
	nsId ...uint64) (uint64, T, bool, error) {

	var wasFound bool
	engine.mutex.Lock()
	defer engine.mutex.Unlock()
	if len(nsId) > 1 {
		return 0, object, false, errors.New("only one namespace is allowed")
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

// Deprecated: Use NewClient and client.Get instead.
func Get[T any, R UniqueField](ctx context.Context, engine *Engine, uniqueField R,
	nsId ...uint64) (uint64, T, error) {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()
	var obj T
	if len(nsId) > 1 {
		return 0, obj, errors.New("only one namespace is allowed")
	}
	ctx, ns, err := getDefaultNamespace(ctx, engine, nsId...)
	if err != nil {
		return 0, obj, err
	}
	if uid, ok := any(uniqueField).(uint64); ok {
		return getByGid[T](ctx, ns, uid)
	}

	if cf, ok := any(uniqueField).(ConstrainedField); ok {
		objType := reflect.TypeOf(obj)
		sch, err := getSchema(ctx, ns)
		if err != nil {
			return 0, obj, err
		}
		for _, t := range sch.Types {
			if t.Name == objType.Name() {
				return getByConstrainedField[T](ctx, ns, cf)
			}
		}
		return 0, obj, errors.New("type not found")
	}

	return 0, obj, errors.New("invalid unique field type")
}

// Deprecated: Use NewClient and client.Query instead.
func Query[T any](ctx context.Context, engine *Engine, queryParams QueryParams,
	nsId ...uint64) ([]uint64, []T, error) {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()
	if len(nsId) > 1 {
		return nil, nil, errors.New("only one namespace is allowed")
	}
	ctx, ns, err := getDefaultNamespace(ctx, engine, nsId...)
	if err != nil {
		return nil, nil, err
	}

	return executeQuery[T](ctx, ns, queryParams, true)
}

// Deprecated: Use NewClient and client.Delete instead.
func Delete[T any, R UniqueField](ctx context.Context, engine *Engine, uniqueField R,
	nsId ...uint64) (uint64, T, error) {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()
	var zeroObj T
	if len(nsId) > 1 {
		return 0, zeroObj, errors.New("only one namespace is allowed")
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

	return 0, zeroObj, errors.New("invalid unique field type")
}
