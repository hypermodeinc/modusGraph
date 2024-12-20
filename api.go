package modusdb

import (
	"context"
	"fmt"
	"reflect"

	"github.com/dgraph-io/dgraph/v24/x"
)

type ModusDbOption func(*modusDbOptions)

type modusDbOptions struct {
	namespace uint64
}

func WithNamespace(namespace uint64) ModusDbOption {
	return func(o *modusDbOptions) {
		o.namespace = namespace
	}
}

func getDefaultNamespace(db *DB, ns ...uint64) (context.Context, *Namespace, error) {
	dbOpts := &modusDbOptions{
		namespace: db.defaultNamespace.ID(),
	}
	for _, ns := range ns {
		WithNamespace(ns)(dbOpts)
	}

	n, err := db.getNamespaceWithLock(dbOpts.namespace)
	if err != nil {
		return nil, nil, err
	}

	ctx := context.Background()
	ctx = x.AttachNamespace(ctx, n.ID())

	return ctx, n, nil
}

func Create[T any](db *DB, object *T, ns ...uint64) (uint64, *T, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	if len(ns) > 1 {
		return 0, object, fmt.Errorf("only one namespace is allowed")
	}
	ctx, n, err := getDefaultNamespace(db, ns...)
	if err != nil {
		return 0, object, err
	}

	gid, err := db.z.nextUID()
	if err != nil {
		return 0, object, err
	}

	dms, sch, err := generateCreateDqlMutationsAndSchema(n, object, gid)
	if err != nil {
		return 0, object, err
	}

	ctx = x.AttachNamespace(ctx, n.ID())

	err = n.alterSchemaWithParsed(ctx, sch)
	if err != nil {
		return 0, object, err
	}

	err = applyDqlMutations(ctx, db, dms)
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
	db.mutex.Lock()
	defer db.mutex.Unlock()
	ctx, n, err := getDefaultNamespace(db, ns...)
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

func Delete[T any, R UniqueField](db *DB, uniqueField R, ns ...uint64) (uint64, *T, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	ctx, n, err := getDefaultNamespace(db, ns...)
	if err != nil {
		return 0, nil, err
	}
	if uid, ok := any(uniqueField).(uint64); ok {
		uid, obj, err := getByGid[T](ctx, n, uid)
		if err != nil {
			return 0, nil, err
		}

		dms := generateDeleteDqlMutations(n, uid)

		err = applyDqlMutations(ctx, db, dms)
		if err != nil {
			return 0, nil, err
		}

		return uid, obj, nil
	}

	if cf, ok := any(uniqueField).(ConstrainedField); ok {
		return getByConstrainedField[T](ctx, n, cf)
	}

	return 0, nil, fmt.Errorf("invalid unique field type")
}
