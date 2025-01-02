package modusdb

import (
	"fmt"

	"github.com/dgraph-io/dgraph/v24/dql"
	"github.com/dgraph-io/dgraph/v24/schema"
)

func Create[T any](db *DB, object T, ns ...uint64) (uint64, T, error) {
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

	dms := make([]*dql.Mutation, 0)
	sch := &schema.ParsedSchema{}
	err = generateCreateDqlMutationsAndSchema[T](ctx, n, object, gid, &dms, sch)
	if err != nil {
		return 0, object, err
	}

	err = n.alterSchemaWithParsed(ctx, sch)
	if err != nil {
		return 0, object, err
	}

	err = applyDqlMutations(ctx, db, dms)
	if err != nil {
		return 0, object, err
	}

	return getByGid[T](ctx, n, gid)
}

func Upsert[T any](db *DB, object T, ns ...uint64) (uint64, T, bool, error) {

	var wasFound bool
	db.mutex.Lock()
	defer db.mutex.Unlock()
	if len(ns) > 1 {
		return 0, object, false, fmt.Errorf("only one namespace is allowed")
	}

	ctx, n, err := getDefaultNamespace(db, ns...)
	if err != nil {
		return 0, object, false, err
	}

	gid, cf, err := getUniqueConstraint[T](object)
	if err != nil {
		return 0, object, false, err
	}

	dms := make([]*dql.Mutation, 0)
	sch := &schema.ParsedSchema{}
	err = generateCreateDqlMutationsAndSchema[T](ctx, n, object, gid, &dms, sch)
	if err != nil {
		return 0, object, false, err
	}

	err = n.alterSchemaWithParsed(ctx, sch)
	if err != nil {
		return 0, object, false, err
	}

	if gid != 0 {
		gid, _, err = getByGidWithObject[T](ctx, n, gid, object)
		if err != nil && err != ErrNoObjFound {
			return 0, object, false, err
		}
		wasFound = err == nil
	} else if cf != nil {
		gid, _, err = getByConstrainedFieldWithObject[T](ctx, n, *cf, object)
		if err != nil && err != ErrNoObjFound {
			return 0, object, false, err
		}
		wasFound = err == nil
	}
	if gid == 0 {
		gid, err = db.z.nextUID()
		if err != nil {
			return 0, object, false, err
		}
	}

	dms = make([]*dql.Mutation, 0)
	err = generateCreateDqlMutationsAndSchema[T](ctx, n, object, gid, &dms, sch)
	if err != nil {
		return 0, object, false, err
	}

	err = applyDqlMutations(ctx, db, dms)
	if err != nil {
		return 0, object, false, err
	}

	gid, object, err = getByGid[T](ctx, n, gid)
	if err != nil {
		return 0, object, false, err
	}

	return gid, object, wasFound, nil
}

func Get[T any, R UniqueField](db *DB, uniqueField R, ns ...uint64) (uint64, T, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	var obj T
	if len(ns) > 1 {
		return 0, obj, fmt.Errorf("only one namespace is allowed")
	}
	ctx, n, err := getDefaultNamespace(db, ns...)
	if err != nil {
		return 0, obj, err
	}
	if uid, ok := any(uniqueField).(uint64); ok {
		return getByGid[T](ctx, n, uid)
	}

	if cf, ok := any(uniqueField).(ConstrainedField); ok {
		return getByConstrainedField[T](ctx, n, cf)
	}

	return 0, obj, fmt.Errorf("invalid unique field type")
}

func Query[T any](db *DB, queryParams QueryParams, ns ...uint64) ([]uint64, []T, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	if len(ns) > 1 {
		return nil, nil, fmt.Errorf("only one namespace is allowed")
	}
	ctx, n, err := getDefaultNamespace(db, ns...)
	if err != nil {
		return nil, nil, err
	}

	return executeQuery[T](ctx, n, queryParams, false)
}

func Delete[T any, R UniqueField](db *DB, uniqueField R, ns ...uint64) (uint64, T, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	var zeroObj T
	if len(ns) > 1 {
		return 0, zeroObj, fmt.Errorf("only one namespace is allowed")
	}
	ctx, n, err := getDefaultNamespace(db, ns...)
	if err != nil {
		return 0, zeroObj, err
	}
	if uid, ok := any(uniqueField).(uint64); ok {
		uid, obj, err := getByGid[T](ctx, n, uid)
		if err != nil {
			return 0, zeroObj, err
		}

		dms := generateDeleteDqlMutations(n, uid)

		err = applyDqlMutations(ctx, db, dms)
		if err != nil {
			return 0, zeroObj, err
		}

		return uid, obj, nil
	}

	if cf, ok := any(uniqueField).(ConstrainedField); ok {
		uid, obj, err := getByConstrainedField[T](ctx, n, cf)
		if err != nil {
			return 0, zeroObj, err
		}

		dms := generateDeleteDqlMutations(n, uid)

		err = applyDqlMutations(ctx, db, dms)
		if err != nil {
			return 0, zeroObj, err
		}

		return uid, obj, nil
	}

	return 0, zeroObj, fmt.Errorf("invalid unique field type")
}
