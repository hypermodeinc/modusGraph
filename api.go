package modusdb

import (
	"fmt"

	"github.com/dgraph-io/dgraph/v24/dql"
	"github.com/dgraph-io/dgraph/v24/schema"
)

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

	dms := make([]*dql.Mutation, 0)
	sch := &schema.ParsedSchema{}
	err = generateCreateDqlMutationsAndSchema[T](ctx, n, *object, gid, &dms, sch)
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

func Upsert[T any](db *DB, object *T, ns ...uint64) (uint64, *T, bool, error) {

	var wasFound bool
	db.mutex.Lock()
	defer db.mutex.Unlock()
	if len(ns) > 1 {
		return 0, object, false, fmt.Errorf("only one namespace is allowed")
	}
	if object == nil {
		return 0, nil, false, fmt.Errorf("object is nil")
	}

	ctx, n, err := getDefaultNamespace(db, ns...)
	if err != nil {
		return 0, object, false, err
	}

	gid, cf, err := getUniqueConstraint[T](*object)
	if err != nil {
		return 0, nil, false, err
	}

	dms := make([]*dql.Mutation, 0)
	sch := &schema.ParsedSchema{}
	err = generateCreateDqlMutationsAndSchema[T](ctx, n, *object, gid, &dms, sch)
	if err != nil {
		return 0, nil, false, err
	}

	err = n.alterSchemaWithParsed(ctx, sch)
	if err != nil {
		return 0, nil, false, err
	}

	if gid != 0 {
		gid, _, err = getByGidWithObject[T](ctx, n, gid, *object)
		if err != nil && err != ErrNoObjFound {
			return 0, nil, false, err
		}
		wasFound = err == nil
	} else if cf != nil {
		gid, _, err = getByConstrainedFieldWithObject[T](ctx, n, *cf, *object)
		if err != nil && err != ErrNoObjFound {
			return 0, nil, false, err
		}
		wasFound = err == nil
	}
	if gid == 0 {
		gid, err = db.z.nextUID()
		if err != nil {
			return 0, nil, false, err
		}
	}

	dms = make([]*dql.Mutation, 0)
	err = generateCreateDqlMutationsAndSchema[T](ctx, n, *object, gid, &dms, sch)
	if err != nil {
		return 0, nil, false, err
	}

	err = applyDqlMutations(ctx, db, dms)
	if err != nil {
		return 0, nil, false, err
	}

	gid, object, err = getByGid[T](ctx, n, gid)
	if err != nil {
		return 0, nil, false, err
	}

	return gid, object, wasFound, nil
}

func Get[T any, R UniqueField](db *DB, uniqueField R, ns ...uint64) (uint64, *T, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	if len(ns) > 1 {
		return 0, nil, fmt.Errorf("only one namespace is allowed")
	}
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
	if len(ns) > 1 {
		return 0, nil, fmt.Errorf("only one namespace is allowed")
	}
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
		uid, obj, err := getByConstrainedField[T](ctx, n, cf)
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

	return 0, nil, fmt.Errorf("invalid unique field type")
}
