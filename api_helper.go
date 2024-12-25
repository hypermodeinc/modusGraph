package modusdb

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/dgraph-io/dgraph/v24/dql"
	"github.com/dgraph-io/dgraph/v24/protos/pb"
	"github.com/dgraph-io/dgraph/v24/query"
	"github.com/dgraph-io/dgraph/v24/schema"
	"github.com/dgraph-io/dgraph/v24/worker"
	"github.com/dgraph-io/dgraph/v24/x"
)

func generateCreateDqlMutationsAndSchema[T any](ctx context.Context, n *Namespace, object T,
	gid uint64, dms *[]*dql.Mutation, sch *schema.ParsedSchema) error {
	t := reflect.TypeOf(object)
	if t.Kind() != reflect.Struct {
		return fmt.Errorf("expected struct, got %s", t.Kind())
	}

	fieldToJsonTags, jsonToDbTags, jsonToReverseEdgeTags, err := getFieldTags(t)
	if err != nil {
		return err
	}
	jsonTagToValue := getJsonTagToValues(object, fieldToJsonTags)

	nquads := make([]*api.NQuad, 0)
	for jsonName, value := range jsonTagToValue {
		if jsonToReverseEdgeTags[jsonName] != "" {
			continue
		}
		if jsonName == "gid" {
			continue
		}
		var val *api.Value
		var valType pb.Posting_ValType

		reflectValueType := reflect.TypeOf(value)
		var nquad *api.NQuad
		if reflectValueType.Kind() == reflect.Struct {
			value = reflect.ValueOf(value).Interface()
			newGid, err := getUidOrMutate(ctx, n.db, n, value)
			if err != nil {
				return err
			}
			valType = pb.Posting_UID

			nquad = &api.NQuad{
				Namespace: n.ID(),
				Subject:   fmt.Sprint(gid),
				Predicate: getPredicateName(t.Name(), jsonName),
				ObjectId:  fmt.Sprint(newGid),
			}
		} else if reflectValueType.Kind() == reflect.Pointer {
			// dereference the pointer
			reflectValueType = reflectValueType.Elem()
			if reflectValueType.Kind() == reflect.Struct {
				// convert value to pointer, and then dereference
				value = reflect.ValueOf(value).Elem().Interface()
				newGid, err := getUidOrMutate(ctx, n.db, n, value)
				if err != nil {
					return err
				}
				valType = pb.Posting_UID

				nquad = &api.NQuad{
					Namespace: n.ID(),
					Subject:   fmt.Sprint(gid),
					Predicate: getPredicateName(t.Name(), jsonName),
					ObjectId:  fmt.Sprint(newGid),
				}
			}
		} else {
			valType, err = valueToPosting_ValType(value)
			if err != nil {
				return err
			}
			val, err = valueToApiVal(value)
			if err != nil {
				return err
			}

			nquad = &api.NQuad{
				Namespace:   n.ID(),
				Subject:     fmt.Sprint(gid),
				Predicate:   getPredicateName(t.Name(), jsonName),
				ObjectValue: val,
			}
		}
		u := &pb.SchemaUpdate{
			Predicate: addNamespace(n.id, getPredicateName(t.Name(), jsonName)),
			ValueType: valType,
		}
		if jsonToDbTags[jsonName] != nil {
			constraint := jsonToDbTags[jsonName].constraint
			if constraint == "unique" || constraint == "term" {
				u.Directive = pb.SchemaUpdate_INDEX
				if constraint == "unique" {
					u.Tokenizer = []string{"exact"}
				} else {
					u.Tokenizer = []string{"term"}
				}
			}
		}

		sch.Preds = append(sch.Preds, u)
		nquads = append(nquads, nquad)
	}
	sch.Types = append(sch.Types, &pb.TypeUpdate{
		TypeName: addNamespace(n.id, t.Name()),
		Fields:   sch.Preds,
	})

	val, err := valueToApiVal(t.Name())
	if err != nil {
		return err
	}
	nquad := &api.NQuad{
		Namespace:   n.ID(),
		Subject:     fmt.Sprint(gid),
		Predicate:   "dgraph.type",
		ObjectValue: val,
	}
	nquads = append(nquads, nquad)

	*dms = append(*dms, &dql.Mutation{
		Set: nquads,
	})

	return nil
}

func generateDeleteDqlMutations(n *Namespace, gid uint64) []*dql.Mutation {
	return []*dql.Mutation{{
		Del: []*api.NQuad{
			{
				Namespace: n.ID(),
				Subject:   fmt.Sprint(gid),
				Predicate: x.Star,
				ObjectValue: &api.Value{
					Val: &api.Value_DefaultVal{DefaultVal: x.Star},
				},
			},
		},
	}}
}

func getByGid[T any](ctx context.Context, n *Namespace, gid uint64) (uint64, *T, error) {
	query := `
	{
	  obj(func: uid(%d)) {
		uid
		expand(_all_) {
			uid
			expand(_all_)
			dgraph.type
		}
		dgraph.type
		%s
	  }
	}
	  `

	return executeGet[T](ctx, n, query, gid)
}

func getByGidWithObject[T any](ctx context.Context, n *Namespace, gid uint64, obj T) (uint64, *T, error) {
	query := `
	{
	  obj(func: uid(%d)) {
		uid
		expand(_all_) {
			uid
			expand(_all_)
			dgraph.type
		}
		dgraph.type
		%s
	  }
	}
	  `

	return executeGetWithObject[T](ctx, n, query, obj, false, gid)
}

func getByConstrainedField[T any](ctx context.Context, n *Namespace, cf ConstrainedField) (uint64, *T, error) {
	query := `
	{
	  obj(func: eq(%s, %s)) {
		uid
		expand(_all_) {
			uid
			expand(_all_)
			dgraph.type
		}
		dgraph.type
		%s
	  }
	}
	  `

	return executeGet[T](ctx, n, query, cf)
}

func getByConstrainedFieldWithObject[T any](ctx context.Context, n *Namespace,
	cf ConstrainedField, obj T) (uint64, *T, error) {

	query := `
	{
	  obj(func: eq(%s, %s)) {
		uid
		expand(_all_) {
			uid
			expand(_all_)
			dgraph.type
		}
		dgraph.type
		%s
	  }
	}
	  `

	return executeGetWithObject[T](ctx, n, query, obj, false, cf)
}

func executeGet[T any, R UniqueField](ctx context.Context, n *Namespace, query string, args ...R) (uint64, *T, error) {
	if len(args) != 1 {
		return 0, nil, fmt.Errorf("expected 1 argument, got %d", len(args))
	}

	var obj T

	return executeGetWithObject(ctx, n, query, obj, true, args...)
}

func executeGetWithObject[T any, R UniqueField](ctx context.Context, n *Namespace, query string,
	obj T, withReverse bool, args ...R) (uint64, *T, error) {
	t := reflect.TypeOf(obj)

	fieldToJsonTags, jsonToDbTag, jsonToReverseEdgeTags, err := getFieldTags(t)
	if err != nil {
		return 0, nil, err
	}
	readFromQuery := ""
	if withReverse {
		for jsonTag, reverseEdgeTag := range jsonToReverseEdgeTags {
			readFromQuery += fmt.Sprintf(`
		%s: ~%s {
			uid
			expand(_all_)
			dgraph.type
		}
		`, getPredicateName(t.Name(), jsonTag), reverseEdgeTag)
		}
	}

	var cf ConstrainedField
	gid, ok := any(args[0]).(uint64)
	if ok {
		query = fmt.Sprintf(query, gid, readFromQuery)
	} else if cf, ok = any(args[0]).(ConstrainedField); ok {
		query = fmt.Sprintf(query, getPredicateName(t.Name(), cf.Key), cf.Value, readFromQuery)
	} else {
		return 0, nil, fmt.Errorf("invalid unique field type")
	}

	if jsonToDbTag[cf.Key] != nil && jsonToDbTag[cf.Key].constraint == "" {
		return 0, nil, fmt.Errorf("constraint not defined for field %s", cf.Key)
	}

	resp, err := n.queryWithLock(ctx, query)
	if err != nil {
		return 0, nil, err
	}

	dynamicType := createDynamicStruct(t, fieldToJsonTags, 1)

	dynamicInstance := reflect.New(dynamicType).Interface()

	var result struct {
		Obj []any `json:"obj"`
	}

	result.Obj = append(result.Obj, dynamicInstance)

	// Unmarshal the JSON response into the dynamic struct
	if err := json.Unmarshal(resp.Json, &result); err != nil {
		return 0, nil, err
	}

	// Check if we have at least one object in the response
	if len(result.Obj) == 0 {
		return 0, nil, ErrNoObjFound
	}

	// Map the dynamic struct to the final type T
	finalObject := reflect.New(t).Interface()
	gid, err = mapDynamicToFinal(result.Obj[0], finalObject)
	if err != nil {
		return 0, nil, err
	}

	// Convert to *interface{} then to *T
	if ifacePtr, ok := finalObject.(*interface{}); ok {
		if typedPtr, ok := (*ifacePtr).(*T); ok {
			return gid, typedPtr, nil
		}
	}

	// If conversion fails, try direct conversion
	if typedPtr, ok := finalObject.(*T); ok {
		return gid, typedPtr, nil
	}

	if dirType, ok := finalObject.(T); ok {
		return gid, &dirType, nil
	}

	return 0, nil, fmt.Errorf("failed to convert type %T to %T", finalObject, obj)
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

func getUniqueConstraint[T any](object T) (uint64, *ConstrainedField, error) {
	t := reflect.TypeOf(object)
	fieldToJsonTags, jsonToDbTags, _, err := getFieldTags(t)
	if err != nil {
		return 0, nil, err
	}
	jsonTagToValue := getJsonTagToValues(object, fieldToJsonTags)

	for jsonName, value := range jsonTagToValue {
		if jsonName == "gid" {
			gid, ok := value.(uint64)
			if !ok {
				continue
			}
			if gid != 0 {
				return gid, nil, nil
			}
		}
		if jsonToDbTags[jsonName] != nil && jsonToDbTags[jsonName].constraint == "unique" {
			// check if value is zero or nil
			if value == reflect.Zero(reflect.TypeOf(value)).Interface() || value == nil {
				continue
			}
			return 0, &ConstrainedField{
				Key:   jsonName,
				Value: value,
			}, nil
		}
	}

	return 0, nil, fmt.Errorf("unique constraint not defined for any field on type %s", t.Name())
}

func getUidOrMutate[T any](ctx context.Context, db *DB, n *Namespace, object T) (uint64, error) {
	gid, cf, err := getUniqueConstraint[T](object)
	if err != nil {
		return 0, err
	}

	dms := make([]*dql.Mutation, 0)
	sch := &schema.ParsedSchema{}
	err = generateCreateDqlMutationsAndSchema(ctx, n, object, gid, &dms, sch)
	if err != nil {
		return 0, err
	}

	err = n.alterSchemaWithParsed(ctx, sch)
	if err != nil {
		return 0, err
	}
	if gid != 0 {
		gid, _, err = getByGidWithObject[T](ctx, n, gid, object)
		if err != nil && err != ErrNoObjFound {
			return 0, err
		}
		if err == nil {
			return gid, nil
		}
	} else if cf != nil {
		gid, _, err = getByConstrainedFieldWithObject[T](ctx, n, *cf, object)
		if err != nil && err != ErrNoObjFound {
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
	err = generateCreateDqlMutationsAndSchema(ctx, n, object, gid, &dms, sch)
	if err != nil {
		return 0, err
	}

	err = applyDqlMutations(ctx, db, dms)
	if err != nil {
		return 0, err
	}

	return gid, nil
}
