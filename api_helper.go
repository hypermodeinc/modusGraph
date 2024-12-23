package modusdb

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/dgraph-io/dgraph/v24/dql"
	"github.com/dgraph-io/dgraph/v24/protos/pb"
	"github.com/dgraph-io/dgraph/v24/query"
	"github.com/dgraph-io/dgraph/v24/schema"
	"github.com/dgraph-io/dgraph/v24/worker"
	"github.com/dgraph-io/dgraph/v24/x"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkb"
)

func getPredicateName(typeName, fieldName string) string {
	return fmt.Sprint(typeName, ".", fieldName)
}

func addNamespace(ns uint64, pred string) string {
	return x.NamespaceAttr(ns, pred)
}

func valueToPosting_ValType(v any) (pb.Posting_ValType, error) {
	switch v.(type) {
	case string:
		return pb.Posting_STRING, nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return pb.Posting_INT, nil
	case bool:
		return pb.Posting_BOOL, nil
	case float32, float64:
		return pb.Posting_FLOAT, nil
	case []byte:
		return pb.Posting_BINARY, nil
	case time.Time:
		return pb.Posting_DATETIME, nil
	case geom.Point:
		return pb.Posting_GEO, nil
	case []float32, []float64:
		return pb.Posting_VFLOAT, nil
	default:
		return pb.Posting_DEFAULT, fmt.Errorf("unsupported type %T", v)
	}
}

func valueToApiVal(v any) (*api.Value, error) {
	switch val := v.(type) {
	case string:
		return &api.Value{Val: &api.Value_StrVal{StrVal: val}}, nil
	case int:
		return &api.Value{Val: &api.Value_IntVal{IntVal: int64(val)}}, nil
	case int8:
		return &api.Value{Val: &api.Value_IntVal{IntVal: int64(val)}}, nil
	case int16:
		return &api.Value{Val: &api.Value_IntVal{IntVal: int64(val)}}, nil
	case int32:
		return &api.Value{Val: &api.Value_IntVal{IntVal: int64(val)}}, nil
	case int64:
		return &api.Value{Val: &api.Value_IntVal{IntVal: val}}, nil
	case uint8:
		return &api.Value{Val: &api.Value_IntVal{IntVal: int64(val)}}, nil
	case uint16:
		return &api.Value{Val: &api.Value_IntVal{IntVal: int64(val)}}, nil
	case uint32:
		return &api.Value{Val: &api.Value_IntVal{IntVal: int64(val)}}, nil
	case bool:
		return &api.Value{Val: &api.Value_BoolVal{BoolVal: val}}, nil
	case float32:
		return &api.Value{Val: &api.Value_DoubleVal{DoubleVal: float64(val)}}, nil
	case float64:
		return &api.Value{Val: &api.Value_DoubleVal{DoubleVal: val}}, nil
	case []byte:
		return &api.Value{Val: &api.Value_BytesVal{BytesVal: val}}, nil
	case time.Time:
		bytes, err := val.MarshalBinary()
		if err != nil {
			return nil, err
		}
		return &api.Value{Val: &api.Value_DateVal{DateVal: bytes}}, nil
	case geom.Point:
		bytes, err := wkb.Marshal(&val, binary.LittleEndian)
		if err != nil {
			return nil, err
		}
		return &api.Value{Val: &api.Value_GeoVal{GeoVal: bytes}}, nil
	case uint, uint64:
		return &api.Value{Val: &api.Value_DefaultVal{DefaultVal: fmt.Sprint(v)}}, nil
	default:
		return nil, fmt.Errorf("unsupported type %T", v)
	}
}

func generateCreateDqlMutationsAndSchema[T any](ctx context.Context, n *Namespace, object *T,
	gid uint64, dms *[]*dql.Mutation, sch *schema.ParsedSchema) error {
	t := reflect.TypeOf(*object)
	if t.Kind() != reflect.Struct {
		return fmt.Errorf("expected struct, got %s", t.Kind())
	}

	fieldToJsonTags, jsonToDbTags, _, err := getFieldTags(t)
	if err != nil {
		return err
	}
	values := getJsonTagToValues(object, fieldToJsonTags)

	nquads := make([]*api.NQuad, 0)
	for jsonName, value := range values {
		if jsonName == "gid" {
			continue
		}
		var val *api.Value
		var valType pb.Posting_ValType
		if reflect.TypeOf(value).Kind() == reflect.Struct {
			gid, _, _, err := upsertHelper(ctx, n.db, n, &value, false)
			if err != nil {
				return err
			}
			valType, err = valueToPosting_ValType(fmt.Sprint(gid))
			if err != nil {
				return err
			}
			val, err = valueToApiVal(fmt.Sprint(gid))
			if err != nil {
				return err
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
		nquad := &api.NQuad{
			Namespace:   n.ID(),
			Subject:     fmt.Sprint(gid),
			Predicate:   getPredicateName(t.Name(), jsonName),
			ObjectValue: val,
		}
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

func getByGid[T any](ctx context.Context, n *Namespace, gid uint64, readFrom bool) (uint64, *T, error) {
	query := `
	{
	  obj(func: uid(%d)) {
		uid
		expand(_all_)
		dgraph.type
		%s
	  }
	}
	  `

	return executeGet[T](ctx, n, query, readFrom, gid)
}

func getByConstrainedField[T any](ctx context.Context, n *Namespace, cf ConstrainedField, readFrom bool) (uint64, *T, error) {
	query := `
	{
	  obj(func: eq(%s, %s)) {
		uid
		expand(_all_)
		dgraph.type
		%s
	  }
	}
	  `

	return executeGet[T](ctx, n, query, readFrom, cf)
}

func executeGet[T any, R UniqueField](ctx context.Context, n *Namespace, query string, readFrom bool, args ...R) (uint64, *T, error) {
	if len(args) != 1 {
		return 0, nil, fmt.Errorf("expected 1 argument, got %d", len(args))
	}

	var obj T

	t := reflect.TypeOf(obj)

	fieldToJsonTags, jsonToDbTag, reverseEdgeTags, err := getFieldTags(t)
	if err != nil {
		return 0, nil, err
	}
	readFromQuery := ""
	for fieldName, reverseEdgeTag := range reverseEdgeTags {
		readFromQuery += fmt.Sprintf(`
		%s: ~%s {
			uid
			expand(_all_)
			dgraph.type
		}
		`, getPredicateName(t.Name(), fieldToJsonTags[fieldName]), reverseEdgeTag)
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

	dynamicType := createDynamicStruct(t, fieldToJsonTags)

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

	return gid, finalObject.(*T), nil
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

func getUniqueConstraint[T any](object *T) (uint64, *ConstrainedField, error) {
	t := reflect.TypeOf(*object)
	fieldToJsonTags, jsonToDbTags, _, err := getFieldTags(t)
	if err != nil {
		return 0, nil, err
	}
	values := getJsonTagToValues(object, fieldToJsonTags)

	for jsonName, value := range values {
		if jsonName == "gid" {
			gid, ok := value.(uint64)
			if !ok {
				return 0, nil, fmt.Errorf("expected uint64 type for gid, got %T", value)
			}
			if gid != 0 {
				return gid, nil, nil
			}
		}
		if jsonToDbTags[jsonName] != nil && jsonToDbTags[jsonName].constraint == "unique" {
			return 0, &ConstrainedField{
				Key:   jsonName,
				Value: value,
			}, nil
		}
	}

	return 0, nil, fmt.Errorf("unique constraint not defined for any field on type %s", t.Name())
}

func upsertHelper[T any](ctx context.Context, db *DB, n *Namespace, object *T, readFrom bool) (uint64, *T, bool, error) {
	gid, cf, err := getUniqueConstraint(object)
	if err != nil {
		return 0, object, false, err
	}
	if gid != 0 {
		gid, object, err := getByGid[T](ctx, n, gid, readFrom)
		if err != nil {
			return 0, object, false, err
		}
		return gid, object, true, nil
	} else if cf != nil {
		gid, object, err := getByConstrainedField[T](ctx, n, *cf, readFrom)
		if err == nil {
			return gid, object, true, nil
		}
	}

	gid, err = db.z.nextUID()
	if err != nil {
		return 0, object, false, err
	}

	dms := make([]*dql.Mutation, 0)
	sch := &schema.ParsedSchema{}
	err = generateCreateDqlMutationsAndSchema(ctx, n, object, gid, &dms, sch)
	if err != nil {
		return 0, object, false, err
	}

	ctx = x.AttachNamespace(ctx, n.ID())

	err = n.alterSchemaWithParsed(ctx, sch)
	if err != nil {
		return 0, object, false, err
	}

	err = applyDqlMutations(ctx, db, dms)
	if err != nil {
		return 0, object, false, err
	}

	v := reflect.ValueOf(object).Elem()

	gidField := v.FieldByName("Gid")

	if gidField.IsValid() && gidField.CanSet() && gidField.Kind() == reflect.Uint64 {
		gidField.SetUint(gid)
	}

	return gid, object, false, nil
}
