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

func valueToValType(v any) (*api.Value, error) {
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

func generateCreateDqlMutationsAndSchema[T any](n *Namespace, object *T,
	gid uint64) ([]*dql.Mutation, *schema.ParsedSchema, error) {
	t := reflect.TypeOf(*object)
	if t.Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("expected struct, got %s", t.Kind())
	}

	jsonFields, dbFields, _, err := getFieldTags(t)
	if err != nil {
		return nil, nil, err
	}
	values := getFieldValues(object, jsonFields)
	sch := &schema.ParsedSchema{}

	nquads := make([]*api.NQuad, 0)
	for jsonName, value := range values {
		if jsonName == "gid" {
			continue
		}
		valType, err := valueToPosting_ValType(value)
		if err != nil {
			return nil, nil, err
		}
		u := &pb.SchemaUpdate{
			Predicate: addNamespace(n.id, getPredicateName(t.Name(), jsonName)),
			ValueType: valType,
		}
		if dbFields[jsonName] != nil && dbFields[jsonName].constraint == "unique" {
			u.Directive = pb.SchemaUpdate_INDEX
			u.Tokenizer = []string{"exact"}
		}
		sch.Preds = append(sch.Preds, u)
		val, err := valueToValType(value)
		if err != nil {
			return nil, nil, err
		}
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

	val, err := valueToValType(t.Name())
	if err != nil {
		return nil, nil, err
	}
	nquad := &api.NQuad{
		Namespace:   n.ID(),
		Subject:     fmt.Sprint(gid),
		Predicate:   "dgraph.type",
		ObjectValue: val,
	}
	nquads = append(nquads, nquad)

	dms := make([]*dql.Mutation, 0)
	dms = append(dms, &dql.Mutation{
		Set: nquads,
	})

	return dms, sch, nil
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
	query := fmt.Sprintf(`
	{
	  obj(func: uid(%d)) {
		uid
		expand(_all_)
		dgraph.type
	  }
	}
	  `, gid)

	return executeGet[T](ctx, n, query, nil)
}

func getByConstrainedField[T any](ctx context.Context, n *Namespace, cf ConstrainedField) (uint64, *T, error) {
	var obj T

	t := reflect.TypeOf(obj)
	query := fmt.Sprintf(`
	{
	  obj(func: eq(%s, %s)) {
		uid
		expand(_all_)
		dgraph.type
	  }
	}
	  `, getPredicateName(t.Name(), cf.Key), cf.Value)

	return executeGet[T](ctx, n, query, &cf)
}

func executeGet[T any](ctx context.Context, n *Namespace, query string, cf *ConstrainedField) (uint64, *T, error) {
	var obj T

	t := reflect.TypeOf(obj)

	jsonFields, dbTags, _, err := getFieldTags(t)
	if err != nil {
		return 0, nil, err
	}

	if cf != nil && dbTags[cf.Key].constraint == "" {
		return 0, nil, fmt.Errorf("constraint not defined for field %s", cf.Key)
	}

	resp, err := n.queryWithLock(ctx, query)
	if err != nil {
		return 0, nil, err
	}

	dynamicType := createDynamicStruct(t, jsonFields)

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
	gid, err := mapDynamicToFinal(result.Obj[0], finalObject)
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
