package modusdb

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
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

type UniqueField interface {
	uint64 | ConstrainedField
}
type ConstrainedField struct {
	Key   string
	Value any
}

func getFieldTags(t reflect.Type) (jsonTags map[string]string, reverseEdgeTags map[string]string, err error) {
	jsonTags = make(map[string]string)
	reverseEdgeTags = make(map[string]string)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		jsonTag := field.Tag.Get("json")
		if jsonTag == "" {
			return nil, nil, fmt.Errorf("field %s has no json tag", field.Name)
		}
		jsonName := strings.Split(jsonTag, ",")[0]
		jsonTags[field.Name] = jsonName
		reverseEdgeTag := field.Tag.Get("readFrom")
		if reverseEdgeTag != "" {
			typeAndField := strings.Split(reverseEdgeTag, ",")
			if len(typeAndField) != 2 {
				return nil, nil, fmt.Errorf(`field %s has invalid readFrom tag, 
				expected format is type=<type>,field=<field>`, field.Name)
			}
			t := strings.Split(typeAndField[0], "=")[1]
			f := strings.Split(typeAndField[1], "=")[1]
			reverseEdgeTags[field.Name] = getPredicateName(t, f)
		}
	}
	return jsonTags, reverseEdgeTags, nil
}

func getFieldValues(object any, jsonFields map[string]string) map[string]any {
	values := make(map[string]any)
	v := reflect.ValueOf(object).Elem()
	for fieldName, jsonName := range jsonFields {
		fieldValue := v.FieldByName(fieldName)
		values[jsonName] = fieldValue.Interface()

	}
	return values
}

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

func generateDqlMutationAndSchema[T any](n *Namespace, object *T,
	uid uint64) ([]*dql.Mutation, *schema.ParsedSchema, error) {
	t := reflect.TypeOf(*object)
	if t.Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("expected struct, got %s", t.Kind())
	}

	jsonFields, _, err := getFieldTags(t)
	if err != nil {
		return nil, nil, err
	}
	values := getFieldValues(object, jsonFields)
	sch := &schema.ParsedSchema{}

	nquads := make([]*api.NQuad, 0)
	for jsonName, value := range values {
		if jsonName == "uid" {
			continue
		}
		valType, err := valueToPosting_ValType(value)
		if err != nil {
			return nil, nil, err
		}
		sch.Preds = append(sch.Preds, &pb.SchemaUpdate{
			Predicate: addNamespace(n.id, getPredicateName(t.Name(), jsonName)),
			ValueType: valType,
		})
		val, err := valueToValType(value)
		if err != nil {
			return nil, nil, err
		}
		nquad := &api.NQuad{
			Namespace:   n.ID(),
			Subject:     fmt.Sprint(uid),
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
		Subject:     fmt.Sprint(uid),
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

func Create[T any](ctx context.Context, n *Namespace, object *T) (uint64, *T, error) {
	n.db.mutex.Lock()
	defer n.db.mutex.Unlock()
	uid, err := n.db.z.nextUID()
	if err != nil {
		return 0, object, err
	}

	dms, sch, err := generateDqlMutationAndSchema(n, object, uid)
	if err != nil {
		return 0, object, err
	}

	edges, err := query.ToDirectedEdges(dms, nil)
	if err != nil {
		return 0, object, err
	}
	ctx = x.AttachNamespace(ctx, n.ID())

	err = n.alterSchemaWithParsed(ctx, sch)
	if err != nil {
		return 0, object, err
	}

	if !n.db.isOpen {
		return 0, object, ErrClosedDB
	}

	startTs, err := n.db.z.nextTs()
	if err != nil {
		return 0, object, err
	}
	commitTs, err := n.db.z.nextTs()
	if err != nil {
		return 0, object, err
	}

	m := &pb.Mutations{
		GroupId: 1,
		StartTs: startTs,
		Edges:   edges,
	}
	m.Edges, err = query.ExpandEdges(ctx, m)
	if err != nil {
		return 0, object, fmt.Errorf("error expanding edges: %w", err)
	}

	p := &pb.Proposal{Mutations: m, StartTs: startTs}
	if err := worker.ApplyMutations(ctx, p); err != nil {
		return 0, object, err
	}

	err = worker.ApplyCommited(ctx, &pb.OracleDelta{
		Txns: []*pb.TxnStatus{{StartTs: startTs, CommitTs: commitTs}},
	})
	if err != nil {
		return 0, object, err
	}

	v := reflect.ValueOf(object).Elem()

	uidField := v.FieldByName("Uid")

	if uidField.IsValid() && uidField.CanSet() && uidField.Kind() == reflect.Uint64 {
		uidField.SetUint(uid)
	}

	return uid, object, nil
}

func createDynamicStruct(t reflect.Type, jsonFields map[string]string) reflect.Type {
	fields := make([]reflect.StructField, 0, len(jsonFields))
	for fieldName, jsonName := range jsonFields {
		field, _ := t.FieldByName(fieldName)
		fields = append(fields, reflect.StructField{
			Name: field.Name,
			Type: field.Type,
			Tag:  reflect.StructTag(fmt.Sprintf(`json:"%s.%s"`, t.Name(), jsonName)),
		})
	}
	return reflect.StructOf(fields)
}

func mapDynamicToFinal(dynamic any, final any) {
	vFinal := reflect.ValueOf(final).Elem()
	vDynamic := reflect.ValueOf(dynamic).Elem()

	for i := 0; i < vDynamic.NumField(); i++ {
		field := vDynamic.Type().Field(i)
		value := vDynamic.Field(i)

		finalField := vFinal.FieldByName(field.Name)
		if finalField.IsValid() && finalField.CanSet() {
			finalField.Set(value)
		}
	}
}

func Get[T any, R UniqueField](ctx context.Context, n *Namespace, uniqueField R) (*T, error) {
	if uid, ok := any(uniqueField).(uint64); ok {
		return getByUid[T](ctx, n, uid)
	}

	if cf, ok := any(uniqueField).(ConstrainedField); ok {
		return getByConstrainedField[T](ctx, n, cf)
	}

	return nil, fmt.Errorf("invalid unique field type")
}

func getByUid[T any](ctx context.Context, n *Namespace, uid uint64) (*T, error) {
	query := fmt.Sprintf(`
	{
	  obj(func: uid(%d)) {
		uid
		expand(_all_)
	  }
	}
	  `, uid)

	return executeGet[T](ctx, n, query)
}

func getByConstrainedField[T any](ctx context.Context, n *Namespace, cf ConstrainedField) (*T, error) {
	query := fmt.Sprintf(`
	{
	  obj(func: eq(%s, %s)) {
		uid
		expand(_all_)
	  }
	}
	  `, cf.Key, cf.Value)

	return executeGet[T](ctx, n, query)
}

func executeGet[T any](ctx context.Context, n *Namespace, query string) (*T, error) {
	resp, err := n.Query(ctx, query)
	if err != nil {
		return nil, err
	}

	var obj T

	t := reflect.TypeOf(obj)

	jsonFields, _, err := getFieldTags(t)
	if err != nil {
		return nil, err
	}

	dynamicType := createDynamicStruct(t, jsonFields)

	dynamicInstance := reflect.New(dynamicType).Interface()

	var result struct {
		Obj []any `json:"obj"`
	}

	result.Obj = append(result.Obj, dynamicInstance)

	// Unmarshal the JSON response into the dynamic struct
	if err := json.Unmarshal(resp.Json, &result); err != nil {
		return nil, err
	}

	// Check if we have at least one object in the response
	if len(result.Obj) == 0 {
		return nil, fmt.Errorf("no object found")
	}

	// Map the dynamic struct to the final type T
	finalObject := reflect.New(t).Interface()
	mapDynamicToFinal(result.Obj[0], finalObject)

	return finalObject.(*T), nil
}
