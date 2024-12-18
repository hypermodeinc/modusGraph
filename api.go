package modusdb

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/dgraph-io/dgraph/v24/dql"
	"github.com/dgraph-io/dgraph/v24/protos/pb"
	"github.com/dgraph-io/dgraph/v24/query"
	"github.com/dgraph-io/dgraph/v24/schema"
	"github.com/dgraph-io/dgraph/v24/worker"
	"github.com/dgraph-io/dgraph/v24/x"
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

func valueToPosting_ValType(v any) pb.Posting_ValType {
	switch v.(type) {
	case string:
		return pb.Posting_STRING
	case int:
		return pb.Posting_INT
	case int64:
		return pb.Posting_INT
	case bool:
		return pb.Posting_BOOL
	case float64:
		return pb.Posting_FLOAT
	default:
		return pb.Posting_DEFAULT
	}
}

func valueToValType(v any) *api.Value {
	switch val := v.(type) {
	case string:
		return &api.Value{Val: &api.Value_StrVal{StrVal: val}}
	case int:
		return &api.Value{Val: &api.Value_IntVal{IntVal: int64(val)}}
	case int64:
		return &api.Value{Val: &api.Value_IntVal{IntVal: val}}
	case bool:
		return &api.Value{Val: &api.Value_BoolVal{BoolVal: val}}
	case float64:
		return &api.Value{Val: &api.Value_DoubleVal{DoubleVal: val}}
	default:
		return &api.Value{Val: &api.Value_DefaultVal{DefaultVal: fmt.Sprint(v)}}
	}
}

func Create[T any](ctx context.Context, n *Namespace, object *T) (uint64, *T, error) {
	n.db.mutex.Lock()
	defer n.db.mutex.Unlock()
	uids, err := n.db.z.nextUIDs(&pb.Num{Val: uint64(1), Type: pb.Num_UID})
	if err != nil {
		return 0, object, err
	}

	t := reflect.TypeOf(*object)
	if t.Kind() != reflect.Struct {
		return 0, object, fmt.Errorf("expected struct, got %s", t.Kind())
	}

	jsonFields, _, err := getFieldTags(t)
	if err != nil {
		return 0, object, err
	}
	values := getFieldValues(object, jsonFields)
	sch := &schema.ParsedSchema{}

	nquads := make([]*api.NQuad, 0)
	for jsonName, value := range values {
		if jsonName == "uid" {
			continue
		}
		sch.Preds = append(sch.Preds, &pb.SchemaUpdate{
			Predicate: addNamespace(n.id, getPredicateName(t.Name(), jsonName)),
			ValueType: valueToPosting_ValType(value),
		})
		nquad := &api.NQuad{
			Namespace:   n.ID(),
			Subject:     fmt.Sprint(uids.StartId),
			Predicate:   getPredicateName(t.Name(), jsonName),
			ObjectValue: valueToValType(value),
		}
		nquads = append(nquads, nquad)
	}
	sch.Types = append(sch.Types, &pb.TypeUpdate{
		TypeName: addNamespace(n.id, t.Name()),
		Fields:   sch.Preds,
	})

	dms := make([]*dql.Mutation, 0)
	dms = append(dms, &dql.Mutation{
		Set: nquads,
	})
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
		uidField.SetUint(uids.StartId)
	}

	return uids.StartId, object, nil
}
