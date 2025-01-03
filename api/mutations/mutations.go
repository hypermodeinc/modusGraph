package mutations

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/dgraph-io/dgraph/v24/protos/pb"
	"github.com/dgraph-io/dgraph/v24/schema"
	"github.com/hypermodeinc/modusdb/api/utils"
)

func HandleReverseEdge(jsonName string, value reflect.Type, nsId uint64, sch *schema.ParsedSchema,
	jsonToReverseEdgeTags map[string]string) error {
	if jsonToReverseEdgeTags[jsonName] == "" {
		return nil
	}

	if value.Kind() != reflect.Slice || value.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("reverse edge %s should be a slice of structs", jsonName)
	}

	reverseEdge := jsonToReverseEdgeTags[jsonName]
	typeName := strings.Split(reverseEdge, ".")[0]
	u := &pb.SchemaUpdate{
		Predicate: utils.AddNamespace(nsId, reverseEdge),
		ValueType: pb.Posting_UID,
		Directive: pb.SchemaUpdate_REVERSE,
	}

	sch.Preds = append(sch.Preds, u)
	sch.Types = append(sch.Types, &pb.TypeUpdate{
		TypeName: utils.AddNamespace(nsId, typeName),
		Fields:   []*pb.SchemaUpdate{u},
	})
	return nil
}

func CreateNQuadAndSchema(value any, gid uint64, jsonName string, t reflect.Type,
	nsId uint64) (*api.NQuad, *pb.SchemaUpdate, error) {
	valType, err := utils.ValueToPosting_ValType(value)
	if err != nil {
		return nil, nil, err
	}

	val, err := utils.ValueToApiVal(value)
	if err != nil {
		return nil, nil, err
	}

	nquad := &api.NQuad{
		Namespace: nsId,
		Subject:   fmt.Sprint(gid),
		Predicate: utils.GetPredicateName(t.Name(), jsonName),
	}

	u := &pb.SchemaUpdate{
		Predicate: utils.AddNamespace(nsId, utils.GetPredicateName(t.Name(), jsonName)),
		ValueType: valType,
	}

	if valType == pb.Posting_UID {
		nquad.ObjectId = fmt.Sprint(value)
		u.Directive = pb.SchemaUpdate_REVERSE
	} else {
		nquad.ObjectValue = val
	}

	return nquad, u, nil
}
