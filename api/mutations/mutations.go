/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package mutations

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/hypermodeinc/dgraph/v24/protos/pb"
	"github.com/hypermodeinc/dgraph/v24/schema"
	"github.com/hypermodeinc/modusdb/api/apiutils"
	"github.com/hypermodeinc/modusdb/api/dgraphtypes"
)

func HandleReverseEdge(jsonName string, value reflect.Type, nsId uint64, sch *schema.ParsedSchema,
	reverseEdgeStr string) error {
	if reverseEdgeStr == "" {
		return nil
	}

	if value.Kind() != reflect.Slice || value.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("reverse edge %s should be a slice of structs", jsonName)
	}

	typeName := strings.Split(reverseEdgeStr, ".")[0]
	u := &pb.SchemaUpdate{
		Predicate: apiutils.AddNamespace(nsId, reverseEdgeStr),
		ValueType: pb.Posting_UID,
		Directive: pb.SchemaUpdate_REVERSE,
	}

	sch.Preds = append(sch.Preds, u)
	sch.Types = append(sch.Types, &pb.TypeUpdate{
		TypeName: apiutils.AddNamespace(nsId, typeName),
		Fields:   []*pb.SchemaUpdate{u},
	})
	return nil
}

func CreateNQuadAndSchema(value any, gid uint64, jsonName string, t reflect.Type,
	nsId uint64) (*api.NQuad, *pb.SchemaUpdate, error) {
	valType, err := dgraphtypes.ValueToPosting_ValType(value)
	if err != nil {
		return nil, nil, err
	}

	// val can be null here for "empty" no-scalar types
	val, err := dgraphtypes.ValueToApiVal(value)
	if err != nil {
		return nil, nil, err
	}

	nquad := &api.NQuad{
		Namespace: nsId,
		Subject:   fmt.Sprint(gid),
		Predicate: apiutils.GetPredicateName(t.Name(), jsonName),
	}

	u := &pb.SchemaUpdate{
		Predicate: apiutils.AddNamespace(nsId, apiutils.GetPredicateName(t.Name(), jsonName)),
		ValueType: valType,
	}

	if valType == pb.Posting_UID {
		nquad.ObjectId = fmt.Sprint(value)
		u.Directive = pb.SchemaUpdate_REVERSE
	} else if val != nil {
		nquad.ObjectValue = val
	}

	return nquad, u, nil
}
