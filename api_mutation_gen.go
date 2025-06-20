/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusgraph

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/hypermodeinc/dgraph/v25/dql"
	"github.com/hypermodeinc/dgraph/v25/protos/pb"
	"github.com/hypermodeinc/dgraph/v25/schema"
	"github.com/hypermodeinc/dgraph/v25/x"
	"github.com/hypermodeinc/modusgraph/api/apiutils"
	"github.com/hypermodeinc/modusgraph/api/dgraphtypes"
	"github.com/hypermodeinc/modusgraph/api/mutations"
	"github.com/hypermodeinc/modusgraph/api/structreflect"
)

func generateSetDqlMutationsAndSchema[T any](ctx context.Context, n *Namespace, object T,
	gid uint64, dms *[]*dql.Mutation, sch *schema.ParsedSchema) error {
	t := reflect.TypeOf(object)
	if t.Kind() != reflect.Struct {
		return fmt.Errorf("expected struct, got %s", t.Kind())
	}

	tagMaps, err := structreflect.GetFieldTags(t)
	if err != nil {
		return err
	}
	jsonTagToValue := structreflect.GetJsonTagToValues(object, tagMaps.FieldToJson)

	nquads := make([]*api.NQuad, 0)
	uniqueConstraintFound := false
	for jsonName, value := range jsonTagToValue {

		reflectValueType := reflect.TypeOf(value)
		var nquad *api.NQuad

		if tagMaps.JsonToReverseEdge[jsonName] != "" {
			reverseEdgeStr := tagMaps.JsonToReverseEdge[jsonName]
			typeName := strings.Split(reverseEdgeStr, ".")[0]
			currSchema, err := getSchema(ctx, n)
			if err != nil {
				return err
			}

			typeFound := false
			predicateFound := false
			for _, t := range currSchema.Types {
				if t.Name == typeName {
					typeFound = true
					for _, f := range t.Fields {
						if f.Name == reverseEdgeStr {
							predicateFound = true
							break
						}
					}
					break
				}
			}

			if !(typeFound && predicateFound) {
				if err := mutations.HandleReverseEdge(jsonName, reflectValueType, n.ID(), sch,
					reverseEdgeStr); err != nil {
					return err
				}
			}
			continue
		}
		if jsonName == "gid" {
			uniqueConstraintFound = true
			continue
		}

		value, err = processStructValue(ctx, value, n)
		if err != nil {
			return err
		}

		value, err = processPointerValue(ctx, value, n)
		if err != nil {
			return err
		}

		nquad, u, err := mutations.CreateNQuadAndSchema(value, gid, jsonName, t, n.ID())
		if err != nil {
			return err
		}

		uniqueConstraintFound, err = dgraphtypes.HandleConstraints(u, tagMaps.JsonToDb,
			jsonName, u.ValueType, uniqueConstraintFound)
		if err != nil {
			return err
		}

		sch.Preds = append(sch.Preds, u)
		// Handle nil object values - only skip geo types with nil values
		if nquad.ObjectValue == nil && (strings.Contains(nquad.Predicate, ".multiArea") ||
			strings.Contains(nquad.Predicate, ".area") ||
			strings.Contains(nquad.Predicate, ".loc")) {
			continue
		}
		nquads = append(nquads, nquad)
	}
	if !uniqueConstraintFound {
		return fmt.Errorf(apiutils.NoUniqueConstr, t.Name())
	}

	sch.Types = append(sch.Types, &pb.TypeUpdate{
		TypeName: apiutils.AddNamespace(n.ID(), t.Name()),
		Fields:   sch.Preds,
	})

	val, err := dgraphtypes.ValueToApiVal(t.Name())
	if err != nil {
		return err
	}
	typeNquad := &api.NQuad{
		Namespace:   n.ID(),
		Subject:     fmt.Sprint(gid),
		Predicate:   "dgraph.type",
		ObjectValue: val,
	}
	nquads = append(nquads, typeNquad)

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
