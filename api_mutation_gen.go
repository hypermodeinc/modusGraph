/*
 * Copyright 2025 Hypermode Inc.
 * Licensed under the terms of the Apache License, Version 2.0
 * See the LICENSE file that accompanied this code for further details.
 *
 * SPDX-FileCopyrightText: 2025 Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusdb

import (
	"context"
	"fmt"
	"reflect"

	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/dgraph-io/dgraph/v24/dql"
	"github.com/dgraph-io/dgraph/v24/protos/pb"
	"github.com/dgraph-io/dgraph/v24/schema"
	"github.com/dgraph-io/dgraph/v24/x"
	"github.com/hypermodeinc/modusdb/api/mutations"
	"github.com/hypermodeinc/modusdb/api/utils"
)

func generateSetDqlMutationsAndSchema[T any](ctx context.Context, n *Namespace, object T,
	gid uint64, dms *[]*dql.Mutation, sch *schema.ParsedSchema) error {
	t := reflect.TypeOf(object)
	if t.Kind() != reflect.Struct {
		return fmt.Errorf("expected struct, got %s", t.Kind())
	}

	fieldToJsonTags, jsonToDbTags, jsonToReverseEdgeTags, err := utils.GetFieldTags(t)
	if err != nil {
		return err
	}
	jsonTagToValue := utils.GetJsonTagToValues(object, fieldToJsonTags)

	nquads := make([]*api.NQuad, 0)
	uniqueConstraintFound := false
	for jsonName, value := range jsonTagToValue {

		reflectValueType := reflect.TypeOf(value)
		var nquad *api.NQuad

		if jsonToReverseEdgeTags[jsonName] != "" {
			if err := mutations.HandleReverseEdge(jsonName, reflectValueType, n.ID(), sch, jsonToReverseEdgeTags); err != nil {
				return err
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

		uniqueConstraintFound, err = utils.HandleConstraints(u, jsonToDbTags, jsonName, u.ValueType, uniqueConstraintFound)
		if err != nil {
			return err
		}

		sch.Preds = append(sch.Preds, u)
		nquads = append(nquads, nquad)
	}
	if !uniqueConstraintFound {
		return fmt.Errorf(utils.NoUniqueConstr, t.Name())
	}

	sch.Types = append(sch.Types, &pb.TypeUpdate{
		TypeName: utils.AddNamespace(n.ID(), t.Name()),
		Fields:   sch.Preds,
	})

	val, err := utils.ValueToApiVal(t.Name())
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
