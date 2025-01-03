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
	uniqueConstraintFound := false
	for jsonName, value := range jsonTagToValue {
		if jsonToReverseEdgeTags[jsonName] != "" {
			continue
		}
		if jsonName == "gid" {
			uniqueConstraintFound = true
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
			value = newGid
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
				value = newGid
			}
		}
		valType, err = valueToPosting_ValType(value)
		if err != nil {
			return err
		}
		val, err = valueToApiVal(value)
		if err != nil {
			return err
		}

		nquad = &api.NQuad{
			Namespace: n.ID(),
			Subject:   fmt.Sprint(gid),
			Predicate: getPredicateName(t.Name(), jsonName),
		}

		if valType == pb.Posting_UID {
			nquad.ObjectId = fmt.Sprint(value)
		} else {
			nquad.ObjectValue = val
		}

		u := &pb.SchemaUpdate{
			Predicate: addNamespace(n.id, getPredicateName(t.Name(), jsonName)),
			ValueType: valType,
		}
		if jsonToDbTags[jsonName] != nil {
			constraint := jsonToDbTags[jsonName].constraint
			if constraint == "vector" && valType != pb.Posting_VFLOAT {
				return fmt.Errorf("vector index can only be applied to []float values")
			}
			uniqueConstraintFound = addIndex(u, constraint, uniqueConstraintFound)
		}

		sch.Preds = append(sch.Preds, u)
		nquads = append(nquads, nquad)
	}
	if !uniqueConstraintFound {
		return fmt.Errorf(NoUniqueConstr, t.Name())
	}
	sch.Types = append(sch.Types, &pb.TypeUpdate{
		TypeName: addNamespace(n.id, t.Name()),
		Fields:   sch.Preds,
	})

	val, err := valueToApiVal(t.Name())
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

func addIndex(u *pb.SchemaUpdate, index string, uniqueConstraintExists bool) bool {
	u.Directive = pb.SchemaUpdate_INDEX
	switch index {
	case "exact":
		u.Tokenizer = []string{"exact"}
	case "term":
		u.Tokenizer = []string{"term"}
	case "hash":
		u.Tokenizer = []string{"hash"}
	case "unique":
		u.Tokenizer = []string{"exact"}
		u.Unique = true
		u.Upsert = true
		uniqueConstraintExists = true
	case "fulltext":
		u.Tokenizer = []string{"fulltext"}
	case "trigram":
		u.Tokenizer = []string{"trigram"}
	case "vector":
		u.IndexSpecs = []*pb.VectorIndexSpec{
			{
				Name: "hnsw",
				Options: []*pb.OptionPair{
					{
						Key:   "metric",
						Value: "cosine",
					},
				},
			},
		}
	default:
		return uniqueConstraintExists
	}
	return uniqueConstraintExists
}
