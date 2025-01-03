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
	"strings"

	"github.com/dgraph-io/dgraph/v24/x"
	"github.com/hypermodeinc/modusdb/api/query_gen"
	"github.com/hypermodeinc/modusdb/api/utils"
)

type UniqueField interface {
	uint64 | ConstrainedField
}
type ConstrainedField struct {
	Key   string
	Value any
}

type QueryParams struct {
	Filter     *Filter
	Pagination *Pagination
	Sorting    *Sorting
}

type Filter struct {
	Field  string
	String StringPredicate
	Vector VectorPredicate
	And    *Filter
	Or     *Filter
	Not    *Filter
}

type Pagination struct {
	Limit  int64
	Offset int64
	After  string
}

type Sorting struct {
	OrderAscField  string
	OrderDescField string
	OrderDescFirst bool
}

type StringPredicate struct {
	Equals         string
	LessThan       string
	LessOrEqual    string
	GreaterThan    string
	GreaterOrEqual string
	AllOfTerms     []string
	AnyOfTerms     []string
	AllOfText      []string
	AnyOfText      []string
	RegExp         string
}

type VectorPredicate struct {
	SimilarTo []float32
	TopK      int64
}

type ModusDbOption func(*modusDbOptions)

type modusDbOptions struct {
	namespace uint64
}

func WithNamespace(namespace uint64) ModusDbOption {
	return func(o *modusDbOptions) {
		o.namespace = namespace
	}
}

func getDefaultNamespace(db *DB, ns ...uint64) (context.Context, *Namespace, error) {
	dbOpts := &modusDbOptions{
		namespace: db.defaultNamespace.ID(),
	}
	for _, ns := range ns {
		WithNamespace(ns)(dbOpts)
	}

	n, err := db.getNamespaceWithLock(dbOpts.namespace)
	if err != nil {
		return nil, nil, err
	}

	ctx := context.Background()
	ctx = x.AttachNamespace(ctx, n.ID())

	return ctx, n, nil
}

func filterToQueryFunc(typeName string, f Filter) query_gen.QueryFunc {
	// Handle logical operators first
	if f.And != nil {
		return query_gen.And(filterToQueryFunc(typeName, *f.And))
	}
	if f.Or != nil {
		return query_gen.Or(filterToQueryFunc(typeName, *f.Or))
	}
	if f.Not != nil {
		return query_gen.Not(filterToQueryFunc(typeName, *f.Not))
	}

	// Handle field predicates
	if f.String.Equals != "" {
		return query_gen.BuildEqQuery(utils.GetPredicateName(typeName, f.Field), f.String.Equals)
	}
	if len(f.String.AllOfTerms) != 0 {
		return query_gen.BuildAllOfTermsQuery(utils.GetPredicateName(typeName,
			f.Field), strings.Join(f.String.AllOfTerms, " "))
	}
	if len(f.String.AnyOfTerms) != 0 {
		return query_gen.BuildAnyOfTermsQuery(utils.GetPredicateName(typeName,
			f.Field), strings.Join(f.String.AnyOfTerms, " "))
	}
	if len(f.String.AllOfText) != 0 {
		return query_gen.BuildAllOfTextQuery(utils.GetPredicateName(typeName,
			f.Field), strings.Join(f.String.AllOfText, " "))
	}
	if len(f.String.AnyOfText) != 0 {
		return query_gen.BuildAnyOfTextQuery(utils.GetPredicateName(typeName,
			f.Field), strings.Join(f.String.AnyOfText, " "))
	}
	if f.String.RegExp != "" {
		return query_gen.BuildRegExpQuery(utils.GetPredicateName(typeName,
			f.Field), f.String.RegExp)
	}
	if f.String.LessThan != "" {
		return query_gen.BuildLtQuery(utils.GetPredicateName(typeName,
			f.Field), f.String.LessThan)
	}
	if f.String.LessOrEqual != "" {
		return query_gen.BuildLeQuery(utils.GetPredicateName(typeName,
			f.Field), f.String.LessOrEqual)
	}
	if f.String.GreaterThan != "" {
		return query_gen.BuildGtQuery(utils.GetPredicateName(typeName,
			f.Field), f.String.GreaterThan)
	}
	if f.String.GreaterOrEqual != "" {
		return query_gen.BuildGeQuery(utils.GetPredicateName(typeName,
			f.Field), f.String.GreaterOrEqual)
	}
	if f.Vector.SimilarTo != nil {
		return query_gen.BuildSimilarToQuery(utils.GetPredicateName(typeName,
			f.Field), f.Vector.TopK, f.Vector.SimilarTo)
	}

	// Return empty query if no conditions match
	return func() string { return "" }
}

// Helper function to combine multiple filters
func filtersToQueryFunc(typeName string, filter Filter) query_gen.QueryFunc {
	return filterToQueryFunc(typeName, filter)
}

func paginationToQueryString(p Pagination) string {
	paginationStr := ""
	if p.Limit > 0 {
		paginationStr += ", " + fmt.Sprintf("first: %d", p.Limit)
	}
	if p.Offset > 0 {
		paginationStr += ", " + fmt.Sprintf("offset: %d", p.Offset)
	} else if p.After != "" {
		paginationStr += ", " + fmt.Sprintf("after: %s", p.After)
	}
	if paginationStr == "" {
		return ""
	}
	return paginationStr
}

func sortingToQueryString(typeName string, s Sorting) string {
	if s.OrderAscField == "" && s.OrderDescField == "" {
		return ""
	}

	var parts []string
	first, second := s.OrderDescField, s.OrderAscField
	firstOp, secondOp := "orderdesc", "orderasc"

	if !s.OrderDescFirst {
		first, second = s.OrderAscField, s.OrderDescField
		firstOp, secondOp = "orderasc", "orderdesc"
	}

	if first != "" {
		parts = append(parts, fmt.Sprintf("%s: %s", firstOp, utils.GetPredicateName(typeName, first)))
	}
	if second != "" {
		parts = append(parts, fmt.Sprintf("%s: %s", secondOp, utils.GetPredicateName(typeName, second)))
	}

	return ", " + strings.Join(parts, ", ")
}
