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

	"github.com/hypermodeinc/dgraph/v24/x"
	"github.com/hypermodeinc/modusdb/api/apiutils"
	"github.com/hypermodeinc/modusdb/api/querygen"
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
	ns uint64
}

func WithNamespace(ns uint64) ModusDbOption {
	return func(o *modusDbOptions) {
		o.ns = ns
	}
}

func getDefaultNamespace(engine *Engine, nsId ...uint64) (context.Context, *Namespace, error) {
	dbOpts := &modusDbOptions{
		ns: engine.db0.ID(),
	}
	for _, ns := range nsId {
		WithNamespace(ns)(dbOpts)
	}

	d, err := engine.getNamespaceWithLock(dbOpts.ns)
	if err != nil {
		return nil, nil, err
	}

	ctx := context.Background()
	ctx = x.AttachNamespace(ctx, d.ID())

	return ctx, d, nil
}

func filterToQueryFunc(typeName string, f Filter) querygen.QueryFunc {
	// Handle logical operators first
	if f.And != nil {
		return querygen.And(filterToQueryFunc(typeName, *f.And))
	}
	if f.Or != nil {
		return querygen.Or(filterToQueryFunc(typeName, *f.Or))
	}
	if f.Not != nil {
		return querygen.Not(filterToQueryFunc(typeName, *f.Not))
	}

	// Handle field predicates
	if f.String.Equals != "" {
		return querygen.BuildEqQuery(apiutils.GetPredicateName(typeName, f.Field), f.String.Equals)
	}
	if len(f.String.AllOfTerms) != 0 {
		return querygen.BuildAllOfTermsQuery(apiutils.GetPredicateName(typeName,
			f.Field), strings.Join(f.String.AllOfTerms, " "))
	}
	if len(f.String.AnyOfTerms) != 0 {
		return querygen.BuildAnyOfTermsQuery(apiutils.GetPredicateName(typeName,
			f.Field), strings.Join(f.String.AnyOfTerms, " "))
	}
	if len(f.String.AllOfText) != 0 {
		return querygen.BuildAllOfTextQuery(apiutils.GetPredicateName(typeName,
			f.Field), strings.Join(f.String.AllOfText, " "))
	}
	if len(f.String.AnyOfText) != 0 {
		return querygen.BuildAnyOfTextQuery(apiutils.GetPredicateName(typeName,
			f.Field), strings.Join(f.String.AnyOfText, " "))
	}
	if f.String.RegExp != "" {
		return querygen.BuildRegExpQuery(apiutils.GetPredicateName(typeName,
			f.Field), f.String.RegExp)
	}
	if f.String.LessThan != "" {
		return querygen.BuildLtQuery(apiutils.GetPredicateName(typeName,
			f.Field), f.String.LessThan)
	}
	if f.String.LessOrEqual != "" {
		return querygen.BuildLeQuery(apiutils.GetPredicateName(typeName,
			f.Field), f.String.LessOrEqual)
	}
	if f.String.GreaterThan != "" {
		return querygen.BuildGtQuery(apiutils.GetPredicateName(typeName,
			f.Field), f.String.GreaterThan)
	}
	if f.String.GreaterOrEqual != "" {
		return querygen.BuildGeQuery(apiutils.GetPredicateName(typeName,
			f.Field), f.String.GreaterOrEqual)
	}
	if f.Vector.SimilarTo != nil {
		return querygen.BuildSimilarToQuery(apiutils.GetPredicateName(typeName,
			f.Field), f.Vector.TopK, f.Vector.SimilarTo)
	}

	// Return empty query if no conditions match
	return func() string { return "" }
}

// Helper function to combine multiple filters
func filtersToQueryFunc(typeName string, filter Filter) querygen.QueryFunc {
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
		parts = append(parts, fmt.Sprintf("%s: %s", firstOp, apiutils.GetPredicateName(typeName, first)))
	}
	if second != "" {
		parts = append(parts, fmt.Sprintf("%s: %s", secondOp, apiutils.GetPredicateName(typeName, second)))
	}

	return ", " + strings.Join(parts, ", ")
}
