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
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/hypermodeinc/modusdb/api/query_gen"
	"github.com/hypermodeinc/modusdb/api/utils"
)

func getByGid[T any](ctx context.Context, n *Namespace, gid uint64) (uint64, T, error) {
	return executeGet[T](ctx, n, gid)
}

func getByGidWithObject[T any](ctx context.Context, n *Namespace, gid uint64, obj T) (uint64, T, error) {
	return executeGetWithObject[T](ctx, n, obj, false, gid)
}

func getByConstrainedField[T any](ctx context.Context, n *Namespace, cf ConstrainedField) (uint64, T, error) {
	return executeGet[T](ctx, n, cf)
}

func getByConstrainedFieldWithObject[T any](ctx context.Context, n *Namespace,
	cf ConstrainedField, obj T) (uint64, T, error) {

	return executeGetWithObject[T](ctx, n, obj, false, cf)
}

func executeGet[T any, R UniqueField](ctx context.Context, n *Namespace, args ...R) (uint64, T, error) {
	var obj T
	if len(args) != 1 {
		return 0, obj, fmt.Errorf("expected 1 argument, got %d", len(args))
	}

	return executeGetWithObject(ctx, n, obj, true, args...)
}

func executeGetWithObject[T any, R UniqueField](ctx context.Context, n *Namespace,
	obj T, withReverse bool, args ...R) (uint64, T, error) {
	t := reflect.TypeOf(obj)

	fieldToJsonTags, jsonToDbTag, jsonToReverseEdgeTags, err := utils.GetFieldTags(t)
	if err != nil {
		return 0, obj, err
	}
	readFromQuery := ""
	if withReverse {
		for jsonTag, reverseEdgeTag := range jsonToReverseEdgeTags {
			readFromQuery += fmt.Sprintf(query_gen.ReverseEdgeQuery,
				utils.GetPredicateName(t.Name(), jsonTag), reverseEdgeTag)
		}
	}

	var cf ConstrainedField
	var query string
	gid, ok := any(args[0]).(uint64)
	if ok {
		query = query_gen.FormatObjQuery(query_gen.BuildUidQuery(gid), readFromQuery)
	} else if cf, ok = any(args[0]).(ConstrainedField); ok {
		query = query_gen.FormatObjQuery(query_gen.BuildEqQuery(utils.GetPredicateName(t.Name(),
			cf.Key), cf.Value), readFromQuery)
	} else {
		return 0, obj, fmt.Errorf("invalid unique field type")
	}

	if jsonToDbTag[cf.Key] != nil && jsonToDbTag[cf.Key].Constraint == "" {
		return 0, obj, fmt.Errorf("constraint not defined for field %s", cf.Key)
	}

	resp, err := n.queryWithLock(ctx, query)
	if err != nil {
		return 0, obj, err
	}

	dynamicType := utils.CreateDynamicStruct(t, fieldToJsonTags, 1)

	dynamicInstance := reflect.New(dynamicType).Interface()

	var result struct {
		Obj []any `json:"obj"`
	}

	result.Obj = append(result.Obj, dynamicInstance)

	// Unmarshal the JSON response into the dynamic struct
	if err := json.Unmarshal(resp.Json, &result); err != nil {
		return 0, obj, err
	}

	// Check if we have at least one object in the response
	if len(result.Obj) == 0 {
		return 0, obj, utils.ErrNoObjFound
	}

	return utils.ConvertDynamicToTyped[T](result.Obj[0], t)
}

func executeQuery[T any](ctx context.Context, n *Namespace, queryParams QueryParams,
	withReverse bool) ([]uint64, []T, error) {
	var obj T
	t := reflect.TypeOf(obj)
	fieldToJsonTags, _, jsonToReverseEdgeTags, err := utils.GetFieldTags(t)
	if err != nil {
		return nil, nil, err
	}

	var filterQueryFunc query_gen.QueryFunc = func() string {
		return ""
	}
	var paginationAndSorting string
	if queryParams.Filter != nil {
		filterQueryFunc = filtersToQueryFunc(t.Name(), *queryParams.Filter)
	}
	if queryParams.Pagination != nil || queryParams.Sorting != nil {
		var pagination, sorting string
		if queryParams.Pagination != nil {
			pagination = paginationToQueryString(*queryParams.Pagination)
		}
		if queryParams.Sorting != nil {
			sorting = sortingToQueryString(t.Name(), *queryParams.Sorting)
		}
		paginationAndSorting = fmt.Sprintf("%s %s", pagination, sorting)
	}

	readFromQuery := ""
	if withReverse {
		for jsonTag, reverseEdgeTag := range jsonToReverseEdgeTags {
			readFromQuery += fmt.Sprintf(query_gen.ReverseEdgeQuery, utils.GetPredicateName(t.Name(), jsonTag), reverseEdgeTag)
		}
	}

	query := query_gen.FormatObjsQuery(t.Name(), filterQueryFunc, paginationAndSorting, readFromQuery)

	resp, err := n.queryWithLock(ctx, query)
	if err != nil {
		return nil, nil, err
	}

	dynamicType := utils.CreateDynamicStruct(t, fieldToJsonTags, 1)

	var result struct {
		Objs []any `json:"objs"`
	}

	var tempMap map[string][]any
	if err := json.Unmarshal(resp.Json, &tempMap); err != nil {
		return nil, nil, err
	}

	// Determine the number of elements
	numElements := len(tempMap["objs"])

	// Append the interface the correct number of times
	for i := 0; i < numElements; i++ {
		result.Objs = append(result.Objs, reflect.New(dynamicType).Interface())
	}

	// Unmarshal the JSON response into the dynamic struct
	if err := json.Unmarshal(resp.Json, &result); err != nil {
		return nil, nil, err
	}

	gids := make([]uint64, len(result.Objs))
	objs := make([]T, len(result.Objs))
	for i, obj := range result.Objs {
		gid, typedObj, err := utils.ConvertDynamicToTyped[T](obj, t)
		if err != nil {
			return nil, nil, err
		}
		gids[i] = gid
		objs[i] = typedObj
	}

	return gids, objs, nil
}

func getExistingObject[T any](ctx context.Context, n *Namespace, gid uint64, cf *ConstrainedField,
	object T) (uint64, error) {
	var err error
	if gid != 0 {
		gid, _, err = getByGidWithObject[T](ctx, n, gid, object)
	} else if cf != nil {
		gid, _, err = getByConstrainedFieldWithObject[T](ctx, n, *cf, object)
	}
	if err != nil {
		return 0, err
	}
	return gid, nil
}
