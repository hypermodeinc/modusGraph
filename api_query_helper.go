package modusdb

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func getByGid[T any](ctx context.Context, n *Namespace, gid uint64) (uint64, *T, error) {
	return executeGet[T](ctx, n, gid)
}

func getByGidWithObject[T any](ctx context.Context, n *Namespace, gid uint64, obj T) (uint64, *T, error) {
	return executeGetWithObject[T](ctx, n, obj, false, gid)
}

func getByConstrainedField[T any](ctx context.Context, n *Namespace, cf ConstrainedField) (uint64, *T, error) {
	return executeGet[T](ctx, n, cf)
}

func getByConstrainedFieldWithObject[T any](ctx context.Context, n *Namespace,
	cf ConstrainedField, obj T) (uint64, *T, error) {

	return executeGetWithObject[T](ctx, n, obj, false, cf)
}

func executeGet[T any, R UniqueField](ctx context.Context, n *Namespace, args ...R) (uint64, *T, error) {
	if len(args) != 1 {
		return 0, nil, fmt.Errorf("expected 1 argument, got %d", len(args))
	}

	var obj T

	return executeGetWithObject(ctx, n, obj, true, args...)
}

func executeGetWithObject[T any, R UniqueField](ctx context.Context, n *Namespace,
	obj T, withReverse bool, args ...R) (uint64, *T, error) {
	t := reflect.TypeOf(obj)

	fieldToJsonTags, jsonToDbTag, jsonToReverseEdgeTags, err := getFieldTags(t)
	if err != nil {
		return 0, nil, err
	}
	readFromQuery := ""
	if withReverse {
		for jsonTag, reverseEdgeTag := range jsonToReverseEdgeTags {
			readFromQuery += fmt.Sprintf(`
		%s: ~%s {
			uid
			expand(_all_)
			dgraph.type
		}
		`, getPredicateName(t.Name(), jsonTag), reverseEdgeTag)
		}
	}

	var cf ConstrainedField
	var query string
	gid, ok := any(args[0]).(uint64)
	if ok {
		query = formatObjQuery(buildUidQuery(gid), readFromQuery)
	} else if cf, ok = any(args[0]).(ConstrainedField); ok {
		query = formatObjQuery(buildEqQuery(getPredicateName(t.Name(), cf.Key), cf.Value), readFromQuery)
	} else {
		return 0, nil, fmt.Errorf("invalid unique field type")
	}

	if jsonToDbTag[cf.Key] != nil && jsonToDbTag[cf.Key].constraint == "" {
		return 0, nil, fmt.Errorf("constraint not defined for field %s", cf.Key)
	}

	resp, err := n.queryWithLock(ctx, query)
	if err != nil {
		return 0, nil, err
	}

	dynamicType := createDynamicStruct(t, fieldToJsonTags, 1)

	dynamicInstance := reflect.New(dynamicType).Interface()

	var result struct {
		Obj []any `json:"obj"`
	}

	result.Obj = append(result.Obj, dynamicInstance)

	// Unmarshal the JSON response into the dynamic struct
	if err := json.Unmarshal(resp.Json, &result); err != nil {
		return 0, nil, err
	}

	// Check if we have at least one object in the response
	if len(result.Obj) == 0 {
		return 0, nil, ErrNoObjFound
	}

	// Map the dynamic struct to the final type T
	finalObject := reflect.New(t).Interface()
	gid, err = mapDynamicToFinal(result.Obj[0], finalObject)
	if err != nil {
		return 0, nil, err
	}

	// Convert to *interface{} then to *T
	if ifacePtr, ok := finalObject.(*interface{}); ok {
		if typedPtr, ok := (*ifacePtr).(*T); ok {
			return gid, typedPtr, nil
		}
	}

	// If conversion fails, try direct conversion
	if typedPtr, ok := finalObject.(*T); ok {
		return gid, typedPtr, nil
	}

	if dirType, ok := finalObject.(T); ok {
		return gid, &dirType, nil
	}

	return 0, nil, fmt.Errorf("failed to convert type %T to %T", finalObject, obj)
}

func getByGidRaw(ctx context.Context, n *Namespace, gid uint64,
	fields []string) (uint64, map[string]any, error) {
	return executeGetRaw(ctx, n, fields, gid)
}

func getByConstrainedFieldRaw(ctx context.Context, n *Namespace,
	cf ConstrainedField, fields []string) (uint64, map[string]any, error) {
	return executeGetRaw(ctx, n, fields, cf)
}

func executeGetRaw[R UniqueField](ctx context.Context, n *Namespace,
	fields []string, args ...R) (uint64, map[string]any, error) {
	if len(args) != 1 {
		return 0, nil, fmt.Errorf("expected 1 argument, got %d", len(args))
	}

	var query string
	gid, ok := any(args[0]).(uint64)
	fieldsStr := strings.Join(fields, "\n")
	if ok {
		query = formatUnstructuredQuery(buildUidQuery(gid), fieldsStr)
	} else if cf, ok := any(args[0]).(ConstrainedField); ok {
		query = formatUnstructuredQuery(buildEqQuery(cf.Key, cf.Value), fieldsStr)
	} else {
		return 0, nil, fmt.Errorf("invalid unique field type")
	}

	resp, err := n.queryWithLock(ctx, query)
	if err != nil {
		return 0, nil, err
	}

	var result struct {
		Obj []map[string]any `json:"obj"`
	}

	// Unmarshal the JSON response into the dynamic struct
	if err := json.Unmarshal(resp.Json, &result); err != nil {
		return 0, nil, err
	}

	// Check if we have at least one object in the response
	if len(result.Obj) == 0 {
		return 0, nil, ErrNoObjFound
	}

	var returnGid uint64
	if result.Obj[0]["uid"] == nil {
		return 0, nil, ErrNoObjFound
	} else {
		gidStr := result.Obj[0]["uid"].(string)
		returnGid, err = strconv.ParseUint(gidStr, 0, 64)
		if err != nil {
			return 0, nil, err
		}
	}

	return returnGid, result.Obj[0], nil
}
