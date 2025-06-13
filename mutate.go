/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusgraph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	dg "github.com/dolan-in/dgman/v2"
)

func (c client) process(ctx context.Context,
	obj any, operation string,
	txFunc func(*dg.TxnContext, any) ([]string, error)) error {

	objType := reflect.TypeOf(obj)
	objKind := objType.Kind()
	var schemaObj any

	if objKind == reflect.Slice {
		sliceValue := reflect.Indirect(reflect.ValueOf(obj))
		if sliceValue.Len() == 0 {
			return errors.New("slice cannot be empty")
		}
		schemaObj = sliceValue.Index(0).Interface()
	} else {
		schemaObj = obj
	}

	err := checkPointer(schemaObj)
	if err != nil {
		if objKind == reflect.Slice {
			err = errors.Join(err, errors.New("slice elements must be pointers"))
		}
		return err
	}

	if c.options.autoSchema {
		err := c.UpdateSchema(ctx, schemaObj)
		if err != nil {
			return err
		}
	}

	client, err := c.pool.get()
	if err != nil {
		c.logger.Error(err, "Failed to get client from pool")
		return err
	}
	defer c.pool.put(client)

	tx := dg.NewTxnContext(ctx, client).SetCommitNow()
	uids, err := txFunc(tx, obj)
	if err != nil {
		return err
	}
	c.logger.V(2).Info(operation+" successful", "uidCount", len(uids))
	return nil
}

func (c client) mutate(ctx context.Context, obj any, insert bool) error {
	objType := reflect.TypeOf(obj)
	objKind := objType.Kind()
	var schemaObj any

	if objKind == reflect.Slice {
		sliceValue := reflect.Indirect(reflect.ValueOf(obj))
		if sliceValue.Len() == 0 {
			err := errors.New("slice cannot be empty")
			return err
		}
		schemaObj = sliceValue.Index(0).Interface()
	} else {
		schemaObj = obj
	}

	err := checkPointer(schemaObj)
	if err != nil {
		if objKind == reflect.Slice {
			err = errors.Join(err, errors.New("slice elements must be pointers"))
		}
		return err
	}

	if c.options.autoSchema {
		err := c.UpdateSchema(ctx, schemaObj)
		if err != nil {
			return err
		}
	}

	uniquePredicates := getUniquePredicates(schemaObj)
	if len(uniquePredicates) > 0 {
		nodeType := getNodeType(schemaObj)
		query, vars := generateUniquePredicateQuery(uniquePredicates, nodeType)
		resp, err := c.QueryRaw(ctx, query, vars)
		if err != nil {
			return err
		}
		uid, err := extractUIDFromDgraphQueryResult(resp)
		if err != nil {
			return err
		}
		if uid != "" && (insert || uid != getUIDValue(schemaObj)) {
			return &dg.UniqueError{NodeType: nodeType, UID: uid}
		}
	}

	client, err := c.pool.get()
	if err != nil {
		c.logger.Error(err, "Failed to get client from pool")
		return err
	}
	defer c.pool.put(client)

	tx := dg.NewTxnContext(ctx, client).SetCommitNow()
	uids, err := tx.MutateBasic(obj)
	if err != nil {
		return err
	}
	c.logger.V(2).Info("mutation successful", "uidCount", len(uids))
	return nil
}

func (c client) upsert(ctx context.Context, obj any, upsertPredicate string) error {
	objType := reflect.TypeOf(obj)
	objKind := objType.Kind()
	var schemaObj any

	if objKind == reflect.Slice {
		sliceValue := reflect.Indirect(reflect.ValueOf(obj))
		if sliceValue.Len() == 0 {
			err := errors.New("slice cannot be empty")
			return err
		}
		schemaObj = sliceValue.Index(0).Interface()
	} else {
		schemaObj = obj
	}

	err := checkPointer(schemaObj)
	if err != nil {
		if objKind == reflect.Slice {
			err = errors.Join(err, errors.New("slice elements must be pointers"))
		}
		return err
	}

	if c.options.autoSchema {
		err := c.UpdateSchema(ctx, schemaObj)
		if err != nil {
			return err
		}
	}

	upsertPredicates := getUpsertPredicates(schemaObj, upsertPredicate == "")
	if len(upsertPredicates) == 0 {
		return errors.New("no upsert predicates found")
	}
	var firstPredicate string
	for k := range upsertPredicates {
		firstPredicate = k
		break
	}
	if len(upsertPredicates) > 1 {
		c.logger.V(1).Info("Multiple upsert predicates found, using first", "predicate", firstPredicate)
	}
	if upsertPredicate == "" {
		upsertPredicate = firstPredicate
	} else {
		if _, ok := upsertPredicates[upsertPredicate]; !ok {
			return fmt.Errorf("upsert predicate %q not found", upsertPredicate)
		}
	}

	query := fmt.Sprintf(`{
		q(func: eq(%s, "%s")) {
			uid
		}
	}`, upsertPredicate, upsertPredicates[upsertPredicate])

	resp, err := c.QueryRaw(ctx, query, nil)
	if err != nil {
		return err
	}
	uid, err := extractUIDFromDgraphQueryResult(resp)
	if err != nil {
		return err
	}

	if uid == "" {
		return c.Insert(ctx, obj)
	}
	objValue := reflect.ValueOf(schemaObj)
	objValue.Elem().FieldByName("UID").SetString(uid)
	return c.Update(ctx, objValue.Interface())
}

func generateUniquePredicateQuery(predicates map[string]interface{}, nodeType string) (string, map[string]string) {
	var queryBuf bytes.Buffer
	vars := make(map[string]string)

	// Build variable declarations and OR conditions
	varDecls := make([]string, 0, len(predicates))
	conditions := make([]string, 0, len(predicates))
	for key, val := range predicates {
		varType := "string"
		switch val.(type) {
		case int, int32, int64, float32, float64:
			varType = "int"
		}
		varDecls = append(varDecls, fmt.Sprintf("$%s: %s", key, varType))
		conditions = append(conditions, fmt.Sprintf("eq(%s, $%s)", key, key))
		key = fmt.Sprintf("$%s", key)
		vars[key] = fmt.Sprintf("%v", val)
	}
	queryBuf.WriteString("query q(")
	queryBuf.WriteString(strings.Join(varDecls, ", "))
	queryBuf.WriteString(") {\n")
	queryBuf.WriteString(fmt.Sprintf("  q(func: type(%s)) @filter(%s) {\n", nodeType, strings.Join(conditions, " OR ")))
	queryBuf.WriteString("    uid\n  }\n")
	queryBuf.WriteString("}\n")

	return queryBuf.String(), vars
}

func getNodeType(obj any) string {
	v := reflect.ValueOf(obj)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	dtypeField := v.FieldByName("DType")
	var nodeType string
	if dtypeField.IsValid() && dtypeField.Kind() == reflect.Slice && dtypeField.Len() > 0 {
		nodeType = dtypeField.Index(0).String()
	} else {
		nodeType = v.Type().Name() // fallback if DType is not present or empty
	}
	return nodeType
}

func getUIDValue(obj any) string {
	v := reflect.ValueOf(obj)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	return v.FieldByName("UID").String()
}

func extractUIDFromDgraphQueryResult(resp []byte) (string, error) {
	var result map[string]interface{}
	if err := json.Unmarshal(resp, &result); err != nil {
		return "", err
	}

	q, ok := result["q"].([]interface{})
	if !ok || len(q) == 0 {
		return "", nil
	}

	firstItem, ok := q[0].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("invalid structure in 'q' array")
	}

	uid, ok := firstItem["uid"].(string)
	if !ok {
		return "", fmt.Errorf("uid not found or not a string")
	}
	return uid, nil
}

func getUpsertPredicates(obj any, firstOnly bool) map[string]any {
	return getPredicatesByTag(obj, "upsert", firstOnly)
}

func getUniquePredicates(obj any) map[string]any {
	return getPredicatesByTag(obj, "unique", false)
}

func getPredicatesByTag(obj any, tagName string, firstOnly bool) map[string]any {
	result := make(map[string]any)
	v := reflect.ValueOf(obj)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return result
	}
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("dgraph")
		if tag == "" || !strings.Contains(tag, tagName) {
			continue
		}

		var predName string
		if idx := strings.Index(tag, "predicate="); idx != -1 {
			// Find the first comma or space after predicate=
			endIdx := len(tag)
			commaIdx := strings.Index(tag[idx:], ",")
			spaceIdx := strings.Index(tag[idx:], " ")
			if commaIdx != -1 && (spaceIdx == -1 || commaIdx < spaceIdx) {
				endIdx = idx + commaIdx
			} else if spaceIdx != -1 {
				endIdx = idx + spaceIdx
			}
			predName = tag[idx+len("predicate=") : endIdx]
		} else {
			jsonTag := field.Tag.Get("json")
			if jsonTag != "" && jsonTag != "-" {
				commaIdx := strings.Index(jsonTag, ",")
				if commaIdx != -1 {
					predName = jsonTag[:commaIdx]
				} else {
					predName = jsonTag
				}
			} else {
				predName = field.Name
			}
		}
		result[predName] = v.Field(i).Interface()
		if firstOnly {
			break
		}
	}
	return result
}
