/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package structreflect

import (
	"reflect"
)

func GetJsonTagToValues(object any, fieldToJsonTags map[string]string) map[string]any {
	values := make(map[string]any)
	v := reflect.ValueOf(object)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	for fieldName, jsonName := range fieldToJsonTags {
		fieldValue := v.FieldByName(fieldName)
		values[jsonName] = fieldValue.Interface()
	}
	return values
}
