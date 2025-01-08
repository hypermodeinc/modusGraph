/*
 * Copyright 2025 Hypermode Inc.
 * Licensed under the terms of the Apache License, Version 2.0
 * See the LICENSE file that accompanied this code for further details.
 *
 * SPDX-FileCopyrightText: 2025 Hypermode Inc. <hello@hypermode.com>
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
