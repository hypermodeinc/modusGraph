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
	"fmt"
	"reflect"

	"github.com/hypermodeinc/modusdb/api/utils"
)

func GetUniqueConstraint[T any](object T) (uint64, *ConstrainedField, error) {
	t := reflect.TypeOf(object)
	fieldToJsonTags, jsonToDbTags, _, err := utils.GetFieldTags(t)
	if err != nil {
		return 0, nil, err
	}
	jsonTagToValue := utils.GetJsonTagToValues(object, fieldToJsonTags)

	for jsonName, value := range jsonTagToValue {
		if jsonName == "gid" {
			gid, ok := value.(uint64)
			if !ok {
				continue
			}
			if gid != 0 {
				return gid, nil, nil
			}
		}
		if jsonToDbTags[jsonName] != nil && IsValidUniqueIndex(jsonToDbTags[jsonName].Constraint) {
			// check if value is zero or nil
			if value == reflect.Zero(reflect.TypeOf(value)).Interface() || value == nil {
				continue
			}
			return 0, &ConstrainedField{
				Key:   jsonName,
				Value: value,
			}, nil
		}
	}

	return 0, nil, fmt.Errorf(utils.NoUniqueConstr, t.Name())
}

func IsValidUniqueIndex(name string) bool {
	return name == "unique"
}
