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
	"fmt"
	"reflect"
	"strings"

	"github.com/hypermodeinc/modusdb/api/apiutils"
)

func parseJsonTag(field reflect.StructField) (string, error) {
	jsonTag := field.Tag.Get("json")
	if jsonTag == "" {
		return "", fmt.Errorf("field %s has no json tag", field.Name)
	}
	return strings.Split(jsonTag, ",")[0], nil
}

func parseDbTag(field reflect.StructField) *DbTag {
	dbConstraintsTag := field.Tag.Get("db")
	if dbConstraintsTag == "" {
		return nil
	}

	dbTag := &DbTag{}
	dbTagsSplit := strings.Split(dbConstraintsTag, ",")
	for _, tag := range dbTagsSplit {
		split := strings.Split(tag, "=")
		if split[0] == "constraint" {
			dbTag.Constraint = split[1]
		}
	}
	return dbTag
}

func parseReverseEdgeTag(field reflect.StructField) (string, error) {
	reverseEdgeTag := field.Tag.Get("readFrom")
	if reverseEdgeTag == "" {
		return "", nil
	}

	typeAndField := strings.Split(reverseEdgeTag, ",")
	if len(typeAndField) != 2 {
		return "", fmt.Errorf(`field %s has invalid readFrom tag, expected format is type=<type>,field=<field>`, field.Name)
	}

	t := strings.Split(typeAndField[0], "=")[1]
	f := strings.Split(typeAndField[1], "=")[1]
	return apiutils.GetPredicateName(t, f), nil
}
