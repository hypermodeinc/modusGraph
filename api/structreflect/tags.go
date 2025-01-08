/*
 * Copyright 2025 Hypermode Inc.
 * Licensed under the terms of the Apache License, Version 2.0
 * See the LICENSE file that accompanied this code for further details.
 *
 * SPDX-FileCopyrightText: 2025 Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package structreflect

type DbTag struct {
	Constraint string
}

type TagMaps struct {
	FieldToJson       map[string]string
	JsonToDb          map[string]*DbTag
	JsonToReverseEdge map[string]string
}
