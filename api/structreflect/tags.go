/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
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
