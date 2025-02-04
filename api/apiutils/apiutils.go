/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package apiutils

import (
	"fmt"

	"github.com/hypermodeinc/dgraph/v24/x"
)

var (
	ErrNoObjFound  = fmt.Errorf("no object found")
	NoUniqueConstr = "unique constraint not defined for any field on type %s"
)

func GetPredicateName(typeName, fieldName string) string {
	return fmt.Sprint(typeName, ".", fieldName)
}

func AddNamespace(ns uint64, pred string) string {
	return x.NamespaceAttr(ns, pred)
}
