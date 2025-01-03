/*
 * This example is part of the ModusDB project, licensed under the Apache License 2.0.
 * You may modify and use this example in accordance with the license.
 * See the LICENSE file that accompanied this code for further details.
 */

package modusdb

import (
	"fmt"

	"github.com/dgraph-io/dgraph/v24/x"
)

func getPredicateName(typeName, fieldName string) string {
	return fmt.Sprint(typeName, ".", fieldName)
}

func addNamespace(ns uint64, pred string) string {
	return x.NamespaceAttr(ns, pred)
}
