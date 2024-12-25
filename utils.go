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
