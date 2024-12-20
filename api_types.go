package modusdb

import "fmt"

var (
	ErrNoObjFound = fmt.Errorf("no object found")
)

type UniqueField interface {
	uint64 | ConstrainedField
}
type ConstrainedField struct {
	Key   string
	Value any
}
