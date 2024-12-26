package modusdb

import "fmt"

type QueryFunc func() string

const (
	objQuery = `
    {
      obj(%s) {
        uid
        expand(_all_) {
            uid
            expand(_all_)
            dgraph.type
        }
        dgraph.type
        %s
      }
    }
    `

	funcUid = `func: uid(%d)`
	funcEq  = `func: eq(%s, %s)`
)

func buildUidQuery(gid uint64) QueryFunc {
	return func() string {
		return fmt.Sprintf(funcUid, gid)
	}
}

func buildEqQuery(key, value any) QueryFunc {
	return func() string {
		return fmt.Sprintf(funcEq, key, value)
	}
}

func formatObjQuery(qf QueryFunc, extraFields string) string {
	return fmt.Sprintf(objQuery, qf(), extraFields)
}
