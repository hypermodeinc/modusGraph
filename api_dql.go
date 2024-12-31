package modusdb

import (
	"fmt"
)

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

	unstructuredQuery = `
    {
      obj(%s) {
        uid
        %s
      }
    }
    `

	funcUid = `func: uid(%d)`
	funcEq  = `func: eq(%s, %s)`
	// funcSimilarTo = `func: similar_to(%s, %d, "%s")`
)

func buildUidQuery(gid uint64) QueryFunc {
	return func() string {
		return fmt.Sprintf(funcUid, gid)
	}
}

func buildEqQuery(key string, value any) QueryFunc {
	return func() string {
		return fmt.Sprintf(funcEq, key, value)
	}
}

// func buildVecSearchQuery(indexAttr string, topK int64, vec []float32) QueryFunc {
// 	vecStrArr := make([]string, len(vec))
// 	for i := range vec {
// 		vecStrArr[i] = strconv.FormatFloat(float64(vec[i]), 'f', -1, 32)
// 	}
// 	vecStr := strings.Join(vecStrArr, ",")
// 	vecStr = "[" + vecStr + "]"
// 	return func() string {
// 		return fmt.Sprintf(funcSimilarTo, indexAttr, topK, vecStr)
// 	}
// }

func formatObjQuery(qf QueryFunc, extraFields string) string {
	return fmt.Sprintf(objQuery, qf(), extraFields)
}

func formatUnstructuredQuery(qf QueryFunc, extraFields string) string {
	return fmt.Sprintf(unstructuredQuery, qf(), extraFields)
}
