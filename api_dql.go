package modusdb

import (
	"fmt"
	"strconv"
	"strings"
)

type QueryFunc func() string

const (
	objQuery = `
    {
      obj(func: %s) {
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

	objsQuery = `
    {
      objs(func: type("%s")%s) @filter(%s) {
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

	funcUid        = `uid(%d)`
	funcEq         = `eq(%s, %s)`
	funcSimilarTo  = `similar_to(%s, %d, "[%s]")`
	funcAllOfTerms = `allofterms(%s, "%s")`
	funcAnyOfTerms = `anyofterms(%s, "%s")`
	funcAllOfText  = `alloftext(%s, "%s")`
	funcAnyOfText  = `anyoftext(%s, "%s")`
	funcRegExp     = `regexp(%s, /%s/)`
	funcLe         = `le(%s, %s)`
	funcGe         = `ge(%s, %s)`
	funcGt         = `gt(%s, %s)`
	funcLt         = `lt(%s, %s)`
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

func buildSimilarToQuery(indexAttr string, topK int64, vec []float32) QueryFunc {
	vecStrArr := make([]string, len(vec))
	for i := range vec {
		vecStrArr[i] = strconv.FormatFloat(float64(vec[i]), 'f', -1, 32)
	}
	vecStr := strings.Join(vecStrArr, ",")
	return func() string {
		return fmt.Sprintf(funcSimilarTo, indexAttr, topK, vecStr)
	}
}

func buildAllOfTermsQuery(attr string, terms string) QueryFunc {
	return func() string {
		return fmt.Sprintf(funcAllOfTerms, attr, terms)
	}
}

func buildAnyOfTermsQuery(attr string, terms string) QueryFunc {
	return func() string {
		return fmt.Sprintf(funcAnyOfTerms, attr, terms)
	}
}

func buildAllOfTextQuery(attr, text string) QueryFunc {
	return func() string {
		return fmt.Sprintf(funcAllOfText, attr, text)
	}
}

func buildAnyOfTextQuery(attr, text string) QueryFunc {
	return func() string {
		return fmt.Sprintf(funcAnyOfText, attr, text)
	}
}

func buildRegExpQuery(attr, pattern string) QueryFunc {
	return func() string {
		return fmt.Sprintf(funcRegExp, attr, pattern)
	}
}

func buildLeQuery(attr, value string) QueryFunc {
	return func() string {
		return fmt.Sprintf(funcLe, attr, value)
	}
}

func buildGeQuery(attr, value string) QueryFunc {
	return func() string {
		return fmt.Sprintf(funcGe, attr, value)
	}
}

func buildGtQuery(attr, value string) QueryFunc {
	return func() string {
		return fmt.Sprintf(funcGt, attr, value)
	}
}

func buildLtQuery(attr, value string) QueryFunc {
	return func() string {
		return fmt.Sprintf(funcLt, attr, value)
	}
}

func And(qfs ...QueryFunc) QueryFunc {
	return func() string {
		qs := make([]string, len(qfs))
		for i, qf := range qfs {
			qs[i] = qf()
		}
		return strings.Join(qs, " AND ")
	}
}

func Or(qfs ...QueryFunc) QueryFunc {
	return func() string {
		qs := make([]string, len(qfs))
		for i, qf := range qfs {
			qs[i] = qf()
		}
		return strings.Join(qs, " OR ")
	}
}

func Not(qf QueryFunc) QueryFunc {
	return func() string {
		return "NOT " + qf()
	}
}

func formatObjQuery(qf QueryFunc, extraFields string) string {
	return fmt.Sprintf(objQuery, qf(), extraFields)
}

func formatObjsQuery(typeName string, qf QueryFunc, paginationAndSorting string, extraFields string) string {
	return fmt.Sprintf(objsQuery, typeName, paginationAndSorting, qf(), extraFields)
}

// Helper function to combine multiple filters
func filtersToQueryFunc(typeName string, filter Filter) QueryFunc {
	return filterToQueryFunc(typeName, filter)
}

func paginationToQueryString(p Pagination) string {
	paginationStr := ""
	if p.Limit > 0 {
		paginationStr += ", " + fmt.Sprintf("first: %d", p.Limit)
	}
	if p.Offset > 0 {
		paginationStr += ", " + fmt.Sprintf("offset: %d", p.Offset)
	} else if p.After != "" {
		paginationStr += ", " + fmt.Sprintf("after: %s", p.After)
	}
	if paginationStr == "" {
		return ""
	}
	return paginationStr
}

func sortingToQueryString(typeName string, s Sorting) string {
	if s.OrderAscField == "" && s.OrderDescField == "" {
		return ""
	}

	var parts []string
	first, second := s.OrderDescField, s.OrderAscField
	firstOp, secondOp := "orderdesc", "orderasc"

	if !s.OrderDescFirst {
		first, second = s.OrderAscField, s.OrderDescField
		firstOp, secondOp = "orderasc", "orderdesc"
	}

	if first != "" {
		parts = append(parts, fmt.Sprintf("%s: %s", firstOp, getPredicateName(typeName, first)))
	}
	if second != "" {
		parts = append(parts, fmt.Sprintf("%s: %s", secondOp, getPredicateName(typeName, second)))
	}

	return ", " + strings.Join(parts, ", ")
}
