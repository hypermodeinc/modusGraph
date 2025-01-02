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
      objs(func: type("%s")) @filter(%s) {
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

func buildAllOfTermsQuery(attr string, terms []string) QueryFunc {
	termsStr := strings.Join(terms, " ")
	return func() string {
		return fmt.Sprintf(funcAllOfTerms, attr, termsStr)
	}
}

func buildAnyOfTermsQuery(attr string, terms []string) QueryFunc {
	termsStr := strings.Join(terms, " ")
	return func() string {
		return fmt.Sprintf(funcAnyOfTerms, attr, termsStr)
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

func formatObjsQuery(typeName string, qf QueryFunc, extraFields string) string {
	return fmt.Sprintf(objsQuery, typeName, qf(), extraFields)
}

func filterToQueryFunc(typeName string, f Filter) QueryFunc {
	// Handle logical operators first
	if f.And != nil {
		return And(filterToQueryFunc(typeName, *f.And))
	}
	if f.Or != nil {
		return Or(filterToQueryFunc(typeName, *f.Or))
	}
	if f.Not != nil {
		return Not(filterToQueryFunc(typeName, *f.Not))
	}

	// Handle field predicates
	if f.StringHash.Equals != "" {
		return buildEqQuery(getPredicateName(typeName, f.Field), f.StringHash.Equals)
	}
	if f.StringTerm.Equals != "" {
		return buildEqQuery(getPredicateName(typeName, f.Field), f.StringTerm.Equals)
	}
	if f.StringTerm.AllOfTerms != nil {
		return buildAllOfTermsQuery(getPredicateName(typeName, f.Field), f.StringTerm.AllOfTerms)
	}
	if f.StringTerm.AnyOfTerms != nil {
		return buildAnyOfTermsQuery(getPredicateName(typeName, f.Field), f.StringTerm.AnyOfTerms)
	}
	if f.StringExact.Equals != "" {
		return buildEqQuery(getPredicateName(typeName, f.Field), f.StringExact.Equals)
	}
	if f.StringExact.LessThan != "" {
		return buildLtQuery(getPredicateName(typeName, f.Field), f.StringExact.LessThan)
	}
	if f.StringExact.LessOrEqual != "" {
		return buildLeQuery(getPredicateName(typeName, f.Field), f.StringExact.LessOrEqual)
	}
	if f.StringExact.GreaterThan != "" {
		return buildGtQuery(getPredicateName(typeName, f.Field), f.StringExact.GreaterThan)
	}
	if f.StringExact.GreaterOrEqual != "" {
		return buildGeQuery(getPredicateName(typeName, f.Field), f.StringExact.GreaterOrEqual)
	}
	if f.Vector.SimilarTo != nil {
		return buildSimilarToQuery(getPredicateName(typeName, f.Field), f.Vector.TopK, f.Vector.SimilarTo)
	}

	// Return empty query if no conditions match
	return func() string { return "" }
}

// Helper function to combine multiple filters
func filtersToQueryFunc(typeName string, filters []Filter) QueryFunc {
	if len(filters) == 0 {
		return func() string { return "" }
	}

	queryFuncs := make([]QueryFunc, len(filters))
	for i, filter := range filters {
		queryFuncs[i] = filterToQueryFunc(typeName, filter)
	}

	return And(queryFuncs...)
}
