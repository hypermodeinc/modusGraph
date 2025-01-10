/*
 * Copyright 2025 Hypermode Inc.
 * Licensed under the terms of the Apache License, Version 2.0
 * See the LICENSE file that accompanied this code for further details.
 *
 * SPDX-FileCopyrightText: 2025 Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package querygen

import (
	"fmt"
	"strconv"
	"strings"
)

type SchemaField struct {
	Name string `json:"name"`
}

type SchemaType struct {
	Name   string        `json:"name,omitempty"`
	Fields []SchemaField `json:"fields,omitempty"`
}

type SchemaResponse struct {
	Types []SchemaType `json:"types,omitempty"`
}

type QueryFunc func() string

const (
	ObjQuery = `
    {
      obj(func: %s) {
        gid: uid
        expand(_all_) {
            gid: uid
            expand(_all_)
            dgraph.type
        }
        dgraph.type
        %s
      }
    }
    `

	ObjsQuery = `
    {
      objs(func: type("%s")%s) @filter(%s) {
        gid: uid
        expand(_all_) {
            gid: uid
            expand(_all_)
            dgraph.type
        }
        dgraph.type
        %s
      }
    }
  `

	ReverseEdgeQuery = `
  %s: ~%s {
			gid: uid
			expand(_all_)
			dgraph.type
		}
  `

	SchemaQuery = `
	schema{}
	`

	FuncUid        = `uid(%d)`
	FuncEq         = `eq(%s, %s)`
	FuncSimilarTo  = `similar_to(%s, %d, "[%s]")`
	FuncAllOfTerms = `allofterms(%s, "%s")`
	FuncAnyOfTerms = `anyofterms(%s, "%s")`
	FuncAllOfText  = `alloftext(%s, "%s")`
	FuncAnyOfText  = `anyoftext(%s, "%s")`
	FuncRegExp     = `regexp(%s, /%s/)`
	FuncLe         = `le(%s, %s)`
	FuncGe         = `ge(%s, %s)`
	FuncGt         = `gt(%s, %s)`
	FuncLt         = `lt(%s, %s)`
)

func BuildUidQuery(gid uint64) QueryFunc {
	return func() string {
		return fmt.Sprintf(FuncUid, gid)
	}
}

func BuildEqQuery(key string, value any) QueryFunc {
	return func() string {
		return fmt.Sprintf(FuncEq, key, value)
	}
}

func BuildSimilarToQuery(indexAttr string, topK int64, vec []float32) QueryFunc {
	vecStrArr := make([]string, len(vec))
	for i := range vec {
		vecStrArr[i] = strconv.FormatFloat(float64(vec[i]), 'f', -1, 32)
	}
	vecStr := strings.Join(vecStrArr, ",")
	return func() string {
		return fmt.Sprintf(FuncSimilarTo, indexAttr, topK, vecStr)
	}
}

func BuildAllOfTermsQuery(attr string, terms string) QueryFunc {
	return func() string {
		return fmt.Sprintf(FuncAllOfTerms, attr, terms)
	}
}

func BuildAnyOfTermsQuery(attr string, terms string) QueryFunc {
	return func() string {
		return fmt.Sprintf(FuncAnyOfTerms, attr, terms)
	}
}

func BuildAllOfTextQuery(attr, text string) QueryFunc {
	return func() string {
		return fmt.Sprintf(FuncAllOfText, attr, text)
	}
}

func BuildAnyOfTextQuery(attr, text string) QueryFunc {
	return func() string {
		return fmt.Sprintf(FuncAnyOfText, attr, text)
	}
}

func BuildRegExpQuery(attr, pattern string) QueryFunc {
	return func() string {
		return fmt.Sprintf(FuncRegExp, attr, pattern)
	}
}

func BuildLeQuery(attr, value string) QueryFunc {
	return func() string {
		return fmt.Sprintf(FuncLe, attr, value)
	}
}

func BuildGeQuery(attr, value string) QueryFunc {
	return func() string {
		return fmt.Sprintf(FuncGe, attr, value)
	}
}

func BuildGtQuery(attr, value string) QueryFunc {
	return func() string {
		return fmt.Sprintf(FuncGt, attr, value)
	}
}

func BuildLtQuery(attr, value string) QueryFunc {
	return func() string {
		return fmt.Sprintf(FuncLt, attr, value)
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

func FormatObjQuery(qf QueryFunc, extraFields string) string {
	return fmt.Sprintf(ObjQuery, qf(), extraFields)
}

func FormatObjsQuery(typeName string, qf QueryFunc, paginationAndSorting string, extraFields string) string {
	return fmt.Sprintf(ObjsQuery, typeName, paginationAndSorting, qf(), extraFields)
}
