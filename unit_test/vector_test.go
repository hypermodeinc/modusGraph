/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package unit_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/hypermodeinc/dgraph/v24/dgraphapi"
	"github.com/stretchr/testify/require"

	"github.com/hypermodeinc/modusdb"
)

const (
	vectorSchemaWithIndex = `%v: float32vector @index(hnsw(exponent: "%v", metric: "%v")) .`
	numVectors            = 1000
)

func TestVectorDelete(t *testing.T) {
	engine, err := modusdb.NewEngine(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	require.NoError(t, engine.DropAll(context.Background()))
	require.NoError(t, engine.GetDefaultNamespace().AlterSchema(context.Background(),
		fmt.Sprintf(vectorSchemaWithIndex, "vtest", "4", "euclidean")))

	// insert random vectors
	assignIDs, err := engine.LeaseUIDs(numVectors + 1)
	require.NoError(t, err)
	//nolint:gosec
	rdf, vectors := dgraphapi.GenerateRandomVectors(int(assignIDs.StartId)-10, int(assignIDs.EndId)-10, 10, "vtest")
	_, err = engine.GetDefaultNamespace().Mutate(context.Background(), []*api.Mutation{{SetNquads: []byte(rdf)}})
	require.NoError(t, err)

	// check the count of the vectors inserted
	const q1 = `{
		 vector(func: has(vtest)) {
				count(uid)
			 }
	 }`
	resp, err := engine.GetDefaultNamespace().Query(context.Background(), q1)
	require.NoError(t, err)
	require.JSONEq(t, fmt.Sprintf(`{"vector":[{"count":%d}]}`, numVectors), string(resp.Json))

	// check whether all the vectors are inserted
	const vectorQuery = `
		{
			vector(func: has(vtest)) {
				uid
				vtest
			}
		}`

	require.Equal(t, vectors, queryVectors(t, engine, vectorQuery))

	triples := strings.Split(rdf, "\n")
	deleteTriple := func(idx int) string {
		_, err := engine.GetDefaultNamespace().Mutate(context.Background(), []*api.Mutation{{
			DelNquads: []byte(triples[idx]),
		}})
		require.NoError(t, err)

		uid := strings.Split(triples[idx], " ")[0]
		q2 := fmt.Sprintf(`{
		  vector(func: uid(%s)) {
		   vtest
		  }
		}`, uid[1:len(uid)-1])

		res, err := engine.GetDefaultNamespace().Query(context.Background(), q2)
		require.NoError(t, err)
		require.JSONEq(t, `{"vector":[]}`, string(res.Json))
		return triples[idx]
	}

	const q3 = `
		{
			vector(func: similar_to(vtest, 1, "%v")) {
					uid
					vtest
			}
		}`
	for i := 0; i < len(triples)-2; i++ {
		triple := deleteTriple(i)
		vectorQuery := fmt.Sprintf(q3, strings.Split(triple, `"`)[1])
		respVectors := queryVectors(t, engine, vectorQuery)
		require.Len(t, respVectors, 1)
		require.Contains(t, vectors, respVectors[0])
	}

	triple := deleteTriple(len(triples) - 2)
	_ = queryVectors(t, engine, fmt.Sprintf(q3, strings.Split(triple, `"`)[1]))
}

func queryVectors(t *testing.T, engine *modusdb.Engine, query string) [][]float32 {
	resp, err := engine.GetDefaultNamespace().Query(context.Background(), query)
	require.NoError(t, err)

	var data struct {
		Vector []struct {
			UID   string    `json:"uid"`
			VTest []float32 `json:"vtest"`
		} `json:"vector"`
	}
	require.NoError(t, json.Unmarshal(resp.Json, &data))

	vectors := make([][]float32, 0)
	for _, vector := range data.Vector {
		vectors = append(vectors, vector.VTest)
	}
	return vectors
}
