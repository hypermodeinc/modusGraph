/*
 * Copyright 2025 Hypermode Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package modusgraph_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	dg "github.com/dolan-in/dgman/v2"
	"github.com/stretchr/testify/require"
)

func TestClientSimpleGet(t *testing.T) {

	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "GetWithFileURI",
			uri:  "file://" + t.TempDir(),
		},
		{
			name: "GetWithDgraphURI",
			uri:  "dgraph://" + os.Getenv("MODUSGRAPH_TEST_ADDR"),
			skip: os.Getenv("MODUSGRAPH_TEST_ADDR") == "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: MODUSGRAPH_TEST_ADDR not set", tc.name)
				return
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			entity := TestEntity{
				Name:        "Test Entity",
				Description: "This is a test entity for the Get method",
				CreatedAt:   time.Now(),
			}
			ctx := context.Background()
			err := client.Insert(ctx, &entity)
			require.NoError(t, err, "Insert should succeed")

			err = client.Query(ctx, TestEntity{}).Node(&entity)
			require.NoError(t, err, "Get should succeed")
			require.Equal(t, entity.Name, "Test Entity", "Name should match")
			require.Equal(t, entity.Description, "This is a test entity for the Get method", "Description should match")
		})
	}
}

type QueryTestRecord struct {
	Name      string    `json:"name,omitempty" dgraph:"index=exact,term unique"`
	Age       int       `json:"age,omitempty"`
	BirthDate time.Time `json:"birthDate,omitzero"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

func TestClientQuery(t *testing.T) {

	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "QueryWithFileURI",
			uri:  "file://" + t.TempDir(),
		},
		{
			name: "QueryWithDgraphURI",
			uri:  "dgraph://" + os.Getenv("MODUSGRAPH_TEST_ADDR"),
			skip: os.Getenv("MODUSGRAPH_TEST_ADDR") == "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: MODUSGRAPH_TEST_ADDR not set", tc.name)
				return
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			entities := make([]*QueryTestRecord, 10)
			birthDate := time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC)
			for i := range 10 {
				entities[i] = &QueryTestRecord{
					Name:      fmt.Sprintf("Test Entity %d", i),
					Age:       30 + i,
					BirthDate: birthDate.AddDate(0, 0, i),
				}
			}
			ctx := context.Background()
			err := client.Insert(ctx, entities)
			require.NoError(t, err, "Insert should succeed")

			// Run query sub-tests
			t.Run("QueryAll", func(t *testing.T) {
				var result []QueryTestRecord
				err := client.Query(ctx, QueryTestRecord{}).Nodes(&result)
				require.NoError(t, err, "Query should succeed")
				require.Len(t, result, 10, "Should have 10 entities")
			})

			t.Run("QueryOrdering", func(t *testing.T) {
				var result []QueryTestRecord
				err := client.Query(ctx, QueryTestRecord{}).OrderAsc("age").Nodes(&result)
				require.NoError(t, err, "Query should succeed")
				require.Len(t, result, 10, "Should have 10 entities")
				for i := range 10 {
					require.Equal(t, result[i].Name, fmt.Sprintf("Test Entity %d", i), "Name should match")
					require.Equal(t, result[i].Age, 30+i, "Age should match")
					require.Equal(t, result[i].BirthDate, birthDate.AddDate(0, 0, i), "BirthDate should match")
				}
			})

			t.Run("QueryWithFilter", func(t *testing.T) {
				var result []QueryTestRecord
				err := client.Query(ctx, QueryTestRecord{}).
					Filter(`(ge(age, 30) and le(age, 35))`).
					Nodes(&result)
				require.NoError(t, err, "Query should succeed")
				require.Len(t, result, 6, "Should have 6 entities")
				for _, entity := range result {
					require.GreaterOrEqual(t, entity.Age, 30, "Age should be between 30 and 35")
					require.LessOrEqual(t, entity.Age, 35, "Age should be between 30 and 35")
				}
			})

			t.Run("QueryWithPagination", func(t *testing.T) {
				var result []QueryTestRecord
				count, err := client.Query(ctx, QueryTestRecord{}).First(5).NodesAndCount(&result)
				require.NoError(t, err, "Query should succeed")
				require.Len(t, result, 5, "Should have 5 entities")
				require.Equal(t, count, 10, "Should have 10 entities")

				err = client.Query(ctx, QueryTestRecord{}).
					OrderAsc("age").
					First(5).
					Offset(5).
					Nodes(&result)
				require.NoError(t, err, "Query should succeed")
				require.Len(t, result, 5, "Should have 5 entities")
				for i := range 5 {
					require.Equal(t, result[i].Name, fmt.Sprintf("Test Entity %d", 5+i), "Name should match")
					require.Equal(t, result[i].Age, 30+5+i, "Age should match")
					require.Equal(t, result[i].BirthDate, birthDate.AddDate(0, 0, 5+i), "BirthDate should match")
				}
			})

			t.Run("QueryRaw", func(t *testing.T) {
				var result struct {
					Data []QueryTestRecord `json:"q"`
				}
				resp, err := client.QueryRaw(ctx,
					`query { q(func: type(QueryTestRecord), orderasc: age) { uid name age birthDate }}`)
				require.NoError(t, err, "Query should succeed")
				require.NoError(t, json.Unmarshal(resp, &result), "Failed to unmarshal response")
				require.Len(t, result.Data, 10, "Should have 10 entities")
				for i := range 10 {
					require.Equal(t, result.Data[i].Name, fmt.Sprintf("Test Entity %d", i), "Name should match")
					require.Equal(t, result.Data[i].Age, 30+i, "Age should match")
					require.Equal(t, result.Data[i].BirthDate, birthDate.AddDate(0, 0, i), "BirthDate should match")
				}
			})
		})
	}
}

type TestItem struct {
	Name        string            `json:"name,omitempty" dgraph:"index=term"`
	Description string            `json:"description,omitempty"`
	Vector      *dg.VectorFloat32 `json:"vector,omitempty" dgraph:"index=hnsw(metric:\"cosine\")"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

func TestVectorSimilaritySearch(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "VectorSimilaritySearchWithFileURI",
			uri:  "file://" + t.TempDir(),
		},
		/*
			{
				name: "VectorSimilaritySearchWithDgraphURI",
				uri:  "dgraph://" + os.Getenv("MODUSGRAPH_TEST_ADDR"),
				skip: os.Getenv("MODUSGRAPH_TEST_ADDR") == "",
			},
		*/
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: MODUSGRAPH_TEST_ADDR not set", tc.name)
				return
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			// Insert several items with different vectors
			items := []*TestItem{
				{
					Name:        "Item A",
					Description: "First vector",
					Vector:      &dg.VectorFloat32{Values: []float32{0.1, 0.2, 0.3, 0.4, 0.5}},
				},
				{
					Name:        "Item B",
					Description: "Second vector",
					Vector:      &dg.VectorFloat32{Values: []float32{0.5, 0.4, 0.3, 0.2, 0.1}},
				},
				{
					Name:        "Item C",
					Description: "Third vector",
					Vector:      &dg.VectorFloat32{Values: []float32{1.0, 1.0, 1.0, 1.0, 1.0}},
				},
			}

			ctx := context.Background()
			err := client.Insert(ctx, items)
			require.NoError(t, err, "Insert should succeed")

			var testItem TestItem
			vectorVar := "[0.51, 0.39, 0.29, 0.19, 0.09]"
			query := dg.NewQuery().Model(&testItem).RootFunc("similar_to(vector, 1, $vec)")

			dgo, err := client.DgraphClient()
			require.NoError(t, err)
			tx := dg.NewReadOnlyTxn(dgo)
			err = tx.Query(query).Vars("similar_to($vec: string)", map[string]string{"$vec": vectorVar}).Scan()
			require.NoError(t, err)

			require.Equal(t, "Item B", testItem.Name)
		})
	}
}
