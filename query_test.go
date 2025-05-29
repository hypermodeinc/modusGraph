/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
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
			uri:  "file://" + GetTempDir(t),
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

type GeoLocation struct {
	Type  string    `json:"type"`
	Coord []float64 `json:"coordinates"`
}

type QueryTestRecord struct {
	Name      string       `json:"name,omitempty" dgraph:"index=exact,term unique"`
	Age       int          `json:"age,omitempty" dgraph:"index=int"`
	BirthDate time.Time    `json:"birthDate,omitzero"`
	Location  *GeoLocation `json:"location,omitempty" dgraph:"type=geo index=geo"`

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
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "QueryWithDgraphURI",
			uri:  "dgraph://" + os.Getenv("MODUSGRAPH_TEST_ADDR"),
			skip: os.Getenv("MODUSGRAPH_TEST_ADDR") == "",
		},
	}

	locations := [][]float64{
		{-122.4194, 37.7749}, // San Francisco, USA
		{2.2945, 48.8584},    // Paris (Eiffel Tower), France
		{-74.0060, 40.7128},  // New York City, USA
		{-0.1276, 51.5072},   // London, UK
		{139.7690, 35.6804},  // Tokyo, Japan
		{77.2090, 28.6139},   // New Delhi, India
		{31.2357, 30.0444},   // Cairo, Egypt
		{151.2093, -33.8688}, // Sydney, Australia
		{-43.1729, -22.9068}, // Rio de Janeiro, Brazil
		{116.4074, 39.9042},  // Beijing, China
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
					Location: &GeoLocation{
						Type:  "Point",
						Coord: locations[i],
					},
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
					require.Equal(t, result[i].Location.Coord, locations[i], "Location coordinates should match")
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

			t.Run("QueryWithGeoFilters", func(t *testing.T) {
				var result []QueryTestRecord
				err := client.Query(ctx, QueryTestRecord{}).
					Filter(`(near(location, [2.2946, 48.8585], 1000))`). // just a few meters from the Eiffel Tower
					Nodes(&result)
				require.NoError(t, err, "Query should succeed")
				require.Len(t, result, 1, "Should have 1 entity")
				require.Equal(t, result[0].Name, "Test Entity 1", "Name should match")

				var rawResult struct {
					Data []QueryTestRecord `json:"q"`
				}
				parisQuery := `query {
					q(func: within(location, [[[2.2945, 48.8584], [2.2690, 48.8800], [2.3300, 48.9000],
												[2.4100, 48.8800], [2.4150, 48.8300], [2.3650, 48.8150],
												[2.3000, 48.8100], [2.2600, 48.8350], [2.2945, 48.8584]]])) {
						uid
						name
					}
				}`
				resp, err := client.QueryRaw(ctx, parisQuery, nil)
				require.NoError(t, err, "Query should succeed")
				require.NoError(t, json.Unmarshal(resp, &rawResult), "Failed to unmarshal response")
				require.Len(t, rawResult.Data, 1, "Should have 1 entity")
				require.Equal(t, rawResult.Data[0].Name, "Test Entity 1", "Name should match")
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
					`query { q(func: type(QueryTestRecord), orderasc: age) { uid name age birthDate }}`,
					nil)
				require.NoError(t, err, "Query should succeed")
				require.NoError(t, json.Unmarshal(resp, &result), "Failed to unmarshal response")
				require.Len(t, result.Data, 10, "Should have 10 entities")
				for i := range 10 {
					require.Equal(t, result.Data[i].Name, fmt.Sprintf("Test Entity %d", i), "Name should match")
					require.Equal(t, result.Data[i].Age, 30+i, "Age should match")
					require.Equal(t, result.Data[i].BirthDate, birthDate.AddDate(0, 0, i), "BirthDate should match")
				}
			})

			t.Run("QueryRawWithVars", func(t *testing.T) {
				var result struct {
					Data []QueryTestRecord `json:"q"`
				}
				resp, err := client.QueryRaw(ctx,
					`query older_than_inclusive($1: int) { q(func: ge(age, $1)) { uid name age }}`,
					map[string]string{"$1": "38"})
				require.NoError(t, err, "Query should succeed")
				require.NoError(t, json.Unmarshal(resp, &result), "Failed to unmarshal response")
				require.Len(t, result.Data, 2, "Should have 2 entities")
				for i := range 2 {
					require.GreaterOrEqual(t, result.Data[i].Age, 38, "Age should be greater than or equal to 38")
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
			uri:  "file://" + GetTempDir(t),
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

			dgo, cleanup, err := client.DgraphClient()
			require.NoError(t, err)
			defer cleanup()
			tx := dg.NewReadOnlyTxn(dgo)
			err = tx.Query(query).Vars("similar_to($vec: string)", map[string]string{"$vec": vectorVar}).Scan()
			require.NoError(t, err)

			require.Equal(t, "Item B", testItem.Name)
		})
	}
}

type Student struct {
	Name string `json:"name,omitempty" dgraph:"index=exact"`

	// The reverse directive tells Dgraph to maintain a reverse edge
	Takes_Class []*Class `json:"takes_class,omitempty" dgraph:"reverse"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

type Class struct {
	Name string `json:"name,omitempty" dgraph:"index=exact"`

	// The tilde prefix tells modusGraph not to manage this field in the schema,
	// but we still need it in the struct in order for results to scan correctly
	Students []*Student `json:"~takes_class,omitempty"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

func TestReverseEdgeQuery(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "ReverseEdgeQueryWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "ReverseEdgeQueryWithDgraphURI",
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

			ctx := context.Background()
			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			class := Class{
				Name: "Math",
			}
			err := client.Insert(ctx, &class)
			require.NoError(t, err)
			require.Equal(t, "Math", class.Name)

			student := Student{
				Name:        "Bob",
				Takes_Class: []*Class{&class},
			}
			err = client.Insert(ctx, &student)
			require.NoError(t, err)
			require.NotEmpty(t, student.UID)

			student = Student{
				Name:        "Alice",
				Takes_Class: []*Class{&class},
			}
			err = client.Insert(ctx, &student)
			require.NoError(t, err)
			require.NotEmpty(t, student.UID)

			schema, err := client.GetSchema(ctx)
			require.NoError(t, err)
			require.Contains(t, schema, "type Student")
			require.Contains(t, schema, "type Class")

			// We cannot use the 'contructor' style querying because graph
			// querying uses the `expand(_all_)` operator, which does not
			// support reverse edges.
			var result []Class
			query := dg.NewQuery().Model(&result).Query(`
			{
				name
				uid
				~takes_class {
					name
					uid
				}
			}`)
			dgo, cleanup, err := client.DgraphClient()
			require.NoError(t, err)
			defer cleanup()
			tx := dg.NewReadOnlyTxn(dgo)
			err = tx.Query(query).Scan()
			require.NoError(t, err)

			require.Len(t, result, 1, "Should have found 1 class")
			require.Equal(t, "Math", result[0].Name, "Class name should match")
			require.Len(t, result[0].Students, 2, "Should have found 2 students")
		})
	}
}
