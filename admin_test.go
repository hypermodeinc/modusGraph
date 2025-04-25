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
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDropData(t *testing.T) {

	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "DropDataWithFileURI",
			uri:  "file://" + t.TempDir(),
		},
		{
			name: "DropDataWithDgraphURI",
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
				Description: "This is a test entity for the Insert method",
				CreatedAt:   time.Now(),
			}

			ctx := context.Background()
			err := client.Insert(ctx, &entity)
			require.NoError(t, err, "Insert should succeed")
			require.NotEmpty(t, entity.UID, "UID should be assigned")

			uid := entity.UID
			err = client.Get(ctx, &entity, uid)
			require.NoError(t, err, "Get should succeed")
			require.Equal(t, entity.Name, "Test Entity", "Name should match")
			require.Equal(t, entity.Description, "This is a test entity for the Insert method", "Description should match")

			err = client.DropData(ctx)
			require.NoError(t, err, "DropData should succeed")

			err = client.Get(ctx, &entity, uid)
			require.Error(t, err, "Get should fail after DropData")

			schema, err := client.GetSchema(ctx)
			require.NoError(t, err, "GetSchema should succeed")
			require.Contains(t, schema, "type TestEntity")
		})
	}
}

func TestDropAll(t *testing.T) {

	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "DropAllWithFileURI",
			uri:  "file://" + t.TempDir(),
		},
		{
			name: "DropAllWithDgraphURI",
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
				Description: "This is a test entity for the Insert method",
				CreatedAt:   time.Now(),
			}

			ctx := context.Background()
			err := client.Insert(ctx, &entity)
			require.NoError(t, err, "Insert should succeed")
			require.NotEmpty(t, entity.UID, "UID should be assigned")

			uid := entity.UID
			err = client.Get(ctx, &entity, uid)
			require.NoError(t, err, "Get should succeed")
			require.Equal(t, entity.Name, "Test Entity", "Name should match")
			require.Equal(t, entity.Description, "This is a test entity for the Insert method", "Description should match")

			err = client.DropAll(ctx)
			require.NoError(t, err, "DropAll should succeed")

			err = client.Get(ctx, &entity, uid)
			require.Error(t, err, "Get should fail after DropAll")

			schema, err := client.GetSchema(ctx)
			require.NoError(t, err, "GetSchema should succeed")
			require.NotContains(t, schema, "type TestEntity")
		})
	}
}

type Struct1 struct {
	UID   string   `json:"uid,omitempty"`
	Name  string   `json:"name,omitempty" dgraph:"index=term"`
	DType []string `json:"dgraph.type,omitempty"`
}

type Struct2 struct {
	UID   string   `json:"uid,omitempty"`
	Name  string   `json:"name,omitempty" dgraph:"index=term"`
	DType []string `json:"dgraph.type,omitempty"`
}

type Struct3 struct {
	UID   string   `json:"uid,omitempty"`
	Name  string   `json:"name,omitempty" dgraph:"index=term"`
	DType []string `json:"dgraph.type,omitempty"`

	Struct1 *Struct1 `json:"struct1,omitempty"`
	Struct2 *Struct2 `json:"struct2,omitempty"`
}

func TestCreateSchema(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "CreateSchemaWithFileURI",
			uri:  "file://" + t.TempDir(),
		},
		{
			name: "CreateSchemaWithDgraphURI",
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

			err := client.UpdateSchema(context.Background(), &Struct1{}, &Struct2{})
			require.NoError(t, err, "UpdateSchema should succeed")

			schema, err := client.GetSchema(context.Background())
			require.NoError(t, err, "GetSchema should succeed")
			require.Contains(t, schema, "type Struct1")
			require.Contains(t, schema, "type Struct2")

			err = client.DropAll(context.Background())
			require.NoError(t, err, "DropAll should succeed")

			// Test updating schema with nested types
			err = client.UpdateSchema(context.Background(), &Struct3{})
			require.NoError(t, err, "UpdateSchema should succeed")

			schema, err = client.GetSchema(context.Background())
			require.NoError(t, err, "GetSchema should succeed")
			require.Contains(t, schema, "type Struct1")
			require.Contains(t, schema, "type Struct2")
			require.Contains(t, schema, "type Struct3")
		})
	}
}
