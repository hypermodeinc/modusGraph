/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusgraph_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClientUpdate(t *testing.T) {

	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "UpdateWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "UpdateWithDgraphURI",
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
				Description: "This is a test entity for the Update method",
				CreatedAt:   time.Now(),
			}

			ctx := context.Background()
			err := client.Insert(ctx, &entity)
			require.NoError(t, err, "Insert should succeed")
			require.NotEmpty(t, entity.UID, "UID should be assigned")

			uid := entity.UID
			err = client.Get(ctx, &entity, uid)
			require.NoError(t, err, "Get should succeed")
			require.Equal(t, entity.UID, uid, "UID should match")

			entity.Description = "Four score and seven years ago"
			err = client.Update(ctx, &entity)
			require.NoError(t, err, "Update should succeed")

			err = client.Get(ctx, &entity, uid)
			require.NoError(t, err, "Get should succeed")
			require.Equal(t, entity.Description, "Four score and seven years ago", "Description should match")

			entity = TestEntity{
				Name: "Test Entity 2",
			}
			err = client.Insert(ctx, &entity)
			require.NoError(t, err, "Insert should succeed")
			require.NotEmpty(t, entity.UID, "UID should be assigned")
			require.NotEqual(t, entity.UID, uid, "UID should be different")

			entity.Name = "Test Entity"
			err = client.Update(ctx, &entity)
			require.Error(t, err, "Update should fail because Name is unique")
		})
	}
}

func TestClientUpdateWithSlices(t *testing.T) {

	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "UpdateWithSlicesWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "UpdateWithSlicesWithDgraph",
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

			ctx := context.Background()
			err := client.DropAll(ctx)
			require.NoError(t, err, "DropAll should succeed")

			// 1. Test successful batch update
			entitiesToInsert := []*TestEntity{
				{Name: "Batch Entity 1", Description: "Initial description 1"},
				{Name: "Batch Entity 2", Description: "Initial description 2"},
				{Name: "Batch Entity 3", Description: "Initial description 3"},
			}

			err = client.Insert(ctx, entitiesToInsert)
			require.NoError(t, err, "Batch insert should succeed")
			for i, entity := range entitiesToInsert {
				require.NotEmpty(t, entity.UID, "Entity %d should have a UID", i)
			}

			for i := range entitiesToInsert {
				entitiesToInsert[i].Description = fmt.Sprintf("Updated description %d", i+1)
			}

			err = client.Update(ctx, entitiesToInsert)
			require.NoError(t, err, "Batch update should succeed")

			for i, entity := range entitiesToInsert {
				var fetched TestEntity
				err = client.Get(ctx, &fetched, entity.UID)
				require.NoError(t, err, "Get should succeed for entity %d", i)
				require.Equal(t, fmt.Sprintf("Updated description %d", i+1), fetched.Description,
					"Description should be updated for entity %d", i)
			}

			// 2. Test in-memory duplicate detection in slice
			duplicateEntities := []*TestEntity{
				{UID: entitiesToInsert[0].UID, Name: "Duplicate Name"},
				{UID: entitiesToInsert[1].UID, Name: "Duplicate Name"}, // Same name as first entity
			}

			err = client.Update(ctx, &duplicateEntities)
			require.Error(t, err, "Update should fail due to duplicate names in the slice")

			// 3. Test database-level unique constraint violation
			uniqueName := "Unique Entity Name"
			newEntity := TestEntity{Name: uniqueName}
			err = client.Insert(ctx, &newEntity)
			require.NoError(t, err, "Insert should succeed for new entity")

			conflictEntity := TestEntity{
				UID:  entitiesToInsert[0].UID,
				Name: uniqueName, // This will conflict with newEntity
			}
			err = client.Update(ctx, &conflictEntity)
			require.Error(t, err, "Update should fail due to unique constraint violation")

			// 4. Test mixed batch with some valid and some invalid updates
			mixedEntities := []*TestEntity{
				{UID: entitiesToInsert[0].UID, Description: "This update would be valid"}, // Valid
				{UID: entitiesToInsert[1].UID, Name: uniqueName},                          // Invalid - name conflict
			}

			err = client.Update(ctx, mixedEntities)
			require.Error(t, err, "Update should fail due to unique constraint violation in batch")
		})
	}
}

type EmbeddedNodeType struct {
	Name string `json:"node.name,omitempty" dgraph:"predicate=node.name"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

type AllTypes struct {
	Name      string    `json:"name,omitempty" dgraph:"index=exact"`
	Age       int       `json:"age,omitempty" dgraph:"index=int"`
	Bool      bool      `json:"bool,omitempty"`
	Value     float64   `json:"value,omitempty" dgraph:"index=float"`
	CreatedAt time.Time `json:"createdAt,omitzero" dgraph:"index=day"`
	Strings   []string  `json:"strings,omitempty" dgraph:"index=term"`

	Node  *EmbeddedNodeType   `json:"node,omitempty"`
	Nodes []*EmbeddedNodeType `json:"nodes,omitempty"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

func TestClientUpdateAllTypes(t *testing.T) {

	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "UpdateWithAllTypes",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "UpdateWithAllTypesWithDgraph",
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

			entity := AllTypes{
				Name:      "Test Entity",
				Age:       42,
				Value:     3.14,
				CreatedAt: time.Now(),
				Strings:   []string{"one", "two", "three"},
				Bool:      true,
				Node: &EmbeddedNodeType{
					Name: "Test Node",
				},
				Nodes: []*EmbeddedNodeType{
					{
						Name: "Node In Array, 1",
					},
					{
						Name: "Node In Array, 2",
					},
				},
			}

			ctx := context.Background()
			err := client.Insert(ctx, &entity)
			require.NoError(t, err, "Insert should succeed")
			require.NotEmpty(t, entity.UID, "UID should be assigned")

			uid := entity.UID
			entity = AllTypes{}
			err = client.Get(ctx, &entity, uid)
			require.NoError(t, err, "Get should succeed")
			require.Equal(t, "Test Entity", entity.Name, "Name should match")
			require.Equal(t, 42, entity.Age, "Age should match")
			require.Equal(t, 3.14, entity.Value, "Value should match")
			require.Equal(t, entity.CreatedAt, entity.CreatedAt, "CreatedAt should match")
			require.Equal(t, []string{"one", "two", "three"}, entity.Strings, "Strings should match")
			require.Equal(t, true, entity.Bool, "Bool should match")
			require.Equal(t, "Test Node", entity.Node.Name, "Node Name should match")
			require.NotEmpty(t, entity.Node.UID, "Node UID should be assigned")
			nodeNames := []string{entity.Nodes[0].Name, entity.Nodes[1].Name}
			require.ElementsMatch(t, []string{"Node In Array, 1", "Node In Array, 2"},
				nodeNames, "Node In Array Names should match")

			entity.Age = 43
			entity.Node.Name = "Updated Node"
			entity.Nodes[0].Name = "Updated Node In Array, 1"
			entity.Nodes[1].Name = "Updated Node In Array, 2"
			err = client.Update(ctx, &entity)
			require.NoError(t, err, "Update should succeed")

			entity = AllTypes{}
			err = client.Get(ctx, &entity, uid)
			require.NoError(t, err, "Get should succeed")
			require.Equal(t, 43, entity.Age, "Age should match")
			require.Equal(t, "Updated Node", entity.Node.Name, "Node Name should match")
			nodeNames = []string{entity.Nodes[0].Name, entity.Nodes[1].Name}
			require.ElementsMatch(t, []string{"Updated Node In Array, 1", "Updated Node In Array, 2"},
				nodeNames, "Node In Array Names should match")
		})
	}
}
