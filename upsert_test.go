/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusgraph_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type UpsertTestEntity struct {
	Name        string    `json:"name,omitempty" dgraph:"index=exact upsert"`
	AnotherName string    `json:"anotherName,omitempty" dgraph:"index=exact upsert"`
	Description string    `json:"description,omitempty" dgraph:"index=term"`
	CreatedAt   time.Time `json:"createdAt,omitzero"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

func TestClientUpsert(t *testing.T) {

	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "InsertWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "InsertWithDgraphURI",
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

			t.Run("basic test", func(t *testing.T) {
				entity := UpsertTestEntity{
					Name:        "Test Entity", // This is the upsert field
					Description: "This is a test entity for the Upsert method",
					CreatedAt:   time.Date(2021, 6, 9, 17, 22, 33, 0, time.UTC),
				}

				ctx := context.Background()
				err := client.Upsert(ctx, &entity)
				require.NoError(t, err, "Upsert should succeed")
				require.NotEmpty(t, entity.UID, "UID should be assigned")

				uid := entity.UID
				err = client.Get(ctx, &entity, uid)
				require.NoError(t, err, "Get should succeed")
				require.Equal(t, "Test Entity", entity.Name, "Name should match")
				require.Equal(t, "This is a test entity for the Upsert method", entity.Description, "Description should match")

				newTime := time.Now().UTC().Truncate(time.Second)
				entity = UpsertTestEntity{
					Name:        "Test Entity", // This is the upsert field
					Description: "Updated description",
					CreatedAt:   newTime,
				}

				err = client.Upsert(ctx, &entity)
				require.NoError(t, err, "Upsert should succeed")

				uid = entity.UID
				err = client.Get(ctx, &entity, uid)
				require.NoError(t, err, "Get should succeed")
				require.Equal(t, "Test Entity", entity.Name, "Name should match")
				require.Equal(t, "Updated description", entity.Description, "Description should match")
				require.Equal(t, newTime, entity.CreatedAt, "CreatedAt should match")

				var entities []UpsertTestEntity
				err = client.Query(ctx, UpsertTestEntity{}).Nodes(&entities)
				require.NoError(t, err, "Query should succeed")
				require.Len(t, entities, 1, "There should only be one entity")
			})
			t.Run("upsert with predicate", func(t *testing.T) {

				ctx := context.Background()
				require.NoError(t, client.DropAll(ctx), "Drop all should succeed")

				entity := UpsertTestEntity{
					AnotherName: "Test Entity", // This is another upsert field, we have to define it to the call to upsert
					Description: "This is a test entity for the Upsert method",
					CreatedAt:   time.Date(2021, 6, 9, 17, 22, 33, 0, time.UTC),
				}

				err := client.Upsert(ctx, &entity, "anotherName")
				require.NoError(t, err, "Upsert should succeed")
				require.NotEmpty(t, entity.UID, "UID should be assigned")

				uid := entity.UID
				err = client.Get(ctx, &entity, uid)
				require.NoError(t, err, "Get should succeed")
				require.Equal(t, "Test Entity", entity.AnotherName, "AnotherName should match")
				require.Equal(t, "This is a test entity for the Upsert method", entity.Description, "Description should match")

				newTime := time.Now().UTC().Truncate(time.Second)
				entity = UpsertTestEntity{
					AnotherName: "Test Entity",
					Description: "Updated description",
					CreatedAt:   newTime,
				}

				err = client.Upsert(ctx, &entity, "anotherName")
				require.NoError(t, err, "Upsert should succeed")

				uid = entity.UID
				err = client.Get(ctx, &entity, uid)
				require.NoError(t, err, "Get should succeed")
				require.Equal(t, "Test Entity", entity.AnotherName, "AnotherName should match")
				require.Equal(t, "Updated description", entity.Description, "Description should match")
				require.Equal(t, newTime, entity.CreatedAt, "CreatedAt should match")

				var entities []UpsertTestEntity
				err = client.Query(ctx, UpsertTestEntity{}).Nodes(&entities)
				require.NoError(t, err, "Query should succeed")
				require.Len(t, entities, 1, "There should only be one entity")
			})
		})
	}
}
