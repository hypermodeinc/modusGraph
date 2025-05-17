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

func TestClientUpdate(t *testing.T) {

	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "UpdateWithFileURI",
			uri:  "file://" + t.TempDir(),
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
		})
	}
}
