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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClientDelete(t *testing.T) {

	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "DeleteWithFileURI",
			uri:  "file://" + t.TempDir(),
		},
		{
			name: "DeleteWithDgraphURI",
			uri:  "dgraph://" + os.Getenv("MODUSGRAPH_TEST_ADDR"),
			skip: os.Getenv("MODUSGRAPH_TEST_ADDR") == "",
		},
	}

	createTestEntities := func() []*TestEntity {
		entities := []*TestEntity{}
		for i := range 10 {
			entities = append(entities, &TestEntity{
				Name:        fmt.Sprintf("Test Entity %d", i),
				Description: fmt.Sprintf("This is a test entity (%d) for the Update method", i),
				CreatedAt:   time.Now(),
			})
		}
		return entities
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: MODUSGRAPH_TEST_ADDR not set", tc.name)
				return
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			entities := createTestEntities()

			ctx := context.Background()
			err := client.Insert(ctx, entities)
			require.NoError(t, err, "Insert should succeed")
			require.NotEmpty(t, entities[0].UID, "UID should be assigned")

			// Get the UIDs of the first 5 entities
			uids := make([]string, 5)
			for i, entity := range entities[:5] {
				uids[i] = entity.UID
			}

			err = client.Delete(ctx, uids)
			require.NoError(t, err, "Delete should succeed")

			var result []TestEntity
			err = client.Query(ctx, TestEntity{}).Nodes(&result)
			require.NoError(t, err, "Query should succeed")
			require.Len(t, result, 5, "Should have 5 entities remaining")
		})
	}
}
