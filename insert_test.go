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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEntity is a test struct used for Insert tests
type TestEntity struct {
	UID         string    `json:"uid,omitempty"`
	Name        string    `json:"name,omitempty" dgraph:"index=term,exact unique"`
	Description string    `json:"description,omitempty" dgraph:"index=term"`
	CreatedAt   time.Time `json:"createdAt,omitempty"`
	DType       []string  `json:"dgraph.type,omitempty"`
}

func TestClientInsert(t *testing.T) {

	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "InsertWithFileURI",
			uri:  "file://" + t.TempDir(),
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

			// Try to insert the same entity again, should fail due to unique constraint
			// Note this doesn't work for local file clients at this time (planned improvement)
			if !strings.HasPrefix(tc.uri, "file://") {
				entity = TestEntity{
					Name:        "Test Entity",
					Description: "This is a test entity for the Insert method 2",
					CreatedAt:   time.Now(),
				}
				err = client.Insert(ctx, &entity)
				fmt.Println(err)
				require.Error(t, err, "Insert should fail because Name is unique")
			}
		})
	}
}

func TestClientInsertMultipleEntities(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "InsertMultipleWithFileURI",
			uri:  "file://" + t.TempDir(),
		},
		{
			name: "InsertMultipleWithDgraphURI",
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

			// Note the `*TestEntity`, the elements in the slice must be pointers
			entities := []*TestEntity{
				{
					Name:        "Entity 1",
					Description: "First test entity",
					CreatedAt:   time.Now().Add(-1 * time.Hour),
				},
				{
					Name:        "Entity 2",
					Description: "Second test entity",
					CreatedAt:   time.Now(),
				},
			}

			ctx := context.Background()
			err := client.Insert(ctx, entities)
			require.NoError(t, err, "Insert should succeed")

			var result []TestEntity
			err = client.Query(ctx, TestEntity{}).OrderDesc("createdAt").First(1).Nodes(&result)
			require.NoError(t, err, "Query should succeed")
			assert.Len(t, result, 1, "Should have found one entity")
			assert.Equal(t, entities[1].Name, result[0].Name, "Name should match")
		})
	}
}

type Person struct {
	UID     string    `json:"uid,omitempty"`
	Name    string    `json:"name,omitempty" dgraph:"index=term"`
	Friends []*Person `json:"friends,omitempty"`

	DType []string `json:"dgraph.type,omitempty"`
}

func TestDepthQuery(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "InsertWithFileURI",
			uri:  "file://" + t.TempDir(),
		},
		{
			name: "InsertWithDgraphURI",
			uri:  "dgraph://" + os.Getenv("MODUSGRAPH_TEST_ADDR"),
			skip: os.Getenv("MODUSGRAPH_TEST_ADDR") == "",
		},
	}

	createPerson := func() Person {
		return Person{
			Name: "Alice",
			Friends: []*Person{
				{
					Name: "Bob",
					Friends: []*Person{
						{
							Name: "Charles",
						},
						{
							Name: "David",
							Friends: []*Person{
								{
									Name: "Eve",
									Friends: []*Person{
										{
											Name: "Frank",
										},
									},
								},
								{
									Name: "George",
								},
							},
						},
					},
				},
			},
		}
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
			person := createPerson()
			err := client.Insert(ctx, &person)
			require.NoError(t, err, "Insert should succeed")

			var result []Person
			err = client.Query(ctx, Person{}).Filter(`eq(name, "Alice")`).All(10).Nodes(&result)
			require.NoError(t, err, "Query should succeed")
			assert.Equal(t, person.Name, result[0].Name, "Name should match")

			verifyPersonStructure(t, &person, &result[0])
		})
	}
}

func verifyPersonStructure(t *testing.T, expected *Person, actual *Person) {
	t.Helper()
	require.NotNil(t, actual, "Person should not be nil")
	assert.Equal(t, expected.Name, actual.Name, "Name should match")

	if expected.Friends == nil {
		assert.Empty(t, actual.Friends, "Should have no friends")
		return
	}

	require.Len(t, actual.Friends, len(expected.Friends),
		"%s should have %d friends", expected.Name, len(expected.Friends))

	// Create a map of expected friends by name for easier lookup
	expectedFriends := make(map[string]*Person)
	for _, friend := range expected.Friends {
		expectedFriends[friend.Name] = friend
	}

	// Verify each actual friend
	for _, actualFriend := range actual.Friends {
		expectedFriend, ok := expectedFriends[actualFriend.Name]
		require.True(t, ok, "%s should have a friend named %s",
			expected.Name, actualFriend.Name)

		// Recursively verify this friend's structure
		verifyPersonStructure(t, expectedFriend, actualFriend)
	}
}
