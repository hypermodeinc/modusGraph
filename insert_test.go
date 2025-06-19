/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusgraph_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hypermodeinc/modusgraph"
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
			entity = TestEntity{
				Name:        "Test Entity",
				Description: "This is a test entity for the Insert method 2",
				CreatedAt:   time.Now(),
			}
			err = client.Insert(ctx, &entity)
			require.Error(t, err, "Insert should fail")
			if strings.HasPrefix(tc.uri, "file://") {
				require.IsType(t, &modusgraph.UniqueError{}, err, "Error should be a UniqueError")
				require.Equal(t, uid, err.(*modusgraph.UniqueError).UID, "UID should match")
			}

			var entities []TestEntity
			err = client.Query(ctx, TestEntity{}).Nodes(&entities)
			require.NoError(t, err, "Query should succeed")
			require.Len(t, entities, 1, "There should only be one entity")
		})
	}
}

type OuterTestEntity struct {
	Name   string      `json:"name,omitempty" dgraph:"index=exact unique"`
	Entity *TestEntity `json:"entity"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

func TestEmbeddedInsert(t *testing.T) {
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

			timestamp := time.Now().UTC().Truncate(time.Second)
			entity := OuterTestEntity{
				Name: "Test Outer Entity",
				Entity: &TestEntity{
					Name:        "Test Inner Entity",
					Description: "This is a test entity for the Insert method",
					CreatedAt:   timestamp,
				},
			}

			ctx := context.Background()
			err := client.Insert(ctx, &entity)
			require.NoError(t, err, "Insert should succeed")
			require.NotEmpty(t, entity.UID, "UID should be assigned")

			uid := entity.UID
			entity = OuterTestEntity{}
			err = client.Get(ctx, &entity, uid)
			require.NoError(t, err, "Get should succeed")
			require.Equal(t, "Test Outer Entity", entity.Name, "Name should match")
			require.Equal(t, "Test Inner Entity", entity.Entity.Name, "Entity.Name should match")
			require.Equal(t, "This is a test entity for the Insert method",
				entity.Entity.Description, "Entity.Description should match")
			require.Equal(t, timestamp, entity.Entity.CreatedAt, "Entity.CreatedAt should match")
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
			uri:  "file://" + GetTempDir(t),
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
	Name    string    `json:"name,omitempty" dgraph:"index=term"`
	Friends []*Person `json:"friends,omitempty"`

	UID   string   `json:"uid,omitempty"`
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
			uri:  "file://" + GetTempDir(t),
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
			err = client.Query(ctx, Person{}).Filter(`eq(name, "Alice")`).Nodes(&result)
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
