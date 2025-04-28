/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package unit_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hypermodeinc/modusgraph"
	"github.com/hypermodeinc/modusgraph/api"
	"github.com/hypermodeinc/modusgraph/api/apiutils"
)

type User struct {
	Gid     uint64 `json:"gid,omitempty"`
	Name    string `json:"name,omitempty"`
	Age     int    `json:"age,omitempty"`
	ClerkId string `json:"clerk_id,omitempty" db:"constraint=unique"`
}

func TestFirstTimeUser(t *testing.T) {
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	gid, user, err := modusgraph.Create(context.Background(), engine, User{
		Name:    "A",
		Age:     10,
		ClerkId: "123",
	})

	require.NoError(t, err)
	require.Equal(t, user.Gid, gid)
	require.Equal(t, "A", user.Name)
	require.Equal(t, 10, user.Age)
	require.Equal(t, "123", user.ClerkId)

	gid, queriedUser, err := modusgraph.Get[User](context.Background(), engine, gid)

	require.NoError(t, err)
	require.Equal(t, queriedUser.Gid, gid)
	require.Equal(t, 10, queriedUser.Age)
	require.Equal(t, "A", queriedUser.Name)
	require.Equal(t, "123", queriedUser.ClerkId)

	gid, queriedUser2, err := modusgraph.Get[User](context.Background(), engine, modusgraph.ConstrainedField{
		Key:   "clerk_id",
		Value: "123",
	})

	require.NoError(t, err)
	require.Equal(t, queriedUser.Gid, gid)
	require.Equal(t, 10, queriedUser2.Age)
	require.Equal(t, "A", queriedUser2.Name)
	require.Equal(t, "123", queriedUser2.ClerkId)

	_, _, err = modusgraph.Delete[User](context.Background(), engine, gid)
	require.NoError(t, err)

	_, queriedUser3, err := modusgraph.Get[User](context.Background(), engine, gid)
	require.Error(t, err)
	require.Equal(t, "no object found", err.Error())
	require.Equal(t, queriedUser3, User{})
}

func TestGetBeforeObjectWrite(t *testing.T) {
	ctx := context.Background()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()
	ns, err := engine.CreateNamespace()
	require.NoError(t, err)

	_, _, err = modusgraph.Get[User](ctx, engine, uint64(1), ns.ID())
	require.Error(t, err)

	_, _, err = modusgraph.Get[User](ctx, engine, modusgraph.ConstrainedField{
		Key:   "name",
		Value: "test",
	}, ns.ID())
	require.Error(t, err)
	require.Equal(t, "type not found", err.Error())
}

func TestCreateApi(t *testing.T) {
	ctx := context.Background()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, ns1.DropData(ctx))

	user := User{
		Name:    "B",
		Age:     20,
		ClerkId: "123",
	}

	gid, user, err := modusgraph.Create(context.Background(), engine, user, ns1.ID())
	require.NoError(t, err)

	require.Equal(t, "B", user.Name)
	require.Equal(t, user.Gid, gid)

	query := `{
		me(func: has(User.name)) {
			uid
			User.name
			User.age
			User.clerk_id
		}
	}`
	resp, err := ns1.Query(ctx, query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"uid":"0x2","User.name":"B","User.age":20,"User.clerk_id":"123"}]}`,
		string(resp.GetJson()))
}

func TestCreateApiWithNonStruct(t *testing.T) {
	ctx := context.Background()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, ns1.DropData(ctx))

	user := User{
		Name: "B",
		Age:  20,
	}

	_, _, err = modusgraph.Create[*User](context.Background(), engine, &user, ns1.ID())
	require.Error(t, err)
	require.Equal(t, "expected struct, got ptr", err.Error())

	_, _, err = modusgraph.Create[[]string](context.Background(), engine, []string{"foo", "bar"}, ns1.ID())
	require.Error(t, err)
	require.Equal(t, "expected struct, got slice", err.Error())

	_, _, err = modusgraph.Create[float32](context.Background(), engine, 3.1415, ns1.ID())
	require.Error(t, err)
	require.Equal(t, "expected struct, got float32", err.Error())
}

func TestGetApi(t *testing.T) {
	ctx := context.Background()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, ns1.DropData(ctx))

	user := User{
		Name:    "B",
		Age:     20,
		ClerkId: "123",
	}

	gid, _, err := modusgraph.Create(context.Background(), engine, user, ns1.ID())
	require.NoError(t, err)

	gid, queriedUser, err := modusgraph.Get[User](context.Background(), engine, gid, ns1.ID())

	require.NoError(t, err)
	require.Equal(t, queriedUser.Gid, gid)
	require.Equal(t, 20, queriedUser.Age)
	require.Equal(t, "B", queriedUser.Name)
	require.Equal(t, "123", queriedUser.ClerkId)
}

func TestGetApiWithConstrainedField(t *testing.T) {
	ctx := context.Background()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, ns1.DropData(ctx))

	user := User{
		Name:    "B",
		Age:     20,
		ClerkId: "123",
	}

	_, _, err = modusgraph.Create(context.Background(), engine, user, ns1.ID())
	require.NoError(t, err)

	gid, queriedUser, err := modusgraph.Get[User](context.Background(), engine, modusgraph.ConstrainedField{
		Key:   "clerk_id",
		Value: "123",
	}, ns1.ID())

	require.NoError(t, err)
	require.Equal(t, queriedUser.Gid, gid)
	require.Equal(t, 20, queriedUser.Age)
	require.Equal(t, "B", queriedUser.Name)
	require.Equal(t, "123", queriedUser.ClerkId)
}

func TestDeleteApi(t *testing.T) {
	ctx := context.Background()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, ns1.DropData(ctx))

	user := User{
		Name:    "B",
		Age:     20,
		ClerkId: "123",
	}

	gid, _, err := modusgraph.Create(context.Background(), engine, user, ns1.ID())
	require.NoError(t, err)

	_, _, err = modusgraph.Delete[User](context.Background(), engine, gid, ns1.ID())
	require.NoError(t, err)

	_, queriedUser, err := modusgraph.Get[User](context.Background(), engine, gid, ns1.ID())
	require.Error(t, err)
	require.Equal(t, "no object found", err.Error())
	require.Equal(t, queriedUser, User{})

	_, queriedUser, err = modusgraph.Get[User](context.Background(), engine, modusgraph.ConstrainedField{
		Key:   "clerk_id",
		Value: "123",
	}, ns1.ID())
	require.Error(t, err)
	require.Equal(t, "no object found", err.Error())
	require.Equal(t, queriedUser, User{})
}

func TestUpsertApi(t *testing.T) {
	ctx := context.Background()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, ns1.DropData(ctx))

	user := User{
		Name:    "B",
		Age:     20,
		ClerkId: "123",
	}

	gid, user, _, err := modusgraph.Upsert(context.Background(), engine, user, ns1.ID())
	require.NoError(t, err)
	require.Equal(t, user.Gid, gid)

	user.Age = 21
	gid, _, _, err = modusgraph.Upsert(context.Background(), engine, user, ns1.ID())
	require.NoError(t, err)
	require.Equal(t, user.Gid, gid)

	_, queriedUser, err := modusgraph.Get[User](context.Background(), engine, gid, ns1.ID())
	require.NoError(t, err)
	require.Equal(t, user.Gid, queriedUser.Gid)
	require.Equal(t, 21, queriedUser.Age)
	require.Equal(t, "B", queriedUser.Name)
	require.Equal(t, "123", queriedUser.ClerkId)
}

func TestQueryApi(t *testing.T) {
	ctx := context.Background()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, ns1.DropData(ctx))

	users := []User{
		{Name: "A", Age: 10, ClerkId: "123"},
		{Name: "B", Age: 20, ClerkId: "123"},
		{Name: "C", Age: 30, ClerkId: "123"},
		{Name: "D", Age: 40, ClerkId: "123"},
		{Name: "E", Age: 50, ClerkId: "123"},
	}

	for _, user := range users {
		_, _, err = modusgraph.Create(context.Background(), engine, user, ns1.ID())
		require.NoError(t, err)
	}

	gids, queriedUsers, err := modusgraph.Query[User](context.Background(), engine, modusgraph.QueryParams{}, ns1.ID())
	require.NoError(t, err)
	require.Len(t, queriedUsers, 5)
	require.Len(t, gids, 5)
	require.Equal(t, "A", queriedUsers[0].Name)
	require.Equal(t, "B", queriedUsers[1].Name)
	require.Equal(t, "C", queriedUsers[2].Name)
	require.Equal(t, "D", queriedUsers[3].Name)
	require.Equal(t, "E", queriedUsers[4].Name)

	gids, queriedUsers, err = modusgraph.Query[User](context.Background(), engine, modusgraph.QueryParams{
		Filter: &modusgraph.Filter{
			Field: "age",
			String: modusgraph.StringPredicate{
				// The reason its a string even for int is bc i cant tell if
				// user wants to compare with 0 the number or didn't provide a value
				// TODO: fix this
				GreaterOrEqual: fmt.Sprintf("%d", 20),
			},
		},
	}, ns1.ID())

	require.NoError(t, err)
	require.Len(t, queriedUsers, 4)
	require.Len(t, gids, 4)
	require.Equal(t, "B", queriedUsers[0].Name)
	require.Equal(t, "C", queriedUsers[1].Name)
	require.Equal(t, "D", queriedUsers[2].Name)
	require.Equal(t, "E", queriedUsers[3].Name)
}

func TestQueryApiWithPaginiationAndSorting(t *testing.T) {
	ctx := context.Background()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, ns1.DropData(ctx))

	users := []User{
		{Name: "A", Age: 10, ClerkId: "123"},
		{Name: "B", Age: 20, ClerkId: "123"},
		{Name: "C", Age: 30, ClerkId: "123"},
		{Name: "D", Age: 40, ClerkId: "123"},
		{Name: "E", Age: 50, ClerkId: "123"},
	}

	for _, user := range users {
		_, _, err = modusgraph.Create(context.Background(), engine, user, ns1.ID())
		require.NoError(t, err)
	}

	gids, queriedUsers, err := modusgraph.Query[User](context.Background(), engine, modusgraph.QueryParams{
		Filter: &modusgraph.Filter{
			Field: "age",
			String: modusgraph.StringPredicate{
				GreaterOrEqual: fmt.Sprintf("%d", 20),
			},
		},
		Pagination: &modusgraph.Pagination{
			Limit:  3,
			Offset: 1,
		},
	}, ns1.ID())

	require.NoError(t, err)
	require.Len(t, queriedUsers, 3)
	require.Len(t, gids, 3)
	require.Equal(t, "C", queriedUsers[0].Name)
	require.Equal(t, "D", queriedUsers[1].Name)
	require.Equal(t, "E", queriedUsers[2].Name)

	gids, queriedUsers, err = modusgraph.Query[User](context.Background(), engine, modusgraph.QueryParams{
		Pagination: &modusgraph.Pagination{
			Limit:  3,
			Offset: 1,
		},
		Sorting: &modusgraph.Sorting{
			OrderAscField: "age",
		},
	}, ns1.ID())

	require.NoError(t, err)
	require.Len(t, queriedUsers, 3)
	require.Len(t, gids, 3)
	require.Equal(t, "B", queriedUsers[0].Name)
	require.Equal(t, "C", queriedUsers[1].Name)
	require.Equal(t, "D", queriedUsers[2].Name)
}

type Project struct {
	Gid      uint64   `json:"gid,omitempty"`
	Name     string   `json:"name,omitempty"`
	ClerkId  string   `json:"clerk_id,omitempty" db:"constraint=unique"`
	Branches []Branch `json:"branches,omitempty" readFrom:"type=Branch,field=proj"`
}

type Branch struct {
	Gid     uint64  `json:"gid,omitempty"`
	Name    string  `json:"name,omitempty"`
	ClerkId string  `json:"clerk_id,omitempty" db:"constraint=unique"`
	Proj    Project `json:"proj,omitempty"`
}

func TestReverseEdgeGet(t *testing.T) {
	ctx := context.Background()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, ns1.DropData(ctx))

	projGid, project, err := modusgraph.Create(context.Background(), engine, Project{
		Name:    "P",
		ClerkId: "456",
		Branches: []Branch{
			{Name: "B", ClerkId: "123"},
			{Name: "B2", ClerkId: "456"},
		},
	}, ns1.ID())
	require.NoError(t, err)

	require.Equal(t, "P", project.Name)
	require.Equal(t, project.Gid, projGid)

	// modifying a read-only field will be a no-op
	require.Len(t, project.Branches, 0)

	branch1 := Branch{
		Name:    "B",
		ClerkId: "123",
		Proj: Project{
			Gid: projGid,
		},
	}

	branch1Gid, branch1, err := modusgraph.Create(context.Background(), engine, branch1, ns1.ID())
	require.NoError(t, err)

	require.Equal(t, "B", branch1.Name)
	require.Equal(t, branch1.Gid, branch1Gid)
	require.Equal(t, projGid, branch1.Proj.Gid)
	require.Equal(t, "P", branch1.Proj.Name)

	branch2 := Branch{
		Name:    "B2",
		ClerkId: "456",
		Proj: Project{
			Gid: projGid,
		},
	}

	branch2Gid, branch2, err := modusgraph.Create(context.Background(), engine, branch2, ns1.ID())
	require.NoError(t, err)
	require.Equal(t, "B2", branch2.Name)
	require.Equal(t, branch2.Gid, branch2Gid)
	require.Equal(t, projGid, branch2.Proj.Gid)

	getProjGid, queriedProject, err := modusgraph.Get[Project](context.Background(), engine, projGid, ns1.ID())
	require.NoError(t, err)
	require.Equal(t, projGid, getProjGid)
	require.Equal(t, "P", queriedProject.Name)
	require.Len(t, queriedProject.Branches, 2)
	require.Equal(t, "B", queriedProject.Branches[0].Name)
	require.Equal(t, "B2", queriedProject.Branches[1].Name)

	queryBranchesGids, queriedBranches, err := modusgraph.Query[Branch](context.Background(), engine,
		modusgraph.QueryParams{}, ns1.ID())
	require.NoError(t, err)
	require.Len(t, queriedBranches, 2)
	require.Len(t, queryBranchesGids, 2)
	require.Equal(t, "B", queriedBranches[0].Name)
	require.Equal(t, "B2", queriedBranches[1].Name)

	// max depth is 2, so we should not see the branches within project
	require.Len(t, queriedBranches[0].Proj.Branches, 0)

	_, _, err = modusgraph.Delete[Project](context.Background(), engine, projGid, ns1.ID())
	require.NoError(t, err)

	queryBranchesGids, queriedBranches, err = modusgraph.Query[Branch](context.Background(), engine,
		modusgraph.QueryParams{}, ns1.ID())
	require.NoError(t, err)
	require.Len(t, queriedBranches, 2)
	require.Len(t, queryBranchesGids, 2)
	require.Equal(t, "B", queriedBranches[0].Name)
	require.Equal(t, "B2", queriedBranches[1].Name)
}

func TestReverseEdgeQuery(t *testing.T) {
	ctx := context.Background()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, ns1.DropData(ctx))

	projects := []Project{
		{Name: "P1", ClerkId: "456"},
		{Name: "P2", ClerkId: "789"},
	}

	branchCounter := 1
	clerkCounter := 100

	for _, project := range projects {
		projGid, project, err := modusgraph.Create(context.Background(), engine, project, ns1.ID())
		require.NoError(t, err)
		require.Equal(t, project.Name, project.Name)
		require.Equal(t, project.Gid, projGid)

		branches := []Branch{
			{Name: fmt.Sprintf("B%d", branchCounter), ClerkId: fmt.Sprintf("%d", clerkCounter), Proj: Project{Gid: projGid}},
			{Name: fmt.Sprintf("B%d", branchCounter+1), ClerkId: fmt.Sprintf("%d", clerkCounter+1), Proj: Project{Gid: projGid}},
		}
		branchCounter += 2
		clerkCounter += 2

		for _, branch := range branches {
			branchGid, branch, err := modusgraph.Create(context.Background(), engine, branch, ns1.ID())
			require.NoError(t, err)
			require.Equal(t, branch.Name, branch.Name)
			require.Equal(t, branch.Gid, branchGid)
			require.Equal(t, projGid, branch.Proj.Gid)
		}
	}

	queriedProjectsGids, queriedProjects, err := modusgraph.Query[Project](context.Background(),
		engine, modusgraph.QueryParams{}, ns1.ID())
	require.NoError(t, err)
	require.Len(t, queriedProjects, 2)
	require.Len(t, queriedProjectsGids, 2)
	require.Equal(t, "P1", queriedProjects[0].Name)
	require.Equal(t, "P2", queriedProjects[1].Name)
	require.Len(t, queriedProjects[0].Branches, 2)
	require.Len(t, queriedProjects[1].Branches, 2)
	require.Equal(t, "B1", queriedProjects[0].Branches[0].Name)
	require.Equal(t, "B2", queriedProjects[0].Branches[1].Name)
	require.Equal(t, "B3", queriedProjects[1].Branches[0].Name)
	require.Equal(t, "B4", queriedProjects[1].Branches[1].Name)
}

func TestNestedObjectMutation(t *testing.T) {
	ctx := context.Background()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, ns1.DropData(ctx))

	branch := Branch{
		Name:    "B",
		ClerkId: "123",
		Proj: Project{
			Name:    "P",
			ClerkId: "456",
		},
	}

	gid, branch, err := modusgraph.Create(context.Background(), engine, branch, ns1.ID())
	require.NoError(t, err)

	require.Equal(t, "B", branch.Name)
	require.Equal(t, branch.Gid, gid)
	require.NotEqual(t, uint64(0), branch.Proj.Gid)
	require.Equal(t, "P", branch.Proj.Name)

	query := `{
		me(func: has(Branch.name)) {
			uid
			Branch.name
			Branch.clerk_id
			Branch.proj {
				uid
				Project.name
				Project.clerk_id
			}
		}
	}`
	resp, err := ns1.Query(ctx, query)
	require.NoError(t, err)
	require.JSONEq(t,
		`{"me":[{"uid":"0x2","Branch.name":"B","Branch.clerk_id":"123","Branch.proj": 
		{"uid":"0x3","Project.name":"P","Project.clerk_id":"456"}}]}`,
		string(resp.GetJson()))

	gid, queriedBranch, err := modusgraph.Get[Branch](context.Background(), engine, gid, ns1.ID())
	require.NoError(t, err)
	require.Equal(t, queriedBranch.Gid, gid)
	require.Equal(t, "B", queriedBranch.Name)

}

func TestLinkingObjectsByConstrainedFields(t *testing.T) {
	ctx := context.Background()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, ns1.DropData(ctx))

	projGid, project, err := modusgraph.Create(context.Background(), engine, Project{
		Name:    "P",
		ClerkId: "456",
	}, ns1.ID())
	require.NoError(t, err)

	require.Equal(t, "P", project.Name)
	require.Equal(t, project.Gid, projGid)

	branch := Branch{
		Name:    "B",
		ClerkId: "123",
		Proj: Project{
			Name:    "P",
			ClerkId: "456",
		},
	}

	gid, branch, err := modusgraph.Create(context.Background(), engine, branch, ns1.ID())
	require.NoError(t, err)

	require.Equal(t, "B", branch.Name)
	require.Equal(t, branch.Gid, gid)
	require.Equal(t, projGid, branch.Proj.Gid)
	require.Equal(t, "P", branch.Proj.Name)

	query := `{
		me(func: has(Branch.name)) {
			uid
			Branch.name
			Branch.clerk_id
			Branch.proj {
				uid
				Project.name
				Project.clerk_id
			}
		}
	}`
	resp, err := ns1.Query(ctx, query)
	require.NoError(t, err)
	require.JSONEq(t,
		`{"me":[{"uid":"0x3","Branch.name":"B","Branch.clerk_id":"123","Branch.proj":
		{"uid":"0x2","Project.name":"P","Project.clerk_id":"456"}}]}`,
		string(resp.GetJson()))

	gid, queriedBranch, err := modusgraph.Get[Branch](context.Background(), engine, gid, ns1.ID())
	require.NoError(t, err)
	require.Equal(t, queriedBranch.Gid, gid)
	require.Equal(t, "B", queriedBranch.Name)

}

func TestLinkingObjectsByGid(t *testing.T) {
	ctx := context.Background()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, ns1.DropData(ctx))

	projGid, project, err := modusgraph.Create(context.Background(), engine, Project{
		Name:    "P",
		ClerkId: "456",
	}, ns1.ID())
	require.NoError(t, err)

	require.Equal(t, "P", project.Name)
	require.Equal(t, project.Gid, projGid)

	branch := Branch{
		Name:    "B",
		ClerkId: "123",
		Proj: Project{
			Gid: projGid,
		},
	}

	gid, branch, err := modusgraph.Create(context.Background(), engine, branch, ns1.ID())
	require.NoError(t, err)

	require.Equal(t, "B", branch.Name)
	require.Equal(t, branch.Gid, gid)
	require.Equal(t, projGid, branch.Proj.Gid)
	require.Equal(t, "P", branch.Proj.Name)

	query := `{
		me(func: has(Branch.name)) {
			uid
			Branch.name
			Branch.clerk_id
			Branch.proj {
				uid
				Project.name
				Project.clerk_id
			}
		}
	}`
	resp, err := ns1.Query(ctx, query)
	require.NoError(t, err)
	require.JSONEq(t,
		`{"me":[{"uid":"0x3","Branch.name":"B","Branch.clerk_id":"123",
		"Branch.proj":{"uid":"0x2","Project.name":"P","Project.clerk_id":"456"}}]}`,
		string(resp.GetJson()))

	gid, queriedBranch, err := modusgraph.Get[Branch](context.Background(), engine, gid, ns1.ID())
	require.NoError(t, err)
	require.Equal(t, queriedBranch.Gid, gid)
	require.Equal(t, "B", queriedBranch.Name)

}

type BadProject struct {
	Name    string `json:"name,omitempty"`
	ClerkId string `json:"clerk_id,omitempty"`
}

type BadBranch struct {
	Gid     uint64     `json:"gid,omitempty"`
	Name    string     `json:"name,omitempty"`
	ClerkId string     `json:"clerk_id,omitempty" db:"constraint=unique"`
	Proj    BadProject `json:"proj,omitempty"`
}

func TestNestedObjectMutationWithBadType(t *testing.T) {
	ctx := context.Background()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, ns1.DropData(ctx))

	branch := BadBranch{
		Name:    "B",
		ClerkId: "123",
		Proj: BadProject{
			Name:    "P",
			ClerkId: "456",
		},
	}

	_, _, err = modusgraph.Create(context.Background(), engine, branch, ns1.ID())
	require.Error(t, err)
	require.Equal(t, fmt.Sprintf(apiutils.NoUniqueConstr, "BadProject"), err.Error())

	proj := BadProject{
		Name:    "P",
		ClerkId: "456",
	}

	_, _, err = modusgraph.Create(context.Background(), engine, proj, ns1.ID())
	require.Error(t, err)
	require.Equal(t, fmt.Sprintf(apiutils.NoUniqueConstr, "BadProject"), err.Error())

}

type Document struct {
	Gid     uint64    `json:"gid,omitempty"`
	Text    string    `json:"text,omitempty"`
	TextVec []float32 `json:"textVec,omitempty" db:"constraint=vector"`
}

func TestVectorIndexSearchTyped(t *testing.T) {
	ctx := context.Background()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, ns1.DropData(ctx))

	documents := []Document{
		{Text: "apple", TextVec: []float32{0.1, 0.1, 0.0}},
		{Text: "banana", TextVec: []float32{0.0, 1.0, 0.0}},
		{Text: "carrot", TextVec: []float32{0.0, 0.0, 1.0}},
		{Text: "dog", TextVec: []float32{1.0, 1.0, 0.0}},
		{Text: "elephant", TextVec: []float32{0.0, 1.0, 1.0}},
		{Text: "fox", TextVec: []float32{1.0, 0.0, 1.0}},
		{Text: "gorilla", TextVec: []float32{1.0, 1.0, 1.0}},
	}

	for _, doc := range documents {
		_, _, err = modusgraph.Create(context.Background(), engine, doc, ns1.ID())
		require.NoError(t, err)
	}

	const query = `
		{
			documents(func: similar_to(Document.textVec, 5, "[0.1,0.1,0.1]")) {
					Document.text
			}
		}`

	resp, err := ns1.Query(ctx, query)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"documents":[
			{"Document.text":"apple"},
			{"Document.text":"dog"},
			{"Document.text":"elephant"},
			{"Document.text":"fox"},
			{"Document.text":"gorilla"}
		]
	}`, string(resp.GetJson()))

	const query2 = `
		{
			documents(func: type("Document")) @filter(similar_to(Document.textVec, 5, "[0.1,0.1,0.1]")) {
					Document.text
			}
		}`

	resp, err = ns1.Query(ctx, query2)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"documents":[
			{"Document.text":"apple"},
			{"Document.text":"dog"},
			{"Document.text":"elephant"},
			{"Document.text":"fox"},
			{"Document.text":"gorilla"}
		]
	}`, string(resp.GetJson()))
}

func TestVectorIndexSearchWithQuery(t *testing.T) {
	ctx := context.Background()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, ns1.DropData(ctx))

	documents := []Document{
		{Text: "apple", TextVec: []float32{0.1, 0.1, 0.0}},
		{Text: "banana", TextVec: []float32{0.0, 1.0, 0.0}},
		{Text: "carrot", TextVec: []float32{0.0, 0.0, 1.0}},
		{Text: "dog", TextVec: []float32{1.0, 1.0, 0.0}},
		{Text: "elephant", TextVec: []float32{0.0, 1.0, 1.0}},
		{Text: "fox", TextVec: []float32{1.0, 0.0, 1.0}},
		{Text: "gorilla", TextVec: []float32{1.0, 1.0, 1.0}},
	}

	for _, doc := range documents {
		_, _, err = modusgraph.Create(context.Background(), engine, doc, ns1.ID())
		require.NoError(t, err)
	}

	gids, docs, err := modusgraph.Query[Document](context.Background(), engine, modusgraph.QueryParams{
		Filter: &modusgraph.Filter{
			Field: "textVec",
			Vector: modusgraph.VectorPredicate{
				SimilarTo: []float32{0.1, 0.1, 0.1},
				TopK:      5,
			},
		},
	}, ns1.ID())

	require.NoError(t, err)
	require.Len(t, docs, 5)
	require.Len(t, gids, 5)
	require.Equal(t, "apple", docs[0].Text)
	require.Equal(t, "dog", docs[1].Text)
	require.Equal(t, "elephant", docs[2].Text)
	require.Equal(t, "fox", docs[3].Text)
	require.Equal(t, "gorilla", docs[4].Text)
}

type Alltypes struct {
	Gid        uint64  `json:"gid,omitempty"`
	Name       string  `json:"name,omitempty"`
	Age        int     `json:"age,omitempty"`
	Count      int64   `json:"count,omitempty"`
	Married    bool    `json:"married,omitempty"`
	FloatVal   float32 `json:"floatVal,omitempty"`
	Float64Val float64 `json:"float64Val,omitempty"`
	//Loc        geom.Point `json:"loc,omitempty"`
	DoB time.Time `json:"dob,omitempty"`
}

func TestAllSchemaTypes(t *testing.T) {
	ctx := context.Background()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	require.NoError(t, engine.DropAll(ctx))

	//loc := geom.NewPoint(geom.XY).MustSetCoords(geom.Coord{-122.082506, 37.4249518})
	dob := time.Date(1965, 6, 24, 0, 0, 0, 0, time.UTC)
	_, omnibus, err := modusgraph.Create(context.Background(), engine, Alltypes{
		Name:       "John Doe",
		Age:        30,
		Count:      100,
		Married:    true,
		FloatVal:   3.14159,
		Float64Val: 123.456789,
		//Loc:        *loc,
		DoB: dob,
	})

	require.NoError(t, err)
	require.NotZero(t, omnibus.Gid)
	require.Equal(t, "John Doe", omnibus.Name)
	require.Equal(t, 30, omnibus.Age)
	require.Equal(t, true, omnibus.Married)
	require.Equal(t, int64(100), omnibus.Count)
	require.Equal(t, float32(3.14159), omnibus.FloatVal)
	require.InDelta(t, 123.456789, omnibus.Float64Val, 0.000001)
	//require.Equal(t, loc, omnibus.Loc)
	require.Equal(t, dob, omnibus.DoB)
}

type TimeStruct struct {
	Name    string     `json:"name,omitempty" db:"constraint=unique"`
	Time    time.Time  `json:"time,omitempty"`
	TimePtr *time.Time `json:"timePtr,omitempty"`
}

func TestTime(t *testing.T) {
	ctx := context.Background()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	d := time.Date(1965, 6, 24, 12, 0, 0, 0, time.UTC)
	gid, justTime, err := modusgraph.Create(ctx, engine, TimeStruct{
		Name:    "John Doe",
		Time:    d,
		TimePtr: &d,
	})
	require.NoError(t, err)

	require.Equal(t, "John Doe", justTime.Name)
	require.Equal(t, d, justTime.Time)
	require.Equal(t, d, *justTime.TimePtr)

	_, justTime, err = modusgraph.Get[TimeStruct](ctx, engine, gid)
	require.NoError(t, err)
	require.Equal(t, "John Doe", justTime.Name)
	require.Equal(t, d, justTime.Time)
	require.Equal(t, d, *justTime.TimePtr)

	// Add another time entry
	d2 := time.Date(1965, 6, 24, 11, 59, 59, 0, time.UTC)
	_, _, err = modusgraph.Create(ctx, engine, TimeStruct{
		Name:    "Jane Doe",
		Time:    d2,
		TimePtr: &d2,
	})
	require.NoError(t, err)

	_, entries, err := modusgraph.Query[TimeStruct](ctx, engine, modusgraph.QueryParams{
		Filter: &modusgraph.Filter{
			Field: "time",
			String: modusgraph.StringPredicate{
				// TODO: Not too crazy about this. Thinking we should add XXXPredicate definitions for all scalars -MM
				GreaterOrEqual: fmt.Sprintf("\"%s\"", d.Format(time.RFC3339)),
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "John Doe", entries[0].Name)
	require.Equal(t, d, entries[0].Time)
	require.Equal(t, d, *entries[0].TimePtr)
}

type GeomStruct struct {
	Gid       uint64           `json:"gid,omitempty"`
	Name      string           `json:"name,omitempty" db:"constraint=unique"`
	Point     api.Point        `json:"loc,omitempty"`
	Area      api.Polygon      `json:"area,omitempty"`
	MultiArea api.MultiPolygon `json:"multiArea,omitempty"`
}

func TestPoint(t *testing.T) {
	ctx := context.Background()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	loc := api.Point{
		Coordinates: []float64{-122.082506, 37.4249518},
	}
	gid, geomStruct, err := modusgraph.Create(ctx, engine, GeomStruct{
		Name:  "John Doe",
		Point: loc,
	})
	require.NoError(t, err)
	require.Equal(t, "John Doe", geomStruct.Name)
	require.Equal(t, loc.Coordinates, geomStruct.Point.Coordinates)

	_, geomStruct, err = modusgraph.Get[GeomStruct](ctx, engine, gid)
	require.NoError(t, err)
	require.Equal(t, "John Doe", geomStruct.Name)
	require.Equal(t, loc.Coordinates, geomStruct.Point.Coordinates)

	query := `
		{
			geomStruct(func: type(GeomStruct)) {
				GeomStruct.name
			}
		}`
	resp, err := engine.GetDefaultNamespace().Query(ctx, query)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"geomStruct":[
			{"GeomStruct.name":"John Doe"}
		]
	}`, string(resp.GetJson()))
}

func TestPolygon(t *testing.T) {
	ctx := context.Background()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	polygon := api.NewPolygon([][]float64{
		{-122.083506, 37.4259518}, // Northwest
		{-122.081506, 37.4259518}, // Northeast
		{-122.081506, 37.4239518}, // Southeast
		{-122.083506, 37.4239518}, // Southwest
		{-122.083506, 37.4259518}, // Close the polygon by repeating first point
	})
	_, geomStruct, err := modusgraph.Create(ctx, engine, GeomStruct{
		Name: "Jane Doe",
		Area: *polygon,
	})
	require.NoError(t, err)
	require.Equal(t, "Jane Doe", geomStruct.Name)
	require.Equal(t, polygon.Coordinates, geomStruct.Area.Coordinates)
}

func TestMultiPolygon(t *testing.T) {
	ctx := context.Background()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	multiPolygon := api.NewMultiPolygon([][][]float64{
		{
			{-122.083506, 37.4259518}, // Northwest
			{-122.081506, 37.4259518}, // Northeast
			{-122.081506, 37.4239518}, // Southeast
			{-122.083506, 37.4239518}, // Southwest
			{-122.083506, 37.4259518}, // Close the polygon by repeating first point
		},
		{
			{-122.073506, 37.4359518}, // Northwest
			{-122.071506, 37.4359518}, // Northeast
			{-122.071506, 37.4339518}, // Southeast
			{-122.073506, 37.4339518}, // Southwest
			{-122.073506, 37.4359518}, // Close the polygon by repeating first point
		},
	})
	_, geomStruct, err := modusgraph.Create(ctx, engine, GeomStruct{
		Name:      "Jane Doe",
		MultiArea: *multiPolygon,
	})
	require.NoError(t, err)
	require.Equal(t, "Jane Doe", geomStruct.Name)
	require.Equal(t, multiPolygon.Coordinates, geomStruct.MultiArea.Coordinates)
}

func TestUserStore(t *testing.T) {
	ctx := context.Background()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig("./foo"))
	require.NoError(t, err)
	defer engine.Close()

	user := User{
		Name: "John Doe",
		Age:  30,
	}
	gid, user, err := modusgraph.Create(ctx, engine, user)
	require.NoError(t, err)
	require.NotZero(t, gid)
	require.Equal(t, "John Doe", user.Name)
	require.Equal(t, 30, user.Age)
}
