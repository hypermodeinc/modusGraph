package modusdb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hypermodeinc/modusdb"
)

type User struct {
	Gid     uint64 `json:"gid,omitempty"`
	Name    string `json:"name,omitempty"`
	Age     int    `json:"age,omitempty"`
	ClerkId string `json:"clerk_id,omitempty" db:"constraint=unique"`
}

func TestCreateApi(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	user := &User{
		Name:    "B",
		Age:     20,
		ClerkId: "123",
	}

	gid, _, err := modusdb.Create(db, user, db1.ID())
	require.NoError(t, err)

	require.Equal(t, "B", user.Name)
	require.Equal(t, uint64(2), gid)
	require.Equal(t, uint64(2), user.Gid)

	query := `{
		me(func: has(User.name)) {
			uid
			User.name
			User.age
			User.clerk_id
		}
	}`
	resp, err := db1.Query(ctx, query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"uid":"0x2","User.name":"B","User.age":20,"User.clerk_id":"123"}]}`,
		string(resp.GetJson()))

	// TODO schema{} should work
	schemaQuery := `schema(pred: [User.name, User.age, User.clerk_id]) 
	{
		type
		index
		tokenizer
	}`
	resp, err = db1.Query(ctx, schemaQuery)
	require.NoError(t, err)

	require.JSONEq(t,
		`{"schema":
			[
				{"predicate":"User.age","type":"int"},
				{"predicate":"User.clerk_id","type":"string","index":true,"tokenizer":["exact"]},
				{"predicate":"User.name","type":"string"}
			]
		}`,
		string(resp.GetJson()))
}

func TestCreateApiWithNonStruct(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	user := &User{
		Name: "B",
		Age:  20,
	}

	_, _, err = modusdb.Create[*User](db, &user, db1.ID())
	require.Error(t, err)
	require.Equal(t, "expected struct, got ptr", err.Error())
}

func TestGetApi(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	user := &User{
		Name:    "B",
		Age:     20,
		ClerkId: "123",
	}

	gid, _, err := modusdb.Create(db, user, db1.ID())
	require.NoError(t, err)

	gid, queriedUser, err := modusdb.Get[User](db, gid, db1.ID())

	require.NoError(t, err)
	require.Equal(t, uint64(2), gid)
	require.Equal(t, uint64(2), queriedUser.Gid)
	require.Equal(t, 20, queriedUser.Age)
	require.Equal(t, "B", queriedUser.Name)
	require.Equal(t, "123", queriedUser.ClerkId)
}

func TestGetApiWithConstrainedField(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	user := &User{
		Name:    "B",
		Age:     20,
		ClerkId: "123",
	}

	_, _, err = modusdb.Create(db, user, db1.ID())
	require.NoError(t, err)

	gid, queriedUser, err := modusdb.Get[User](db, modusdb.ConstrainedField{
		Key:   "clerk_id",
		Value: "123",
	}, db1.ID())

	require.NoError(t, err)
	require.Equal(t, uint64(2), gid)
	require.Equal(t, uint64(2), queriedUser.Gid)
	require.Equal(t, 20, queriedUser.Age)
	require.Equal(t, "B", queriedUser.Name)
	require.Equal(t, "123", queriedUser.ClerkId)
}

func TestDeleteApi(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	user := &User{
		Name:    "B",
		Age:     20,
		ClerkId: "123",
	}

	gid, _, err := modusdb.Create(db, user, db1.ID())
	require.NoError(t, err)

	_, _, err = modusdb.Delete[User](db, gid, db1.ID())
	require.NoError(t, err)

	_, queriedUser, err := modusdb.Get[User](db, gid, db1.ID())
	require.Error(t, err)
	require.Equal(t, "no object found", err.Error())
	require.Nil(t, queriedUser)

	_, queriedUser, err = modusdb.Get[User](db, modusdb.ConstrainedField{
		Key:   "clerk_id",
		Value: "123",
	}, db1.ID())
	require.Error(t, err)
	require.Equal(t, "no object found", err.Error())
	require.Nil(t, queriedUser)
}
