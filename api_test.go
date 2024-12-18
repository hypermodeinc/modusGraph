package modusdb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hypermodeinc/modusdb"
)

type User struct {
	Uid  uint64 `json:"uid,omitempty"`
	Name string `json:"name,omitempty"`
	Age  int    `json:"age,omitempty"`
}

func TestCreateApi(t *testing.T) {
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(context.Background()))

	user := &User{
		Name: "B",
		Age:  20,
	}

	uid, _, err := modusdb.Create(context.Background(), db1, user)
	require.NoError(t, err)

	require.Equal(t, "B", user.Name)
	require.Equal(t, uint64(2), uid)
	require.Equal(t, uint64(2), user.Uid)

	query := `{
		me(func: has(User.name)) {
			uid
			User.name
			User.age
		}
	}`
	resp, err := db1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"uid":"0x2","User.name":"B","User.age":20}]}`, string(resp.GetJson()))

	// TODO schema{} should work
	resp, err = db1.Query(context.Background(), `schema(pred: [User.name, User.age]) {type}`)
	require.NoError(t, err)

	require.JSONEq(t,
		`{"schema":[{"predicate":"User.age","type":"int"},{"predicate":"User.name","type":"string"}]}`,
		string(resp.GetJson()))
}

func TestCreateApiWithNonStruct(t *testing.T) {
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(context.Background()))

	user := &User{
		Name: "B",
		Age:  20,
	}

	_, _, err = modusdb.Create[*User](context.Background(), db1, &user)
	require.Error(t, err)
	require.Equal(t, "expected struct, got ptr", err.Error())
}

func TestGetApi(t *testing.T) {
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(context.Background()))

	user := &User{
		Name: "B",
		Age:  20,
	}

	_, _, err = modusdb.Create(context.Background(), db1, user)
	require.NoError(t, err)

	userQuery, err := modusdb.Get[User](context.Background(), db1, uint64(2))

	require.NoError(t, err)
	require.Equal(t, 20, userQuery.Age)
	require.Equal(t, "B", userQuery.Name)
}
