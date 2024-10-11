package modusdb_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/stretchr/testify/require"

	"github.com/hypermodeinc/modusdb"
)

func TestRestart(t *testing.T) {
	dataDir := t.TempDir()

	db, err := modusdb.New(modusdb.NewDefaultConfig().WithDataDir(dataDir))
	require.NoError(t, err)
	defer func() { db.Close() }()

	require.NoError(t, db.DropAll(context.Background()))
	require.NoError(t, db.AlterSchema(context.Background(), "name: string @index(term) ."))

	_, err = db.Mutate(context.Background(), []*api.Mutation{
		{
			Set: []*api.NQuad{
				{
					Namespace:   0,
					Subject:     "_:aman",
					Predicate:   "name",
					ObjectValue: &api.Value{Val: &api.Value_StrVal{StrVal: "A"}},
				},
			},
		},
	})
	require.NoError(t, err)

	query := `{
			me(func: has(name)) {
				name
			}
		}`
	qresp, err := db.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"name":"A"}]}`, string(qresp.GetJson()))

	db.Close()
	db, err = modusdb.New(modusdb.NewDefaultConfig().WithDataDir(dataDir))
	require.NoError(t, err)
	qresp, err = db.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"name":"A"}]}`, string(qresp.GetJson()))

	require.NoError(t, db.DropAll(context.Background()))
}

func TestSchemaQuery(t *testing.T) {
	db, err := modusdb.New(modusdb.NewDefaultConfig().WithDataDir(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.DropAll(context.Background()))
	require.NoError(t, db.AlterSchema(context.Background(), `
		name: string @index(exact) .
		age: int .
		married: bool .
		loc: geo .
		dob: datetime .
	`))

	resp, err := db.Query(context.Background(), `schema(pred: [name, age]) {type}`)
	require.NoError(t, err)

	require.JSONEq(t,
		`{"schema":[{"predicate":"age","type":"int"},{"predicate":"name","type":"string"}]}`,
		string(resp.GetJson()))
}

func TestBasicVector(t *testing.T) {
	vect := []float32{5.1, 5.1, 1.1}
	buf := new(bytes.Buffer)
	for _, v := range vect {
		require.NoError(t, binary.Write(buf, binary.LittleEndian, v))
	}
	vectBytes := buf.Bytes()

	db, err := modusdb.New(modusdb.NewDefaultConfig().WithDataDir(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.DropAll(context.Background()))
	require.NoError(t, db.AlterSchema(context.Background(),
		`project_description_v: float32vector @index(hnsw(exponent: "5", metric: "euclidean")) .`))

	uids, err := db.Mutate(context.Background(), []*api.Mutation{{
		Set: []*api.NQuad{{
			Subject:   "_:vector",
			Predicate: "project_description_v",
			ObjectValue: &api.Value{
				Val: &api.Value_Vfloat32Val{Vfloat32Val: vectBytes},
			},
		}},
	}})
	require.NoError(t, err)

	uid := uids["_:vector"]
	if uid == 0 {
		t.Fatalf("Expected non-zero uid")
	}

	resp, err := db.Query(context.Background(), fmt.Sprintf(`query {
			q (func: uid(%v)) {
				project_description_v
			}
	 	}`, uid))
	require.NoError(t, err)
	require.Equal(t,
		`{"q":[{"project_description_v":[5.1E+00,5.1E+00,1.1E+00]}]}`,
		string(resp.GetJson()))
}
