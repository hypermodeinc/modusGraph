/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package unit_test

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

	engine, err := modusdb.NewEngine(modusdb.NewDefaultConfig(dataDir))
	require.NoError(t, err)
	defer func() { engine.Close() }()

	require.NoError(t, engine.DropAll(context.Background()))
	require.NoError(t, engine.GetDefaultNamespace().AlterSchema(context.Background(), "name: string @index(term) ."))

	_, err = engine.GetDefaultNamespace().Mutate(context.Background(), []*api.Mutation{
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
	qresp, err := engine.GetDefaultNamespace().Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"name":"A"}]}`, string(qresp.GetJson()))

	engine.Close()
	engine, err = modusdb.NewEngine(modusdb.NewDefaultConfig(dataDir))
	require.NoError(t, err)
	qresp, err = engine.GetDefaultNamespace().Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"name":"A"}]}`, string(qresp.GetJson()))

	require.NoError(t, engine.DropAll(context.Background()))
}

func TestSchemaQuery(t *testing.T) {
	engine, err := modusdb.NewEngine(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	require.NoError(t, engine.DropAll(context.Background()))
	require.NoError(t, engine.GetDefaultNamespace().AlterSchema(context.Background(), `
		name: string @index(exact) .
		age: int .
		married: bool .
		loc: geo .
		dob: datetime .
	`))

	resp, err := engine.GetDefaultNamespace().Query(context.Background(), `schema(pred: [name, age]) {type}`)
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

	engine, err := modusdb.NewEngine(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	require.NoError(t, engine.DropAll(context.Background()))
	require.NoError(t, engine.GetDefaultNamespace().AlterSchema(context.Background(),
		`project_description_v: float32vector @index(hnsw(exponent: "5", metric: "euclidean")) .`))

	uids, err := engine.GetDefaultNamespace().Mutate(context.Background(), []*api.Mutation{{
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

	resp, err := engine.GetDefaultNamespace().Query(context.Background(), fmt.Sprintf(`query {
			q (func: uid(%v)) {
				project_description_v
			}
	 	}`, uid))
	require.NoError(t, err)
	require.Equal(t,
		`{"q":[{"project_description_v":[5.1E+00,5.1E+00,1.1E+00]}]}`,
		string(resp.GetJson()))
}
