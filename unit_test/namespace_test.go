/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package unit_test

import (
	"context"
	"testing"

	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/hypermodeinc/modusgraph"
	"github.com/stretchr/testify/require"
)

func TestNonGalaxyDB(t *testing.T) {
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, ns1.DropData(context.Background()))
	require.NoError(t, ns1.AlterSchema(context.Background(), "name: string @index(exact) ."))

	_, err = ns1.Mutate(context.Background(), []*api.Mutation{
		{
			Set: []*api.NQuad{
				{
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
	resp, err := ns1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"name":"A"}]}`, string(resp.GetJson()))

}

func TestDropData(t *testing.T) {
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, ns1.DropData(context.Background()))
	require.NoError(t, ns1.AlterSchema(context.Background(), "name: string @index(exact) ."))

	_, err = ns1.Mutate(context.Background(), []*api.Mutation{
		{
			Set: []*api.NQuad{
				{
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
	resp, err := ns1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"name":"A"}]}`, string(resp.GetJson()))

	require.NoError(t, ns1.DropData(context.Background()))

	resp, err = ns1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[]}`, string(resp.GetJson()))
}

func TestMultipleDBs(t *testing.T) {
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	db0, err := engine.GetNamespace(0)
	require.NoError(t, err)
	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, engine.DropAll(context.Background()))
	require.NoError(t, db0.AlterSchema(context.Background(), "name: string @index(exact) ."))
	require.NoError(t, ns1.AlterSchema(context.Background(), "name: string @index(exact) ."))

	_, err = db0.Mutate(context.Background(), []*api.Mutation{
		{
			Set: []*api.NQuad{
				{
					Subject:     "_:aman",
					Predicate:   "name",
					ObjectValue: &api.Value{Val: &api.Value_StrVal{StrVal: "A"}},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = ns1.Mutate(context.Background(), []*api.Mutation{
		{
			Set: []*api.NQuad{
				{
					Subject:     "_:aman",
					Predicate:   "name",
					ObjectValue: &api.Value{Val: &api.Value_StrVal{StrVal: "B"}},
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
	resp, err := db0.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"name":"A"}]}`, string(resp.GetJson()))

	resp, err = ns1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"name":"B"}]}`, string(resp.GetJson()))

	require.NoError(t, ns1.DropData(context.Background()))
	resp, err = ns1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[]}`, string(resp.GetJson()))
}

func TestQueryWrongDB(t *testing.T) {
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	db0, err := engine.GetNamespace(0)
	require.NoError(t, err)
	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, engine.DropAll(context.Background()))
	require.NoError(t, db0.AlterSchema(context.Background(), "name: string @index(exact) ."))
	require.NoError(t, ns1.AlterSchema(context.Background(), "name: string @index(exact) ."))

	_, err = db0.Mutate(context.Background(), []*api.Mutation{
		{
			Set: []*api.NQuad{
				{
					Namespace:   1,
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

	resp, err := ns1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[]}`, string(resp.GetJson()))
}

func TestTwoDBs(t *testing.T) {
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	db0, err := engine.GetNamespace(0)
	require.NoError(t, err)
	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, engine.DropAll(context.Background()))
	require.NoError(t, db0.AlterSchema(context.Background(), "foo: string @index(exact) ."))
	require.NoError(t, ns1.AlterSchema(context.Background(), "bar: string @index(exact) ."))

	_, err = db0.Mutate(context.Background(), []*api.Mutation{
		{
			Set: []*api.NQuad{
				{
					Subject:     "_:aman",
					Predicate:   "foo",
					ObjectValue: &api.Value{Val: &api.Value_StrVal{StrVal: "A"}},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = ns1.Mutate(context.Background(), []*api.Mutation{
		{
			Set: []*api.NQuad{
				{
					Subject:     "_:aman",
					Predicate:   "bar",
					ObjectValue: &api.Value{Val: &api.Value_StrVal{StrVal: "B"}},
				},
			},
		},
	})
	require.NoError(t, err)

	query := `{
		me(func: has(foo)) {
			foo
		}
	}`
	resp, err := db0.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"foo":"A"}]}`, string(resp.GetJson()))

	query = `{
		me(func: has(bar)) {
			bar
		}
	}`
	resp, err = ns1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"bar":"B"}]}`, string(resp.GetJson()))
}

func TestDBDBRestart(t *testing.T) {
	dataDir := t.TempDir()
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(dataDir))
	require.NoError(t, err)
	defer func() { engine.Close() }()

	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)
	ns1Id := ns1.ID()

	require.NoError(t, ns1.AlterSchema(context.Background(), "bar: string @index(exact) ."))
	_, err = ns1.Mutate(context.Background(), []*api.Mutation{
		{
			Set: []*api.NQuad{
				{
					Subject:     "_:aman",
					Predicate:   "bar",
					ObjectValue: &api.Value{Val: &api.Value_StrVal{StrVal: "B"}},
				},
			},
		},
	})
	require.NoError(t, err)

	engine.Close()
	engine, err = modusgraph.NewEngine(modusgraph.NewDefaultConfig(dataDir))
	require.NoError(t, err)

	db2, err := engine.CreateNamespace()
	require.NoError(t, err)
	require.Greater(t, db2.ID(), ns1Id)

	ns1, err = engine.GetNamespace(ns1Id)
	require.NoError(t, err)

	query := `{
		me(func: has(bar)) {
			bar
		}
	}`
	resp, err := ns1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"bar":"B"}]}`, string(resp.GetJson()))
}
