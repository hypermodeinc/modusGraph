/*
 * Copyright 2025 Hypermode Inc.
 * Licensed under the terms of the Apache License, Version 2.0
 * See the LICENSE file that accompanied this code for further details.
 *
 * SPDX-FileCopyrightText: 2025 Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusdb_test

import (
	"context"
	"testing"

	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/stretchr/testify/require"

	"github.com/hypermodeinc/modusdb"
)

func TestNonGalaxyNamespace(t *testing.T) {
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(context.Background()))
	require.NoError(t, db1.AlterSchema(context.Background(), "name: string @index(exact) ."))

	_, err = db1.Mutate(context.Background(), []*api.Mutation{
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
	resp, err := db1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"name":"A"}]}`, string(resp.GetJson()))

}

func TestDropData(t *testing.T) {
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(context.Background()))
	require.NoError(t, db1.AlterSchema(context.Background(), "name: string @index(exact) ."))

	_, err = db1.Mutate(context.Background(), []*api.Mutation{
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
	resp, err := db1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"name":"A"}]}`, string(resp.GetJson()))

	require.NoError(t, db1.DropData(context.Background()))

	resp, err = db1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[]}`, string(resp.GetJson()))
}

func TestMultipleNamespaces(t *testing.T) {
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db0, err := db.GetNamespace(0)
	require.NoError(t, err)
	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db.DropAll(context.Background()))
	require.NoError(t, db0.AlterSchema(context.Background(), "name: string @index(exact) ."))
	require.NoError(t, db1.AlterSchema(context.Background(), "name: string @index(exact) ."))

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

	_, err = db1.Mutate(context.Background(), []*api.Mutation{
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

	resp, err = db1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"name":"B"}]}`, string(resp.GetJson()))

	require.NoError(t, db1.DropData(context.Background()))
	resp, err = db1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[]}`, string(resp.GetJson()))
}

func TestQueryWrongNamespace(t *testing.T) {
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db0, err := db.GetNamespace(0)
	require.NoError(t, err)
	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db.DropAll(context.Background()))
	require.NoError(t, db0.AlterSchema(context.Background(), "name: string @index(exact) ."))
	require.NoError(t, db1.AlterSchema(context.Background(), "name: string @index(exact) ."))

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

	resp, err := db1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[]}`, string(resp.GetJson()))
}

func TestTwoNamespaces(t *testing.T) {
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db0, err := db.GetNamespace(0)
	require.NoError(t, err)
	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db.DropAll(context.Background()))
	require.NoError(t, db0.AlterSchema(context.Background(), "foo: string @index(exact) ."))
	require.NoError(t, db1.AlterSchema(context.Background(), "bar: string @index(exact) ."))

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

	_, err = db1.Mutate(context.Background(), []*api.Mutation{
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
	resp, err = db1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"bar":"B"}]}`, string(resp.GetJson()))
}

func TestNamespaceDBRestart(t *testing.T) {
	dataDir := t.TempDir()
	db, err := modusdb.New(modusdb.NewDefaultConfig(dataDir))
	require.NoError(t, err)
	defer func() { db.Close() }()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)
	ns1 := db1.ID()

	require.NoError(t, db1.AlterSchema(context.Background(), "bar: string @index(exact) ."))
	_, err = db1.Mutate(context.Background(), []*api.Mutation{
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

	db.Close()
	db, err = modusdb.New(modusdb.NewDefaultConfig(dataDir))
	require.NoError(t, err)

	db2, err := db.CreateNamespace()
	require.NoError(t, err)
	require.Greater(t, db2.ID(), ns1)

	db1, err = db.GetNamespace(ns1)
	require.NoError(t, err)

	query := `{
		me(func: has(bar)) {
			bar
		}
	}`
	resp, err := db1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"bar":"B"}]}`, string(resp.GetJson()))
}
