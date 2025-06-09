/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package unit_test

import (
	"context"
	"testing"

	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/hypermodeinc/modusgraph"
	"github.com/stretchr/testify/require"
)

func TestRDF(t *testing.T) {
	// Create a new engine - this initializes all the necessary components
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	client, err := engine.GetClient()
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Test a simple operation
	txn := client.NewReadOnlyTxn()
	resp, err := txn.Query(ctx, "schema {}")
	require.NoError(t, err)
	require.NotEmpty(t, resp.Json)
	_ = txn.Discard(ctx)

	txn = client.NewTxn()
	// Additional test: Try a mutation in a transaction
	mu := &api.Mutation{
		SetNquads: []byte(`_:person <n> "Test Person" .`),
		//CommitNow: true,
	}
	_, err = txn.Mutate(ctx, mu)
	require.NoError(t, err)
	// Commit the transaction
	err = txn.Commit(ctx)
	require.NoError(t, err)
	_ = txn.Discard(ctx)

	// Create a new transaction for the follow-up query since the previous one was committed
	txn = client.NewTxn()
	// Query to verify the mutation worked
	resp, err = txn.Query(ctx, `{ q(func: has(n)) { n } }`)
	require.NoError(t, err)
	require.NotEmpty(t, resp.Json)
	_ = txn.Discard(ctx)

	err = client.Alter(context.Background(), &api.Operation{DropAll: true})
	if err != nil {
		t.Error(err)
	}
}

// TestMultipleDgraphClients tests multiple clients connecting to the same bufconn server
func TestMultipleDgraphClients(t *testing.T) {
	// Create a new engine - this initializes all the necessary components
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	// Create a context
	ctx := context.Background()

	// Create multiple clients
	client1, err := engine.GetClient()
	require.NoError(t, err)
	defer client1.Close()

	client2, err := engine.GetClient()
	require.NoError(t, err)
	defer client2.Close()

	// Test that both clients can execute operations
	txn1 := client1.NewTxn()
	defer func() {
		err := txn1.Discard(ctx)
		require.NoError(t, err)
	}()

	txn2 := client2.NewTxn()
	defer func() {
		err := txn2.Discard(ctx)
		require.NoError(t, err)
	}()

	_, err = txn1.Query(ctx, "schema {}")
	require.NoError(t, err)

	_, err = txn2.Query(ctx, "schema {}")
	require.NoError(t, err)
}
