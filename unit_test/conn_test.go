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

package unit_test

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/dgraph-io/dgo/v240"
	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/hypermodeinc/dgraph/v24/x"
	"github.com/hypermodeinc/modusgraph"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

// bufSize is the size of the buffer for the bufconn connection
const bufSize = 1024 * 1024

// serverWrapper wraps the edgraph.Server to provide proper context setup
type serverWrapper struct {
	api.DgraphServer
	engine *modusgraph.Engine
}

// Query implements the Dgraph Query method by delegating to the Engine
func (s *serverWrapper) Query(ctx context.Context, req *api.Request) (*api.Response, error) {
	// Try to extract namespace from context (using x package from dgraph)
	nsID, err := x.ExtractNamespace(ctx)
	var ns *modusgraph.Namespace

	if err != nil || nsID == 0 {
		// If no namespace in context, use default
		ns = s.engine.GetDefaultNamespace()
		fmt.Println("Using default namespace:", ns.ID())
	} else {
		// Use the namespace from the context
		ns, err = s.engine.GetNamespace(nsID)
		if err != nil {
			return nil, fmt.Errorf("error getting namespace %d: %w", nsID, err)
		}
		fmt.Println("Using namespace from context:", ns.ID())
	}

	// Check if this is a mutation request
	if len(req.Mutations) > 0 {
		fmt.Println("Received mutation request with", len(req.Mutations), "mutations")

		// Process mutations
		mu := req.Mutations[0] // For simplicity, handle first mutation
		fmt.Printf("Mutation details - SetNquads: %d bytes, DelNquads: %d bytes, CommitNow: %v\n",
			len(mu.SetNquads), len(mu.DelNquads), mu.CommitNow)

		if len(mu.SetNquads) > 0 {
			fmt.Println("SetNquads:", string(mu.SetNquads))
		}

		// Delegate to the Engine's Mutate method
		uids, err := ns.Mutate(ctx, req.Mutations)
		if err != nil {
			return nil, fmt.Errorf("engine mutation error: %w", err)
		}

		// Convert map[string]uint64 to map[string]string for the response
		uidMap := make(map[string]string)
		for k, v := range uids {
			uidMap[k] = fmt.Sprintf("%d", v)
		}

		return &api.Response{
			Uids: uidMap,
		}, nil
	}

	// This is a regular query
	fmt.Println("Processing query:", req.Query)
	return ns.Query(ctx, req.Query)
}

// CommitOrAbort implements the Dgraph CommitOrAbort method
func (s *serverWrapper) CommitOrAbort(ctx context.Context, tc *api.TxnContext) (*api.TxnContext, error) {
	// Extract namespace from context
	nsID, err := x.ExtractNamespace(ctx)
	var ns *modusgraph.Namespace

	if err != nil || nsID == 0 {
		// If no namespace in context, use default
		ns = s.engine.GetDefaultNamespace()
		fmt.Println("CommitOrAbort using default namespace:", ns.ID())
	} else {
		// Use the namespace from the context
		ns, err = s.engine.GetNamespace(nsID)
		if err != nil {
			return nil, fmt.Errorf("error getting namespace %d: %w", nsID, err)
		}
		fmt.Println("CommitOrAbort using namespace from context:", ns.ID())
	}

	fmt.Printf("CommitOrAbort called with transaction: %+v\n", tc)

	// Check if the transaction context is being aborted
	if tc.Aborted {
		fmt.Println("Transaction aborted")
		return tc, nil // Just return as is for aborted transactions
	}

	// For commit, we need to make a dummy mutation that has no effect but will trigger the commit
	// This approach uses an empty mutation with CommitNow:true to leverage the Engine's existing
	// transaction commit mechanism
	emptyMutation := &api.Mutation{
		CommitNow: true,
	}

	// We can't directly attach the transaction ID to the context in this way,
	// but the Server implementation should handle the transaction context
	// using the StartTs value in the empty mutation

	// Send the mutation through the Engine
	_, err = ns.Mutate(ctx, []*api.Mutation{emptyMutation})
	if err != nil {
		return nil, fmt.Errorf("error committing transaction: %w", err)
	}

	fmt.Println("Transaction committed successfully")

	// Return a successful response
	response := &api.TxnContext{
		StartTs:  tc.StartTs,
		CommitTs: tc.StartTs + 1, // We don't know the actual commit timestamp, but this works for testing
	}

	return response, nil
}

// setupBufconnServer creates a bufconn listener and starts a gRPC server with the Dgraph service
func setupBufconnServer(t *testing.T, engine *modusgraph.Engine) (*bufconn.Listener, *grpc.Server) {
	lis := bufconn.Listen(bufSize)
	server := grpc.NewServer()

	// Register our server wrapper that properly handles context and routing
	dgraphServer := &serverWrapper{engine: engine}
	api.RegisterDgraphServer(server, dgraphServer)

	// Start the server in a goroutine
	go func() {
		if err := server.Serve(lis); err != nil {
			t.Logf("Server exited with error: %v", err)
		}
	}()

	return lis, server
}

// bufDialer is the dialer function for bufconn
func bufDialer(listener *bufconn.Listener) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, url string) (net.Conn, error) {
		return listener.Dial()
	}
}

// createDgraphClient creates a Dgraph client that connects to the bufconn server
func createDgraphClient(ctx context.Context, listener *bufconn.Listener) (*dgo.Dgraph, error) {
	// Create a gRPC connection using the bufconn dialer
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(bufDialer(listener)),
		grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	// Create a Dgraph client
	dgraphClient := api.NewDgraphClient(conn)
	return dgo.NewDgraphClient(dgraphClient), nil
}

// TestDgraphWithBufconn tests Dgraph operations using bufconn
func TestDgraphWithBufconn(t *testing.T) {
	// Create a new engine - this initializes all the necessary components
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	// Set up the bufconn server with access to the engine
	listener, server := setupBufconnServer(t, engine)
	defer server.Stop()

	// Create a context
	ctx := context.Background()

	// Create a Dgraph client
	client, err := createDgraphClient(ctx, listener)
	require.NoError(t, err)

	// Test a simple operation
	txn := client.NewReadOnlyTxn()
	resp, err := txn.Query(ctx, "schema {}")
	require.NoError(t, err)
	fmt.Println("resp", resp)
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
	fmt.Println("query after mutation:", resp)
	_ = txn.Discard(ctx)
}

// TestMultipleDgraphClients tests multiple clients connecting to the same bufconn server
func TestMultipleDgraphClients(t *testing.T) {
	// Create a new engine - this initializes all the necessary components
	engine, err := modusgraph.NewEngine(modusgraph.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	// Set up the bufconn server with access to the engine
	listener, server := setupBufconnServer(t, engine)
	defer server.Stop()

	// Create a context
	ctx := context.Background()

	// Create multiple clients
	client1, err := createDgraphClient(ctx, listener)
	require.NoError(t, err)

	client2, err := createDgraphClient(ctx, listener)
	require.NoError(t, err)

	// Test that both clients can execute operations
	txn1 := client1.NewTxn()
	defer txn1.Discard(ctx)

	txn2 := client2.NewTxn()
	defer txn2.Discard(ctx)

	_, err = txn1.Query(ctx, "schema {}")
	require.NoError(t, err)

	_, err = txn2.Query(ctx, "schema {}")
	require.NoError(t, err)
}
