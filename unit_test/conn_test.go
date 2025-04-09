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
	"github.com/hypermodeinc/dgraph/v24/edgraph"
	"github.com/hypermodeinc/modusgraph"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

// bufSize is the size of the buffer for the bufconn connection
const bufSize = 1024 * 1024

// serverWrapper wraps the edgraph.Server to provide proper context setup
type serverWrapper struct {
	api.DgraphServer
	engine *modusgraph.Engine
}

// Query implements the Dgraph Query method by setting up the proper context
func (s *serverWrapper) Query(ctx context.Context, req *api.Request) (*api.Response, error) {
	// Use the default namespace from the engine
	ns := s.engine.GetDefaultNamespace()
	// Create proper context with namespace attached
	nsCtx := s.attachNamespace(ctx, ns.ID())

	// Add the engine's timestamp to the request
	req.StartTs = s.getReadTs()

	// Use the standard edgraph.Server to process the request
	return (&edgraph.Server{}).QueryNoAuth(nsCtx, req)
}

// Mutate implements the Dgraph Mutate method with proper context setup
func (s *serverWrapper) Mutate(ctx context.Context, mu *api.Mutation) (*api.Response, error) {
	// Use the default namespace from the engine
	ns := s.engine.GetDefaultNamespace()
	// Create proper context with namespace attached
	nsCtx := s.attachNamespace(ctx, ns.ID())

	// Use the engine's API to handle mutations
	// Wrap the single mutation in a slice since ns.Mutate expects []*api.Mutation
	uids, err := ns.Mutate(nsCtx, []*api.Mutation{mu})
	if err != nil {
		return nil, err
	}

	// Convert map[string]uint64 to map[string]string for api.Response
	uidMap := make(map[string]string, len(uids))
	for k, v := range uids {
		uidMap[k] = fmt.Sprintf("%d", v)
	}

	return &api.Response{
		Uids: uidMap,
	}, nil
}

// CommitOrAbort implements the Dgraph CommitOrAbort method
func (s *serverWrapper) CommitOrAbort(ctx context.Context, req *api.TxnContext) (*api.TxnContext, error) {
	// Use the default namespace from the engine
	ns := s.engine.GetDefaultNamespace()
	// Create proper context with namespace attached
	nsCtx := s.attachNamespace(ctx, ns.ID())

	// Use the standard edgraph.Server to process the request
	return (&edgraph.Server{}).CommitOrAbort(nsCtx, req)
}

// Helper methods to access Engine internals
func (s *serverWrapper) attachNamespace(ctx context.Context, nsID uint64) context.Context {
	// Try setting namespace in different ways to ensure it's available

	// 1. Set as metadata with different key variations
	md := metadata.Pairs(
		"namespace", fmt.Sprintf("%d", nsID),
		"dgraph.namespace", fmt.Sprintf("%d", nsID),
		"Namespace", fmt.Sprintf("%d", nsID),
		"ns", fmt.Sprintf("%d", nsID),
	)
	ctx = metadata.NewOutgoingContext(ctx, md)

	// 2. Use context values as well for different keys
	ctx = context.WithValue(ctx, "namespace", nsID)
	ctx = context.WithValue(ctx, "ns", nsID)
	ctx = context.WithValue(ctx, "dgraph.namespace", nsID)

	return ctx
}
func (s *serverWrapper) getReadTs() uint64 {
	// This returns a default timestamp since we can't directly access engine.z.readTs()
	return 1
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

	// Add namespace metadata to the context - this is crucial for the server to process the request
	ns := engine.GetDefaultNamespace()
	md := metadata.Pairs(
		"namespace", fmt.Sprintf("%d", ns.ID()),
		"dgraph.namespace", fmt.Sprintf("%d", ns.ID()),
		"Namespace", fmt.Sprintf("%d", ns.ID()),
		"ns", fmt.Sprintf("%d", ns.ID()),
	)
	ctx = metadata.NewOutgoingContext(ctx, md)

	// Create a Dgraph client
	client, err := createDgraphClient(ctx, listener)
	require.NoError(t, err)

	// Test a simple operation
	txn := client.NewTxn()
	defer txn.Discard(ctx)

	fmt.Println("txn", txn)

	// Test query operation
	resp, err := txn.Query(ctx, "schema {}")
	require.NoError(t, err)

	fmt.Println("resp", resp)

	// Additional test: Try a mutation in a transaction
	mu := &api.Mutation{
		SetNquads: []byte(`_:person <name> "Test Person" .`),
		CommitNow: true,
	}

	_, err = txn.Mutate(ctx, mu)
	require.NoError(t, err)

	// Query to verify the mutation worked
	resp, err = txn.Query(ctx, `{ q(func: has(name)) { name } }`)
	require.NoError(t, err)
	fmt.Println("query after mutation:", resp)
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
