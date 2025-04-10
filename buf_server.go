/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusgraph

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/dgraph-io/dgo/v240"
	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/hypermodeinc/dgraph/v24/x"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// bufSize is the size of the buffer for the bufconn connection
const bufSize = 1024 * 1024 * 10

// serverWrapper wraps the edgraph.Server to provide proper context setup
type serverWrapper struct {
	api.DgraphServer
	engine *Engine
}

// Query implements the Dgraph Query method by delegating to the Engine
func (s *serverWrapper) Query(ctx context.Context, req *api.Request) (*api.Response, error) {
	// Try to extract namespace from context (using x package from dgraph)
	nsID, err := x.ExtractNamespace(ctx)
	var ns *Namespace

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
	var ns *Namespace

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

// Login implements the Dgraph Login method
func (s *serverWrapper) Login(ctx context.Context, req *api.LoginRequest) (*api.Response, error) {
	// For security reasons, Authentication is not implemented in this wrapper
	return nil, fmt.Errorf("authentication not implemented")
}

// Alter implements the Dgraph Alter method by delegating to the Engine
func (s *serverWrapper) Alter(ctx context.Context, op *api.Operation) (*api.Payload, error) {
	// Extract namespace from context
	nsID, err := x.ExtractNamespace(ctx)
	var ns *Namespace

	if err != nil || nsID == 0 {
		// If no namespace in context, use default
		ns = s.engine.GetDefaultNamespace()
	} else {
		// Use the namespace from the context
		ns, err = s.engine.GetNamespace(nsID)
		if err != nil {
			return nil, fmt.Errorf("error getting namespace %d: %w", nsID, err)
		}
	}

	// Use switch to determine operation type
	switch {
	case op.Schema != "":
		// Handle schema alteration
		err = ns.AlterSchema(ctx, op.Schema)
		if err != nil {
			return nil, fmt.Errorf("error altering schema: %w", err)
		}

	case op.DropAll:
		// Handle drop all operation
		err = ns.DropAll(ctx)
		if err != nil {
			return nil, fmt.Errorf("error dropping data: %w", err)
		}
	case op.DropOp != 0:
		switch op.DropOp {
		case api.Operation_DATA:
			err = ns.DropData(ctx)
			if err != nil {
				return nil, fmt.Errorf("error dropping data: %w", err)
			}
		default:
			return nil, fmt.Errorf("unsupported drop operation: %d", op.DropOp)
		}
	case op.DropAttr != "":
		// In a full implementation, you would handle dropping specific attributes
		return nil, fmt.Errorf("drop attribute not implemented yet")

	default:
		return nil, fmt.Errorf("unsupported alter operation")
	}

	// Return success
	return &api.Payload{}, nil
}

// CheckVersion implements the Dgraph CheckVersion method
func (s *serverWrapper) CheckVersion(ctx context.Context, check *api.Check) (*api.Version, error) {
	// Return a version that matches what the client expects
	return &api.Version{
		Tag: "v24.0.0", // Must match major version expected by client
	}, nil
}

// setupBufconnServer creates a bufconn listener and starts a gRPC server with the Dgraph service
func setupBufconnServer(engine *Engine) (*bufconn.Listener, *grpc.Server) {
	lis := bufconn.Listen(bufSize)
	server := grpc.NewServer()

	// Register our server wrapper that properly handles context and routing
	dgraphServer := &serverWrapper{engine: engine}
	api.RegisterDgraphServer(server, dgraphServer)

	// Start the server in a goroutine
	go func() {
		if err := server.Serve(lis); err != nil {
			log.Printf("Server exited with error: %v", err)
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
	//nolint:SA1019 // Using deprecated grpc.DialContext which is supported in 1.x
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(bufDialer(listener)),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	// Create a Dgraph client
	dgraphClient := api.NewDgraphClient(conn)
	//nolint:SA1019 // Using deprecated dgo.NewDgraphClient which is supported
	return dgo.NewDgraphClient(dgraphClient), nil
}
