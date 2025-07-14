/*
 * SPDX-FileCopyrightText:  Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusgraph

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/dgraph-io/dgo/v250"
	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/hypermodeinc/dgraph/v25/x"
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
	var ns *Namespace

	nsID, err := x.ExtractNamespace(ctx)
	if err != nil || nsID == 0 {
		ns = s.engine.GetDefaultNamespace()
	} else {
		ns, err = s.engine.GetNamespace(nsID)
		if err != nil {
			return nil, fmt.Errorf("error getting namespace %d: %w", nsID, err)
		}
	}
	s.engine.logger.V(2).Info("Query using namespace", "namespaceID", ns.ID())

	if len(req.Mutations) > 0 {
		s.engine.logger.V(3).Info("Mutating", "mutations", req.Mutations)

		uids, err := ns.Mutate(ctx, req.Mutations)
		if err != nil {
			return nil, fmt.Errorf("engine mutation error: %w", err)
		}

		uidMap := make(map[string]string)
		for k, v := range uids {
			if strings.HasPrefix(k, "_:") {
				uidMap[k[2:]] = fmt.Sprintf("0x%x", v)
			} else {
				uidMap[k] = fmt.Sprintf("0x%x", v)
			}
		}

		return &api.Response{
			Uids: uidMap,
		}, nil
	}

	return ns.QueryWithVars(ctx, req.Query, req.Vars)
}

// CommitOrAbort implements the Dgraph CommitOrAbort method
func (s *serverWrapper) CommitOrAbort(ctx context.Context, tc *api.TxnContext) (*api.TxnContext, error) {
	var ns *Namespace

	nsID, err := x.ExtractNamespace(ctx)
	if err != nil || nsID == 0 {
		ns = s.engine.GetDefaultNamespace()
	} else {
		ns, err = s.engine.GetNamespace(nsID)
		if err != nil {
			return nil, fmt.Errorf("error getting namespace %d: %w", nsID, err)
		}
	}
	s.engine.logger.V(2).Info("CommitOrAbort called with transaction", "transaction", tc, "namespaceID", ns.ID())

	if tc.Aborted {
		return tc, nil
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

	s.engine.logger.V(2).Info("Transaction committed successfully")

	response := &api.TxnContext{
		StartTs:  tc.StartTs,
		CommitTs: tc.StartTs + 1, // We don't know the actual commit timestamp, but this works for testing
	}

	return response, nil
}

// Login implements the Dgraph Login method
func (s *serverWrapper) Login(ctx context.Context, req *api.LoginRequest) (*api.Response, error) {
	// For security reasons, Authentication is not implemented in this wrapper
	return nil, errors.New("authentication not implemented")
}

// Alter implements the Dgraph Alter method by delegating to the Engine
func (s *serverWrapper) Alter(ctx context.Context, op *api.Operation) (*api.Payload, error) {
	var ns *Namespace

	nsID, err := x.ExtractNamespace(ctx)
	if err != nil || nsID == 0 {
		ns = s.engine.GetDefaultNamespace()
	} else {
		ns, err = s.engine.GetNamespace(nsID)
		if err != nil {
			return nil, fmt.Errorf("error getting namespace %d: %w", nsID, err)
		}
	}
	s.engine.logger.V(2).Info("Alter called with operation", "operation", op, "namespaceID", ns.ID())

	switch {
	case op.Schema != "":
		err = ns.AlterSchema(ctx, op.Schema)
		if err != nil {
			s.engine.logger.Error(err, "Error altering schema")
			return nil, fmt.Errorf("error altering schema: %w", err)
		}

	case op.DropAll:
		err = ns.DropAll(ctx)
		if err != nil {
			s.engine.logger.Error(err, "Error dropping all")
			return nil, fmt.Errorf("error dropping all: %w", err)
		}
	case op.DropOp != 0:
		switch op.DropOp {
		case api.Operation_DATA:
			err = ns.DropData(ctx)
			if err != nil {
				s.engine.logger.Error(err, "Error dropping data")
				return nil, fmt.Errorf("error dropping data: %w", err)
			}
		default:
			s.engine.logger.Error(nil, "Unsupported drop operation")
			return nil, fmt.Errorf("unsupported drop operation: %d", op.DropOp)
		}
	case op.DropAttr != "":
		s.engine.logger.Error(nil, "Drop attribute not implemented yet")
		return nil, errors.New("drop attribute not implemented yet")

	default:
		return nil, errors.New("unsupported alter operation")
	}

	return &api.Payload{}, nil
}

// CheckVersion implements the Dgraph CheckVersion method
func (s *serverWrapper) CheckVersion(ctx context.Context, check *api.Check) (*api.Version, error) {
	// Return a version that matches what the client expects (TODO)
	return &api.Version{
		Tag: "v25.0.0", // Must match major version expected by client
	}, nil
}

// setupBufconnServer creates a bufconn listener and starts a gRPC server with the Dgraph service
func setupBufconnServer(engine *Engine) (*bufconn.Listener, *grpc.Server) {
	x.Config.LimitMutationsNquad = 1000000
	x.Config.LimitQueryEdge = 10000000

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
	// nolint:staticcheck // SA1019: grpc.DialContext is deprecated
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(bufDialer(listener)),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	// Create a Dgraph client
	dgraphClient := api.NewDgraphClient(conn)
	// nolint:staticcheck // SA1019: dgo.NewDgraphClient is deprecated but works with our current setup
	return dgo.NewDgraphClient(dgraphClient), nil
}
