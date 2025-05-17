/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusgraph

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"

	"github.com/dgraph-io/dgo/v240"
	"github.com/dgraph-io/dgo/v240/protos/api"
	dg "github.com/dolan-in/dgman/v2"
	"github.com/go-logr/logr"
)

// Client provides an interface for Dgraph operations
type Client interface {
	Insert(context.Context, any) error
	Upsert(context.Context, any) error
	Update(context.Context, any) error
	Get(context.Context, any, string) error
	Query(context.Context, any) *dg.Query
	Delete(context.Context, []string) error
	Close()

	UpdateSchema(context.Context, ...any) error
	GetSchema(context.Context) (string, error)
	DropAll(context.Context) error
	DropData(context.Context) error
	// QueryRaw executes a raw Dgraph query with optional query variables.
	// The `query` parameter is the Dgraph query string.
	// The `vars` parameter is a map of variable names to their values, used to parameterize the query.
	QueryRaw(context.Context, string, map[string]string) ([]byte, error)

	DgraphClient() (*dgo.Dgraph, func(), error)
}

const (
	// DgraphURIPrefix is the prefix for Dgraph server connections
	DgraphURIPrefix = "dgraph://"

	// FileURIPrefix is the prefix for file-based local connections
	FileURIPrefix = "file://"
)

var clientMap = make(map[string]Client)

// clientOptions holds configuration options for the client.
//
// autoSchema: whether to automatically manage the schema.
// poolSize: the size of the dgo client connection pool.
// namespace: the namespace for the client.
// logger: the logger for the client.
type clientOptions struct {
	autoSchema bool
	poolSize   int
	namespace  string
	logger     logr.Logger
}

// ClientOpt is a function that configures a client
type ClientOpt func(*clientOptions)

// WithAutoSchema enables automatic schema management
func WithAutoSchema(enable bool) ClientOpt {
	return func(o *clientOptions) {
		o.autoSchema = enable
	}
}

// WithPoolSize sets the size of the dgo client connection pool
func WithPoolSize(size int) ClientOpt {
	return func(o *clientOptions) {
		o.poolSize = size
	}
}

// WithNamespace sets the namespace for the client
func WithNamespace(namespace string) ClientOpt {
	return func(o *clientOptions) {
		o.namespace = namespace
	}
}

// WithLogger sets a structured logger for the client
func WithLogger(logger logr.Logger) ClientOpt {
	return func(o *clientOptions) {
		o.logger = logger
	}
}

// NewClient creates a new graph database client instance based on the provided URI.
//
// The function supports two URI schemes:
//   - dgraph://host:port - Connects to a remote Dgraph instance
//   - file:///path/to/db - Creates or opens a local file-based database
//
// Optional configuration can be provided via the opts parameter:
//   - WithAutoSchema(bool) - Enable/disable automatic schema creation for inserted objects
//   - WithPoolSize(int) - Set the connection pool size for better performance under load
//   - WithNamespace(string) - Set the database namespace for multi-tenant installations
//   - WithLogger(logr.Logger) - Configure structured logging with custom verbosity levels
//
// The returned Client provides a consistent interface regardless of whether you're
// connected to a remote Dgraph cluster or a local embedded database. This abstraction
// helps prevent the connection issues that can occur with raw gRPC/bufconn setups.
//
// For file-based URIs, the client maintains a singleton Engine instance to ensure
// data consistency across multiple client connections to the same database.
func NewClient(uri string, opts ...ClientOpt) (Client, error) {
	// Default options
	options := clientOptions{
		autoSchema: false,
		poolSize:   10,
		namespace:  "",
		logger:     logr.Discard(), // No-op logger by default
	}

	// Apply provided options
	for _, opt := range opts {
		opt(&options)
	}

	client := client{
		uri:     uri,
		options: options,
		logger:  options.logger,
	}

	switch {
	case strings.HasPrefix(uri, DgraphURIPrefix):
		client.pool = newClientPool(options.poolSize, func() (*dgo.Dgraph, error) {
			client.logger.V(2).Info("Opening new Dgraph connection", "uri", uri)
			return dgo.Open(uri)
		}, client.logger)
		dg.SetLogger(client.logger)
		return client, nil
	case strings.HasPrefix(uri, FileURIPrefix):
		// parse off the file:// prefix
		uri = uri[len(FileURIPrefix):]
		if _, err := os.Stat(uri); err != nil {
			return nil, err
		}
		if c, ok := clientMap[uri]; ok {
			return c, nil
		}
		engine, err := NewEngine(Config{
			dataDir: uri,
			logger:  client.logger,
		})
		if err != nil {
			return nil, err
		}
		client.engine = engine
		clientMap[uri] = client
		client.pool = newClientPool(options.poolSize, func() (*dgo.Dgraph, error) {
			client.logger.V(2).Info("Getting Dgraph client from engine", "location", uri)
			return engine.GetClient()
		}, client.logger)
		dg.SetLogger(client.logger)
		return client, nil
	}
	return nil, errors.New("invalid uri")

}

type client struct {
	uri     string
	engine  *Engine
	options clientOptions
	pool    *clientPool
	logger  logr.Logger
}

func checkPointer(obj any) error {
	if reflect.TypeOf(obj).Kind() != reflect.Ptr {
		return errors.New("object must be a pointer")
	}
	return nil
}

// Insert implements inserting an object or slice of objects in the database.
func (c client) Insert(ctx context.Context, obj any) error {
	return c.process(ctx, obj, "Insert", func(tx *dg.TxnContext, obj any) ([]string, error) {
		return tx.MutateBasic(obj)
	})
}

// Upsert implements inserting or updating an object or slice of objects in the database.
// Note for local file clients, this is not currently implemented.
func (c client) Upsert(ctx context.Context, obj any) error {
	if c.engine != nil {
		return errors.New("not implemented")
	}
	return c.process(ctx, obj, "Upsert", func(tx *dg.TxnContext, obj any) ([]string, error) {
		return tx.Upsert(obj)
	})
}

// Update implements updating an existing object in the database.
func (c client) Update(ctx context.Context, obj any) error {
	return c.process(ctx, obj, "Update", func(tx *dg.TxnContext, obj any) ([]string, error) {
		return tx.MutateBasic(obj)
	})
}

func (c client) process(ctx context.Context,
	obj any, operation string,
	txFunc func(*dg.TxnContext, any) ([]string, error)) error {

	objType := reflect.TypeOf(obj)
	objKind := objType.Kind()
	var schemaObj any

	if objKind == reflect.Slice {
		sliceValue := reflect.Indirect(reflect.ValueOf(obj))
		if sliceValue.Len() == 0 {
			err := errors.New("slice cannot be empty")
			return err
		}
		schemaObj = sliceValue.Index(0).Interface()
	} else {
		schemaObj = obj
	}

	err := checkPointer(schemaObj)
	if err != nil {
		if objKind == reflect.Slice {
			err = errors.Join(err, errors.New("slice elements must be pointers"))
		}
		return err
	}

	if c.options.autoSchema {
		err := c.UpdateSchema(ctx, schemaObj)
		if err != nil {
			return err
		}
	}

	client, err := c.pool.get()
	if err != nil {
		c.logger.Error(err, "Failed to get client from pool")
		return err
	}
	defer c.pool.put(client)

	tx := dg.NewTxnContext(ctx, client).SetCommitNow()
	uids, err := txFunc(tx, obj)
	if err != nil {
		return err
	}
	c.logger.V(2).Info(operation+" successful", "uidCount", len(uids))
	return nil
}

// Delete implements removing objects with the specified UIDs.
func (c client) Delete(ctx context.Context, uids []string) error {
	client, err := c.pool.get()
	if err != nil {
		c.logger.Error(err, "Failed to get client from pool")
		return err
	}
	defer c.pool.put(client)

	txn := dg.NewTxnContext(ctx, client).SetCommitNow()
	return txn.DeleteNode(uids...)
}

// Get implements retrieving a single object by its UID.
func (c client) Get(ctx context.Context, obj any, uid string) error {
	err := checkPointer(obj)
	if err != nil {
		return err
	}
	client, err := c.pool.get()
	if err != nil {
		return err
	}
	defer c.pool.put(client)

	txn := dg.NewReadOnlyTxnContext(ctx, client)
	return txn.Get(obj).UID(uid).Node()
}

// Query implements querying similar to dgman's TxnContext.Get method.
func (c client) Query(ctx context.Context, model any) *dg.Query {
	client, err := c.pool.get()
	if err != nil {
		return nil
	}
	defer c.pool.put(client)

	txn := dg.NewReadOnlyTxnContext(ctx, client)
	return txn.Get(model)
}

// UpdateSchema implements updating the Dgraph schema. Pass one or more
// objects that will be used to generate the schema.
func (c client) UpdateSchema(ctx context.Context, obj ...any) error {
	client, err := c.pool.get()
	if err != nil {
		c.logger.Error(err, "Failed to get client from pool")
		return err
	}
	defer c.pool.put(client)

	_, err = dg.CreateSchema(client, obj...)
	return err
}

// GetSchema implements retrieving the Dgraph schema.
func (c client) GetSchema(ctx context.Context) (string, error) {
	client, err := c.pool.get()
	if err != nil {
		c.logger.Error(err, "Failed to get client from pool")
		return "", err
	}
	defer c.pool.put(client)

	return dg.GetSchema(client)
}

// DropAll implements dropping all data and schema from the database.
func (c client) DropAll(ctx context.Context) error {
	client, err := c.pool.get()
	if err != nil {
		c.logger.Error(err, "Failed to get client from pool")
		return err
	}
	defer c.pool.put(client)

	return client.Alter(ctx, &api.Operation{DropAll: true})
}

// DropData implements dropping data from the database.
func (c client) DropData(ctx context.Context) error {
	client, err := c.pool.get()
	if err != nil {
		c.logger.Error(err, "Failed to get client from pool")
		return err
	}
	defer c.pool.put(client)

	return client.Alter(ctx, &api.Operation{DropOp: api.Operation_DATA})
}

// QueryRaw implements raw querying (DQL syntax) and optional variables.
func (c client) QueryRaw(ctx context.Context, q string, vars map[string]string) ([]byte, error) {
	if c.engine != nil {
		ns := c.engine.GetDefaultNamespace()
		resp, err := ns.QueryWithVars(ctx, q, vars)
		if err != nil {
			return nil, err
		}
		return resp.GetJson(), nil
	}

	client, err := c.pool.get()
	if err != nil {
		c.logger.Error(err, "Failed to get client from pool")
		return nil, err
	}
	defer c.pool.put(client)

	txn := dg.NewReadOnlyTxnContext(ctx, client)
	resp, err := txn.Txn().QueryWithVars(ctx, q, vars)
	if err != nil {
		return nil, err
	}
	return resp.GetJson(), nil
}

// Close releases resources used by the client.
func (c client) Close() {
	c.pool.close()
}

// DgraphClient returns a Dgraph client from the pool and a cleanup function to put it back.
//
// Usage:
//
//	client, cleanup, err := c.DgraphClient()
//	if err != nil { ... }
//	defer cleanup()
//
// The cleanup function is safe to call even if client is nil or err is not nil.
func (c client) DgraphClient() (client *dgo.Dgraph, cleanup func(), err error) {
	client, err = c.pool.get()
	cleanup = func() {
		if client != nil {
			c.pool.put(client)
		}
	}
	return client, cleanup, err
}

type clientPool struct {
	clients chan *dgo.Dgraph
	factory func() (*dgo.Dgraph, error)
	logger  logr.Logger
}

func newClientPool(size int, factory func() (*dgo.Dgraph, error), logger logr.Logger) *clientPool {
	return &clientPool{
		clients: make(chan *dgo.Dgraph, size),
		factory: factory,
		logger:  logger,
	}
}

func (p *clientPool) get() (*dgo.Dgraph, error) {
	// Try to reuse an existing client
	select {
	case client := <-p.clients:
		p.logger.V(2).Info("Reusing client from pool")
		return client, nil
	default:
		// No client in pool, fall through to create a new one
	}

	// Create a new client
	p.logger.V(2).Info("Creating new client")
	client, err := p.factory()
	if err != nil {
		p.logger.Error(err, "Failed to create new client")
	}
	return client, err
}

func (p *clientPool) put(client *dgo.Dgraph) {
	select {
	case p.clients <- client:
		p.logger.V(2).Info("Returned client to pool")
	default:
		// Pool is full, close the client
		p.logger.V(1).Info("Pool full, closing client")
		client.Close()
	}
}

func (p *clientPool) close() {
	count := 0
	for {
		select {
		case client, ok := <-p.clients:
			if !ok {
				return // channel is closed
			}
			client.Close()
			count++
		default:
			// No more clients in the channel
			p.logger.V(2).Info("Client pool closed", "closedConnections", count)
			return
		}
	}
}
