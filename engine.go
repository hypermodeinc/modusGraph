/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusgraph

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgo/v240"
	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/go-logr/logr"
	"github.com/hypermodeinc/dgraph/v24/dql"
	"github.com/hypermodeinc/dgraph/v24/edgraph"
	"github.com/hypermodeinc/dgraph/v24/posting"
	"github.com/hypermodeinc/dgraph/v24/protos/pb"
	"github.com/hypermodeinc/dgraph/v24/query"
	"github.com/hypermodeinc/dgraph/v24/schema"
	"github.com/hypermodeinc/dgraph/v24/worker"
	"github.com/hypermodeinc/dgraph/v24/x"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

var (
	// This ensures that we only have one instance of modusDB in this process.
	singleton atomic.Bool

	ErrSingletonOnly = errors.New("only one modusDB engine is supported")
	ErrEmptyDataDir  = errors.New("data directory is required")
	ErrClosedEngine  = errors.New("modusDB engine is closed")
	ErrNonExistentDB = errors.New("namespace does not exist")
)

// Engine is an instance of modusDB.
// For now, we only support one instance of modusDB per process.
type Engine struct {
	mutex  sync.RWMutex
	isOpen atomic.Bool

	z *zero

	// points to default / 0 / galaxy namespace
	db0 *Namespace

	listener *bufconn.Listener
	server   *grpc.Server
	logger   logr.Logger
}

// NewEngine returns a new modusDB instance.
func NewEngine(conf Config) (*Engine, error) {
	// Ensure that we do not create another instance of modusDB in the same process
	if !singleton.CompareAndSwap(false, true) {
		conf.logger.Error(ErrSingletonOnly, "Failed to create engine")
		return nil, ErrSingletonOnly
	}

	conf.logger.V(1).Info("Creating new modusDB engine", "dataDir", conf.dataDir)

	if err := conf.validate(); err != nil {
		conf.logger.Error(err, "Invalid configuration")
		return nil, err
	}

	// setup data directories
	worker.Config.PostingDir = path.Join(conf.dataDir, "p")
	worker.Config.WALDir = path.Join(conf.dataDir, "w")
	x.WorkerConfig.TmpDir = path.Join(conf.dataDir, "t")

	// TODO: optimize these and more options
	x.WorkerConfig.Badger = badger.DefaultOptions("").FromSuperFlag(worker.BadgerDefaults)
	x.Config.MaxRetries = 10
	x.Config.Limit = z.NewSuperFlag("max-pending-queries=100000")
	x.Config.LimitNormalizeNode = conf.limitNormalizeNode

	// initialize each package
	edgraph.Init()
	worker.State.InitStorage()
	worker.InitForLite(worker.State.Pstore)
	schema.Init(worker.State.Pstore)
	posting.Init(worker.State.Pstore, 0, false) // TODO: set cache size

	engine := &Engine{
		logger: conf.logger,
	}
	engine.isOpen.Store(true)
	engine.logger.V(1).Info("Initializing engine state")
	if err := engine.reset(); err != nil {
		engine.logger.Error(err, "Failed to reset database")
		return nil, fmt.Errorf("error resetting db: %w", err)
	}

	x.UpdateHealthStatus(true)

	engine.db0 = &Namespace{id: 0, engine: engine}

	engine.listener, engine.server = setupBufconnServer(engine)
	return engine, nil
}

func (engine *Engine) GetClient() (*dgo.Dgraph, error) {
	engine.logger.V(2).Info("Getting Dgraph client from engine")
	client, err := createDgraphClient(context.Background(), engine.listener)
	if err != nil {
		engine.logger.Error(err, "Failed to create Dgraph client")
	}
	return client, err
}

func (engine *Engine) CreateNamespace() (*Namespace, error) {
	engine.mutex.RLock()
	defer engine.mutex.RUnlock()

	if !engine.isOpen.Load() {
		return nil, ErrClosedEngine
	}

	startTs, err := engine.z.nextTs()
	if err != nil {
		return nil, err
	}
	nsID, err := engine.z.nextNamespace()
	if err != nil {
		return nil, err
	}

	if err := worker.ApplyInitialSchema(nsID, startTs); err != nil {
		return nil, fmt.Errorf("error applying initial schema: %w", err)
	}
	for _, pred := range schema.State().Predicates() {
		worker.InitTablet(pred)
	}

	return &Namespace{id: nsID, engine: engine}, nil
}

func (engine *Engine) GetNamespace(nsID uint64) (*Namespace, error) {
	engine.mutex.RLock()
	defer engine.mutex.RUnlock()

	return engine.getNamespaceWithLock(nsID)
}

func (engine *Engine) getNamespaceWithLock(nsID uint64) (*Namespace, error) {
	if !engine.isOpen.Load() {
		return nil, ErrClosedEngine
	}

	if nsID > engine.z.lastNamespace {
		return nil, ErrNonExistentDB
	}

	// TODO: when delete namespace is implemented, check if the namespace exists

	return &Namespace{id: nsID, engine: engine}, nil
}

func (engine *Engine) GetDefaultNamespace() *Namespace {
	return engine.db0
}

// DropAll drops all the data and schema in the modusDB instance.
func (engine *Engine) DropAll(ctx context.Context) error {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()

	if !engine.isOpen.Load() {
		return ErrClosedEngine
	}

	p := &pb.Proposal{Mutations: &pb.Mutations{
		GroupId: 1,
		DropOp:  pb.Mutations_ALL,
	}}
	if err := worker.ApplyMutations(ctx, p); err != nil {
		return fmt.Errorf("error applying mutation: %w", err)
	}
	if err := engine.reset(); err != nil {
		return fmt.Errorf("error resetting db: %w", err)
	}

	// TODO: insert drop record
	return nil
}

func (engine *Engine) dropData(ctx context.Context, ns *Namespace) error {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()

	if !engine.isOpen.Load() {
		return ErrClosedEngine
	}

	p := &pb.Proposal{Mutations: &pb.Mutations{
		GroupId:   1,
		DropOp:    pb.Mutations_DATA,
		DropValue: strconv.FormatUint(ns.ID(), 10),
	}}

	if err := worker.ApplyMutations(ctx, p); err != nil {
		return fmt.Errorf("error applying mutation: %w", err)
	}

	// TODO: insert drop record
	// TODO: should we reset back the timestamp as well?
	return nil
}

func (engine *Engine) alterSchema(ctx context.Context, ns *Namespace, sch string) error {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()

	if !engine.isOpen.Load() {
		return ErrClosedEngine
	}

	sc, err := schema.ParseWithNamespace(sch, ns.ID())
	if err != nil {
		return fmt.Errorf("error parsing schema: %w", err)
	}
	return engine.alterSchemaWithParsed(ctx, sc)
}

func (engine *Engine) alterSchemaWithParsed(ctx context.Context, sc *schema.ParsedSchema) error {
	for _, pred := range sc.Preds {
		worker.InitTablet(pred.Predicate)
	}

	startTs, err := engine.z.nextTs()
	if err != nil {
		return err
	}

	p := &pb.Proposal{Mutations: &pb.Mutations{
		GroupId: 1,
		StartTs: startTs,
		Schema:  sc.Preds,
		Types:   sc.Types,
	}}
	if err := worker.ApplyMutations(ctx, p); err != nil {
		return fmt.Errorf("error applying mutation: %w", err)
	}
	return nil
}

func (engine *Engine) query(ctx context.Context,
	ns *Namespace,
	q string,
	vars map[string]string) (*api.Response, error) {
	engine.mutex.RLock()
	defer engine.mutex.RUnlock()

	return engine.queryWithLock(ctx, ns, q, vars)
}

func (engine *Engine) queryWithLock(ctx context.Context,
	ns *Namespace,
	q string,
	vars map[string]string) (*api.Response, error) {
	if !engine.isOpen.Load() {
		return nil, ErrClosedEngine
	}

	ctx = x.AttachNamespace(ctx, ns.ID())
	return (&edgraph.Server{}).QueryNoAuth(ctx, &api.Request{
		ReadOnly: true,
		Query:    q,
		StartTs:  engine.z.readTs(),
		Vars:     vars,
	})
}

func (engine *Engine) mutate(ctx context.Context, ns *Namespace, ms []*api.Mutation) (map[string]uint64, error) {
	if len(ms) == 0 {
		return nil, nil
	}

	engine.mutex.Lock()
	defer engine.mutex.Unlock()
	dms := make([]*dql.Mutation, 0, len(ms))
	for _, mu := range ms {
		dm, err := edgraph.ParseMutationObject(mu, false)
		if err != nil {
			return nil, fmt.Errorf("error parsing mutation: %w", err)
		}
		dms = append(dms, dm)
	}
	newUids, err := query.ExtractBlankUIDs(ctx, dms)
	if err != nil {
		return nil, err
	}
	if len(newUids) > 0 {
		num := &pb.Num{Val: uint64(len(newUids)), Type: pb.Num_UID}
		res, err := engine.z.nextUIDs(num)
		if err != nil {
			return nil, err
		}

		curId := res.StartId
		for k := range newUids {
			x.AssertTruef(curId != 0 && curId <= res.EndId, "not enough uids generated")
			newUids[k] = curId
			curId++
		}
	}

	return engine.mutateWithDqlMutation(ctx, ns, dms, newUids)
}

func (engine *Engine) mutateWithDqlMutation(ctx context.Context, ns *Namespace, dms []*dql.Mutation,
	newUids map[string]uint64) (map[string]uint64, error) {
	edges, err := query.ToDirectedEdges(dms, newUids)
	if err != nil {
		return nil, fmt.Errorf("error converting to directed edges: %w", err)
	}
	ctx = x.AttachNamespace(ctx, ns.ID())

	if !engine.isOpen.Load() {
		return nil, ErrClosedEngine
	}

	startTs, err := engine.z.nextTs()
	if err != nil {
		return nil, err
	}
	commitTs, err := engine.z.nextTs()
	if err != nil {
		return nil, err
	}

	m := &pb.Mutations{
		GroupId: 1,
		StartTs: startTs,
		Edges:   edges,
	}

	m.Edges, err = query.ExpandEdges(ctx, m)
	if err != nil {
		return nil, fmt.Errorf("error expanding edges: %w", err)
	}

	for _, edge := range m.Edges {
		worker.InitTablet(edge.Attr)
	}

	p := &pb.Proposal{Mutations: m, StartTs: startTs}
	if err := worker.ApplyMutations(ctx, p); err != nil {
		return nil, err
	}

	return newUids, worker.ApplyCommited(ctx, &pb.OracleDelta{
		Txns: []*pb.TxnStatus{{StartTs: startTs, CommitTs: commitTs}},
	})
}

func (engine *Engine) Load(ctx context.Context, schemaPath, dataPath string) error {
	return engine.db0.Load(ctx, schemaPath, dataPath)
}

func (engine *Engine) LoadData(inCtx context.Context, dataDir string) error {
	return engine.db0.LoadData(inCtx, dataDir)
}

// Close closes the modusDB instance.
func (engine *Engine) Close() {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()

	if !engine.isOpen.Load() {
		return
	}

	if !singleton.CompareAndSwap(true, false) {
		panic("modusDB instance was not properly opened")
	}

	engine.isOpen.Store(false)
	x.UpdateHealthStatus(false)
	posting.Cleanup()
	worker.State.Dispose()
}

func (ns *Engine) reset() error {
	z, restart, err := newZero()
	if err != nil {
		return fmt.Errorf("error initializing zero: %w", err)
	}

	if !restart {
		if err := worker.ApplyInitialSchema(0, 1); err != nil {
			return fmt.Errorf("error applying initial schema: %w", err)
		}
	}

	if err := schema.LoadFromDb(context.Background()); err != nil {
		return fmt.Errorf("error loading schema: %w", err)
	}
	for _, pred := range schema.State().Predicates() {
		worker.InitTablet(pred)
	}

	ns.z = z
	return nil
}
