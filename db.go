package modusdb

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/dgraph-io/dgraph/v24/dql"
	"github.com/dgraph-io/dgraph/v24/edgraph"
	"github.com/dgraph-io/dgraph/v24/posting"
	"github.com/dgraph-io/dgraph/v24/protos/pb"
	"github.com/dgraph-io/dgraph/v24/query"
	"github.com/dgraph-io/dgraph/v24/schema"
	"github.com/dgraph-io/dgraph/v24/worker"
	"github.com/dgraph-io/dgraph/v24/x"
	"github.com/dgraph-io/ristretto/z"
)

var (
	// This ensures that we only have one instance of modusdb in this process.
	singeton atomic.Bool

	ErrSingletonOnly = errors.New("only one modusdb instance is supported")
	ErrEmptyDataDir  = errors.New("data directory is required")
	ErrDBClosed      = errors.New("modusdb instance is closed")
)

// DB is an instance of modusdb.
// For now, we only support one instance of modusdb per process.
type DB struct {
	mutex  sync.RWMutex
	isOpen bool

	z *zero
}

// New returns a new modusdb instance.
func New(conf Config) (*DB, error) {
	// Ensure that we do not create another instance of modusdb in the same process
	if !singeton.CompareAndSwap(false, true) {
		return nil, ErrSingletonOnly
	}

	if err := conf.validate(); err != nil {
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
	posting.Init(worker.State.Pstore, 0) // TODO: set cache size

	db := &DB{isOpen: true}
	if err := db.reset(); err != nil {
		return nil, fmt.Errorf("error resetting db: %w", err)
	}

	x.UpdateHealthStatus(true)
	return db, nil
}

// Close closes the modusdb instance.
func (db *DB) Close() {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if !db.isOpen {
		return
	}

	if !singeton.CompareAndSwap(true, false) {
		panic("modusdb instance was not properly opened")
	}

	db.isOpen = false
	x.UpdateHealthStatus(false)
	posting.Cleanup()
	worker.State.Dispose()
}

// DropAll drops all the data and schema in the modusdb instance.
func (db *DB) DropAll(ctx context.Context) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if !db.isOpen {
		return ErrDBClosed
	}

	p := &pb.Proposal{Mutations: &pb.Mutations{
		GroupId: 1,
		DropOp:  pb.Mutations_ALL,
	}}
	if err := worker.ApplyMutations(ctx, p); err != nil {
		return fmt.Errorf("error applying mutation: %w", err)
	}
	if err := db.reset(); err != nil {
		return fmt.Errorf("error resetting db: %w", err)
	}

	// TODO: insert drop record
	return nil
}

// DropData drops all the data in the modusdb instance.
func (db *DB) DropData(ctx context.Context) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if !db.isOpen {
		return ErrDBClosed
	}

	p := &pb.Proposal{Mutations: &pb.Mutations{
		GroupId: 1,
		DropOp:  pb.Mutations_DATA,
	}}
	if err := worker.ApplyMutations(ctx, p); err != nil {
		return fmt.Errorf("error applying mutation: %w", err)
	}

	// TODO: insert drop record
	// TODO: should we reset back the timestamp as well?
	return nil
}

func (db *DB) AlterSchema(ctx context.Context, sch string) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if !db.isOpen {
		return ErrDBClosed
	}

	sc, err := schema.ParseWithNamespace(sch, 0)
	if err != nil {
		return fmt.Errorf("error parsing schema: %w", err)
	}
	for _, pred := range sc.Preds {
		worker.InitTablet(pred.Predicate)
	}

	startTs, err := db.z.nextTs()
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

func (db *DB) Mutate(ctx context.Context, ms []*api.Mutation) (map[string]uint64, error) {
	if len(ms) == 0 {
		return nil, nil
	}

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
		res, err := db.z.nextUIDs(num)
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
	edges, err := query.ToDirectedEdges(dms, newUids)
	if err != nil {
		return nil, err
	}
	ctx = x.AttachNamespace(ctx, 0)

	db.mutex.Lock()
	defer db.mutex.Unlock()

	if !db.isOpen {
		return nil, ErrDBClosed
	}

	startTs, err := db.z.nextTs()
	if err != nil {
		return nil, err
	}
	commitTs, err := db.z.nextTs()
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

// Query performs query or mutation or upsert on the given modusdb instance.
func (db *DB) Query(ctx context.Context, query string) (*api.Response, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if !db.isOpen {
		return nil, ErrDBClosed
	}

	return (&edgraph.Server{}).Query(ctx, &api.Request{
		ReadOnly: true,
		Query:    query,
		StartTs:  db.z.readTs(),
	})
}

func (db *DB) reset() error {
	z, restart, err := newZero()
	if err != nil {
		return fmt.Errorf("error initializing zero: %w", err)
	}

	if !restart {
		if err := worker.ApplyInitialSchema(); err != nil {
			return fmt.Errorf("error applying initial schema: %w", err)
		}
	}

	if err := schema.LoadFromDb(context.Background()); err != nil {
		return fmt.Errorf("error loading schema: %w", err)
	}
	for _, pred := range schema.State().Predicates() {
		worker.InitTablet(pred)
	}

	db.z = z
	return nil
}
