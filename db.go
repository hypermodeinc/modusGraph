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
	"github.com/dgraph-io/dgraph/v24/edgraph"
	"github.com/dgraph-io/dgraph/v24/posting"
	"github.com/dgraph-io/dgraph/v24/protos/pb"
	"github.com/dgraph-io/dgraph/v24/schema"
	"github.com/dgraph-io/dgraph/v24/worker"
	"github.com/dgraph-io/dgraph/v24/x"
	"github.com/dgraph-io/ristretto/v2/z"
)

var (
	// This ensures that we only have one instance of modusDB in this process.
	singleton atomic.Bool

	ErrSingletonOnly        = errors.New("only one modusDB instance is supported")
	ErrEmptyDataDir         = errors.New("data directory is required")
	ErrClosedDB             = errors.New("modusDB instance is closed")
	ErrNonExistentNamespace = errors.New("namespace does not exist")
)

// DB is an instance of modusDB.
// For now, we only support one instance of modusDB per process.
type DB struct {
	mutex  sync.RWMutex
	isOpen bool

	z *zero

	// points to default / 0 / galaxy namespace
	gxy *Namespace
}

// New returns a new modusDB instance.
func New(conf Config) (*DB, error) {
	// Ensure that we do not create another instance of modusDB in the same process
	if !singleton.CompareAndSwap(false, true) {
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

	db.gxy = &Namespace{id: 0, db: db}
	return db, nil
}

func (db *DB) CreateNamespace() (*Namespace, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if !db.isOpen {
		return nil, ErrClosedDB
	}

	startTs, err := db.z.nextTs()
	if err != nil {
		return nil, err
	}
	nsID, err := db.z.nextNS()
	if err != nil {
		return nil, err
	}

	if err := worker.ApplyInitialSchema(nsID, startTs); err != nil {
		return nil, fmt.Errorf("error applying initial schema: %w", err)
	}
	for _, pred := range schema.State().Predicates() {
		worker.InitTablet(pred)
	}

	return &Namespace{id: nsID, db: db}, nil
}

func (db *DB) GetNamespace(nsID uint64) (*Namespace, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if !db.isOpen {
		return nil, ErrClosedDB
	}

	if nsID > db.z.lastNS {
		return nil, ErrNonExistentNamespace
	}

	// TODO: when delete namespace is implemented, check if the namespace exists

	return &Namespace{id: nsID, db: db}, nil
}

// DropAll drops all the data and schema in the modusDB instance.
func (db *DB) DropAll(ctx context.Context) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if !db.isOpen {
		return ErrClosedDB
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

func (db *DB) DropData(ctx context.Context) error {
	return db.gxy.DropData(ctx)
}

func (db *DB) AlterSchema(ctx context.Context, sch string) error {
	return db.gxy.AlterSchema(ctx, sch)
}

func (db *DB) Query(ctx context.Context, q string) (*api.Response, error) {
	return db.gxy.Query(ctx, q)
}

func (db *DB) Mutate(ctx context.Context, ms []*api.Mutation) (map[string]uint64, error) {
	return db.gxy.Mutate(ctx, ms)
}

func (db *DB) Load(ctx context.Context, schemaPath, dataPath string) error {
	return db.gxy.Load(ctx, schemaPath, dataPath)
}

func (db *DB) LoadData(inCtx context.Context, dataDir string) error {
	return db.gxy.LoadData(inCtx, dataDir)
}

// Close closes the modusDB instance.
func (db *DB) Close() {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if !db.isOpen {
		return
	}

	if !singleton.CompareAndSwap(true, false) {
		panic("modusDB instance was not properly opened")
	}

	db.isOpen = false
	x.UpdateHealthStatus(false)
	posting.Cleanup()
	worker.State.Dispose()
}

func (db *DB) reset() error {
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

	db.z = z
	return nil
}
