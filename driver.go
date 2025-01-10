/*
 * Copyright 2025 Hypermode Inc.
 * Licensed under the terms of the Apache License, Version 2.0
 * See the LICENSE file that accompanied this code for further details.
 *
 * SPDX-FileCopyrightText: 2025 Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusdb

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
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
	"github.com/dgraph-io/ristretto/v2/z"
)

var (
	// This ensures that we only have one instance of modusDB in this process.
	singleton atomic.Bool

	ErrSingletonOnly = errors.New("only one modusDB driver is supported")
	ErrEmptyDataDir  = errors.New("data directory is required")
	ErrClosedDriver  = errors.New("modusDB driver is closed")
	ErrNonExistentDB = errors.New("namespace does not exist")
)

// Driver is an instance of modusDB.
// For now, we only support one instance of modusDB per process.
type Driver struct {
	mutex  sync.RWMutex
	isOpen atomic.Bool

	z *zero

	// points to default / 0 / galaxy namespace
	db0 *DB
}

// NewDriver returns a new modusDB instance.
func NewDriver(conf Config) (*Driver, error) {
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

	driver := &Driver{}
	driver.isOpen.Store(true)
	if err := driver.reset(); err != nil {
		return nil, fmt.Errorf("error resetting db: %w", err)
	}

	x.UpdateHealthStatus(true)

	driver.db0 = &DB{id: 0, driver: driver}
	return driver, nil
}

func (db *Driver) CreateDB() (*DB, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if !db.isOpen.Load() {
		return nil, ErrClosedDriver
	}

	startTs, err := db.z.nextTs()
	if err != nil {
		return nil, err
	}
	dbID, err := db.z.nextDB()
	if err != nil {
		return nil, err
	}

	if err := worker.ApplyInitialSchema(dbID, startTs); err != nil {
		return nil, fmt.Errorf("error applying initial schema: %w", err)
	}
	for _, pred := range schema.State().Predicates() {
		worker.InitTablet(pred)
	}

	return &DB{id: dbID, driver: db}, nil
}

func (driver *Driver) GetDB(dbID uint64) (*DB, error) {
	driver.mutex.RLock()
	defer driver.mutex.RUnlock()

	return driver.getDBWithLock(dbID)
}

func (driver *Driver) getDBWithLock(dbID uint64) (*DB, error) {
	if !driver.isOpen.Load() {
		return nil, ErrClosedDriver
	}

	if dbID > driver.z.lastDB {
		return nil, ErrNonExistentDB
	}

	// TODO: when delete namespace is implemented, check if the namespace exists

	return &DB{id: dbID, driver: driver}, nil
}

func (driver *Driver) GetDefaultDB() *DB {
	return driver.db0
}

// DropAll drops all the data and schema in the modusDB instance.
func (driver *Driver) DropAll(ctx context.Context) error {
	driver.mutex.Lock()
	defer driver.mutex.Unlock()

	if !driver.isOpen.Load() {
		return ErrClosedDriver
	}

	p := &pb.Proposal{Mutations: &pb.Mutations{
		GroupId: 1,
		DropOp:  pb.Mutations_ALL,
	}}
	if err := worker.ApplyMutations(ctx, p); err != nil {
		return fmt.Errorf("error applying mutation: %w", err)
	}
	if err := driver.reset(); err != nil {
		return fmt.Errorf("error resetting db: %w", err)
	}

	// TODO: insert drop record
	return nil
}

func (driver *Driver) dropData(ctx context.Context, db *DB) error {
	driver.mutex.Lock()
	defer driver.mutex.Unlock()

	if !driver.isOpen.Load() {
		return ErrClosedDriver
	}

	p := &pb.Proposal{Mutations: &pb.Mutations{
		GroupId:   1,
		DropOp:    pb.Mutations_DATA,
		DropValue: strconv.FormatUint(db.ID(), 10),
	}}

	if err := worker.ApplyMutations(ctx, p); err != nil {
		return fmt.Errorf("error applying mutation: %w", err)
	}

	// TODO: insert drop record
	// TODO: should we reset back the timestamp as well?
	return nil
}

func (driver *Driver) alterSchema(ctx context.Context, db *DB, sch string) error {
	driver.mutex.Lock()
	defer driver.mutex.Unlock()

	if !driver.isOpen.Load() {
		return ErrClosedDriver
	}

	sc, err := schema.ParseWithNamespace(sch, db.ID())
	if err != nil {
		return fmt.Errorf("error parsing schema: %w", err)
	}
	return driver.alterSchemaWithParsed(ctx, sc)
}

func (driver *Driver) alterSchemaWithParsed(ctx context.Context, sc *schema.ParsedSchema) error {
	for _, pred := range sc.Preds {
		worker.InitTablet(pred.Predicate)
	}

	startTs, err := driver.z.nextTs()
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

func (driver *Driver) query(ctx context.Context, db *DB, q string) (*api.Response, error) {
	driver.mutex.RLock()
	defer driver.mutex.RUnlock()

	return driver.queryWithLock(ctx, db, q)
}

func (driver *Driver) queryWithLock(ctx context.Context, db *DB, q string) (*api.Response, error) {
	if !driver.isOpen.Load() {
		return nil, ErrClosedDriver
	}

	ctx = x.AttachNamespace(ctx, db.ID())
	return (&edgraph.Server{}).QueryNoAuth(ctx, &api.Request{
		ReadOnly: true,
		Query:    q,
		StartTs:  driver.z.readTs(),
	})
}

func (driver *Driver) mutate(ctx context.Context, db *DB, ms []*api.Mutation) (map[string]uint64, error) {
	if len(ms) == 0 {
		return nil, nil
	}

	driver.mutex.Lock()
	defer driver.mutex.Unlock()
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
		res, err := driver.z.nextUIDs(num)
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

	return driver.mutateWithDqlMutation(ctx, db, dms, newUids)
}

func (driver *Driver) mutateWithDqlMutation(ctx context.Context, db *DB, dms []*dql.Mutation,
	newUids map[string]uint64) (map[string]uint64, error) {
	edges, err := query.ToDirectedEdges(dms, newUids)
	if err != nil {
		return nil, fmt.Errorf("error converting to directed edges: %w", err)
	}
	ctx = x.AttachNamespace(ctx, db.ID())

	if !driver.isOpen.Load() {
		return nil, ErrClosedDriver
	}

	startTs, err := driver.z.nextTs()
	if err != nil {
		return nil, err
	}
	commitTs, err := driver.z.nextTs()
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

func (driver *Driver) Load(ctx context.Context, schemaPath, dataPath string) error {
	return driver.db0.Load(ctx, schemaPath, dataPath)
}

func (driver *Driver) LoadData(inCtx context.Context, dataDir string) error {
	return driver.db0.LoadData(inCtx, dataDir)
}

// Close closes the modusDB instance.
func (driver *Driver) Close() {
	driver.mutex.Lock()
	defer driver.mutex.Unlock()

	if !driver.isOpen.Load() {
		return
	}

	if !singleton.CompareAndSwap(true, false) {
		panic("modusDB instance was not properly opened")
	}

	driver.isOpen.Store(false)
	x.UpdateHealthStatus(false)
	posting.Cleanup()
	worker.State.Dispose()
}

func (db *Driver) reset() error {
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
