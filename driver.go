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

func (driver *Driver) DropData(ctx context.Context) error {
	return driver.db0.DropData(ctx)
}

func (driver *Driver) AlterSchema(ctx context.Context, sch string) error {
	return driver.db0.AlterSchema(ctx, sch)
}

func (driver *Driver) Query(ctx context.Context, q string) (*api.Response, error) {
	return driver.db0.Query(ctx, q)
}

func (driver *Driver) Mutate(ctx context.Context, ms []*api.Mutation) (map[string]uint64, error) {
	return driver.db0.Mutate(ctx, ms)
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
