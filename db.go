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

	"github.com/dgraph-io/dgo/v240/protos/api"
)

// DB is one of the namespaces in modusDB.
type DB struct {
	id     uint64
	driver *Driver
}

func (db *DB) ID() uint64 {
	return db.id
}

// DropData drops all the data in the modusDB instance.
func (db *DB) DropData(ctx context.Context) error {
	return db.driver.dropData(ctx, db)
}

func (db *DB) AlterSchema(ctx context.Context, sch string) error {
	return db.driver.alterSchema(ctx, db, sch)
}

func (db *DB) Mutate(ctx context.Context, ms []*api.Mutation) (map[string]uint64, error) {
	return db.driver.mutate(ctx, db, ms)
}

// Query performs query or mutation or upsert on the given modusDB instance.
func (db *DB) Query(ctx context.Context, query string) (*api.Response, error) {
	return db.driver.query(ctx, db, query)
}
