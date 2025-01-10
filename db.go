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
	"fmt"
	"strconv"

	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/dgraph-io/dgraph/v24/dql"
	"github.com/dgraph-io/dgraph/v24/edgraph"
	"github.com/dgraph-io/dgraph/v24/protos/pb"
	"github.com/dgraph-io/dgraph/v24/query"
	"github.com/dgraph-io/dgraph/v24/schema"
	"github.com/dgraph-io/dgraph/v24/worker"
	"github.com/dgraph-io/dgraph/v24/x"
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
	db.driver.mutex.Lock()
	defer db.driver.mutex.Unlock()

	if !db.driver.isOpen.Load() {
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

func (db *DB) AlterSchema(ctx context.Context, sch string) error {
	db.driver.mutex.Lock()
	defer db.driver.mutex.Unlock()

	if !db.driver.isOpen.Load() {
		return ErrClosedDriver
	}

	sc, err := schema.ParseWithNamespace(sch, db.ID())
	if err != nil {
		return fmt.Errorf("error parsing schema: %w", err)
	}
	return db.alterSchemaWithParsed(ctx, sc)
}

func (db *DB) alterSchemaWithParsed(ctx context.Context, sc *schema.ParsedSchema) error {
	for _, pred := range sc.Preds {
		worker.InitTablet(pred.Predicate)
	}

	startTs, err := db.driver.z.nextTs()
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

	db.driver.mutex.Lock()
	defer db.driver.mutex.Unlock()
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
		res, err := db.driver.z.nextUIDs(num)
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

	return db.mutateWithDqlMutation(ctx, dms, newUids)
}

func (db *DB) mutateWithDqlMutation(ctx context.Context, dms []*dql.Mutation,
	newUids map[string]uint64) (map[string]uint64, error) {
	edges, err := query.ToDirectedEdges(dms, newUids)
	if err != nil {
		return nil, err
	}
	ctx = x.AttachNamespace(ctx, db.ID())

	if !db.driver.isOpen.Load() {
		return nil, ErrClosedDriver
	}

	startTs, err := db.driver.z.nextTs()
	if err != nil {
		return nil, err
	}
	commitTs, err := db.driver.z.nextTs()
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

// Query performs query or mutation or upsert on the given modusDB instance.
func (db *DB) Query(ctx context.Context, query string) (*api.Response, error) {
	db.driver.mutex.RLock()
	defer db.driver.mutex.RUnlock()

	return db.queryWithLock(ctx, query)
}

func (db *DB) queryWithLock(ctx context.Context, query string) (*api.Response, error) {
	if !db.driver.isOpen.Load() {
		return nil, ErrClosedDriver
	}

	ctx = x.AttachNamespace(ctx, db.ID())
	return (&edgraph.Server{}).QueryNoAuth(ctx, &api.Request{
		ReadOnly: true,
		Query:    query,
		StartTs:  db.driver.z.readTs(),
	})
}
