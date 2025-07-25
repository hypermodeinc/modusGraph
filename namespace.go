/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusgraph

import (
	"context"

	"github.com/dgraph-io/dgo/v250/protos/api"
)

// Namespace is one of the namespaces in modusDB.
type Namespace struct {
	id     uint64
	engine *Engine
}

func (ns *Namespace) ID() uint64 {
	return ns.id
}

// DropAll drops all the data and schema in the modusDB instance.
func (ns *Namespace) DropAll(ctx context.Context) error {
	return ns.engine.DropAll(ctx)
}

// DropData drops all the data in the modusDB instance.
func (ns *Namespace) DropData(ctx context.Context) error {
	return ns.engine.dropData(ctx, ns)
}

func (ns *Namespace) AlterSchema(ctx context.Context, sch string) error {
	return ns.engine.alterSchema(ctx, ns, sch)
}

func (ns *Namespace) Mutate(ctx context.Context, ms []*api.Mutation) (map[string]uint64, error) {
	return ns.engine.mutate(ctx, ns, ms)
}

// Query performs query or mutation or upsert on the given modusDB instance.
func (ns *Namespace) Query(ctx context.Context, query string) (*api.Response, error) {
	return ns.engine.query(ctx, ns, query, nil)
}

// QueryWithVars performs query or mutation or upsert on the given modusDB instance.
func (ns *Namespace) QueryWithVars(ctx context.Context, query string, vars map[string]string) (*api.Response, error) {
	return ns.engine.query(ctx, ns, query, vars)
}
