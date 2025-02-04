/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusdb

import (
	"context"

	"github.com/dgraph-io/dgo/v240/protos/api"
)

// Namespace is one of the namespaces in modusDB.
type Namespace struct {
	id     uint64
	engine *Engine
}

func (ns *Namespace) ID() uint64 {
	return ns.id
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
	return ns.engine.query(ctx, ns, query)
}
