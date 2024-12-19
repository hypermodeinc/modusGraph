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

// Namespace is one of the namespaces in modusDB.
type Namespace struct {
	id uint64
}

func (n *Namespace) ID() uint64 {
	return n.id
}

// DropData drops all the data in the modusDB instance.
func (db *DB) DropDataNS(ctx context.Context, ns *Namespace) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if !db.isOpen {
		return ErrClosedDB
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

func (db *DB) AlterSchemaNS(ctx context.Context, ns *Namespace, sch string) error {
	sc, err := schema.ParseWithNamespace(sch, ns.ID())
	if err != nil {
		return fmt.Errorf("error parsing schema: %w", err)
	}
	return db.alterSchemaWithParsed(ctx, ns, sc)
}

func (db *DB) alterSchemaWithParsed(ctx context.Context, ns *Namespace, sch *schema.ParsedSchema) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if !db.isOpen {
		return ErrClosedDB
	}

	for _, pred := range sch.Preds {
		worker.InitTablet(pred.Predicate)
	}

	startTs, err := db.z.nextTS()
	if err != nil {
		return err
	}

	p := &pb.Proposal{Mutations: &pb.Mutations{
		GroupId: 1,
		StartTs: startTs,
		Schema:  sch.Preds,
		Types:   sch.Types,
	}}
	if err := worker.ApplyMutations(ctx, p); err != nil {
		return fmt.Errorf("error applying mutation: %w", err)
	}
	return nil
}

func (db *DB) MutateNS(ctx context.Context, ns *Namespace, ms []*api.Mutation) (map[string]uint64, error) {
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
	newUIDs, err := query.ExtractBlankUIDs(ctx, dms)
	if err != nil {
		return nil, err
	}
	if len(newUIDs) > 0 {
		num := &pb.Num{Val: uint64(len(newUIDs)), Type: pb.Num_UID}
		res, err := db.z.nextUIDs(num)
		if err != nil {
			return nil, err
		}

		curId := res.StartId
		for k := range newUIDs {
			x.AssertTruef(curId != 0 && curId <= res.EndId, "not enough uids generated")
			newUIDs[k] = curId
			curId++
		}
	}

	return db.mutateWithParsed(ctx, ns, dms, newUIDs)
}

func (db *DB) mutateWithParsed(ctx context.Context, ns *Namespace, dms []*dql.Mutation,
	newUIDs map[string]uint64) (map[string]uint64, error) {

	edges, err := query.ToDirectedEdges(dms, newUIDs)
	if err != nil {
		return nil, err
	}
	ctx = x.AttachNamespace(ctx, ns.ID())

	db.mutex.Lock()
	defer db.mutex.Unlock()

	if !db.isOpen {
		return nil, ErrClosedDB
	}

	startTs, err := db.z.nextTS()
	if err != nil {
		return nil, err
	}
	commitTs, err := db.z.nextTS()
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

	return newUIDs, worker.ApplyCommited(ctx, &pb.OracleDelta{
		Txns: []*pb.TxnStatus{{StartTs: startTs, CommitTs: commitTs}},
	})
}

// Query performs query or mutation or upsert on the given modusDB instance.
func (db *DB) QueryNS(ctx context.Context, ns *Namespace, query string) (*api.Response, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if !db.isOpen {
		return nil, ErrClosedDB
	}

	ctx = x.AttachNamespace(ctx, ns.ID())
	return (&edgraph.Server{}).QueryNoAuth(ctx, &api.Request{
		ReadOnly: true,
		Query:    query,
		StartTs:  db.z.readTs(),
	})
}
