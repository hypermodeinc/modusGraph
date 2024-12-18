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
	db *DB
}

func (n *Namespace) ID() uint64 {
	return n.id
}

// DropData drops all the data in the modusDB instance.
func (n *Namespace) DropData(ctx context.Context) error {
	n.db.mutex.Lock()
	defer n.db.mutex.Unlock()

	if !n.db.isOpen {
		return ErrClosedDB
	}

	p := &pb.Proposal{Mutations: &pb.Mutations{
		GroupId:   1,
		DropOp:    pb.Mutations_DATA,
		DropValue: strconv.FormatUint(n.ID(), 10),
	}}

	if err := worker.ApplyMutations(ctx, p); err != nil {
		return fmt.Errorf("error applying mutation: %w", err)
	}

	// TODO: insert drop record
	// TODO: should we reset back the timestamp as well?
	return nil
}

func (n *Namespace) AlterSchema(ctx context.Context, sch string) error {
	n.db.mutex.Lock()
	defer n.db.mutex.Unlock()

	if !n.db.isOpen {
		return ErrClosedDB
	}

	sc, err := schema.ParseWithNamespace(sch, n.ID())
	if err != nil {
		return fmt.Errorf("error parsing schema: %w", err)
	}
	return n.alterSchemaWithParsed(ctx, sc)
}

func (n *Namespace) alterSchemaWithParsed(ctx context.Context, sc *schema.ParsedSchema) error {
	for _, pred := range sc.Preds {
		worker.InitTablet(pred.Predicate)
	}

	startTs, err := n.db.z.nextTs()
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

func (n *Namespace) Mutate(ctx context.Context, ms []*api.Mutation) (map[string]uint64, error) {
	if len(ms) == 0 {
		return nil, nil
	}

	n.db.mutex.Lock()
	defer n.db.mutex.Unlock()
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
		res, err := n.db.z.nextUIDs(num)
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

	return n.mutateWithDqlMutation(ctx, dms, newUids)
}

func (n *Namespace) mutateWithDqlMutation(ctx context.Context, dms []*dql.Mutation,
	newUids map[string]uint64) (map[string]uint64, error) {
	edges, err := query.ToDirectedEdges(dms, newUids)
	if err != nil {
		return nil, err
	}
	ctx = x.AttachNamespace(ctx, n.ID())

	if !n.db.isOpen {
		return nil, ErrClosedDB
	}

	startTs, err := n.db.z.nextTs()
	if err != nil {
		return nil, err
	}
	commitTs, err := n.db.z.nextTs()
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
func (n *Namespace) Query(ctx context.Context, query string) (*api.Response, error) {
	n.db.mutex.RLock()
	defer n.db.mutex.RUnlock()

	if !n.db.isOpen {
		return nil, ErrClosedDB
	}

	ctx = x.AttachNamespace(ctx, n.ID())
	return (&edgraph.Server{}).QueryNoAuth(ctx, &api.Request{
		ReadOnly: true,
		Query:    query,
		StartTs:  n.db.z.readTs(),
	})
}
