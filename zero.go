package modusdb

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgraph/v24/posting"
	"github.com/dgraph-io/dgraph/v24/protos/pb"
	"github.com/dgraph-io/dgraph/v24/worker"
	"github.com/dgraph-io/dgraph/v24/x"
)

const (
	zeroStateUID = 1
	initialUID   = 2

	schemaTs    = 1
	zeroStateTs = 2
	initialTs   = 3

	leaseUIDAtATime = 10000
	leaseTsAtATime  = 10000

	zeroStateKey = "0-dgraph.modusdb.zero"
)

func (db *DB) LeaseUIDs(numUIDs uint64) (pb.AssignedIds, error) {
	num := &pb.Num{Val: numUIDs, Type: pb.Num_UID}
	return db.z.nextUIDs(num)
}

type zero struct {
	minLeasedUID uint64
	maxLeasedUID uint64

	minLeasedTs uint64
	maxLeasedTs uint64
}

func newZero() (*zero, bool, error) {
	zs, err := readZeroState()
	if err != nil {
		return nil, false, err
	}
	restart := zs != nil

	z := &zero{}
	if zs == nil {
		z.minLeasedUID = initialUID
		z.maxLeasedUID = initialUID
		z.minLeasedTs = initialTs
		z.maxLeasedTs = initialTs
	} else {
		z.minLeasedUID = zs.MaxUID
		z.maxLeasedUID = zs.MaxUID
		z.minLeasedTs = zs.MaxTxnTs
		z.maxLeasedTs = zs.MaxTxnTs
	}
	posting.Oracle().ProcessDelta(&pb.OracleDelta{MaxAssigned: z.minLeasedTs - 1})
	worker.SetMaxUID(z.minLeasedUID - 1)

	if err := z.leaseUIDs(); err != nil {
		return nil, false, err
	}
	if err := z.leaseTs(); err != nil {
		return nil, false, err
	}

	return z, restart, nil
}

func (z *zero) nextTs() (uint64, error) {
	if z.minLeasedTs >= z.maxLeasedTs {
		if err := z.leaseTs(); err != nil {
			return 0, fmt.Errorf("error leasing timestamps: %w", err)
		}
	}

	ts := z.minLeasedTs
	z.minLeasedTs += 1
	posting.Oracle().ProcessDelta(&pb.OracleDelta{MaxAssigned: ts})
	return ts, nil
}

func (z *zero) readTs() uint64 {
	return z.minLeasedTs - 1
}

func (z *zero) nextUIDs(num *pb.Num) (pb.AssignedIds, error) {
	var resp pb.AssignedIds
	if num.Bump {
		if z.minLeasedUID >= num.Val {
			resp = pb.AssignedIds{StartId: z.minLeasedUID, EndId: z.minLeasedUID}
			z.minLeasedUID += 1
		} else {
			resp = pb.AssignedIds{StartId: z.minLeasedUID, EndId: num.Val}
			z.minLeasedUID = num.Val + 1
		}
	} else {
		resp = pb.AssignedIds{StartId: z.minLeasedUID, EndId: z.minLeasedUID + num.Val - 1}
		z.minLeasedUID += num.Val
	}

	for z.minLeasedUID >= z.maxLeasedUID {
		if err := z.leaseUIDs(); err != nil {
			return pb.AssignedIds{}, err
		}
	}

	worker.SetMaxUID(z.minLeasedUID - 1)
	return resp, nil
}

func readZeroState() (*pb.MembershipState, error) {
	txn := worker.State.Pstore.NewTransactionAt(zeroStateTs, false)
	defer txn.Discard()

	item, err := txn.Get(x.DataKey(zeroStateKey, zeroStateUID))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("error getting zero state: %v", err)
	}

	var zeroState pb.MembershipState
	err = item.Value(func(val []byte) error {
		return zeroState.Unmarshal(val)
	})
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling zero state: %v", err)
	}
	return &zeroState, nil
}

func writeZeroState(maxUID, maxTs uint64) error {
	zeroState := pb.MembershipState{MaxUID: maxUID, MaxTxnTs: maxTs}
	data, err := zeroState.Marshal()
	if err != nil {
		return fmt.Errorf("error marshalling zero state: %w", err)
	}

	txn := worker.State.Pstore.NewTransactionAt(zeroStateTs, true)
	defer txn.Discard()

	e := &badger.Entry{
		Key:      x.DataKey(zeroStateKey, zeroStateUID),
		Value:    data,
		UserMeta: posting.BitCompletePosting,
	}
	if err := txn.SetEntry(e); err != nil {
		return fmt.Errorf("error setting zero state: %w", err)
	}
	if err := txn.CommitAt(zeroStateTs, nil); err != nil {
		return fmt.Errorf("error committing zero state: %w", err)
	}

	return nil
}

func (z *zero) leaseTs() error {
	if z.minLeasedTs+leaseTsAtATime <= z.maxLeasedTs {
		return nil
	}

	z.maxLeasedTs += z.minLeasedTs + leaseTsAtATime
	if err := writeZeroState(z.maxLeasedUID, z.maxLeasedTs); err != nil {
		return fmt.Errorf("error leasing UIDs: %w", err)
	}

	return nil
}

func (z *zero) leaseUIDs() error {
	if z.minLeasedUID+leaseUIDAtATime <= z.maxLeasedUID {
		return nil
	}

	z.maxLeasedUID += z.minLeasedUID + leaseUIDAtATime
	if err := writeZeroState(z.maxLeasedUID, z.maxLeasedTs); err != nil {
		return fmt.Errorf("error leasing timestamps: %w", err)
	}

	return nil
}
