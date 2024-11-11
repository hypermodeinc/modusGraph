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
	return db.nextUIDs(num)
}

type zero struct {
	minLeasedUID uint64
	maxLeasedUID uint64

	minLeasedTs uint64
	maxLeasedTs uint64
}

func (db *DB) newZero() (bool, error) {
	zs, err := db.readZeroState()
	if err != nil {
		return false, err
	}
	restart := zs != nil

	db.z = &zero{}
	if zs == nil {
		db.z.minLeasedUID = initialUID
		db.z.maxLeasedUID = initialUID
		db.z.minLeasedTs = initialTs
		db.z.maxLeasedTs = initialTs
	} else {
		db.z.minLeasedUID = zs.MaxUID
		db.z.maxLeasedUID = zs.MaxUID
		db.z.minLeasedTs = zs.MaxTxnTs
		db.z.maxLeasedTs = zs.MaxTxnTs
	}
	posting.Oracle().ProcessDelta(&pb.OracleDelta{MaxAssigned: db.z.minLeasedTs - 1})
	worker.SetMaxUID(db.z.minLeasedUID - 1)

	if err := db.leaseUIDs(); err != nil {
		return false, err
	}
	if err := db.leaseTs(); err != nil {
		return false, err
	}

	return restart, nil
}

func (db *DB) nextTs() (uint64, error) {
	if db.z.minLeasedTs >= db.z.maxLeasedTs {
		if err := db.leaseTs(); err != nil {
			return 0, fmt.Errorf("error leasing timestamps: %w", err)
		}
	}

	ts := db.z.minLeasedTs
	db.z.minLeasedTs += 1
	posting.Oracle().ProcessDelta(&pb.OracleDelta{MaxAssigned: ts})
	return ts, nil
}

func (z *zero) readTs() uint64 {
	return z.minLeasedTs - 1
}

func (db *DB) nextUIDs(num *pb.Num) (pb.AssignedIds, error) {
	var resp pb.AssignedIds
	if num.Bump {
		if db.z.minLeasedUID >= num.Val {
			resp = pb.AssignedIds{StartId: db.z.minLeasedUID, EndId: db.z.minLeasedUID}
			db.z.minLeasedUID += 1
		} else {
			resp = pb.AssignedIds{StartId: db.z.minLeasedUID, EndId: num.Val}
			db.z.minLeasedUID = num.Val + 1
		}
	} else {
		resp = pb.AssignedIds{StartId: db.z.minLeasedUID, EndId: db.z.minLeasedUID + num.Val - 1}
		db.z.minLeasedUID += num.Val
	}

	for db.z.minLeasedUID >= db.z.maxLeasedUID {
		if err := db.leaseUIDs(); err != nil {
			return pb.AssignedIds{}, err
		}
	}

	worker.SetMaxUID(db.z.minLeasedUID - 1)
	return resp, nil
}

func (db *DB) readZeroState() (*pb.MembershipState, error) {
	txn := db.ss.Pstore.NewTransactionAt(zeroStateTs, false)
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

func (db *DB) writeZeroState(maxUID, maxTs uint64) error {
	zeroState := pb.MembershipState{MaxUID: maxUID, MaxTxnTs: maxTs}
	data, err := zeroState.Marshal()
	if err != nil {
		return fmt.Errorf("error marshalling zero state: %w", err)
	}

	txn := db.ss.Pstore.NewTransactionAt(zeroStateTs, true)
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

func (db *DB) leaseTs() error {
	if db.z.minLeasedTs+leaseTsAtATime <= db.z.maxLeasedTs {
		return nil
	}

	db.z.maxLeasedTs += db.z.minLeasedTs + leaseTsAtATime
	if err := db.writeZeroState(db.z.maxLeasedUID, db.z.maxLeasedTs); err != nil {
		return fmt.Errorf("error leasing UIDs: %w", err)
	}

	return nil
}

func (db *DB) leaseUIDs() error {
	if db.z.minLeasedUID+leaseUIDAtATime <= db.z.maxLeasedUID {
		return nil
	}

	db.z.maxLeasedUID += db.z.minLeasedUID + leaseUIDAtATime
	if err := db.writeZeroState(db.z.maxLeasedUID, db.z.maxLeasedTs); err != nil {
		return fmt.Errorf("error leasing timestamps: %w", err)
	}

	return nil
}
