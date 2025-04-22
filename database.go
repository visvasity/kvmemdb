// Copyright (c) 2024 Visvasity LLC

package kvmemdb

import (
	"context"
	"math"
	"slices"
	"sync"

	"github.com/visvasity/kv"
	"github.com/visvasity/kvmemdb/mvcc"
	"github.com/visvasity/syncmap"
)

type Database struct {
	mu sync.Mutex

	// liveTxes holds list of all live transactions in no-specific order.
	liveTxes []*Transaction

	// liveSnaps holds list of all live snapshots in no-specific order.
	liveSnaps []*Snapshot

	// concurrentMap holds mapping from a live transaction to the list of other
	// transactions that have an overlapping, some of which could've already been
	// committed (i.e., not live).
	concurrentMap map[*Transaction][]*Transaction

	// maxCommitVersion holds the largest tx version that has been committed
	// successfully.
	//
	// New snapshots and transactions will reference the database state at this
	// version as their private snapshot. Future updates to the database by other
	// transactions are not invisible to them.
	maxCommitVersion int64

	// kvs holds the successfully committed key-value pairs of the
	// database. Uncommitted changes are cached in their respective transactions.
	kvs syncmap.Map[string, *mvcc.MultiValue]
}

var _ kv.Database[*Transaction, *Snapshot] = &Database{}

// New creates an empty in-memory database.
func New() *Database {
	return &Database{
		concurrentMap: make(map[*Transaction][]*Transaction),
	}
}

// minVersionLocked returns the smallest value version among all live snapshots
// and transactions with their concurrent counterparts.
func (d *Database) minVersionLocked() int64 {
	v := int64(math.MaxInt64)
	for _, tx := range d.liveTxes {
		v = min(v, tx.snapshotVersion)
		for _, ctx := range d.concurrentMap[tx] {
			v = min(v, ctx.snapshotVersion)
		}
	}
	for _, s := range d.liveSnaps {
		v = min(v, s.snapshotVersion)
	}
	return v
}

// NewSnapshot creates a read-only snapshot of the database.
func (d *Database) NewSnapshot(ctx context.Context) (*Snapshot, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	s := &Snapshot{
		db:              d,
		snapshotVersion: d.maxCommitVersion,
	}
	return s, nil
}

func (d *Database) closeSnapshot(s *Snapshot) {
	d.liveSnaps = slices.DeleteFunc(d.liveSnaps, func(v *Snapshot) bool { return v == s })
	s.db = nil
}

// NewTransaction creates a read-write transaction on the database.
func (d *Database) NewTransaction(ctx context.Context) (*Transaction, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	t := &Transaction{
		db:              d,
		snapshotVersion: d.maxCommitVersion,
		reads:           make(map[string]*mvcc.Value),
		writes:          make(map[string]*string),
	}

	// Update the live and concurrent transactions mappings.
	d.concurrentMap[t] = slices.Clone(d.liveTxes)
	for _, tx := range d.liveTxes {
		d.concurrentMap[tx] = append(d.concurrentMap[tx], t)
	}
	d.liveTxes = append(d.liveTxes, t)
	return t, nil
}

func (d *Database) closeTransaction(t *Transaction) {
	d.liveTxes = slices.DeleteFunc(d.liveTxes, func(v *Transaction) bool { return v == t })
	delete(d.concurrentMap, t)
	t.db = nil
}
