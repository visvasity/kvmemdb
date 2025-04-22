// Copyright (c) 2024 Visvasity LLC

package kvmemdb

import (
	"fmt"
	"math"
	"os"

	"github.com/visvasity/kvmemdb/mvcc"
)

func commit(db *Database, tx *Transaction) error {
	if tx.db == nil {
		return fmt.Errorf("input transaction is already closed: %w", os.ErrInvalid)
	}
	if tx.db != db {
		return fmt.Errorf("input transaction does not belong to this db: %w", os.ErrInvalid)
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if tx.committed {
		return fmt.Errorf("tx is already committed: %w", os.ErrInvalid)
	}

	// Read-Only transactions can be committed immediately. They don't conflict
	// with any other transaction.
	if len(tx.writes) == 0 {
		tx.committed = true
		return nil
	}

	// Serializable Snapshot Isolation requires that we identify rw-dependencies
	// between concurrent transactions and allow the first-committer-win policy.
	//
	// We identify the rw-dependencies for the current transaction by looking for
	// overlapping keys in read and write sets between this transaction and the
	// already committed concurrent transactions. Uncommitted concurrent
	// transactions will check for rw-dependencies when they try to commit after
	// this transaction is successful.

	for _, v := range db.concurrentMap[tx] {
		// Skip uncommitted transactions.
		if !v.committed {
			continue
		}
		// Skip committed read-only transactions.
		if len(v.writes) == 0 {
			continue
		}
		if ks := overlappingKeys(tx.reads, v.writes); len(ks) > 0 {
			return fmt.Errorf("ssi: keys %v read were updated by a committed tx %v", ks, v)
		}
		if ks := overlappingKeys(v.reads, tx.writes); len(ks) > 0 {
			return fmt.Errorf("ssi: keys %v written were read by a committed tx %v", ks, v)
		}
	}

	// Check for all write-write conflicts with the current state of the
	// database.
	for key := range tx.writes {
		mv, ok := db.kvs.Load(key)
		if !ok {
			continue
		}
		current, cok := mv.Fetch(math.MaxInt64)
		initial, iok := mv.Fetch(tx.snapshotVersion)
		if !cok && !iok {
			// A new key is created by this tx, which did not exist before this
			// transaction and also doesn't exist currently.
			continue
		}
		if !cok && iok {
			return fmt.Errorf("ww-conflict: key %v is deleted by another tx", key)
		}
		if cok && !iok {
			return fmt.Errorf("ww-conflict: key %v is also created by another tx", key)
		}
		if current.Version() != initial.Version() {
			return fmt.Errorf("ww-conflict: key %v is updated after this tx has begun", key)
		}
	}

	minVersion := db.minVersionLocked()
	newCommitVersion := db.maxCommitVersion + 1

	// Update the database with the transaction's side effects.
	for key, value := range tx.writes {
		v := mvcc.NewValue(newCommitVersion)
		if value == nil {
			v.Delete()
		} else {
			v.SetData(*value)
		}

		mv, ok := db.kvs.Load(key)
		if !ok {
			db.kvs.Store(key, mvcc.NewMultiValue(v))
			continue
		}

		// Remove unnecessary versions from very old transactions.
		nmv := mvcc.Compact(mvcc.Append(mv, v), minVersion)
		if nmv == nil {
			db.kvs.Delete(key)
		} else {
			db.kvs.Store(key, nmv)
		}
	}
	db.maxCommitVersion = newCommitVersion

	tx.committed = true
	return nil
}

func overlappingKeys(reads map[string]*mvcc.Value, writes map[string]*string) []string {
	var keys []string
	for k := range reads {
		if _, ok := writes[k]; ok {
			keys = append(keys, k)
		}
	}
	return keys
}
