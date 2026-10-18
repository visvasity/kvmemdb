// Copyright (c) 2025 Visvasity LLC

package kvmemdb

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/visvasity/kv"
)

// TestMissingKeyGetRecordsRead verifies that a Get on a key that does not exist
// records the observed absence in the transaction's read set. Without this, a
// subsequent write to the key would be treated as a blind write and skipped by
// commit-time conflict detection.
func TestMissingKeyGetRecordsRead(t *testing.T) {
	ctx := context.Background()
	mdb := New()

	tx, err := mdb.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Get(ctx, "absent"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Get(absent): want os.ErrNotExist, got %v", err)
	}
	if _, ok := tx.reads["absent"]; !ok {
		t.Errorf("Get on a missing key did not record a read; reads = %v", tx.reads)
	}
}

// TestDeletedKeyGetRecordsRead verifies the same read tracking for a key that
// exists in the database but is deleted at the transaction's read version.
func TestDeletedKeyGetRecordsRead(t *testing.T) {
	ctx := context.Background()
	mdb := New()

	if err := withTx(ctx, mdb, func(tx *Transaction) error {
		return tx.Set(ctx, "k", strings.NewReader("v"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := withTx(ctx, mdb, func(tx *Transaction) error {
		return tx.Delete(ctx, "k")
	}); err != nil {
		t.Fatal(err)
	}

	tx, err := mdb.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Get(ctx, "k"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Get(deleted k): want os.ErrNotExist, got %v", err)
	}
	if _, ok := tx.reads["k"]; !ok {
		t.Errorf("Get on a deleted key did not record a read; reads = %v", tx.reads)
	}
}

// TestCreateCreateConflict is the end-to-end consequence of the missing-key
// read-tracking bug: two concurrent transactions that both observe a key as
// absent and then create it must not both commit. Before the fix, the missing
// Get recorded no read, the creates were treated as blind writes, and both
// transactions committed (a lost update on create).
func TestCreateCreateConflict(t *testing.T) {
	ctx := context.Background()
	mdb := New()
	db := kv.DatabaseFrom(mdb)

	// "newkey" does not exist in the database yet.

	tx1, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx1.Rollback(ctx)

	tx2, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx2.Rollback(ctx)

	// Both transactions observe the key as absent.
	if _, err := tx1.Get(ctx, "newkey"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tx1 Get(newkey): want os.ErrNotExist, got %v", err)
	}
	if _, err := tx2.Get(ctx, "newkey"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tx2 Get(newkey): want os.ErrNotExist, got %v", err)
	}

	// Both try to create it.
	if err := tx1.Set(ctx, "newkey", strings.NewReader("value1")); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Set(ctx, "newkey", strings.NewReader("value2")); err != nil {
		t.Fatal(err)
	}

	err1 := tx1.Commit(ctx)
	err2 := tx2.Commit(ctx)

	if err1 == nil && err2 == nil {
		t.Error("both transactions committed a conflicting create; expected exactly one to fail")
	}
	if err1 != nil && err2 != nil {
		t.Errorf("both transactions failed; expected exactly one to succeed: err1=%v err2=%v", err1, err2)
	}
}
