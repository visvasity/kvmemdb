// Copyright (c) 2026 Visvasity LLC

package kvmemdb

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/visvasity/kv"
)

// Compile-time assertions that the snapshot and transaction types implement
// the optional kv.KeyRanger interface.
var (
	_ kv.KeyRanger = (*Snapshot)(nil)
	_ kv.KeyRanger = (*Transaction)(nil)
)

func TestAscendDescendKeys(t *testing.T) {
	ctx := context.Background()

	mdb := New()

	// Setup test data.
	tx, err := mdb.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("NewTransaction failed: %v", err)
	}
	for _, k := range []string{"key1", "key2", "key3"} {
		if err := tx.Set(ctx, k, strings.NewReader("value-"+k)); err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tests := []struct {
		name     string
		beg, end string
		ascend   []string
		descend  []string
		wantErr  bool
	}{
		{
			name:    "Full range (empty beg and end)",
			ascend:  []string{"key1", "key2", "key3"},
			descend: []string{"key3", "key2", "key1"},
		},
		{
			name:    "From key1 to key3 (inclusive-exclusive)",
			beg:     "key1",
			end:     "key3",
			ascend:  []string{"key1", "key2"},
			descend: []string{"key2", "key1"},
		},
		{
			name:    "From smallest key (empty beg)",
			end:     "key2",
			ascend:  []string{"key1"},
			descend: []string{"key1"},
		},
		{
			name:    "To largest key (empty end)",
			beg:     "key2",
			ascend:  []string{"key2", "key3"},
			descend: []string{"key3", "key2"},
		},
		{
			name:    "Invalid range (beg >= end)",
			beg:     "key3",
			end:     "key1",
			wantErr: true,
		},
	}

	// collectAsc/collectDesc exercise the kv.KeyRanger methods against both the
	// snapshot and the transaction read paths.
	collect := func(t *testing.T, seqFor func(r kv.KeyRanger, errp *error) func(func(string) bool), wantErr bool, want []string) {
		t.Helper()

		run := func(r kv.KeyRanger) {
			var gotErr error
			var got []string
			for k := range seqFor(r, &gotErr) {
				got = append(got, k)
			}
			if wantErr && !errors.Is(gotErr, os.ErrInvalid) {
				t.Errorf("expected error os.ErrInvalid, got %v", gotErr)
			}
			if !wantErr && gotErr != nil {
				t.Errorf("unexpected error: %v", gotErr)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("keys = %v, want %v", got, want)
			}
		}

		snap, err := mdb.NewSnapshot(ctx)
		if err != nil {
			t.Fatalf("NewSnapshot failed: %v", err)
		}
		defer snap.Discard(ctx)
		run(snap)

		tx, err := mdb.NewTransaction(ctx)
		if err != nil {
			t.Fatalf("NewTransaction failed: %v", err)
		}
		defer tx.Rollback(ctx)
		run(tx)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collect(t, func(r kv.KeyRanger, errp *error) func(func(string) bool) {
				return r.AscendKeys(ctx, tt.beg, tt.end, errp)
			}, tt.wantErr, tt.ascend)

			collect(t, func(r kv.KeyRanger, errp *error) func(func(string) bool) {
				return r.DescendKeys(ctx, tt.beg, tt.end, errp)
			}, tt.wantErr, tt.descend)
		})
	}
}

// TestAscendKeysSkipsDeleted verifies that keys deleted at the read version are
// not yielded by the key-only iterators.
func TestAscendKeysSkipsDeleted(t *testing.T) {
	ctx := context.Background()
	mdb := New()

	err := withTx(ctx, mdb, func(tx *Transaction) error {
		for _, k := range []string{"a", "b", "c"} {
			if err := tx.Set(ctx, k, strings.NewReader(k)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := withTx(ctx, mdb, func(tx *Transaction) error {
		return tx.Delete(ctx, "b")
	}); err != nil {
		t.Fatal(err)
	}

	snap, err := mdb.NewSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Discard(ctx)

	var gotErr error
	var got []string
	for k := range snap.AscendKeys(ctx, "", "", &gotErr) {
		got = append(got, k)
	}
	if gotErr != nil {
		t.Fatalf("unexpected error: %v", gotErr)
	}
	if want := []string{"a", "c"}; !reflect.DeepEqual(got, want) {
		t.Errorf("AscendKeys = %v, want %v (deleted key must be skipped)", got, want)
	}
}

// TestAscendKeysRecordsReads verifies that key-only iteration over a
// transaction records the visited keys in the read set, so that it
// participates in serializable-snapshot-isolation conflict detection just like
// the value-returning Ascend.
func TestAscendKeysRecordsReads(t *testing.T) {
	ctx := context.Background()
	mdb := New()

	if err := withTx(ctx, mdb, func(tx *Transaction) error {
		return tx.Set(ctx, "k", strings.NewReader("v"))
	}); err != nil {
		t.Fatal(err)
	}

	tx, err := mdb.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)

	var gotErr error
	for range tx.AscendKeys(ctx, "", "", &gotErr) {
	}
	if gotErr != nil {
		t.Fatalf("unexpected error: %v", gotErr)
	}
	if _, ok := tx.reads["k"]; !ok {
		t.Errorf("AscendKeys did not record a read for key %q; read set = %v", "k", tx.reads)
	}
}

func withTx(ctx context.Context, mdb *Database, fn func(*Transaction) error) error {
	tx, err := mdb.NewTransaction(ctx)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}
