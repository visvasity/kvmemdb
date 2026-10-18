// Copyright (c) 2025 Visvasity LLC

package kvmemdb

import (
	"context"
	"errors"
	"io"
	"iter"
	"os"
	"strings"
	"testing"
)

// TestClosedTransactionOps verifies that every operation on a transaction that
// has already been committed or rolled back returns os.ErrClosed instead of
// panicking or silently succeeding.
func TestClosedTransactionOps(t *testing.T) {
	ctx := context.Background()
	mdb := New()

	check := func(t *testing.T, tx *Transaction) {
		t.Helper()

		if _, err := tx.Get(ctx, "k"); !errors.Is(err, os.ErrClosed) {
			t.Errorf("Get on closed tx = %v, want os.ErrClosed", err)
		}
		if err := tx.Set(ctx, "k", strings.NewReader("v")); !errors.Is(err, os.ErrClosed) {
			t.Errorf("Set on closed tx = %v, want os.ErrClosed", err)
		}
		if err := tx.Delete(ctx, "k"); !errors.Is(err, os.ErrClosed) {
			t.Errorf("Delete on closed tx = %v, want os.ErrClosed", err)
		}

		// Value iterators must yield nothing and report os.ErrClosed.
		for name, mk := range map[string]func(*error) iter.Seq2[string, io.Reader]{
			"Scan":    func(e *error) iter.Seq2[string, io.Reader] { return tx.Scan(ctx, e) },
			"Ascend":  func(e *error) iter.Seq2[string, io.Reader] { return tx.Ascend(ctx, "", "", e) },
			"Descend": func(e *error) iter.Seq2[string, io.Reader] { return tx.Descend(ctx, "", "", e) },
		} {
			var err error
			n := 0
			for range mk(&err) {
				n++
			}
			if n != 0 {
				t.Errorf("%s on closed tx yielded %d items, want 0", name, n)
			}
			if !errors.Is(err, os.ErrClosed) {
				t.Errorf("%s on closed tx errp = %v, want os.ErrClosed", name, err)
			}
		}

		// Key-only iterators must yield nothing and report os.ErrClosed.
		for name, mk := range map[string]func(*error) iter.Seq[string]{
			"AscendKeys":  func(e *error) iter.Seq[string] { return tx.AscendKeys(ctx, "", "", e) },
			"DescendKeys": func(e *error) iter.Seq[string] { return tx.DescendKeys(ctx, "", "", e) },
		} {
			var err error
			n := 0
			for range mk(&err) {
				n++
			}
			if n != 0 {
				t.Errorf("%s on closed tx yielded %d items, want 0", name, n)
			}
			if !errors.Is(err, os.ErrClosed) {
				t.Errorf("%s on closed tx errp = %v, want os.ErrClosed", name, err)
			}
		}
	}

	t.Run("committed", func(t *testing.T) {
		tx, err := mdb.NewTransaction(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		check(t, tx)
	})

	t.Run("rolledback", func(t *testing.T) {
		tx, err := mdb.NewTransaction(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		check(t, tx)
	})
}
