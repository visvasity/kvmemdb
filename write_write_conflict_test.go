// Copyright (c) 2025 Visvasity LLC

package kvmemdb

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/visvasity/kv"
)

func TestWriteWriteConflict(t *testing.T) {
	ctx := context.Background()

	db := New()

	// Initialize with a key
	err := kv.WithReadWriter(ctx, db, func(ctx context.Context, rw kv.ReadWriter) error {
		return rw.Set(ctx, "key1", strings.NewReader("initial"))
	})
	if err != nil {
		t.Fatalf("Failed to setup initial data: %v", err)
	}

	// Run two interleaved transactions accessing same key.

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

	if _, err := tx1.Get(ctx, "key1"); err != nil {
		t.Fatal(err)
	}
	if err := tx1.Set(ctx, "key1", strings.NewReader("value1")); err != nil {
		t.Fatal(err)
	}

	if _, err := tx2.Get(ctx, "key1"); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Set(ctx, "key1", strings.NewReader("value2")); err != nil {
		t.Fatal(err)
	}

	err1 := tx1.Commit(ctx)
	err2 := tx2.Commit(ctx)

	// Verify only one transaction committed
	if err1 == nil && err2 == nil {
		t.Error("Both transactions committed, expected one to fail")
	}
	if err1 != nil && err2 != nil {
		t.Error("Both transactions failed, expected one to succeed")
	}

	// Check final state
	var finalValue string
	err = kv.WithReader(ctx, db, func(ctx context.Context, r kv.Reader) error {
		reader, err := r.Get(ctx, "key1")
		if err != nil {
			return err
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		finalValue = string(data)
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to read final state: %v", err)
	}

	// Verify the final value matches the successful transaction
	if err1 == nil && finalValue != "value1" {
		t.Errorf("Final value = %s, want value1", finalValue)
	}
	if err2 == nil && finalValue != "value2" {
		t.Errorf("Final value = %s, want value2", finalValue)
	}
}
