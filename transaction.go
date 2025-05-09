// Copyright (c) 2024 Visvasity LLC

package kvmemdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"slices"
	"sort"
	"strings"

	"github.com/visvasity/kvmemdb/mvcc"
)

type Transaction struct {
	db *Database

	// snapshotVersion is the max version number readable by this
	// transaction. This is also the maxCommitVersion of the database at the
	// creation of this transaction. Multiple transactions can exist with the
	// same snapshotVersion value.
	snapshotVersion int64

	// committed flag is set to true when tx is committed. It remains false when
	// tx live or if it is aborted.
	committed bool

	// reads map holds all key-value pairs read by this transaction. Updates to
	// these key-value pairs will *move* the entry to the following 'writes' map.
	reads map[string]*mvcc.Value

	// writes map holds all updates performed by this transaction. A nil string
	// value for a key represents a deleted key.
	writes map[string]*string
}

// Set creates or updates a key-value pair in the database. The input key
// cannot be empty and input value cannot be nil.
func (t *Transaction) Set(ctx context.Context, key string, value io.Reader) error {
	if len(key) == 0 || value == nil {
		return os.ErrInvalid
	}

	data, err := io.ReadAll(value)
	if err != nil {
		return err
	}

	s := string(data)
	t.writes[key] = &s
	return nil
}

// Delete removes the input key and the associated value. Returns nil even when
// the input key doesn't exist.
func (t *Transaction) Delete(ctx context.Context, key string) error {
	if len(key) == 0 {
		return os.ErrInvalid
	}

	t.writes[key] = nil
	return nil
}

// Get returns the value associated with the input key. Returns os.ErrNotExist
// if key was deleted or doesn't exist.
func (t *Transaction) Get(ctx context.Context, key string) (io.Reader, error) {
	if len(key) == 0 {
		return nil, os.ErrInvalid
	}

	if v, ok := t.writes[key]; ok {
		if v == nil {
			return nil, fmt.Errorf("key %s is deleted by this tx: %w", key, os.ErrNotExist)
		}
		return strings.NewReader(*v), nil
	}

	if v, ok := t.reads[key]; ok {
		return strings.NewReader(v.Data()), nil
	}

	if mv, ok := t.db.kvs.Load(key); ok {
		if v, ok := mv.Fetch(t.snapshotVersion); ok {
			if v.IsDeleted() {
				return nil, fmt.Errorf("key %s is deleted at this tx read version: %w", key, os.ErrNotExist)
			}
			t.reads[key] = v
			return strings.NewReader(v.Data()), nil
		}
	}
	return nil, fmt.Errorf("key %s does not exist in the db: %w", key, os.ErrNotExist)
}

// keys returns all keys between the [begin, end) range in no-specific order.
func (t *Transaction) keys(begin, end string) []string {
	kset := make(map[string]struct{})
	for k := range t.reads {
		kset[k] = struct{}{}
	}
	for k := range t.writes {
		kset[k] = struct{}{}
	}
	for k := range t.db.kvs.Range {
		if _, ok := kset[k]; !ok {
			kset[k] = struct{}{}
		}
	}

	keys := make([]string, 0, len(kset))
	for k := range kset {
		keys = append(keys, k)
	}

	keys = slices.DeleteFunc(keys, func(k string) bool {
		if begin == "" && end == "" {
			return false
		}
		if begin != "" && end == "" {
			return k < begin
		}
		if begin == "" && end != "" {
			return k >= end
		}
		return k < begin || k >= end
	})

	return keys
}

// Commit attempts to save all updates performed by the transaction to the
// database. Returns nil on success. Transaction is effectively destroyed
// irrespective of the result and no operations should be performed any
// further.
func (t *Transaction) Commit(ctx context.Context) error {
	if t.db == nil {
		return os.ErrInvalid
	}
	defer t.db.closeTransaction(t)

	if err := commit(t.db, t); err != nil {
		return err
	}
	return nil
}

// Rollback drops all updates performed by the transaction. Transaction is
// effectively destroyed and no operations should be performed any further.
func (t *Transaction) Rollback(ctx context.Context) error {
	if t.db == nil {
		return os.ErrInvalid
	}
	t.db.closeTransaction(t)
	return nil
}

// Scan implements kv.Scanner interface to range over all key-value pairs in
// the database.
func (t *Transaction) Scan(ctx context.Context, errp *error) iter.Seq2[string, io.Reader] {
	return func(yield func(string, io.Reader) bool) {
		for _, key := range t.keys("", "") {
			value, err := t.Get(ctx, key)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				*errp = err
				return
			}
			if !yield(key, value) {
				return
			}
		}
	}
}

// Ascend implements kv.Scanner interface to range over key-value pairs between
// 'begin' and 'end' keys in the database in ascending order.
func (t *Transaction) Ascend(ctx context.Context, begin, end string, errp *error) iter.Seq2[string, io.Reader] {
	return func(yield func(string, io.Reader) bool) {
		if begin != "" && end != "" && begin > end {
			*errp = os.ErrInvalid
			return
		}

		keys := t.keys(begin, end)
		sort.Strings(keys)

		for _, key := range keys {
			value, err := t.Get(ctx, key)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				log.Printf("get on key %q out of %v failed: %v", key, keys, err)
				*errp = err
				return
			}
			if !yield(key, value) {
				return
			}
		}
	}
}

// Ascend implements kv.Scanner interface to range over key-value pairs between
// 'begin' and 'end' keys in the database in descending order.
func (t *Transaction) Descend(ctx context.Context, begin, end string, errp *error) iter.Seq2[string, io.Reader] {
	return func(yield func(string, io.Reader) bool) {
		if begin != "" && end != "" && begin > end {
			*errp = os.ErrInvalid
			return
		}

		keys := t.keys(begin, end)
		sort.Strings(keys)
		slices.Reverse(keys)

		for _, key := range keys {
			value, err := t.Get(ctx, key)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				log.Printf("get on key %q out of %v failed: %v", key, keys, err)
				*errp = err
				return
			}
			if !yield(key, value) {
				return
			}
		}
	}
}
