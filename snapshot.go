// Copyright (c) 2024 Visvasity LLC

package kvmemdb

import (
	"context"
	"io"
	"iter"
	"os"
	"slices"
	"sort"
	"strings"
)

type Snapshot struct {
	db *Database

	// snapshotVersion is the max value version readable by this snapshot. This
	// is also the maxCommitVersion of the database at the creation of this
	// snapshot.
	snapshotVersion int64
}

// Get returns the value associated with the input key. Returns os.ErrNotExist
// if key was deleted or doesn't exist.
func (s *Snapshot) Get(ctx context.Context, key string) (io.Reader, error) {
	if len(key) == 0 {
		return nil, os.ErrInvalid
	}

	if mv, ok := s.db.kvs.Load(key); ok {
		if v, ok := mv.Fetch(s.snapshotVersion); ok {
			if v.IsDeleted() {
				return nil, os.ErrNotExist
			}
			return strings.NewReader(v.Data()), nil
		}
	}
	return nil, os.ErrNotExist
}

// keys returns all keys between the [begin, end) range in no-specific order.
func (s *Snapshot) keys(begin, end string) []string {
	kset := make(map[string]struct{})
	for k := range s.db.kvs.Range {
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

// Scan implements kv.Scanner interface to range over all key-value pairs in
// the database.
func (s *Snapshot) Scan(ctx context.Context, errp *error) iter.Seq2[string, io.Reader] {
	return func(yield func(string, io.Reader) bool) {
		for _, key := range s.keys("", "") {
			value, err := s.Get(ctx, key)
			if err != nil {
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
func (s *Snapshot) Ascend(ctx context.Context, begin, end string, errp *error) iter.Seq2[string, io.Reader] {
	return func(yield func(string, io.Reader) bool) {
		if begin != "" && end != "" && begin > end {
			*errp = os.ErrInvalid
			return
		}

		keys := s.keys(begin, end)
		sort.Strings(keys)

		for _, key := range keys {
			value, err := s.Get(ctx, key)
			if err != nil {
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
func (s *Snapshot) Descend(ctx context.Context, begin, end string, errp *error) iter.Seq2[string, io.Reader] {
	return func(yield func(string, io.Reader) bool) {
		if begin != "" && end != "" && begin > end {
			*errp = os.ErrInvalid
			return
		}

		keys := s.keys(begin, end)
		sort.Strings(keys)
		slices.Reverse(keys)
		for _, key := range keys {
			value, err := s.Get(ctx, key)
			if err != nil {
				*errp = err
				return
			}
			if !yield(key, value) {
				return
			}
		}
	}
}

// Discard releases the snapshot.
func (s *Snapshot) Discard(ctx context.Context) error {
	if s.db == nil {
		return os.ErrInvalid
	}
	s.db.closeSnapshot(s)
	return nil
}
