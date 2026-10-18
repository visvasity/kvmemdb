// Copyright (c) 2025 Visvasity LLC

package kvmemdb

import (
	"context"
	"testing"

	"github.com/visvasity/kv"
	"github.com/visvasity/kvtests"
)

func TestAllKeyValueTests(t *testing.T) {
	ctx := context.Background()

	db := kv.DatabaseFrom(New())
	if db == nil {
		t.Fatal("failed to open database")
	}

	kvtests.TestEmptyKeyInvalid(ctx, t, db)
	kvtests.TestNonExistentKey(ctx, t, db)
	kvtests.TestNilValueInvalid(ctx, t, db)
	kvtests.TestCommitAfterRollbackIgnored(ctx, t, db)
	kvtests.TestRollbackAfterCommitIgnored(ctx, t, db)
	kvtests.TestSnapshotIsolation(ctx, t, db)
	kvtests.TestSnapshotRepeatableRead(ctx, t, db)
	kvtests.TestSnapshotFrozenAtCreation(ctx, t, db)
	kvtests.TestDisjointTransactionCommit(ctx, t, db)
	kvtests.TestConflictingTransactionCommit(ctx, t, db)
	kvtests.TestRangeBeginEndInvalid(ctx, t, db)
	kvtests.TestRangeFullDatabaseScan(ctx, t, db)
	kvtests.TestRangeBoundsInclusion(ctx, t, db)
	kvtests.TestRangeDescendBounds(ctx, t, db)
	kvtests.TestSnapshotIteratorStability(ctx, t, db)
	kvtests.TestSnapshotIteratorPrefixRange(ctx, t, db)
	kvtests.TestDiscardedSnapshotBehavior(ctx, t, db)
	kvtests.TestTransactionVisibility(ctx, t, db)
	kvtests.TestTransactionDeleteVisibility(ctx, t, db)
	kvtests.TestTransactionDeleteRecreate(ctx, t, db)
	kvtests.TestTransactionRollbackVisibility(ctx, t, db)
	kvtests.TestLargeValueRoundtrip(ctx, t, db)
	kvtests.TestZeroLengthValue(ctx, t, db)
	kvtests.TestPrefixCleanupTrailingFF(ctx, t, db)
}

func TestAllKeyValueTestsOverHTTP(t *testing.T) {
	ctx := context.Background()
	// slog.SetLogLoggerLevel(slog.LevelDebug)

	db := kv.DatabaseFrom(New())

	kvtests.TestEmptyKeyInvalid(ctx, t, db)
	kvtests.TestNonExistentKey(ctx, t, db)
	kvtests.TestNilValueInvalid(ctx, t, db)
	kvtests.TestCommitAfterRollbackIgnored(ctx, t, db)
	kvtests.TestRollbackAfterCommitIgnored(ctx, t, db)
	// kvtests.TestSnapshotIsolation(ctx, t, db)
	kvtests.TestSnapshotRepeatableRead(ctx, t, db)
	kvtests.TestSnapshotFrozenAtCreation(ctx, t, db)
	kvtests.TestDisjointTransactionCommit(ctx, t, db)
	kvtests.TestConflictingTransactionCommit(ctx, t, db)
	kvtests.TestRangeBeginEndInvalid(ctx, t, db)
	kvtests.TestRangeFullDatabaseScan(ctx, t, db)
	kvtests.TestRangeBoundsInclusion(ctx, t, db)
	kvtests.TestRangeDescendBounds(ctx, t, db)
	kvtests.TestSnapshotIteratorStability(ctx, t, db)
	kvtests.TestSnapshotIteratorPrefixRange(ctx, t, db)
	kvtests.TestDiscardedSnapshotBehavior(ctx, t, db)
	kvtests.TestTransactionVisibility(ctx, t, db)
	kvtests.TestTransactionDeleteVisibility(ctx, t, db)
	kvtests.TestTransactionDeleteRecreate(ctx, t, db)
	kvtests.TestTransactionRollbackVisibility(ctx, t, db)
	// kvtests.TestLargeValueRoundtrip(ctx, t, db)
	kvtests.TestZeroLengthValue(ctx, t, db)
	kvtests.TestPrefixCleanupTrailingFF(ctx, t, db)
}
