// Copyright (c) 2024 Visvasity LLC

package mvcc

import (
	"slices"
	"strings"
)

// MultiValue represents multiple values with at versions in sorted order.
type MultiValue struct {
	values []*Value
}

// NewMultiValue creates a multi-value with initial version.
func NewMultiValue(v *Value) *MultiValue {
	return &MultiValue{
		values: []*Value{v},
	}
}

// String returns the MultiValue as a human-readable string.
func (mv *MultiValue) String() string {
	var sb strings.Builder
	sb.WriteRune('[')
	for _, v := range mv.values {
		sb.WriteString(v.String())
	}
	sb.WriteRune(']')
	return sb.String()
}

// Fetch returns the value found at the given version or the closest lower
// version to the given version. Returned value can be a deleted value.
func (mv *MultiValue) Fetch(version int64) (v *Value, found bool) {
	index, ok := slices.BinarySearchFunc(mv.values, version, findValue)
	if ok {
		return mv.values[index], true
	}

	closest := index - 1
	if closest >= 0 && closest < len(mv.values) {
		return mv.values[closest], true
	}

	return nil, false
}

// findValue is the helper function for binary search a value based on the
// version.
func findValue(v *Value, version int64) int {
	ver := v.Version()
	if ver == version {
		return 0
	}
	if ver < version {
		return -1
	}
	return 1
}

// Append returns a copy of the input multi-value with a newer version to
// end. Version of the appending value *must* be larger than all existing
// versions.
func Append(mv *MultiValue, v *Value) *MultiValue {
	if mv == nil || len(mv.values) == 0 {
		return &MultiValue{
			values: []*Value{v},
		}
	}

	if !slices.IsSortedFunc(mv.values, func(a, b *Value) int {
		return int(a.Version() - b.Version())
	}) {
		panic("multi-value versions are not in sorted order")
	}

	nvalues := len(mv.values)
	if last := mv.values[nvalues-1]; v.Version() <= last.Version() {
		panic("appending version is not larger than existing multi-value versions")
	}

	newvs := slices.Clone(mv.values)
	newvs = append(newvs, v)

	if !slices.IsSortedFunc(newvs, func(a, b *Value) int {
		return int(a.Version() - b.Version())
	}) {
		panic("newer multi-value versions are not in sorted order")
	}

	return &MultiValue{values: newvs}
}

// Compact drops older data before the given version unless it is not the only
// version and is not deleted. Returns the same input multi-value if no
// compaction can be performed; otherwise, returns a clone of the input
// multi-value.
func Compact(mv *MultiValue, minVersion int64) *MultiValue {
	if mv == nil || len(mv.values) == 0 {
		return nil
	}

	// If there is only one version and it is not a deleted version, then we
	// should keep it.
	if len(mv.values) == 1 {
		v := mv.values[0]
		if v.IsDeleted() && v.Version() < minVersion {
			return nil
		}
		return mv
	}

	index, ok := slices.BinarySearchFunc(mv.values, minVersion, findValue)
	if !ok {
		index = index - 1
	}
	if index < 0 {
		return mv
	}

	newvs := slices.DeleteFunc(slices.Clone(mv.values), func(v *Value) bool {
		return v.Version() < mv.values[index].Version()
	})

	if len(newvs) == len(mv.values) {
		return mv
	}
	return &MultiValue{values: newvs}
}
