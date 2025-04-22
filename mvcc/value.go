// Copyright (c) 2024 Visvasity LLC

package mvcc

import (
	"fmt"
)

type Value struct {
	version int64
	data    string
}

// NewValue creates a value with given version. Input byte slice should not be
// modified any further.
func NewValue(ver int64) *Value {
	if ver <= 0 {
		panic("version value cannot be zero or -ve")
	}
	return &Value{
		version: ver,
	}
}

func (v *Value) Clone(ver int64) *Value {
	if ver <= 0 {
		panic("version value cannot be -ve")
	}
	if ver <= v.version {
		panic(fmt.Sprintf("new version %d cannot be smaller than data version %d", ver, v.Version()))
	}
	return &Value{
		version: ver,
		data:    v.data,
	}
}

func (v *Value) String() string {
	if v.IsDeleted() {
		return fmt.Sprintf("{version:%d deleted}", v.Version())
	}
	return fmt.Sprintf("{version:%d data:%s}", v.Version(), v.data)
}

func (v *Value) Data() string {
	return v.data
}

func (v *Value) SetData(data string) {
	if v.IsDeleted() {
		v.version = -v.version
	}
	v.data = data
}

func (v *Value) Delete() {
	if v.version > 0 {
		v.data = ""
		v.version = -v.version
	}
}

func (v *Value) Version() int64 {
	if v.IsDeleted() {
		return -v.version
	}
	return v.version
}

func (v *Value) IsDeleted() bool {
	return v.version < 0
}
