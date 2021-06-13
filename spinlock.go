package cuckoo

import (
	"sync/atomic"
)

type SMutex struct {
	locked uint64
}

func (m *SMutex) Lock() {
	for atomic.CompareAndSwapUint64(&m.locked, 0, 1) == false {
	}
}

func (m *SMutex) Unlock() {
	atomic.StoreUint64(&m.locked, 0)
}
