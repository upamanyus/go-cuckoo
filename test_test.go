package cuckoo

import (
	"testing"
	"github.com/tchajed/goose/machine"
)

func checkEquivalence(m map[uint64]uint64, c *CuckooMap) bool {
	var vtemp uint64
	for k, v := range m {
		if !c.Get(k, &vtemp) {
			// k should be in c, but it isn't; error
			return false
		} else if vtemp != v {
			// value doesn't match
			return false
		}
	}
	return true
}

func TestMapSingleThreaded(t *testing.T) {
	c := MakeCuckooMap(13)
	m := make(map[uint64]uint64)

	N := 10000
	for i := 0; i < N; i++ {
		k := machine.RandomUint64()
		v := machine.RandomUint64()
		m[k] = v
		if c.Insert(k, v) != INSERT_OK {
			t.Fatalf("Insert failed")
		}
		if !checkEquivalence(m, c) {
			t.Fatalf("Maps not equivalent")
		}
	}
}
