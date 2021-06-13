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
	c := MakeCuckooMap(16)
	m := make(map[uint64]uint64)

	N := 70000
	for i := 0; i < N; i++ {
		k := machine.RandomUint64()
		v := machine.RandomUint64()
		m[k] = v
		r := c.Insert(k, v)
		if r == INSERT_FAIL {
			t.Fatalf("CuckooMap.Insert() failed")
		} else if r == INSERT_DUP {
			t.Fatalf("CuckooMap.Insert() duplicate")
		}
	}
	if !checkEquivalence(m, c) {
		t.Fatalf("Maps not equivalent")
	}
}
