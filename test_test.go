package cuckoo

import (
	"fmt"
	"github.com/tchajed/goose/machine"
	"sync"
	"testing"
)

func checkEquivalence(m map[uint64]uint64, c *CuckooMap) bool {
	var vtemp uint64
	for k, v := range m {
		if !c.Get(k, &vtemp) {
			panic("Missing key")
		} else if vtemp != v {
			fmt.Printf("%d -> %d instead of %d", k, vtemp, v)
			panic("Value doesn't match")
		}
	}
	return true
}

func TestMapSingleThreaded(t *testing.T) {
	c := MakeCuckooMap(15)
	m := make(map[uint64]uint64)

	N := 127000
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

func TestMapSequential(t *testing.T) {
	hashpower := uint64(18)
	c := MakeCuckooMap(hashpower)
	m := make(map[uint64]uint64)

	// when inserting sequentially, can get to 100% load factor!
	N := SLOTS_PER_BUCKET * (1 << hashpower)
	for i := 0; i < N; i++ {
		k := uint64(i)
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

func TestMapDisjointConcurrent(t *testing.T) {
	hashpower := uint64(16)
	c := MakeCuckooMap(hashpower)

	N := 10000
	nthread := uint64(5)
	var wg sync.WaitGroup
	for i := uint64(0); i < nthread; i++ {
		wg.Add(1)
		go func(e uint64) {
			defer wg.Done()
			m := make(map[uint64]uint64)
			for j := 0; j < N; j++ {
				k := machine.RandomUint64()
				k = (k/nthread) * nthread + e
				v := machine.RandomUint64()
				if _, ok := m[k]; ok {
					continue // don't insert duplicates
				}
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

		}(i)
	}
	wg.Wait()
}
