package cuckoo

import (
	"github.com/tchajed/goose/machine"
)

type KVPair struct {
	k uint64
	v uint64
}

const SLOTS_PER_BUCKET = 4
const LOCK_STRIPES = (1 << 16)
const LOCK_STRIPE_MASK = LOCK_STRIPES - 1

// Should be as big as a single cache-line
type Bucket struct {
	kvpairs  [SLOTS_PER_BUCKET]KVPair // must have size SLOTS_PER_BUCKET
	occupied [SLOTS_PER_BUCKET]bool   // must have size SLOTS_PER_BUCKET
}

func (b *Bucket) tryGet(k uint64, v *uint64) bool {
	var ret bool
	ret = false
	for _, kv := range b.kvpairs {
		if kv.k == k {
			*v = kv.v
			ret = true
			break
		} else {
			continue
		}
	}
	return ret
}

type CuckooMap struct {
	numBuckets uint64 // fixed capacity; never gets larger
	mask       uint64
	buckets    []Bucket
	locks      []*SMutex
}

func MakeCuckooMap(hashpower uint64) *CuckooMap {
	r := new(CuckooMap)
	r.numBuckets = 1 << hashpower
	r.mask = (r.numBuckets - 1)
	r.buckets = make([]Bucket, r.numBuckets)
	r.locks = make([]*SMutex, LOCK_STRIPES)

	for i, _ := range r.locks {
		r.locks[i] = new(SMutex)
	}

	return r
}

func (m *CuckooMap) index1(k uint64) uint64 {
	return (k & m.mask)
}

func (m *CuckooMap) index2(k uint64) uint64 {
	// from cuckoohash_map.hh
	nonzero_tag := (k & 0xff) + uint64(1)
	return (k ^ (nonzero_tag * 0xc6a4a7935bd1e995)) & m.mask
}

func lockind(i uint64) uint64 {
	return i & LOCK_STRIPE_MASK
}

func (m *CuckooMap) Get(k uint64, v *uint64) bool {
	i1 := m.index1(k)
	i2 := m.index2(k)

	l1 := m.locks[lockind(i1)]
	l1.Lock()
	if m.buckets[i1].tryGet(k, v) {
		l1.Unlock()
		return true
	}
	l1.Unlock()
	l2 := m.locks[lockind(i2)]
	l2.Lock()
	if m.buckets[i2].tryGet(k, v) {
		l2.Unlock()
		return true
	}
	l2.Unlock()
	return false
}

const (
	INSERT_OK = iota
	INSERT_DUP
	INSERT_FAIL
)

func (m *CuckooMap) Insert(k uint64, v uint64) uint64 {
	// try to see if we can insert in one of the two buckets.
	// If we can't, then try cuckoo eviction.
	i1 := m.index1(k)
	i2 := m.index2(k)
	m.lock_two(i1, i2)

	temp := new(uint64)
	if m.buckets[i1].tryGet(k, temp) {
		m.unlock_two(i1, i2)
		return INSERT_DUP
	}
	if m.buckets[i1].tryGet(k, temp) {
		m.unlock_two(i1, i2)
		return INSERT_DUP
	}

	for j, o := range m.buckets[i1].occupied {
		if !o {
			m.buckets[i1].kvpairs[j] = KVPair{k: k, v: v}
			m.buckets[i1].occupied[j] = true
			m.unlock_two(i1, i2)
			return INSERT_OK
		}
	}

	for j, o := range m.buckets[i2].occupied {
		if !o {
			m.buckets[i2].kvpairs[j] = KVPair{k: k, v: v}
			m.buckets[i2].occupied[j] = true
			m.unlock_two(i1, i2)
			return INSERT_OK
		}
	}

	// do cuckoo hashing

	m.unlock_two(i1, i2)
	return INSERT_FAIL
}

func (m *CuckooMap) lock_two(i1 uint64, i2 uint64) {
	j1 := lockind(i1)
	j2 := lockind(i2)
	if j1 == j2 {
		m.locks[j1].Lock()
	} else if j2 < j1 {
		m.locks[j2].Lock()
		m.locks[j1].Lock()
	} else { // j1 < j2
		m.locks[j1].Lock()
		m.locks[j2].Lock()
	}
	return
}

func (m *CuckooMap) unlock_two(i1 uint64, i2 uint64) {
	j1 := lockind(i1)
	j2 := lockind(i2)
	if j1 == j2 {
		m.locks[j1].Unlock()
	} else {
		m.locks[j1].Unlock()
		m.locks[j2].Unlock()
	}
}

type SlotNum = uint64

type cuckooPath struct {
	startingBucket uint64
	slots          []SlotNum
}

type cuckooSearchEntry struct {
	path cuckooPath
	i uint64
}

func (m *CuckooMap) cuckoo_search(i1 uint64, i2 uint64) cuckooPath {
	q := make([]cuckooSearchEntry, 256)
	for len(q) > 0 {
		e := q[0]
		q = q[1:]
		m.locks[lockind(e.i)].Lock()

		for j, o := range m.buckets[e.i].occupied {
			if !o {
				e.path.slots = append(e.path.slots, uint64(j))
				return e.path
			}
		}

		// pick a random slot to try evicting

		slotToEvict := machine.RandomUint64() % SLOTS_PER_BUCKET
		altI := m.index2(m.buckets[e.i].kvpairs[slotToEvict].k)
		newPathSlots := make([]SlotNum, 0, len(e.path.slots) + 1)
		newPathSlots = append(newPathSlots, e.path.slots...)
		newPathSlots = append(newPathSlots, slotToEvict)

		newPath := cuckooPath{startingBucket:e.path.startingBucket, slots:newPathSlots}

		newE := cuckooSearchEntry{path: newPath, i:altI}
		q = append(q, newE)

		m.locks[lockind(e.i)].Unlock()
	}
	return cuckooPath{}
}
