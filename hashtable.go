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

func (m *CuckooMap) tryInsert(i1 uint64, i2 uint64, k uint64, v uint64) uint64 {
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
	return INSERT_FAIL
}

func (m *CuckooMap) Insert(k uint64, v uint64) uint64 {
	// try to see if we can insert in one of the two buckets.
	// If we can't, then try cuckoo eviction.
	i1 := m.index1(k)
	i2 := m.index2(k)

	triesLeft := 1

	m.lock_two(i1, i2)
	for m.tryInsert(i1, i2, k, v) == INSERT_FAIL {
		m.unlock_two(i1, i2)
		if triesLeft == 0 {
			return INSERT_FAIL
		}
		triesLeft--

		p := m.cuckoo_search(i1, i2)
		if !m.cuckoo_move(p) {
			panic("unable to move along cuckoo path")
		}
		m.lock_two(i1, i2)
	}
	m.unlock_two(i1, i2)
	return INSERT_OK
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

type cuckooPathRecord struct {
	bucket uint64
	slot   SlotNum
	hash   uint64 // used for confirming that the hash is as expected, so we can safely move to the alternative bucket
}

type cuckooPath = []cuckooPathRecord

type cuckooSearchEntry struct {
	path cuckooPath
	i    uint64
}

func (m *CuckooMap) cuckoo_search(i1 uint64, i2 uint64) cuckooPath {
	q := make([]cuckooSearchEntry, 0, 256)
	q = append(q, cuckooSearchEntry{path:nil, i:i1})
	q = append(q, cuckooSearchEntry{path:nil, i:i2})
	for len(q) > 0 {
		e := q[0]
		q = q[1:]
		m.locks[lockind(e.i)].Lock()

		for j, o := range m.buckets[e.i].occupied {
			if !o {
				hash := uint64(0) // no key there; this hash value is never used
				e.path = append(e.path, cuckooPathRecord{bucket:e.i, slot:uint64(j), hash:hash})
				m.locks[lockind(e.i)].Unlock()
				return e.path
			}
		}

		// XXX: this picks a random slot to try evicting.  As a result, this is
		// basically DFS with two starting nodes. Should probably try doing BFS,
		// or if we really want DFS, should optimize the data structures in this
		// function for DFS.
		slotToEvict := machine.RandomUint64() % SLOTS_PER_BUCKET
		hash := m.buckets[e.i].kvpairs[slotToEvict].k // XXX: identity hash fn
		altI := m.index2(m.buckets[e.i].kvpairs[slotToEvict].k)

		newPath := make([]cuckooPathRecord, 0, len(e.path)+1)
		newPath = append(newPath, e.path...)
		newPath = append(newPath, cuckooPathRecord{bucket:e.i, slot:slotToEvict, hash:hash})

		newE := cuckooSearchEntry{path: newPath, i: altI}
		q = append(q, newE)

		m.locks[lockind(e.i)].Unlock()
	}
	return cuckooPath{}
}

// attempts to clear the slot in bucket cuckooPath[0].bucket and slotNum
// cuckooPath[0].slot
// returns false if there was a failure, true if success.
func (m *CuckooMap) cuckoo_move(path cuckooPath) bool {
	var j uint64
	j = uint64(len(path)) - 1
	for j > 0 {
		fromI := path[j - 1].bucket
		toI := path[j].bucket
		m.lock_two(fromI, toI)

		fromSlot := path[j - 1].slot
		toSlot := path[j].slot
		hash := path[j - 1].hash
		j = j - 1

		if !m.buckets[fromI].occupied[fromSlot] {
			// we got lucky, and don't even need to move anything to make space!
			m.unlock_two(fromI, toI)
			continue
		} else if m.buckets[toI].occupied[toSlot] {
			// the bucket that we're supposed to move to is full; failure
			m.unlock_two(fromI, toI)
			return false
		} else if m.buckets[fromI].kvpairs[fromSlot].k != hash {
			m.unlock_two(fromI, toI)
			return false
		}
		// Otherwise, there is an entry in fromI.fromSlot and space in
		// toI.toSlot. And, the hash of the kvpair that we want to move is the
		// hash needed for the path to be valid.

		// move one kvpair along the path
		m.buckets[toI].kvpairs[toSlot] = m.buckets[fromI].kvpairs[fromSlot]
		m.buckets[toI].occupied[toSlot] = true
		m.buckets[fromI].occupied[fromSlot] = false

		m.unlock_two(fromI, toI)
	}
	// after this loop, we've made space in one of i1 or i2; we've let go of the
	// lock before we exit the loop. This means that the caller can't be
	// sure that there's still space in the bucket, since we briefly unlocked
	// the bucket.

	return true
}
