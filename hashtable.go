package cuckoo

import (
	"sync"
)

type KVPair struct {
	k uint64
	v uint64
}

const SLOTS_PER_BUCKET uint64 = 4
const LOCK_STRIPES uint64 = (1 << 16)
const LOCK_STRIPE_MASK uint64 = LOCK_STRIPES - 1

// Should be as big as a single cache-line
type Bucket struct {
	kvpairs  []KVPair // must have size SLOTS_PER_BUCKET
	occupied []bool   // must have size SLOTS_PER_BUCKET
}

func (b *Bucket) tryGet(k uint64, v *uint64) bool {
	var ret bool
	ret = false
	for j := uint64(0); j < SLOTS_PER_BUCKET; j++ {
		if b.kvpairs[j].k == k && b.occupied[j] {
			*v = b.kvpairs[j].v
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
	locks      []*sync.Mutex
}

func MakeCuckooMap(hashpower uint64) *CuckooMap {
	r := new(CuckooMap)
	r.numBuckets = 1 << hashpower
	r.mask = (r.numBuckets - 1)
	r.buckets = make([]Bucket, r.numBuckets)
	r.locks = make([]*sync.Mutex, LOCK_STRIPES)

	for i, _ := range r.locks {
		r.locks[i] = new(sync.Mutex)
	}

	// bucketArena := make([]KVPair, SLOTS_PER_BUCKET*r.numBuckets)
	// occupiedArena := make([]bool, SLOTS_PER_BUCKET*r.numBuckets)
	// start := uint64(0)
	// end := SLOTS_PER_BUCKET
	for i, _ := range r.buckets {
		r.buckets[i] = Bucket{
		kvpairs:  make([]KVPair, SLOTS_PER_BUCKET),
		occupied: make([]bool, SLOTS_PER_BUCKET),
		}
		// r.buckets[i].kvpairs = bucketArena[start:end]
		// r.buckets[i].occupied = occupiedArena[start:end]
		// start += SLOTS_PER_BUCKET
		// end += SLOTS_PER_BUCKET
	}

	return r
}

func (m *CuckooMap) index1(k uint64) uint64 {
	return (k & m.mask)
}

func (m *CuckooMap) index2(k uint64) uint64 {
	// from cuckoohash_map.hh
	t1 := k ^ (k>>32)&0xffffffff
	t2 := t1 ^ (t1>>16)&0xffff
	partial := t2 ^ (t2>>8)&0xff
	nonzero_tag := partial + uint64(1)
	return (k ^ (nonzero_tag * 0xc6a4a7935bd1e995)) & m.mask
}

func lockind(i uint64) uint64 {
	return i & LOCK_STRIPE_MASK
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


func (m *CuckooMap) Get(k uint64, v *uint64) bool {
	i1 := m.index1(k)
	i2 := m.index2(k)

	m.lock_two(i1, i2)
	if m.buckets[i1].tryGet(k, v) {
		m.unlock_two(i1, i2)
		return true
	}
	if m.buckets[i2].tryGet(k, v) {
		m.unlock_two(i1, i2)
		return true
	}
	m.unlock_two(i1, i2)
	return false
}

const (
	INSERT_OK = uint64(0)
	INSERT_DUP = uint64(1)
	INSERT_FAIL = uint64(2)
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

	var r uint64 = INSERT_FAIL
	for j, o := range m.buckets[i1].occupied {
		if !o {
			m.buckets[i1].kvpairs[j] = KVPair{k: k, v: v}
			m.buckets[i1].occupied[j] = true
			m.unlock_two(i1, i2)
			r = INSERT_OK
			break
		}
	}
	if r == INSERT_OK {
		return r
	}

	for j, o := range m.buckets[i2].occupied {
		if !o {
			m.buckets[i2].kvpairs[j] = KVPair{k: k, v: v}
			m.buckets[i2].occupied[j] = true
			m.unlock_two(i1, i2)
			r = INSERT_OK
		}
	}
	return r
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
	var q []cuckooSearchEntry
	var r cuckooPath
	q = make([]cuckooSearchEntry, 0, 256)
	q = append(q, cuckooSearchEntry{path: nil, i: i1})
	q = append(q, cuckooSearchEntry{path: nil, i: i2})
	for len(q) > 0 {
		e := q[0]
		q = q[1:]
		m.locks[lockind(e.i)].Lock()

		for j, o := range m.buckets[e.i].occupied {
			if !o {
				hash := uint64(0) // no key there; this hash value is never used
				e.path = append(e.path, cuckooPathRecord{bucket: e.i, slot: uint64(j), hash: hash})
				m.locks[lockind(e.i)].Unlock()
				q = nil
				r = e.path
				break
			} else if len(e.path) < 5 {
				slotToEvict := uint64(j)
				hash := m.buckets[e.i].kvpairs[slotToEvict].k // XXX: identity hash fn
				var altI uint64
				altI = m.index1(m.buckets[e.i].kvpairs[slotToEvict].k)
				if altI == e.i {
					altI = m.index2(m.buckets[e.i].kvpairs[slotToEvict].k)
				}

				var newPath []cuckooPathRecord
				newPath = make([]cuckooPathRecord, 0, len(e.path)+1)
				newPath = append(newPath, e.path...)
				newPath = append(newPath, cuckooPathRecord{bucket: e.i, slot: slotToEvict, hash: hash})

				newE := cuckooSearchEntry{path: newPath, i: altI}
				q = append(q, newE)
				continue
			}
		}

		m.locks[lockind(e.i)].Unlock()
	}
	return r
}

// attempts to clear the slot in bucket cuckooPath[0].bucket and slotNum
// cuckooPath[0].slot
// returns false if there was a failure, true if success.
func (m *CuckooMap) cuckoo_move(path cuckooPath) bool {
	var j uint64
	var r bool
	r = true
	j = uint64(len(path)) - 1
	for j > 0 {
		fromI := path[j-1].bucket
		toI := path[j].bucket
		m.lock_two(fromI, toI)

		fromSlot := path[j-1].slot
		toSlot := path[j].slot
		hash := path[j-1].hash
		j = j - 1

		if !m.buckets[fromI].occupied[fromSlot] {
			// we got lucky, and don't even need to move anything to make space!
			m.unlock_two(fromI, toI)
			continue
		} else if m.buckets[toI].occupied[toSlot] {
			// the bucket that we're supposed to move to is full; failure
			m.unlock_two(fromI, toI)
			r = false
			break
		} else if m.buckets[fromI].kvpairs[fromSlot].k != hash {
			m.unlock_two(fromI, toI)
			r = false
			break
		} else {
			// Otherwise, there is an entry in fromI.fromSlot and space in
			// toI.toSlot. And, the hash of the kvpair that we want to move is the
			// hash needed for the path to be valid.

			// move one kvpair along the path
			m.buckets[toI].kvpairs[toSlot] = m.buckets[fromI].kvpairs[fromSlot]
			m.buckets[toI].occupied[toSlot] = true
			m.buckets[fromI].occupied[fromSlot] = false

			m.unlock_two(fromI, toI)
			continue
		}
	}
	// after this loop, we've made space in one of i1 or i2; we've let go of the
	// lock before we exit the loop. This means that the caller can't be
	// sure that there's still space in the bucket, since we briefly unlocked
	// the bucket.

	return r
}

func (m *CuckooMap) Insert(k uint64, v uint64) uint64 {
	// try to see if we can insert in one of the two buckets.
	// If we can't, then try cuckoo eviction.
	i1 := m.index1(k)
	i2 := m.index2(k)

	var triesLeft uint64 = 1

	var r uint64
	m.lock_two(i1, i2)
	for {
		r = m.tryInsert(i1, i2, k, v)
		m.unlock_two(i1, i2)
		if r != INSERT_FAIL {
			break
		}

		if triesLeft == 0 {
			r = INSERT_FAIL
			break
		}
		triesLeft--

		p := m.cuckoo_search(i1, i2)
		if len(p) == 0 {
			m.lock_two(i1, i2)
			continue
		}
		if !m.cuckoo_move(p) {
			panic("unable to move along cuckoo path")
		}
		m.lock_two(i1, i2)
	}
	return r
}
