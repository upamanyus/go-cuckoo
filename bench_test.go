package cuckoo

import (
	"sync"
	"sync/atomic"
	"math/rand"
	"testing"
)

const (
	SIZE    = 1*1024*1024
)

func BenchmarkGoMapMultipleThreads(b *testing.B) {
	lookup := make(map[uint64]uint64, SIZE)
	lookupMu := new(sync.Mutex)
	for i := uint64(0); i < SIZE; i += 1 {
		lookup[i] = i*i
	}

	NUM_THREADS := 4
	rand.Seed(int64(b.N))

	totalOps := uint64(0)
	done := make(chan struct{})
	b.ResetTimer()
	for i := 0; i < NUM_THREADS; i++ {
		go func() {
			for {
				k := rand.Uint64() & (SIZE - 1)
				lookupMu.Lock()
				if v, ok := lookup[k]; ok {
					if v != k*k {
						panic("Bad value in lookup")
					}
				}
				lookupMu.Unlock()
				atomic.AddUint64(&totalOps, 1)
				if atomic.LoadUint64(&totalOps) > uint64(b.N) {
					done <-struct{}{}
				}
			}
		}()
	}
	_ = <-done

}

func BenchmarkCuckooMapSingleThread(b *testing.B) {
	lookup := MakeCuckooMap(20)
	for i := uint64(0); i < SIZE; i += 1 {
		lookup.Insert(i, i*i)
	}

	rand.Seed(int64(b.N))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := rand.Uint64() & (SIZE - 1)
		var v uint64
		if lookup.Get(k, &v) {
			if v != k*k {
				panic("Bad value in lookup")
			}
		}
	}
}

func BenchmarkCuckooMapParallel(b *testing.B) {
	lookup := MakeCuckooMap(19)
	for i := uint64(0); i < SIZE; i += 1 {
		lookup.Insert(i, i*i)
	}

	b.RunParallel(func(pb *testing.PB) {
		var v uint64
		k := uint64(0)
		for pb.Next() {
			k += 1
			if lookup.Get(k, &v) {
				if v != k*k {
					panic("Bad value in lookup")
				}
			}
		}
	})
}

func BenchmarkGoMapParallel(b *testing.B) {
	lookup := make(map[uint64]uint64)
	lookupMu := new(sync.Mutex)
	for i := uint64(0); i < SIZE; i += 1 {
		lookup[i] = i*i
	}

	b.RunParallel(func(pb *testing.PB) {
		k := uint64(0)
		for pb.Next() {
			k += 1
			lookupMu.Lock()
			if v, ok := lookup[k]; ok {
				if v != k*k {
					panic("Bad value in lookup")
				}
			}
			lookupMu.Unlock()
		}
	})
}

// XXX: want a better benchmark setup than this
func BenchmarkCuckooMapMultipleThreads(b *testing.B) {
	lookup := MakeCuckooMap(20)
	for i := uint64(0); i < SIZE; i += 1 {
		lookup.Insert(i, i*i)
	}

	NUM_THREADS := 4
	rand.Seed(int64(b.N))

	totalOps := uint64(0)
	done := make(chan struct{})
	b.ResetTimer()
	for i := 0; i < NUM_THREADS; i++ {
		go func() {
			for {
				k := rand.Uint64() & (SIZE - 1)
				var v uint64
				if lookup.Get(k, &v) {
					if v != k*k {
						panic("Bad value in lookup")
					}
				}
				atomic.AddUint64(&totalOps, 1)
				if atomic.LoadUint64(&totalOps) > uint64(b.N) {
					done <-struct{}{}
				}
			}
		}()
	}
	_ = <-done
}

func BenchmarkGoroutine(b *testing.B) {
	c := make(chan struct{})

	rand.Seed(int64(b.N))
	b.ResetTimer()
	for n := uint64(0); n < uint64(b.N); n++ {
		go func() {
			c <- struct{}{}
		}()
		_ = <-c
	}
}
