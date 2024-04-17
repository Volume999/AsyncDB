package asyncdb

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestThreadSafeMap_Concurrent_Increment(t *testing.T) {
	m := NewThreadSafeMap[int, int]()
	threads := 10
	iters := 100000
	m.Put(1, 0)
	f := func(wg *sync.WaitGroup) {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			m.Lock()
			v, _ := m.GetUnsafe(1)
			m.PutUnsafe(1, v+1)
			m.Unlock()
		}
	}
	var wg sync.WaitGroup
	wg.Add(threads)
	for i := 0; i < threads; i++ {
		go f(&wg)
	}
	wg.Wait()
	v, _ := m.Get(1)
	assert.Equal(t, threads*iters, v)
}

func TestThreadSafeMap_When_Reading_Writing_Concurrently(t *testing.T) {
	// This code tests data race for the ThreadSafeMap. Run it with -race flag.
	m := NewThreadSafeMap[int, int]()
	for _ = range 10 {
		go func() {
			for _ = range 100000 {
				m.Put(1, 2)
			}
		}()
		go func() {
			for _ = range 100000 {
				m.Get(1)
			}
		}()
	}
}
