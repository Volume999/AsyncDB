package asyncdb

import "sync"

type ThreadSafeMap[K comparable, V any] struct {
	m    map[K]V
	lock *sync.RWMutex
}

func NewThreadSafeMap[K comparable, V any]() *ThreadSafeMap[K, V] {
	return &ThreadSafeMap[K, V]{
		m:    make(map[K]V),
		lock: &sync.RWMutex{},
	}
}

func (t *ThreadSafeMap[K, V]) Lock() {
	t.lock.Lock()
}

func (t *ThreadSafeMap[K, V]) Unlock() {
	t.lock.Unlock()
}

func (t *ThreadSafeMap[K, V]) Get(key K, unsafe bool) (value V, ok bool) {
	if !unsafe {
		t.lock.RLock()
		defer t.lock.RUnlock()
	}
	value, ok = t.m[key]
	return value, ok
}

func (t *ThreadSafeMap[K, V]) Put(key K, value V, unsafe bool) {
	if !unsafe {
		t.lock.Lock()
		defer t.lock.Unlock()
	}
	t.m[key] = value
}

func (t *ThreadSafeMap[K, V]) Delete(key K, unsafe bool) {
	if !unsafe {
		t.lock.Lock()
		defer t.lock.Unlock()
	}
	delete(t.m, key)
}
