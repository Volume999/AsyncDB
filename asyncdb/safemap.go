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

func (t *ThreadSafeMap[K, V]) Keys() []K {
	t.lock.RLock()
	defer t.lock.RUnlock()
	keys := make([]K, 0, len(t.m))
	for k := range t.m {
		keys = append(keys, k)
	}
	return keys
}

func (t *ThreadSafeMap[K, V]) Values() []V {
	t.lock.RLock()
	defer t.lock.RUnlock()
	values := make([]V, 0, len(t.m))
	for _, v := range t.m {
		values = append(values, v)
	}
	return values
}

func (t *ThreadSafeMap[K, V]) Get(key K) (value V, ok bool) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	value, ok = t.m[key]
	return value, ok
}

func (t *ThreadSafeMap[K, V]) Put(key K, value V) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.m[key] = value
}

func (t *ThreadSafeMap[K, V]) Delete(key K) {
	t.lock.Lock()
	defer t.lock.Unlock()
	delete(t.m, key)
}

func (t *ThreadSafeMap[K, V]) GetUnsafe(key K) (value V, ok bool) {
	v, ok := t.m[key]
	return v, ok
}

func (t *ThreadSafeMap[K, V]) PutUnsafe(key K, value V) {
	t.m[key] = value
}

func (t *ThreadSafeMap[K, V]) DeleteUnsafe(key K) {
	delete(t.m, key)
}
