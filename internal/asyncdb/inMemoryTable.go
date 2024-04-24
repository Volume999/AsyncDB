package asyncdb

import "fmt"

type InMemoryTable[K comparable, V any] struct {
	name string
	data map[K]V
}

func NewInMemoryTable[K comparable, V any](name string) (*InMemoryTable[K, V], error) {
	if name == "" {
		return nil, ErrEmptyTableName
	}
	return &InMemoryTable[K, V]{
		name: name,
		data: make(map[K]V),
	}, nil
}

func (t *InMemoryTable[K, V]) Name() string {
	return t.name
}

func (t *InMemoryTable[K, V]) Get(key interface{}) (value interface{}, err error) {
	keyTyped, ok := key.(K)
	if !ok {
		return nil, fmt.Errorf("%w: expected key type - %T, got - %T", ErrTypeMismatch, *new(K), key)
	}
	v, ok := t.data[keyTyped]
	if !ok {
		return *new(V), fmt.Errorf("%w - %v", ErrKeyNotFound, key)
	}
	return v, nil
}

func (t *InMemoryTable[K, V]) Put(key interface{}, value interface{}) error {
	keyTyped, keyOk := key.(K)
	if !keyOk {
		return fmt.Errorf("%w: expected key type - %T, got - %T", ErrTypeMismatch, *new(K), key)
	}
	valueTyped, valueOk := value.(V)
	if !valueOk {
		return fmt.Errorf("%w: expected value type - %T, got - %T", ErrTypeMismatch, *new(V), value)
	}
	t.data[keyTyped] = valueTyped
	return nil
}

func (t *InMemoryTable[K, V]) Delete(key interface{}) error {
	keyTyped, ok := key.(K)
	if !ok {
		return fmt.Errorf("%w: %T", ErrTypeMismatch, key)
	}
	delete(t.data, keyTyped)
	return nil
}

func (t *InMemoryTable[K, V]) ValidateTypes(key interface{}, value interface{}) error {
	_, keyOk := key.(K)
	if !keyOk {
		return fmt.Errorf("%w: expected key type - %T, got - %T", ErrTypeMismatch, *new(K), key)
	}
	if value != nil {
		_, valueOk := value.(V)
		if !valueOk {
			return fmt.Errorf("%w: expected value type - %T, got - %T", ErrTypeMismatch, *new(V), value)
		}
	}
	return nil
}

func LoadTable[K comparable, V any](name string, data map[K]V, table *InMemoryTable[K, V]) {
	table.name = name
	table.data = data
}
