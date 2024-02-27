package asyncdb

import (
	"errors"
	"fmt"
)

var ErrKeyNotFound = errors.New("key not found")
var ErrTypeMismatch = errors.New("type mismatch")

type Table interface {
	Hash() uint64
	Get(key interface{}) (value interface{}, err error)
	Put(key interface{}, value interface{}) error
}

type GenericTable[K comparable, V any] struct {
	name string
	data map[K]V
}

func NewGenericTable[K comparable, V any](name string) *GenericTable[K, V] {
	return &GenericTable[K, V]{
		name: name,
		data: make(map[K]V),
	}
}

func (t *GenericTable[K, V]) Hash() uint64 {
	return HashStringUint64(t.name)
}

func (t *GenericTable[K, V]) Get(key interface{}) (value interface{}, err error) {
	keyTyped, ok := key.(K)
	if !ok {
		return nil, fmt.Errorf("%w: %T", ErrTypeMismatch, key)
	}
	v, ok := t.data[keyTyped]
	if !ok {
		return *new(V), ErrKeyNotFound
	}
	return v, nil
}

func (t *GenericTable[K, V]) Put(key interface{}, value interface{}) error {
	keyTyped, keyOk := key.(K)
	valueTyped, valueOk := value.(V)
	if !keyOk || !valueOk {
		return fmt.Errorf("%w: key - %T, value - %T", ErrTypeMismatch, key, value)
	}
	t.data[keyTyped] = valueTyped
	return nil
}