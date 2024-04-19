package asyncdb

import (
	"errors"
	"fmt"
)

var ErrKeyNotFound = errors.New("key not found")
var ErrTypeMismatch = errors.New("type mismatch")
var ErrEmptyTableName = errors.New("table name cannot be empty")

type Table interface {
	Name() string
	Get(key interface{}) (value interface{}, err error)
	Put(key interface{}, value interface{}) error
	Delete(key interface{}) error
	ValidateTypes(key interface{}, value interface{}) error
}

type GenericTable[K comparable, V any] struct {
	name string
	data map[K]V
}

func NewGenericTable[K comparable, V any](name string) (*GenericTable[K, V], error) {
	if name == "" {
		return nil, ErrEmptyTableName
	}
	return &GenericTable[K, V]{
		name: name,
		data: make(map[K]V),
	}, nil
}

func (t *GenericTable[K, V]) Name() string {
	return t.name
}

func (t *GenericTable[K, V]) Get(key interface{}) (value interface{}, err error) {
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

func (t *GenericTable[K, V]) Put(key interface{}, value interface{}) error {
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

func (t *GenericTable[K, V]) Delete(key interface{}) error {
	keyTyped, ok := key.(K)
	if !ok {
		return fmt.Errorf("%w: %T", ErrTypeMismatch, key)
	}
	delete(t.data, keyTyped)
	return nil
}

func (t *GenericTable[K, V]) ValidateTypes(key interface{}, value interface{}) error {
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

func LoadTable[K comparable, V any](name string, data map[K]V, table *GenericTable[K, V]) {
	table.name = name
	table.data = data
}
