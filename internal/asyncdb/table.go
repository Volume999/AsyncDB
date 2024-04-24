package asyncdb

import (
	"errors"
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
