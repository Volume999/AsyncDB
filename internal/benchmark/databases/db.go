package databases

import (
	"errors"
	"github.com/google/uuid"
)

type Txn interface {
}

type ConnectionContext struct {
	ID  uuid.UUID
	Txn *Txn
}

type RequestResult struct {
	Data interface{}
	Err  error
}

type AsyncDatabase interface {
	Connect() (*ConnectionContext, error)
	Disconnect(*ConnectionContext) error
	Put(ctx *ConnectionContext, dataType interface{}, key interface{}, value interface{}) (resultChan <-chan RequestResult)
	Get(ctx *ConnectionContext, dataType interface{}, key interface{}) (resultChan <-chan RequestResult)
	Delete(ctx *ConnectionContext, dataType interface{}, key interface{}) (resultChan <-chan RequestResult)
	BeginTransaction(ctx *ConnectionContext) error
	CommitTransaction(ctx *ConnectionContext) error
	RollbackTransaction(ctx *ConnectionContext) error
}

var ErrKeyNotFound = errors.New("key not found")
