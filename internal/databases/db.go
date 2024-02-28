package databases

import (
	"errors"
)

type Txn interface{}

type ConnectionContext interface{}

type RequestResult struct {
	Data interface{}
	Err  error
}

type AsyncDatabase interface {
	Connect() (*ConnectionContext, error)
	Disconnect(*ConnectionContext) error
	// TODO: Figure out DDL API
	CreateTable(ctx *ConnectionContext, dataType interface{}) error
	Put(ctx *ConnectionContext, dataType interface{}, key interface{}, value interface{}) (resultChan <-chan RequestResult)
	Get(ctx *ConnectionContext, dataType interface{}, key interface{}) (resultChan <-chan RequestResult)
	Delete(ctx *ConnectionContext, dataType interface{}, key interface{}) (resultChan <-chan RequestResult)
	BeginTransaction(ctx *ConnectionContext) error
	CommitTransaction(ctx *ConnectionContext) error
	RollbackTransaction(ctx *ConnectionContext) error
}

var ErrKeyNotFound = errors.New("key not found")
var ErrTableNotFound = errors.New("table not found")
