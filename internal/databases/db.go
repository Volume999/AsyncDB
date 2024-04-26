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

// TODO: This is not implemented by anyone
//type AsyncDatabase interface {
//	Connect() (*ConnectionContext, error)
//	Disconnect(context *ConnectionContext) error
//	GetTable(ctx *ConnectionContext, table asyncdb.Table) error
//	Put(ctx *ConnectionContext, tableName string, key interface{}, value interface{}) (resultChan <-chan RequestResult)
//	Get(ctx *ConnectionContext, tableName string, key interface{}) (resultChan <-chan RequestResult)
//	Delete(ctx *ConnectionContext, tableName string, key interface{}) (resultChan <-chan RequestResult)
//	StartTransaction(ctx *ConnectionContext) error
//	CommitTransaction(ctx *ConnectionContext) error
//	RollbackTransaction(ctx *ConnectionContext) error
//}

var ErrKeyNotFound = errors.New("key not found")
var ErrTableNotFound = errors.New("table not found")
