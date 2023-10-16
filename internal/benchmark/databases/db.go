package databases

import "github.com/google/uuid"

type ConnectionContext struct {
	ID uuid.UUID
}

type Database interface {
	Connect() (*ConnectionContext, error)
	Disconnect(*ConnectionContext) error
	Put(ctx *ConnectionContext, key interface{}, value interface{}) error
	Get(ctx *ConnectionContext, key interface{}) (interface{}, error)
	Delete(ctx *ConnectionContext, key interface{}) error
	BeginTransaction(ctx *ConnectionContext) error
	CommitTransaction(ctx *ConnectionContext) error
	RollbackTransaction(ctx *ConnectionContext) error
}
