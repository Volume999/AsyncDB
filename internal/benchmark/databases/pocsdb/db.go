package pocsdb

import (
	"POCS_Projects/internal/benchmark/databases"
	"POCS_Projects/internal/benchmark/dataloaders"
)

type PocsDB struct {
	data dataloaders.GeneratedData
}

func NewPocsDB() *PocsDB {
	return &PocsDB{}
}

func (p PocsDB) LoadData(data dataloaders.GeneratedData) error {
	p.data = data
	return nil
}

func (p PocsDB) Connect() (*databases.ConnectionContext, error) {
	//TODO implement me
	panic("implement me")
}

func (p PocsDB) Disconnect(context *databases.ConnectionContext) error {
	//TODO implement me
	panic("implement me")
}

func (p PocsDB) Put(ctx *databases.ConnectionContext, key interface{}, value interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (p PocsDB) Get(ctx *databases.ConnectionContext, key interface{}) (interface{}, error) {
	//TODO implement me
	panic("implement me")
}

func (p PocsDB) Delete(ctx *databases.ConnectionContext, key interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (p PocsDB) BeginTransaction(ctx *databases.ConnectionContext) error {
	//TODO implement me
	panic("implement me")
}

func (p PocsDB) CommitTransaction(ctx *databases.ConnectionContext) error {
	//TODO implement me
	panic("implement me")
}

func (p PocsDB) RollbackTransaction(ctx *databases.ConnectionContext) error {
	//TODO implement me
	panic("implement me")
}
