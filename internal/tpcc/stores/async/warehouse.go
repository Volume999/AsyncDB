package async

import (
	"AsyncDB/internal/asyncdb"
	"AsyncDB/internal/databases"
	"AsyncDB/internal/tpcc/models"
	"log"
)

type WarehouseStore struct {
	l  *log.Logger
	db *asyncdb.AsyncDB
}

func (w WarehouseStore) Put(ctx *asyncdb.ConnectionContext, value models.Warehouse) <-chan databases.RequestResult {
	return w.db.Put(ctx, models.Warehouse{}, models.WarehousePK{Id: value.Id}, value)
}

func (w WarehouseStore) Get(ctx *asyncdb.ConnectionContext, key models.WarehousePK) <-chan databases.RequestResult {
	return w.db.Get(ctx, models.Warehouse{}, key)
}

func (w WarehouseStore) Delete(ctx *asyncdb.ConnectionContext, key models.WarehousePK) <-chan databases.RequestResult {
	return w.db.Delete(ctx, models.Warehouse{}, key)
}

func NewWarehouseStore(l *log.Logger, db *asyncdb.AsyncDB) Store[models.Warehouse, models.WarehousePK] {
	return &WarehouseStore{db: db, l: l}
}
