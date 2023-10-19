package async

import (
	"POCS_Projects/internal/cmd/benchmark/databases"
	"POCS_Projects/internal/cmd/benchmark/databases/pocsdb"
	"POCS_Projects/internal/models"
	"log"
)

type WarehouseStore struct {
	l  *log.Logger
	db *pocsdb.PocsDB
}

func (w WarehouseStore) Put(ctx *databases.ConnectionContext, value models.Warehouse) <-chan databases.RequestResult {
	return w.db.Put(ctx, models.Warehouse{}, models.WarehousePK{Id: value.Id}, value)
}

func (w WarehouseStore) Get(ctx *databases.ConnectionContext, key models.WarehousePK) <-chan databases.RequestResult {
	return w.db.Get(ctx, models.Warehouse{}, key)
}

func (w WarehouseStore) Delete(ctx *databases.ConnectionContext, key models.WarehousePK) <-chan databases.RequestResult {
	return w.db.Delete(ctx, models.Warehouse{}, key)
}

func NewWarehouseStore(l *log.Logger, db *pocsdb.PocsDB) Store[models.Warehouse, models.WarehousePK] {
	return &WarehouseStore{db: db, l: l}
}
