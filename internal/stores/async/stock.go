package async

import (
	"POCS_Projects/internal/benchmark/databases"
	"POCS_Projects/internal/benchmark/databases/pocsdb"
	"POCS_Projects/internal/models"
	"log"
)

type StockStore struct {
	l  *log.Logger
	db *pocsdb.PocsDB
}

func NewStockStore(l *log.Logger, db *pocsdb.PocsDB) Store[models.Stock, models.StockPK] {
	return &StockStore{db: db, l: l}
}

func (s StockStore) Put(ctx *databases.ConnectionContext, value models.Stock) <-chan databases.RequestResult {
	return s.db.Put(ctx, models.Stock{}, models.StockPK{ItemId: value.ItemId, WarehouseId: value.WarehouseId}, value)
}

func (s StockStore) Get(ctx *databases.ConnectionContext, key models.StockPK) <-chan databases.RequestResult {
	return s.db.Get(ctx, models.Stock{}, key)
}

func (s StockStore) Delete(ctx *databases.ConnectionContext, key models.StockPK) <-chan databases.RequestResult {
	return s.db.Delete(ctx, models.Stock{}, key)
}
