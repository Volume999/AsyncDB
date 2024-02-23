package async

import (
	"AsyncDB/internal/asyncdb"
	"AsyncDB/internal/databases"
	"AsyncDB/internal/tpcc/models"
	"log"
)

type StockStore struct {
	l  *log.Logger
	db *asyncdb.PocsDB
}

func NewStockStore(l *log.Logger, db *asyncdb.PocsDB) Store[models.Stock, models.StockPK] {
	return &StockStore{db: db, l: l}
}

func (s StockStore) Put(ctx *asyncdb.ConnectionContext, value models.Stock) <-chan databases.RequestResult {
	return s.db.Put(ctx, models.Stock{}, models.StockPK{ItemId: value.ItemId, WarehouseId: value.WarehouseId}, value)
}

func (s StockStore) Get(ctx *asyncdb.ConnectionContext, key models.StockPK) <-chan databases.RequestResult {
	return s.db.Get(ctx, models.Stock{}, key)
}

func (s StockStore) Delete(ctx *asyncdb.ConnectionContext, key models.StockPK) <-chan databases.RequestResult {
	return s.db.Delete(ctx, models.Stock{}, key)
}
