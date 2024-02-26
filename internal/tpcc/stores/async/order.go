package async

import (
	"AsyncDB/internal/asyncdb"
	"AsyncDB/internal/databases"
	"AsyncDB/internal/tpcc/models"
	"log"
)

type OrderStore struct {
	l  *log.Logger
	db *asyncdb.AsyncDB
}

func NewOrderStore(l *log.Logger, db *asyncdb.AsyncDB) Store[models.Order, models.OrderPK] {
	return &OrderStore{db: db, l: l}
}

func (o OrderStore) Put(ctx *asyncdb.ConnectionContext, value models.Order) <-chan databases.RequestResult {
	return o.db.Put(ctx, models.Order{}, models.OrderPK{Id: value.Id, DistrictId: value.DistrictId, WarehouseId: value.WarehouseId}, value)
}

func (o OrderStore) Get(ctx *asyncdb.ConnectionContext, key models.OrderPK) <-chan databases.RequestResult {
	return o.db.Get(ctx, models.Order{}, key)
}

func (o OrderStore) Delete(ctx *asyncdb.ConnectionContext, key models.OrderPK) <-chan databases.RequestResult {
	return o.db.Delete(ctx, models.Order{}, key)
}
