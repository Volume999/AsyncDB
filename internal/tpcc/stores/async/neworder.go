package async

import (
	"AsyncDB/internal/asyncdb"
	"AsyncDB/internal/databases"
	"AsyncDB/internal/tpcc/models"
	"log"
)

type NOrderStore struct {
	l  *log.Logger
	db *asyncdb.AsyncDB
}

func NewNOrderStore(l *log.Logger, db *asyncdb.AsyncDB) Store[models.NewOrder, models.NewOrderPK] {
	return &NOrderStore{db: db, l: l}
}

func (n *NOrderStore) Put(ctx *asyncdb.ConnectionContext, value models.NewOrder) <-chan databases.RequestResult {
	return n.db.Put(ctx, models.NewOrder{}, models.NewOrderPK{OrderId: value.OrderId, DistrictId: value.DistrictId, WarehouseId: value.WarehouseId}, value)
}

func (n *NOrderStore) Get(ctx *asyncdb.ConnectionContext, key models.NewOrderPK) <-chan databases.RequestResult {
	return n.db.Get(ctx, models.NewOrder{}, key)
}

func (n *NOrderStore) Delete(ctx *asyncdb.ConnectionContext, key models.NewOrderPK) <-chan databases.RequestResult {
	return n.db.Delete(ctx, models.NewOrder{}, key)
}
