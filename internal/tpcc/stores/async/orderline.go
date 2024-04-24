package async

import (
	"AsyncDB/asyncdb"
	"AsyncDB/internal/databases"
	"AsyncDB/internal/tpcc/models"
	"log"
)

type OrderLineStore struct {
	l  *log.Logger
	db *asyncdb.AsyncDB
}

func NewOrderLineStore(l *log.Logger, db *asyncdb.AsyncDB) Store[models.OrderLine, models.OrderLinePK] {
	return &OrderLineStore{db: db, l: l}
}

func (o OrderLineStore) Put(ctx *asyncdb.ConnectionContext, value models.OrderLine) <-chan databases.RequestResult {
	return o.db.Put(ctx, "OrderLine", models.OrderLinePK{OrderId: value.OrderId,
		DistrictId: value.DistrictId, WarehouseId: value.WarehouseId, LineNumber: value.LineNumber}, value)
}

func (o OrderLineStore) Get(ctx *asyncdb.ConnectionContext, key models.OrderLinePK) <-chan databases.RequestResult {
	return o.db.Get(ctx, "OrderLine", key)
}

func (o OrderLineStore) Delete(ctx *asyncdb.ConnectionContext, key models.OrderLinePK) <-chan databases.RequestResult {
	return o.db.Delete(ctx, "OrderLine", key)
}
