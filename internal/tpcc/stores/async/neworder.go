package async

import (
	"github.com/Volume999/AsyncDB/asyncdb"
	"github.com/Volume999/AsyncDB/internal/databases"
	"github.com/Volume999/AsyncDB/internal/tpcc/models"
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
	return n.db.Put(ctx, "NewOrder", models.NewOrderPK{OrderId: value.OrderId, DistrictId: value.DistrictId, WarehouseId: value.WarehouseId}, value)
}

func (n *NOrderStore) Get(ctx *asyncdb.ConnectionContext, key models.NewOrderPK) <-chan databases.RequestResult {
	return n.db.Get(ctx, "NewOrder", key)
}

func (n *NOrderStore) Delete(ctx *asyncdb.ConnectionContext, key models.NewOrderPK) <-chan databases.RequestResult {
	return n.db.Delete(ctx, "NewOrder", key)
}
