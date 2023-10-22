package async

import (
	"POCS_Projects/internal/cmd/benchmark/databases"
	"POCS_Projects/internal/cmd/benchmark/databases/pocsdb"
	"POCS_Projects/internal/models"
	"log"
)

type NOrderStore struct {
	l  *log.Logger
	db *pocsdb.PocsDB
}

func NewNOrderStore(l *log.Logger, db *pocsdb.PocsDB) Store[models.NewOrder, models.NewOrderPK] {
	return &NOrderStore{db: db, l: l}
}

func (n *NOrderStore) Put(ctx *pocsdb.ConnectionContext, value models.NewOrder) <-chan databases.RequestResult {
	return n.db.Put(ctx, models.NewOrder{}, models.NewOrderPK{OrderId: value.OrderId, DistrictId: value.DistrictId, WarehouseId: value.WarehouseId}, value)
}

func (n *NOrderStore) Get(ctx *pocsdb.ConnectionContext, key models.NewOrderPK) <-chan databases.RequestResult {
	return n.db.Get(ctx, models.NewOrder{}, key)
}

func (n *NOrderStore) Delete(ctx *pocsdb.ConnectionContext, key models.NewOrderPK) <-chan databases.RequestResult {
	return n.db.Delete(ctx, models.NewOrder{}, key)
}
