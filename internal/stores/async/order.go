package async

import (
	"POCS_Projects/internal/cmd/benchmark/databases"
	"POCS_Projects/internal/cmd/benchmark/databases/pocsdb"
	"POCS_Projects/internal/models"
	"log"
)

type OrderStore struct {
	l  *log.Logger
	db *pocsdb.PocsDB
}

func NewOrderStore(l *log.Logger, db *pocsdb.PocsDB) Store[models.Order, models.OrderPK] {
	return &OrderStore{db: db, l: l}
}

func (o OrderStore) Put(ctx *pocsdb.ConnectionContext, value models.Order) <-chan databases.RequestResult {
	return o.db.Put(ctx, models.Order{}, models.OrderPK{Id: value.Id, DistrictId: value.DistrictId, WarehouseId: value.WarehouseId}, value)
}

func (o OrderStore) Get(ctx *pocsdb.ConnectionContext, key models.OrderPK) <-chan databases.RequestResult {
	return o.db.Get(ctx, models.Order{}, key)
}

func (o OrderStore) Delete(ctx *pocsdb.ConnectionContext, key models.OrderPK) <-chan databases.RequestResult {
	return o.db.Delete(ctx, models.Order{}, key)
}
