package async

import (
	"POCS_Projects/internal/benchmark/databases"
	"POCS_Projects/internal/benchmark/databases/pocsdb"
	"POCS_Projects/internal/models"
	"log"
)

type OrderLine struct {
	l  *log.Logger
	db *pocsdb.PocsDB
}

func NewOrderLine(l *log.Logger, db *pocsdb.PocsDB) Store[models.OrderLine, models.OrderLinePK] {
	return &OrderLine{db: db, l: l}
}

func (o OrderLine) Put(ctx *databases.ConnectionContext, value models.OrderLine) <-chan databases.RequestResult {
	return o.db.Put(ctx, models.OrderLine{}, models.OrderLinePK{OrderId: value.OrderId,
		DistrictId: value.DistrictId, WarehouseId: value.WarehouseId, LineNumber: value.LineNumber}, value)
}

func (o OrderLine) Get(ctx *databases.ConnectionContext, key models.OrderLinePK) <-chan databases.RequestResult {
	return o.db.Get(ctx, models.OrderLine{}, key)
}

func (o OrderLine) Delete(ctx *databases.ConnectionContext, key models.OrderLinePK) <-chan databases.RequestResult {
	return o.db.Delete(ctx, models.OrderLine{}, key)
}
