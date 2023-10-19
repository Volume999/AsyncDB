package async

import (
	"POCS_Projects/internal/cmd/benchmark/databases"
	"POCS_Projects/internal/models"
)

type Store[V any, K any] interface {
	Put(ctx *databases.ConnectionContext, value V) <-chan databases.RequestResult
	Get(ctx *databases.ConnectionContext, key K) <-chan databases.RequestResult
	Delete(ctx *databases.ConnectionContext, key K) <-chan databases.RequestResult
}

type Stores struct {
	Stock     Store[models.Stock, models.StockPK]
	Item      Store[models.Item, models.ItemPK]
	Customer  Store[models.Customer, models.CustomerPK]
	District  Store[models.District, models.DistrictPK]
	Warehouse Store[models.Warehouse, models.WarehousePK]
	Order     Store[models.Order, models.OrderPK]
	OrderLine Store[models.OrderLine, models.OrderLinePK]
	History   Store[models.History, models.HistoryPK]
	NewOrder  Store[models.NewOrder, models.NewOrderPK]
}
