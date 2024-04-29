package loaders

import (
	"github.com/Volume999/AsyncDB/asyncdb"
	"github.com/Volume999/AsyncDB/internal/tpcc/dataloaders"
	"github.com/Volume999/AsyncDB/internal/tpcc/models"
)

type AsyncDBLoader struct {
	db   *asyncdb.AsyncDB
	data *dataloaders.GeneratedData
}

func NewAsyncDBLoader(db *asyncdb.AsyncDB, data *dataloaders.GeneratedData) *AsyncDBLoader {
	return &AsyncDBLoader{db: db, data: data}
}

func (a *AsyncDBLoader) Load() {
	warehouses, _ := asyncdb.NewInMemoryTable[models.WarehousePK, models.Warehouse]("Warehouse")
	districts, _ := asyncdb.NewInMemoryTable[models.DistrictPK, models.District]("District")
	customers, _ := asyncdb.NewInMemoryTable[models.CustomerPK, models.Customer]("Customer")
	history, _ := asyncdb.NewInMemoryTable[models.HistoryPK, models.History]("History")
	newOrders, _ := asyncdb.NewInMemoryTable[models.NewOrderPK, models.NewOrder]("NewOrder")
	orders, _ := asyncdb.NewInMemoryTable[models.OrderPK, models.Order]("Order")
	orderLines, _ := asyncdb.NewInMemoryTable[models.OrderLinePK, models.OrderLine]("OrderLine")
	items, _ := asyncdb.NewInMemoryTable[models.ItemPK, models.Item]("Item")
	stocks, _ := asyncdb.NewInMemoryTable[models.StockPK, models.Stock]("Stock")
	tables := []asyncdb.Table{
		warehouses,
		districts,
		customers,
		history,
		newOrders,
		orders,
		orderLines,
		items,
		stocks,
	}
	ctx, _ := a.db.Connect()
	for _, table := range tables {
		_ = a.db.CreateTable(ctx, table)
	}
	asyncdb.LoadTable[models.WarehousePK, models.Warehouse]("Warehouse", a.data.Warehouses, tables[0].(*asyncdb.InMemoryTable[models.WarehousePK, models.Warehouse]))
	asyncdb.LoadTable[models.DistrictPK, models.District]("District", a.data.Districts, tables[1].(*asyncdb.InMemoryTable[models.DistrictPK, models.District]))
	asyncdb.LoadTable[models.CustomerPK, models.Customer]("Customer", a.data.Customers, tables[2].(*asyncdb.InMemoryTable[models.CustomerPK, models.Customer]))
	asyncdb.LoadTable[models.HistoryPK, models.History]("History", a.data.History, tables[3].(*asyncdb.InMemoryTable[models.HistoryPK, models.History]))
	asyncdb.LoadTable[models.NewOrderPK, models.NewOrder]("NewOrder", a.data.NewOrders, tables[4].(*asyncdb.InMemoryTable[models.NewOrderPK, models.NewOrder]))
	asyncdb.LoadTable[models.OrderPK, models.Order]("Order", a.data.Orders, tables[5].(*asyncdb.InMemoryTable[models.OrderPK, models.Order]))
	asyncdb.LoadTable[models.OrderLinePK, models.OrderLine]("OrderLine", a.data.OrderLines, tables[6].(*asyncdb.InMemoryTable[models.OrderLinePK, models.OrderLine]))
	asyncdb.LoadTable[models.ItemPK, models.Item]("Item", a.data.Items, tables[7].(*asyncdb.InMemoryTable[models.ItemPK, models.Item]))
	asyncdb.LoadTable[models.StockPK, models.Stock]("Stock", a.data.Stocks, tables[8].(*asyncdb.InMemoryTable[models.StockPK, models.Stock]))
	_ = a.db.Disconnect(ctx)
}
