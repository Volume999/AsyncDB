package loaders

import (
	"AsyncDB/internal/asyncdb"
	"AsyncDB/internal/tpcc/dataloaders"
	"AsyncDB/internal/tpcc/models"
)

type AsyncDBLoader struct {
	db   *asyncdb.AsyncDB
	data *dataloaders.GeneratedData
}

func NewAsyncDBLoader(db *asyncdb.AsyncDB, data *dataloaders.GeneratedData) *AsyncDBLoader {
	return &AsyncDBLoader{db: db, data: data}
}

func (a *AsyncDBLoader) Load() {
	warehouses, _ := asyncdb.NewGenericTable[models.WarehousePK, models.Warehouse]("Warehouse")
	districts, _ := asyncdb.NewGenericTable[models.DistrictPK, models.District]("District")
	customers, _ := asyncdb.NewGenericTable[models.CustomerPK, models.Customer]("Customer")
	history, _ := asyncdb.NewGenericTable[models.HistoryPK, models.History]("History")
	newOrders, _ := asyncdb.NewGenericTable[models.NewOrderPK, models.NewOrder]("NewOrder")
	orders, _ := asyncdb.NewGenericTable[models.OrderPK, models.Order]("Order")
	orderLines, _ := asyncdb.NewGenericTable[models.OrderLinePK, models.OrderLine]("OrderLine")
	items, _ := asyncdb.NewGenericTable[models.ItemPK, models.Item]("Item")
	stocks, _ := asyncdb.NewGenericTable[models.StockPK, models.Stock]("Stock")
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
	asyncdb.LoadTable[models.WarehousePK, models.Warehouse]("Warehouse", a.data.Warehouses, tables[0].(*asyncdb.GenericTable[models.WarehousePK, models.Warehouse]))
	asyncdb.LoadTable[models.DistrictPK, models.District]("District", a.data.Districts, tables[1].(*asyncdb.GenericTable[models.DistrictPK, models.District]))
	asyncdb.LoadTable[models.CustomerPK, models.Customer]("Customer", a.data.Customers, tables[2].(*asyncdb.GenericTable[models.CustomerPK, models.Customer]))
	asyncdb.LoadTable[models.HistoryPK, models.History]("History", a.data.History, tables[3].(*asyncdb.GenericTable[models.HistoryPK, models.History]))
	asyncdb.LoadTable[models.NewOrderPK, models.NewOrder]("NewOrder", a.data.NewOrders, tables[4].(*asyncdb.GenericTable[models.NewOrderPK, models.NewOrder]))
	asyncdb.LoadTable[models.OrderPK, models.Order]("Order", a.data.Orders, tables[5].(*asyncdb.GenericTable[models.OrderPK, models.Order]))
	asyncdb.LoadTable[models.OrderLinePK, models.OrderLine]("OrderLine", a.data.OrderLines, tables[6].(*asyncdb.GenericTable[models.OrderLinePK, models.OrderLine]))
	asyncdb.LoadTable[models.ItemPK, models.Item]("Item", a.data.Items, tables[7].(*asyncdb.GenericTable[models.ItemPK, models.Item]))
	asyncdb.LoadTable[models.StockPK, models.Stock]("Stock", a.data.Stocks, tables[8].(*asyncdb.GenericTable[models.StockPK, models.Stock]))
	_ = a.db.Disconnect(ctx)
}
