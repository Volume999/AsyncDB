package main

import (
	"AsyncDB/internal/asyncdb"
	"AsyncDB/internal/asyncdb/config"
	"AsyncDB/internal/tpcc/dataloaders"
	"AsyncDB/internal/tpcc/models"
	"AsyncDB/internal/tpcc/services/order"
	async2 "AsyncDB/internal/tpcc/stores/async"
	"fmt"
	"github.com/kr/pretty"
)

func debug() {
	// Try generating data
	constants := config.NewConstants()
	data, _ := dataloaders.NewDataGeneratorImpl(1, constants, nil).GenerateData()
	fmt.Println("Data generated successfully!")
	fmt.Println("Warehouses: ", len(data.Warehouses))
	fmt.Println("Customers: ", len(data.Customers))
	fmt.Println("Items: ", len(data.Items))
	fmt.Println("Stocks: ", len(data.Stocks))
	fmt.Println("Orders: ", len(data.Orders))
	fmt.Println("OrderLines: ", len(data.OrderLines))
	fmt.Println("NewOrders: ", len(data.NewOrders))
	fmt.Println("History: ", len(data.History))
	fmt.Println("Districts: ", len(data.Districts))

	tm := asyncdb.NewTransactionManager()
	lm := asyncdb.NewLockManager()
	db := asyncdb.NewAsyncDB(tm, lm)
	_ = db.LoadData(data)
	ctx, _ := db.Connect()

	fmt.Println("DB connected successfully!")
	fmt.Printf("Connection Context: %# v\n", pretty.Formatter(ctx))

	// Try putting
	resChan := db.Put(ctx, models.Item{}, models.ItemPK{Id: 1}, models.Item{Name: "Item 1", Price: 1000, Data: "Data 1"})
	res := <-resChan
	fmt.Printf("Result: %# v\n", pretty.Formatter(res))

	// Try getting
	resChan = db.Get(ctx, models.Item{}, models.ItemPK{Id: 1})
	res = <-resChan
	fmt.Printf("Result: %# v\n", pretty.Formatter(res))

	// Try deleting
	resChan = db.Delete(ctx, models.Item{}, models.ItemPK{Id: 1})
	res = <-resChan
	fmt.Printf("Result: %# v\n", pretty.Formatter(res))

	// Try getting again
	resChan = db.Get(ctx, models.Item{}, models.ItemPK{Id: 1})
	res = <-resChan
	fmt.Printf("Result: %# v\n", pretty.Formatter(res))

	// Customer Store
	customerStore := async2.NewCustomerStore(nil, db)
	resChan = customerStore.Put(ctx, models.Customer{ID: 1, DistrictId: 1, WarehouseId: 1})
	res = <-resChan
	fmt.Printf("Result: %# v\n", pretty.Formatter(res))
	resChan = customerStore.Get(ctx, models.CustomerPK{ID: 1})
	res = <-resChan
	fmt.Printf("Result: %# v\n", pretty.Formatter(res))

	// New Order Service
	stores := async2.Stores{
		Stock:     async2.NewStockStore(nil, db),
		Item:      async2.NewItemStore(nil, db),
		Customer:  async2.NewCustomerStore(nil, db),
		District:  async2.NewDiscrictStore(nil, db),
		Warehouse: async2.NewWarehouseStore(nil, db),
		Order:     async2.NewOrderStore(nil, db),
		OrderLine: async2.NewOrderLineStore(nil, db),
		History:   async2.NewHistoryStore(nil, db),
		NewOrder:  async2.NewNOrderStore(nil, db),
	}
	orderService := order.NewMonoService(nil, db, stores)
	ord := orderService.CreateOrder(order.Command{
		WarehouseId: 1,
		DistrictId:  1,
		CustomerId:  5,
		Items: []order.CommandItems{
			{
				ItemId:            2,
				SupplyWarehouseId: 2,
				Quantity:          2,
			},
		},
	})
	fmt.Printf("Result: %# v\n", pretty.Formatter(ord))
}

func main() {
	debug()
}
