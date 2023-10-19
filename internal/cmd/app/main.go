package main

import (
	commands2 "POCS_Projects/internal/cmd"
	"POCS_Projects/internal/cmd/benchmark/databases/pocsdb"
	"POCS_Projects/internal/cmd/benchmark/dataloaders"
	"POCS_Projects/internal/models"
	"POCS_Projects/internal/services/order"
	"POCS_Projects/internal/stores/async"
	"fmt"
	"github.com/kr/pretty"
)

const (
	configPath = "./internal/config/"
)

func main() {
	// Try generating data
	constants := commands2.NewConstants()
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

	db := pocsdb.NewPocsDB()
	_ = db.LoadData(data)
	ctx, _ := db.Connect()

	fmt.Println("DB connected successfully!")
	fmt.Printf("Connection Context: %# v\n", pretty.Formatter(ctx))

	// Try putting
	resChan := db.Put(ctx, models.Item{}, models.ItemPK{Id: 2}, models.Item{})
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
	customerStore := async.NewCustomerStore(nil, db)
	resChan = customerStore.Put(ctx, models.Customer{ID: 1, DistrictId: 1, WarehouseId: 1})
	res = <-resChan
	fmt.Printf("Result: %# v\n", pretty.Formatter(res))
	resChan = customerStore.Get(ctx, models.CustomerPK{ID: 1})
	res = <-resChan
	fmt.Printf("Result: %# v\n", pretty.Formatter(res))

	// New Order Service
	stores := async.Stores{
		Stock:     async.NewStockStore(nil, db),
		Item:      async.NewItemStore(nil, db),
		Customer:  async.NewCustomerStore(nil, db),
		District:  async.NewDiscrictStore(nil, db),
		Warehouse: async.NewWarehouseStore(nil, db),
		Order:     async.NewOrderStore(nil, db),
		OrderLine: async.NewOrderLineStore(nil, db),
		History:   async.NewHistoryStore(nil, db),
		NewOrder:  async.NewNOrderStore(nil, db),
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
