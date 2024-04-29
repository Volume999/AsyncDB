package main

import (
	"fmt"
	"github.com/Volume999/AsyncDB/asyncdb"
	"github.com/Volume999/AsyncDB/internal/tpcc/config"
	"github.com/Volume999/AsyncDB/internal/tpcc/dataloaders"
	"github.com/Volume999/AsyncDB/internal/tpcc/dataloaders/loaders"
	"github.com/Volume999/AsyncDB/internal/tpcc/models"
	"github.com/Volume999/AsyncDB/internal/tpcc/services/order"
	async2 "github.com/Volume999/AsyncDB/internal/tpcc/stores/async"
	"github.com/kr/pretty"
)

func debugPgTable() {
	factory, _ := asyncdb.NewPgTableFactory("postgres://postgres:secret@localhost:5432/postgres")
	defer factory.Close()
	_ = factory.DeleteTable("test_table")
	table, err := factory.GetTable("test_table")
	if err != nil {
		panic(err)
	}
	fmt.Println("Table created successfully!")
	fmt.Println("Table Name: ", table.Name())
	err = table.ValidateTypes("1", "test")
	if err == nil {
		fmt.Println("Types validated successfully!")
	} else {
		fmt.Println(err)
	}
	// Get non-existent value from DB
	val, err := table.Get("1")
	fmt.Println("Non-existent Get Results:", val, err)

	// Put value to DB
	err = table.Put("1", "test")
	fmt.Println("Put 'test' Results:", err)

	// Check if value is updated
	val, err = table.Get("1")
	fmt.Println("Get 'test' Results:", val, err)

	// Check if updates using PUT work
	err = table.Put("1", "Test2")
	fmt.Println("Put 'Test2' Results:", err)

	// Check if value is updated
	val, err = table.Get("1")
	fmt.Println("Get 'Test2' Results:", val, err)

	// Delete value from DB
	err = table.Delete("1")
	fmt.Println("Delete Results:", err)

	// Check if value is deleted
	val, err = table.Get("1")
	fmt.Println("Get after Delete Results:", val, err)

	// Delete non-existent value from DB
	err = table.Delete("2")
	fmt.Println("Delete non-existent Results:", err)

	// Insert a composite key
	key := struct {
		Id   int
		Name string
	}{Id: 1, Name: "Test"}
	err = table.Put(key, "Test")
	fmt.Println("Composite key:", fmt.Sprintf("%v", key))
	fmt.Println("Put composite key Results:", err)

	val, err = table.Get(key)
	fmt.Println("Get composite key Results:", val, err)

	tm := asyncdb.NewTransactionManager()
	lm := asyncdb.NewLockManager()
	h := asyncdb.NewStringHasher()
	db := asyncdb.NewAsyncDB(tm, lm, h)
	_, _ = db.Connect()
}

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
	h := asyncdb.NewStringHasher()
	db := asyncdb.NewAsyncDB(tm, lm, h)
	ctx, _ := db.Connect()
	loader := loaders.NewAsyncDBLoader(db, data)
	loader.Load()

	fmt.Println("DB connected successfully!")
	fmt.Printf("Connection Context: %# v\n", pretty.Formatter(ctx))

	// Try putting
	resChan := db.Put(ctx, "Item", models.ItemPK{Id: 1}, models.Item{Name: "Item 1", Price: 1000, Data: "Data 1"})
	res := <-resChan
	fmt.Printf("Result: %# v\n", pretty.Formatter(res))

	// Try getting
	//resChan = db.Get(ctx, "Item", models.ItemPK{Id: 1})
	//res = <-resChan
	//fmt.Printf("Result: %# v\n", pretty.Formatter(res))

	// Try deleting
	resChan = db.Delete(ctx, "Item", models.ItemPK{Id: 1})
	res = <-resChan
	fmt.Printf("Result: %# v\n", pretty.Formatter(res))

	// Try getting again
	//resChan = db.Get(ctx, "Item", models.ItemPK{Id: 1})
	//res = <-resChan
	//fmt.Printf("Result: %# v\n", pretty.Formatter(res))

	// Customer Store
	customerStore := async2.NewCustomerStore(nil, db)
	resChan = customerStore.Put(ctx, models.Customer{ID: 1, DistrictId: 1, WarehouseId: 1})
	res = <-resChan
	fmt.Printf("Result: %# v\n", pretty.Formatter(res))
	resChan = customerStore.Get(ctx, models.CustomerPK{ID: 1, DistrictId: 1, WarehouseId: 1})
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
				SupplyWarehouseId: 1,
				Quantity:          2,
			},
		},
	})
	fmt.Printf("Result: %# v\n", pretty.Formatter(ord))
}

func main() {
	debugPgTable()
}
