package main

import (
	"errors"
	"fmt"
	"github.com/Volume999/AsyncDB/asyncdb"
	"github.com/Volume999/AsyncDB/internal/databases"
	"github.com/Volume999/AsyncDB/internal/tpcc/config"
	"github.com/Volume999/AsyncDB/internal/tpcc/dataloaders"
	"github.com/Volume999/AsyncDB/internal/tpcc/dataloaders/loaders"
	"github.com/Volume999/AsyncDB/internal/tpcc/models"
	"github.com/Volume999/AsyncDB/internal/tpcc/services/order"
	async2 "github.com/Volume999/AsyncDB/internal/tpcc/stores/async"
	"github.com/kr/pretty"
	"sync"
	"time"
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

func SetupAsyncDBWorkflow(db *asyncdb.AsyncDB, pgFactory *asyncdb.PgTableFactory, keys int) error {
	tables := []string{"Orders", "Items", "StockKeepingUnits", "Customers", "ItemOffers", "OrderPayments", "ItemOptions", "CustomerOffersUsage", "TaxProviders", "OrderTaxes"}
	ctx, _ := db.Connect()
	for _, table := range tables {
		tbl, err := pgFactory.GetTable(table)
		if err != nil {
			return err
		}
		for i := 0; i <= keys; i++ {
			err = tbl.Put(i, "value")
			if err != nil {
				return err
			}
		}
		err = db.CreateTable(ctx, tbl)
		if err != nil {
			return err
		}
	}
	err := db.Disconnect(ctx)
	return err
}
func withTransaction(db *asyncdb.AsyncDB, ctx *asyncdb.ConnectionContext, idx int, workflow func() error) {
	err := db.BeginTransaction(ctx)
	if idx == -1 {
		ctx.Txn.SetTimestamp(0)
	}
	fmt.Printf("Transaction %v Started\n", idx)
	if err != nil {
		panic("Failed to begin transaction: " + err.Error())
	}
	abortCount := 0
	err = workflow()
	for err != nil {
		abortCount++
		fmt.Printf("Transaction aborted, idx: %v, count: %d, error: %v\n", idx, abortCount, err.Error())
		time.Sleep(1 * time.Second)
		err = workflow()
	}
	err = db.CommitTransaction(ctx)
	if err != nil {
		panic("Failed to commit transaction: " + err.Error())
	}
	fmt.Println("Transaction aborted count: ", abortCount)
}

func executeWorkflow(db *asyncdb.AsyncDB, idx int) {
	ctx, _ := db.Connect()
	withTransaction(db, ctx, idx, func() error {
		resChan := make(chan databases.RequestResult, 10)
		for i := 0; i < 10; i++ {
			go func() {
				resChan <- <-db.Put(ctx, "Orders", i, "value")
			}()
		}
		var err error
		for i := 0; i < 10; i++ {
			res := <-resChan
			err = errors.Join(err, res.Err)
		}
		return err
	})
}

func debugAsyncDBWorkflow() {
	lm := asyncdb.NewLockManager()
	tm := asyncdb.NewTransactionManager()
	h := asyncdb.NewStringHasher()
	db := asyncdb.NewAsyncDB(tm, lm, h, asyncdb.WithExplicitTxn())
	connString := "postgres://postgres:secret@localhost:5432/postgres"
	pgFactory, err := asyncdb.NewPgTableFactory(connString)
	if err != nil {
		panic("Failed to create PgTableFactory: " + err.Error())
	}
	if err = SetupAsyncDBWorkflow(db, pgFactory, 10); err != nil {
		panic("Failed to setup AsyncDB workflow: " + err.Error())
	}
	wg := sync.WaitGroup{}
	workflowCount := 10
	wg.Add(workflowCount)
	go func() {
		defer wg.Done()
		executeWorkflow(db, -1)
	}()
	for i := range workflowCount - 1 {
		go func() {
			defer wg.Done()
			executeWorkflow(db, i)
		}()
	}
	wg.Wait()
}

func main() {
	debugAsyncDBWorkflow()
}
