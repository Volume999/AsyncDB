package main

import (
	"POCS_Projects/internal/benchmark"
	"POCS_Projects/internal/benchmark/databases/pocsdb"
	"POCS_Projects/internal/benchmark/dataloaders"
	"fmt"
	"github.com/kr/pretty"
)

const (
	configPath = "./internal/config/"
)

func main() {
	// Try generating data
	constants := benchmark.NewConstants()
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
	fmt.Printf("Connection Context: %# v", pretty.Formatter(ctx))
}
