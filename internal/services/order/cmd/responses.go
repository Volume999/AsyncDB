package cmd

import "time"

type NewOrderResponse struct {
	WarehouseId      int         `json:"warehouseId"`
	DistrictId       int         `json:"districtId"`
	CustomerId       int         `json:"customerId"`
	CustomerLastName string      `json:"customerLastName"`
	CustomerCredit   string      `json:"customerCredit"`
	CustomerDiscount float64     `json:"customerDiscount"`
	WarehouseTax     float64     `json:"warehouseTax"`
	DistrictTax      float64     `json:"districtTax"`
	OrderLinesCount  int         `json:"orderLinesCount"`
	OrderId          int         `json:"orderId"`
	OrderEntryDate   time.Time   `json:"orderEntryDate"`
	TotalAmount      float64     `json:"totalAmount"`
	OrderLines       []OrderLine `json:"orderLines"`
	ExecutionStatus  string      `json:"executionStatus"`
}

type OrderLine struct {
	SupplyWarehouseId int     `json:"supplyWarehouseId"`
	ItemId            int     `json:"itemId"`
	ItemName          string  `json:"itemName"`
	LineQuantity      int     `json:"lineQuantity"`
	StockQuantity     int     `json:"stockQuantity"`
	BrandGeneric      string  `json:"brandGeneric"`
	ItemPrice         float64 `json:"itemPrice"`
	Amount            float64 `json:"amount"`
}
