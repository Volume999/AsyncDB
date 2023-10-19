package order

import "time"

type Command struct {
	WarehouseId int            `json:"warehouseId"`
	DistrictId  int            `json:"districtId"`
	CustomerId  int            `json:"customerId"`
	Items       []CommandItems `json:"items"`
}

type CommandItems struct {
	ItemId            int `json:"itemId"`
	Quantity          int `json:"quantity"`
	SupplyWarehouseId int `json:"supplyWarehouseId"`
}

type ResponseItems struct {
	SupplyWarehouseId int     `json:"supplyWarehouseId"`
	ItemId            int     `json:"itemId"`
	ItemName          string  `json:"itemName"`
	LineQuantity      int     `json:"lineQuantity"`
	StockQuantity     int     `json:"stockQuantity"`
	BrandGeneric      string  `json:"brandGeneric"`
	ItemPrice         float64 `json:"itemPrice"`
	Amount            float64 `json:"amount"`
}

type Response struct {
	WarehouseId      int             `json:"warehouseId"`
	DistrictId       int             `json:"districtId"`
	CustomerId       int             `json:"customerId"`
	CustomerLastName string          `json:"customerLastName"`
	CustomerCredit   string          `json:"customerCredit"`
	CustomerDiscount float64         `json:"customerDiscount"`
	WarehouseTax     float64         `json:"warehouseTax"`
	DistrictTax      float64         `json:"districtTax"`
	OrderLinesCount  int             `json:"orderLinesCount"`
	OrderId          int             `json:"orderId"`
	OrderEntryDate   time.Time       `json:"orderEntryDate"`
	TotalAmount      float64         `json:"totalAmount"`
	OrderLines       []ResponseItems `json:"orderLines"`
	ExecutionStatus  string          `json:"executionStatus"`
}
