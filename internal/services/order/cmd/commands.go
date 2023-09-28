package cmd

type NewOrderCommand struct {
	WarehouseId int          `json:"warehouseId"`
	DistrictId  int          `json:"districtId"`
	CustomerId  int          `json:"customerId"`
	Items       []OrderItems `json:"items"`
}

type OrderItems struct {
	ItemId            int `json:"itemId"`
	Quantity          int `json:"quantity"`
	SupplyWarehouseId int `json:"supplyWarehouseId"`
}
