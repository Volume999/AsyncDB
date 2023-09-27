package models

type NewOrder struct {
	OrderId     int `db:"NO_O_ID"`
	DistrictId  int `db:"NO_D_ID"`
	WarehouseId int `db:"NO_W_ID"`
}
