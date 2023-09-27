package models

import "time"

type Order struct {
	Id            int       `db:"O_ID"`
	DistrictId    int       `db:"O_D_ID"`
	WarehouseId   int       `db:"O_W_ID"`
	CustomerId    int       `db:"O_C_ID"`
	EntryDate     time.Time `db:"O_ENTRY_D"`
	CarrierId     int       `db:"O_CARRIER_ID"`
	OrderLinesCnt int       `db:"O_OL_CNT"`
	AllLocal      int       `db:"O_ALL_LOCAL"`
}
