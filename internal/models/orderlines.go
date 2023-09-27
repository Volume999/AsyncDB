package models

import "time"

type OrderLine struct {
	OrderId           int       `db:"OL_O_ID"`
	DistrictId        int       `db:"OL_D_ID"`
	WarehouseId       int       `db:"OL_W_ID"`
	LineNumber        int       `db:"OL_NUMBER"`
	ItemId            int       `db:"OL_I_ID"`
	SupplyWarehouseId int       `db:"OL_SUPPLY_W_ID"`
	DeliveryDate      time.Time `db:"OL_DELIVERY_D"`
	Quantity          int       `db:"OL_QUANTITY"`
	Amount            float64   `db:"OL_AMOUNT"`
	DistInfo          string    `db:"OL_DIST_INFO"`
}
