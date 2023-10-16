package models

import "time"

type History struct {
	CustomerID          int       `db:"H_C_ID"`
	CustomerDistrictId  int       `db:"H_C_D_ID"`
	CustomerWarehouseId int       `db:"H_C_W_ID"`
	DistrictID          int       `db:"H_D_ID"`
	WarehouseID         int       `db:"H_W_ID"`
	Date                time.Time `db:"H_DATE"`
	Amount              float64   `db:"H_AMOUNT"`
	Data                string    `db:"H_DATA"`
}

type HistoryPK struct {
	HistoryId int `db:"H_ID"`
}
