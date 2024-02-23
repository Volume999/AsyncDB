package models

type District struct {
	Id          int     `db:"D_ID"`
	WarehouseId int     `db:"D_W_ID"`
	Name        string  `db:"D_NAME"`
	Street1     string  `db:"D_STREET_1"`
	Street2     string  `db:"D_STREET_2"`
	City        string  `db:"D_CITY"`
	State       string  `db:"D_STATE"`
	Zip         string  `db:"D_ZIP"`
	Tax         float64 `db:"D_TAX"`
	Ytd         float64 `db:"D_YTD"`
	NextOId     int     `db:"D_NEXT_O_ID"`
}

type DistrictPK struct {
	Id          int `db:"D_ID"`
	WarehouseId int `db:"D_W_ID"`
}
