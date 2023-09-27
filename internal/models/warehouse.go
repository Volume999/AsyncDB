package models

type Warehouse struct {
	Id      int     `db:"W_ID"`
	Name    string  `db:"W_NAME"`
	Street1 string  `db:"W_STREET_1"`
	Street2 string  `db:"W_STREET_2"`
	City    string  `db:"W_CITY"`
	State   string  `db:"W_STATE"`
	Zip     string  `db:"W_ZIP"`
	Tax     float64 `db:"W_TAX"`
	Ytd     float64 `db:"W_YTD"`
}
