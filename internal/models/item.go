package models

type Item struct {
	Id      int     `db:"I_ID"`
	ImageId int     `db:"I_IM_ID"`
	Name    string  `db:"I_NAME"`
	Price   float64 `db:"I_PRICE"`
	Data    string  `db:"I_DATA"`
}

type ItemPK struct {
	Id int `db:"I_ID"`
}
