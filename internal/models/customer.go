package models

import "time"

type Customer struct {
	ID          int       `db:"C_ID"`
	DistrictId  int       `db:"C_D_ID"`
	WarehouseId int       `db:"C_W_ID"`
	First       string    `db:"C_FIRST"`
	Middle      string    `db:"C_MIDDLE"`
	Last        string    `db:"C_LAST"`
	Street1     string    `db:"C_STREET_1"`
	Street2     string    `db:"C_STREET_2"`
	City        string    `db:"C_CITY"`
	State       string    `db:"C_STATE"`
	Zip         string    `db:"C_ZIP"`
	Phone       string    `db:"C_PHONE"`
	Since       time.Time `db:"C_SINCE"`
	Credit      string    `db:"C_CREDIT"`
	CreditLim   float64   `db:"C_CREDIT_LIM"`
	Discount    float64   `db:"C_DISCOUNT"`
	Balance     float64   `db:"C_BALANCE"`
	YtdPayment  float64   `db:"C_YTD_PAYMENT"`
	PaymentCnt  int       `db:"C_PAYMENT_CNT"`
	DeliveryCnt int       `db:"C_DELIVERY_CNT"`
	Data        string    `db:"C_DATA"`
}
