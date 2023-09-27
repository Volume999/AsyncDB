package models

type Stock struct {
	Id          int     `db:"S_I_ID"`
	WarehouseId int     `db:"S_W_ID"`
	Quantity    int     `db:"S_QUANTITY"`
	Dist01      string  `db:"S_DIST_01"`
	Dist02      string  `db:"S_DIST_02"`
	Dist03      string  `db:"S_DIST_03"`
	Dist04      string  `db:"S_DIST_04"`
	Dist05      string  `db:"S_DIST_05"`
	Dist06      string  `db:"S_DIST_06"`
	Dist07      string  `db:"S_DIST_07"`
	Dist08      string  `db:"S_DIST_08"`
	Dist09      string  `db:"S_DIST_09"`
	Dist10      string  `db:"S_DIST_10"`
	Ytd         float64 `db:"S_YTD"`
	OrderCnt    int     `db:"S_ORDER_CNT"`
	RemoteCnt   int     `db:"S_REMOTE_CNT"`
	Data        string  `db:"S_DATA"`
}
