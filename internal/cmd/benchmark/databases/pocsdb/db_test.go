package pocsdb

import (
	"POCS_Projects/internal/cmd/benchmark/dataloaders"
	"POCS_Projects/internal/models"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

func mockData() dataloaders.GeneratedData {
	warehousePK := models.WarehousePK{Id: 1}
	warehouse := models.Warehouse{
		Id:      1,
		Name:    "name",
		Street1: "street1",
		Street2: "street2",
		City:    "city",
		State:   "state",
		Zip:     "zip",
		Tax:     0.1,
		Ytd:     0.2,
	}
	districtPK := models.DistrictPK{Id: 1, WarehouseId: 1}
	district := models.District{
		Id:      1,
		Name:    "name",
		Street1: "street1",
		Street2: "street2",
		City:    "city",
		State:   "state",
	}
	customerPK := models.CustomerPK{ID: 1, DistrictId: 1, WarehouseId: 1}
	customer := models.Customer{
		ID:          1,
		DistrictId:  1,
		WarehouseId: 1,
		First:       "first",
		Middle:      "middle",
		Last:        "last",
		Street1:     "street1",
		Street2:     "street2",
		City:        "city",
		State:       "state",
		Zip:         "zip",
		Phone:       "phone",
		Credit:      "credit",
		CreditLim:   0.1,
		Discount:    0.2,
		Balance:     0.3,
		YtdPayment:  0.4,
		PaymentCnt:  1,
		DeliveryCnt: 2,
	}
	newOrderPK := models.NewOrderPK{OrderId: 1, DistrictId: 1, WarehouseId: 1}
	newOrder := models.NewOrder{
		OrderId:     1,
		DistrictId:  1,
		WarehouseId: 1,
	}
	orderPK := models.OrderPK{Id: 1, DistrictId: 1, WarehouseId: 1}
	order := models.Order{
		Id:            1,
		DistrictId:    1,
		WarehouseId:   1,
		CustomerId:    1,
		CarrierId:     1,
		OrderLinesCnt: 1,
		AllLocal:      1,
	}
	orderLinePK := models.OrderLinePK{OrderId: 1, DistrictId: 1, WarehouseId: 1, LineNumber: 1}
	orderLine := models.OrderLine{
		OrderId:           1,
		DistrictId:        1,
		WarehouseId:       1,
		LineNumber:        1,
		ItemId:            1,
		SupplyWarehouseId: 1,
		Quantity:          1,
		Amount:            1,
		DistInfo:          "distInfo",
	}
	itemPK := models.ItemPK{Id: 1}
	item := models.Item{
		Id:      1,
		Name:    "name",
		Price:   1,
		ImageId: 1,
		Data:    "data",
	}
	stockPK := models.StockPK{ItemId: 1, WarehouseId: 1}
	stock := models.Stock{
		ItemId:      1,
		WarehouseId: 1,
		Quantity:    1,
		Dist01:      "dist01",
	}
	return dataloaders.GeneratedData{
		Warehouses: map[models.WarehousePK]models.Warehouse{
			warehousePK: warehouse,
		},
		Districts: map[models.DistrictPK]models.District{
			districtPK: district,
		},
		Customers: map[models.CustomerPK]models.Customer{
			customerPK: customer,
		},
		NewOrders: map[models.NewOrderPK]models.NewOrder{
			newOrderPK: newOrder,
		},
		Orders: map[models.OrderPK]models.Order{
			orderPK: order,
		},
		OrderLines: map[models.OrderLinePK]models.OrderLine{
			orderLinePK: orderLine,
		},
		Items: map[models.ItemPK]models.Item{
			itemPK: item,
		},
		Stocks: map[models.StockPK]models.Stock{
			stockPK: stock,
		},
	}
}

type PocsDBSuite struct {
	suite.Suite
	db  *PocsDB
	ctx *ConnectionContext
}

func (suite *PocsDBSuite) SetupTest() {
	tm := NewTransactionManager()
	lm := NewLockManager()
	suite.db = NewPocsDB(tm, lm)
	err := suite.db.LoadData(mockData())
	if err != nil {
		suite.Failf("Failed to load data", "Error: %v", err)
	}
	ctx, err := suite.db.Connect()
	if err != nil {
		suite.Failf("Failed to connect", "Error: %v", err)
	}
	suite.ctx = ctx
}

func (suite *PocsDBSuite) TestPocsDB_Get_ReturnsItem_When_Exists() {
	db := suite.db
	resChan := db.Get(suite.ctx, models.Item{}, models.ItemPK{Id: 1})
	suite.Eventually(func() bool {
		select {
		case res := <-resChan:
			suite.Nil(res.Err)
			suite.Equal(models.Item{
				Id:      1,
				Name:    "name",
				Price:   1,
				ImageId: 1,
				Data:    "data",
			}, res.Data)
			return true
		default:
			return false
		}
	}, time.Second*5, time.Millisecond*100)
}

func (suite *PocsDBSuite) TestPocsDB_Get_ReturnsError_When_NotExists() {
	db := suite.db
	resChan := db.Get(suite.ctx, models.Item{}, models.ItemPK{Id: 2})
	suite.Eventually(func() bool {
		select {
		case res := <-resChan:
			suite.NotNil(res.Err)
			suite.Nil(res.Data)
			return true
		default:
			return false
		}
	}, time.Second*5, time.Millisecond*100)
}

func (suite *PocsDBSuite) TestPocsDB_Put_NoError_When_PutItem() {
	db := suite.db
	resChan := db.Put(suite.ctx, models.Item{}, models.ItemPK{Id: 2}, models.Item{})
	suite.Eventually(func() bool {
		select {
		case res := <-resChan:
			suite.Nil(res.Err)
			return true
		default:
			return false
		}
	}, time.Second*5, time.Millisecond*100)
}

func (suite *PocsDBSuite) TestPocsDB_Put_ReturnsNoError_When_GetItem() {
	db := suite.db
	resChan := db.Put(suite.ctx, models.Item{}, models.ItemPK{Id: 2}, models.Item{})
	suite.Eventuallyf(func() bool {
		select {
		case <-resChan:
			return true
		default:
			return false
		}
	}, time.Second*5, time.Millisecond*100, "Put did not return")
	resChan = db.Get(suite.ctx, models.Item{}, models.ItemPK{Id: 2})
	suite.Eventually(func() bool {
		select {
		case res := <-resChan:
			suite.Nil(res.Err)
			return true
		default:
			return false
		}
	}, time.Second*5, time.Millisecond*100)
}

func (suite *PocsDBSuite) TestPocsDB_Get_ReturnsItem_After_Put() {
	db := suite.db
	item := models.Item{
		Id:      2,
		Name:    "name",
		Price:   1,
		ImageId: 1,
		Data:    "data",
	}
	resChan := db.Put(suite.ctx, models.Item{}, models.ItemPK{Id: 2}, item)
	<-resChan
	resChan = db.Get(suite.ctx, models.Item{}, models.ItemPK{Id: 2})
	suite.Eventually(func() bool {
		select {
		case res := <-resChan:
			suite.Nil(res.Err)
			suite.Equal(item, res.Data)
			return true
		default:
			return false
		}
	}, time.Second*5, time.Millisecond*100)
}

func (suite *PocsDBSuite) TestPocsDB_Put_UpdatesItem_When_Exists() {
	db := suite.db
	item := models.Item{
		Id:      1,
		Name:    "name",
		Price:   1,
		ImageId: 1,
		Data:    "data",
	}
	resChan := db.Put(suite.ctx, models.Item{}, models.ItemPK{Id: 1}, item)
	suite.Eventuallyf(func() bool {
		select {
		case res := <-resChan:
			return suite.Nil(res.Err)
		default:
			return false
		}
	}, time.Second*5, time.Millisecond*100, "Put #1 did not succeed")
	item.Name = "newName"
	resChan = db.Put(suite.ctx, models.Item{}, models.ItemPK{Id: 1}, item)
	suite.Eventuallyf(func() bool {
		select {
		case res := <-resChan:
			return suite.Nil(res.Err)
		default:
			return false
		}
	}, time.Second*5, time.Millisecond*100, "Put #2 did not succeed")
	resChan = db.Get(suite.ctx, models.Item{}, models.ItemPK{Id: 1})
	suite.Eventually(func() bool {
		select {
		case res := <-resChan:
			suite.Nil(res.Err)
			suite.Equal(item, res.Data)
			return true
		default:
			return false
		}
	}, time.Second*5, time.Millisecond*100)
}

func (suite *PocsDBSuite) TestPocsDB_Delete_NoError_When_ItemExists() {
	db := suite.db
	resChan := db.Delete(suite.ctx, models.Item{}, models.ItemPK{Id: 1})
	suite.Eventually(func() bool {
		select {
		case res := <-resChan:
			suite.Nil(res.Err)
			return true
		default:
			return false
		}
	}, time.Second*5, time.Millisecond*100)
}

func (suite *PocsDBSuite) TestPocsDB_Delete_ReturnsError_When_ItemDoesNotExist() {
	db := suite.db
	resChan := db.Delete(suite.ctx, models.Item{}, models.ItemPK{Id: 2})
	suite.Eventually(func() bool {
		select {
		case res := <-resChan:
			suite.NotNil(res.Err)
			return true
		default:
			return false
		}
	}, time.Second*5, time.Millisecond*100)
}

func (suite *PocsDBSuite) TestPocsDB_Get_ReturnsError_After_Delete() {
	db := suite.db
	resChan := db.Delete(suite.ctx, models.Item{}, models.ItemPK{Id: 1})
	suite.Eventuallyf(func() bool {
		select {
		case <-resChan:
			return true
		default:
			return false
		}
	}, time.Second*5, time.Millisecond*100, "Delete did not return")
	resChan = db.Get(suite.ctx, models.Item{}, models.ItemPK{Id: 1})
	suite.Eventually(func() bool {
		select {
		case res := <-resChan:
			suite.NotNil(res.Err)
			suite.Nil(res.Data)
			return true
		default:
			return false
		}
	}, time.Second*5, time.Millisecond*100)
}

func TestPocsDBSuite(t *testing.T) {
	suite.Run(t, new(PocsDBSuite))
}
