package pocsdb

import (
	"POCS_Projects/internal/benchmark/databases"
	"POCS_Projects/internal/benchmark/dataloaders"
	"POCS_Projects/internal/models"
	"github.com/stretchr/testify/assert"
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
	stockPK := models.StockPK{Id: 1, WarehouseId: 1}
	stock := models.Stock{
		Id:          1,
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

func TestPocsDB_Put(t *testing.T) {
	type fields struct {
		data dataloaders.GeneratedData
	}
	type args struct {
		ctx      *databases.ConnectionContext
		dataType interface{}
		key      interface{}
		value    interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "put random item",
			fields: fields{
				data: mockData(),
			},
			args: args{
				ctx:      nil,
				dataType: models.Item{},
				key:      models.ItemPK{Id: 2},
				value: models.Item{
					Id:      2,
					Name:    "name",
					Price:   1,
					ImageId: 1,
					Data:    "data",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := PocsDB{
				data: tt.fields.data,
			}
			resChan := p.Put(tt.args.ctx, tt.args.dataType, tt.args.key, tt.args.value)
			assert.Eventually(t, func() bool {
				select {
				case res := <-resChan:
					assert.Nil(t, res.Err)
					return true
				default:
					return false
				}
			}, time.Second*5, time.Millisecond*100)

		})
	}
}
