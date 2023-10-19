package dataloaders

import (
	commands "POCS_Projects/internal/cmd"
	"POCS_Projects/internal/cmd/benchmark/dataloaders/generators"
	"POCS_Projects/internal/models"
	"fmt"
	"log"
	"time"
)

const (
	minWarehouses = 1
	minDistricts  = 1
	maxDistricts  = 10
	minCustomers  = 1
	maxCustomers  = 3000
	minItems      = 1
	maxItems      = 100000
	minStocks     = 1
	maxStocks     = 100000
	minOrders     = 1
	maxOrders     = 3000
)

type GeneratedData struct {
	Warehouses map[models.WarehousePK]models.Warehouse
	Districts  map[models.DistrictPK]models.District
	Customers  map[models.CustomerPK]models.Customer
	Items      map[models.ItemPK]models.Item
	Stocks     map[models.StockPK]models.Stock
	Orders     map[models.OrderPK]models.Order
	OrderLines map[models.OrderLinePK]models.OrderLine
	NewOrders  map[models.NewOrderPK]models.NewOrder
	History    map[models.HistoryPK]models.History
}

type DataGenerator interface {
	// GenerateData generates data for the application
	GenerateData(constants commands.Constants) (GeneratedData, error)
}

type DataGeneratorImpl struct {
	l               *log.Logger
	warehouseNumber int
	consts          commands.Constants
}

func NewDataGeneratorImpl(warehouseNumber int, consts commands.Constants, l *log.Logger) DataGeneratorImpl {
	return DataGeneratorImpl{
		l:               l,
		warehouseNumber: warehouseNumber,
		consts:          consts,
	}
}

func (gen DataGeneratorImpl) GenerateData() (GeneratedData, error) {
	data := GeneratedData{
		Warehouses: gen.generateWarehouses(),
		Customers:  gen.generateCustomers(),
		Items:      gen.generateItems(),
		Stocks:     gen.generateStocks(),
		Orders:     gen.generateOrders(),
		History:    gen.generateHistory(),
		Districts:  gen.generateDistricts(),
	}
	data.OrderLines = gen.generateOrderLines(data.Orders)
	data.NewOrders = gen.generateNewOrders(data.Orders)
	return data, nil
}

func (gen DataGeneratorImpl) generateWarehouses() map[models.WarehousePK]models.Warehouse {
	warehouses := make(map[models.WarehousePK]models.Warehouse)
	for i := minWarehouses; i <= gen.warehouseNumber; i++ {
		warehouses[models.WarehousePK{Id: i}] = models.Warehouse{
			Id:      i,
			Name:    generators.RandomStrRangeLen(6, 10),
			Street1: generators.RandomStrRangeLen(10, 20),
			Street2: generators.RandomStrRangeLen(10, 20),
			City:    generators.RandomStrRangeLen(10, 20),
			State:   generators.RandomStrRangeLen(2, 2),
			Zip:     generateZip(),
			Tax:     generators.RandomFloatInRange(0.0000, 0.2000, 4),
			Ytd:     300000.00,
		}
	}
	return warehouses
}

func generateZip() string {
	zip := generators.RandomIntInRange(1000, 9999)
	return fmt.Sprintf("%d11111", zip)
}

func (gen DataGeneratorImpl) generateCustomers() map[models.CustomerPK]models.Customer {
	customers := make(map[models.CustomerPK]models.Customer)
	for w := minWarehouses; w <= gen.warehouseNumber; w++ {
		for d := minDistricts; d <= maxDistricts; d++ {
			for c := minCustomers; c <= maxCustomers; c++ {
				customers[models.CustomerPK{
					ID:          c,
					DistrictId:  d,
					WarehouseId: w,
				}] = models.Customer{
					ID:          c,
					DistrictId:  d,
					WarehouseId: w,
					First:       generators.RandomStrRangeLen(8, 16),
					Last:        generateLastName(generators.NURand(255, 0, 999, gen.consts.CLast)),
					Middle:      "OE",
					Street1:     generators.RandomStrRangeLen(10, 20),
					Street2:     generators.RandomStrRangeLen(10, 20),
					City:        generators.RandomStrRangeLen(10, 20),
					State:       generators.RandomStrRangeLen(2, 2),
					Zip:         generateZip(),
					Phone:       generators.RandomNStrRangeLen(16, 16),
					Since:       time.Now(),
					Credit:      generateCredit(),
					CreditLim:   50000.00,
					Discount:    generators.RandomFloatInRange(0.0000, 0.5000, 4),
					Balance:     -10.00,
					YtdPayment:  10.00,
					PaymentCnt:  1,
					DeliveryCnt: 0,
					Data:        generators.RandomStrRangeLen(300, 500),
				}
			}
		}
	}
	return customers
}

func generateCredit() string {
	if c := generators.RandomIntInRange(1, 100); c <= 10 {
		return "BC"
	} else {
		return "GC"
	}
}

func generateLastName(c int) string {
	syls := []string{"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"}
	syl1 := syls[c/100]
	syl2 := syls[(c/10)%10]
	syl3 := syls[c%10]
	return syl1 + syl2 + syl3
}

func (gen DataGeneratorImpl) generateItems() map[models.ItemPK]models.Item {
	items := make(map[models.ItemPK]models.Item)
	for i := minItems; i <= maxItems; i++ {
		items[models.ItemPK{Id: i}] = models.Item{
			Id:      i,
			ImageId: generators.RandomIntInRange(1, 10000),
			Name:    generators.RandomStrRangeLen(14, 24),
			Price:   generators.RandomFloatInRange(1.00, 100.00, 2),
			Data:    generateItemData(26, 50),
		}
	}
	return items
}

func generateItemData(i int, i2 int) string {
	if c := generators.RandomIntInRange(0, 100); c > 10 {
		return generators.RandomStrRangeLen(i, i2)
	} else {
		idx := generators.RandomIntInRange(i, i2-7)
		s1 := generators.RandomStrRangeLen(idx, idx)
		s2 := generators.RandomStrRangeLen(i2-idx-7, i2-idx-7)
		return s1 + "ORIGINAL" + s2
	}
}

func (gen DataGeneratorImpl) generateStocks() map[models.StockPK]models.Stock {
	stocks := make(map[models.StockPK]models.Stock)
	for w := minWarehouses; w <= gen.warehouseNumber; w++ {
		for i := minItems; i <= maxItems; i++ {
			stocks[models.StockPK{ItemId: i, WarehouseId: w}] = models.Stock{
				ItemId:      i,
				WarehouseId: w,
				Quantity:    generators.RandomIntInRange(10, 100),
				Dist01:      generators.RandomStrRangeLen(24, 24),
				Dist02:      generators.RandomStrRangeLen(24, 24),
				Dist03:      generators.RandomStrRangeLen(24, 24),
				Dist04:      generators.RandomStrRangeLen(24, 24),
				Dist05:      generators.RandomStrRangeLen(24, 24),
				Dist06:      generators.RandomStrRangeLen(24, 24),
				Dist07:      generators.RandomStrRangeLen(24, 24),
				Dist08:      generators.RandomStrRangeLen(24, 24),
				Dist09:      generators.RandomStrRangeLen(24, 24),
				Dist10:      generators.RandomStrRangeLen(24, 24),
				Ytd:         0,
				OrderCnt:    0,
				RemoteCnt:   0,
				Data:        generateItemData(26, 50),
			}
		}
	}
	return stocks
}

func (gen DataGeneratorImpl) generateOrders() map[models.OrderPK]models.Order {
	orders := make(map[models.OrderPK]models.Order)
	for w := minWarehouses; w <= gen.warehouseNumber; w++ {
		for d := minDistricts; d <= maxDistricts; d++ {
			customers := generators.RandomPermutationInt(1, 3000)
			for o := minOrders; o <= maxOrders; o++ {
				order := models.Order{
					Id:            o,
					DistrictId:    d,
					WarehouseId:   w,
					CustomerId:    customers[o-1],
					EntryDate:     time.Now(),
					OrderLinesCnt: generators.RandomIntInRange(5, 15),
					AllLocal:      1,
				}
				if order.Id < 2101 {
					order.CarrierId = generators.RandomIntInRange(1, 10)
				}
				orders[models.OrderPK{Id: o, DistrictId: d, WarehouseId: w}] = order
			}
		}
	}
	return orders
}

func (gen DataGeneratorImpl) generateOrderLines(orders map[models.OrderPK]models.Order) map[models.OrderLinePK]models.OrderLine {
	orderLines := make(map[models.OrderLinePK]models.OrderLine)
	for _, order := range orders {
		orderLineCnt := order.OrderLinesCnt
		for i := 1; i <= orderLineCnt; i++ {
			orderLine := models.OrderLine{
				OrderId:           order.Id,
				DistrictId:        order.DistrictId,
				WarehouseId:       order.WarehouseId,
				LineNumber:        i,
				ItemId:            generators.RandomIntInRange(1, 100000),
				SupplyWarehouseId: order.WarehouseId,
				Quantity:          5,
				DistInfo:          generators.RandomStrRangeLen(24, 24),
			}
			if orderLine.OrderId < 2101 {
				orderLine.DeliveryDate = order.EntryDate
				orderLine.Amount = 0
			} else {
				orderLine.Amount = generators.RandomFloatInRange(0.01, 9999.99, 2)
			}
			orderLines[models.OrderLinePK{
				OrderId:     orderLine.OrderId,
				DistrictId:  orderLine.DistrictId,
				WarehouseId: orderLine.WarehouseId,
				LineNumber:  orderLine.LineNumber,
			}] = orderLine
		}
	}
	return orderLines
}

func (gen DataGeneratorImpl) generateNewOrders(orders map[models.OrderPK]models.Order) map[models.NewOrderPK]models.NewOrder {
	newOrders := make(map[models.NewOrderPK]models.NewOrder)
	for _, order := range orders {
		if order.Id > 2100 {
			newOrder := models.NewOrder{
				OrderId:     order.Id,
				DistrictId:  order.DistrictId,
				WarehouseId: order.WarehouseId,
			}
			newOrders[models.NewOrderPK{
				OrderId:     newOrder.OrderId,
				DistrictId:  newOrder.DistrictId,
				WarehouseId: newOrder.WarehouseId,
			}] = newOrder
		}
	}
	return newOrders
}

func (gen DataGeneratorImpl) generateHistory() map[models.HistoryPK]models.History {
	histories := make(map[models.HistoryPK]models.History)
	for w := minWarehouses; w <= gen.warehouseNumber; w++ {
		for d := minDistricts; d <= maxDistricts; d++ {
			for c := minCustomers; c < maxCustomers; c++ {
				histories[models.HistoryPK{HistoryId: w*3000*10 + d*3000 + c}] = models.History{
					CustomerID:          c,
					CustomerDistrictId:  d,
					CustomerWarehouseId: w,
					Date:                time.Now(),
					Amount:              10.00,
					Data:                generators.RandomStrRangeLen(12, 24),
				}
			}
		}
	}
	return histories
}

func (gen DataGeneratorImpl) generateDistricts() map[models.DistrictPK]models.District {
	districts := make(map[models.DistrictPK]models.District)
	for w := minWarehouses; w <= gen.warehouseNumber; w++ {
		for i := minDistricts; i <= maxDistricts; i++ {
			districts[models.DistrictPK{
				Id:          i,
				WarehouseId: w,
			}] = models.District{
				Id:          i,
				WarehouseId: w,
				Name:        generators.RandomStrRangeLen(6, 10),
				Street1:     generators.RandomStrRangeLen(10, 20),
				Street2:     generators.RandomStrRangeLen(10, 20),
				City:        generators.RandomStrRangeLen(10, 20),
				State:       generators.RandomStrRangeLen(2, 2),
				Zip:         generateZip(),
				Tax:         generators.RandomFloatInRange(0.0000, 0.2000, 4),
				Ytd:         30000.00,
				NextOId:     3001,
			}
		}
	}
	return districts
}
