package dataloaders

import (
	"POCS_Projects/internal/benchmark"
	"POCS_Projects/internal/benchmark/dataloaders/generators"
	"POCS_Projects/internal/models"
	"fmt"
	"log"
	"time"
)

type GeneratedData struct {
	warehouses []models.Warehouse
	customers  []models.Customer
	items      []models.Item
	stocks     []models.Stock
	orders     []models.Order
	orderLines []models.OrderLine
	newOrders  []models.NewOrder
	history    []models.History
	districts  []models.District
}

type DataGenerator interface {
	// GenerateData generates data for the application
	GenerateData(constants benchmark.Constants) (GeneratedData, error)
}

type DataGeneratorImpl struct {
	l               *log.Logger
	warehouseNumber int
	consts          benchmark.Constants
}

func (gen DataGeneratorImpl) GenerateData(constants benchmark.Constants) (GeneratedData, error) {
	gen.consts = constants
	data := GeneratedData{
		warehouses: gen.generateWarehouses(),
		customers:  gen.generateCustomers(),
		items:      gen.generateItems(),
		stocks:     gen.generateStocks(),
		orders:     gen.generateOrders(),
		history:    gen.generateHistory(),
		districts:  gen.generateDistricts(),
	}
	data.orderLines = gen.generateOrderLines(data.orders)
	data.newOrders = gen.generateNewOrders(data.orders)
	return data, nil
}

func (gen DataGeneratorImpl) generateWarehouses() []models.Warehouse {
	warehouses := make([]models.Warehouse, gen.warehouseNumber)
	for i := 0; i < gen.warehouseNumber; i++ {
		warehouses[i] = models.Warehouse{
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

func (gen DataGeneratorImpl) generateCustomers() []models.Customer {
	customers := make([]models.Customer, 3000*10*gen.warehouseNumber)
	for w := 0; w < gen.warehouseNumber; w++ {
		for d := 0; d < 10; d++ {
			for c := 0; c < 3000; c++ {
				customers[w*3000*10+d*3000+c] = models.Customer{
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

func (gen DataGeneratorImpl) generateItems() []models.Item {
	items := make([]models.Item, 100000)
	for i := 0; i < 100000; i++ {
		items[i] = models.Item{
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

func (gen DataGeneratorImpl) generateStocks() []models.Stock {
	stocks := make([]models.Stock, 100000*gen.warehouseNumber)
	for w := 0; w < gen.warehouseNumber; w++ {
		for i := 0; i < 100000; i++ {
			stocks[w*100000+i] = models.Stock{
				Id:          i,
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

func (gen DataGeneratorImpl) generateOrders() []models.Order {
	orders := make([]models.Order, 3000*10*gen.warehouseNumber)
	for w := 0; w < gen.warehouseNumber; w++ {
		for d := 0; d < 10; d++ {
			customers := generators.RandomPermutationInt(1, 3000)
			for o := 0; o < 3000; o++ {
				order := models.Order{
					Id:            o,
					DistrictId:    d,
					WarehouseId:   w,
					CustomerId:    customers[o],
					EntryDate:     time.Now(),
					OrderLinesCnt: generators.RandomIntInRange(5, 15),
					AllLocal:      1,
				}
				if order.Id < 2101 {
					order.CarrierId = generators.RandomIntInRange(1, 10)
				}
				orders[w*3000*10+d*3000+o] = order
			}
		}
	}
	return orders
}

func (gen DataGeneratorImpl) generateOrderLines(orders []models.Order) []models.OrderLine {
	orderLines := make([]models.OrderLine, 0)
	for _, order := range orders {
		orderLineCnt := order.OrderLinesCnt
		for i := 0; i < orderLineCnt; i++ {
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
			orderLines = append(orderLines)
		}
	}
	return orderLines
}

func (gen DataGeneratorImpl) generateNewOrders(orders []models.Order) []models.NewOrder {
	newOrders := make([]models.NewOrder, 0)
	for _, order := range orders {
		if order.Id > 2100 {
			newOrder := models.NewOrder{
				OrderId:     order.Id,
				DistrictId:  order.DistrictId,
				WarehouseId: order.WarehouseId,
			}
			newOrders = append(newOrders, newOrder)
		}
	}
	return newOrders
}

func (gen DataGeneratorImpl) generateHistory() []models.History {
	histories := make([]models.History, 3000*10*gen.warehouseNumber)
	for w := 0; w < gen.warehouseNumber; w++ {
		for d := 0; d < 10; d++ {
			for c := 0; c < 3000; c++ {
				histories[w*3000*10+d*3000+c] = models.History{
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

func (gen DataGeneratorImpl) generateDistricts() []models.District {
	districts := make([]models.District, 10*gen.warehouseNumber)
	for w := 0; w < gen.warehouseNumber; w++ {
		for i := 0; i < 10; i++ {
			districts[w*10+i] = models.District{
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
