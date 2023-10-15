package dataloaders

import (
	"POCS_Projects/internal/models"
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
	GenerateData() (GeneratedData, error)
}

type DataGeneratorImpl struct {
	l               *log.Logger
	warehouseNumber int
}

func (d DataGeneratorImpl) GenerateData() (GeneratedData, error) {
	return GeneratedData{
		warehouses: d.generateWarehouses(),
		customers:  d.generateCustomers(),
		items:      d.generateItems(),
		stocks:     d.generateStocks(),
		orders:     d.generateOrders(),
		orderLines: d.generateOrderLines(),
		newOrders:  d.generateNewOrders(),
		history:    d.generateHistory(),
		districts:  d.generateDistricts(),
	}, nil
}

func (d DataGeneratorImpl) generateWarehouses() []models.Warehouse {
	warehouses := make([]models.Warehouse, d.warehouseNumber)
	for i := 0; i < d.warehouseNumber; i++ {
		warehouses[i] = models.Warehouse{
			Id:      i,
			Name:    randomStrRangeLen(6, 10),
			Street1: randomStrRangeLen(10, 20),
			Street2: randomStrRangeLen(10, 20),
			City:    randomStrRangeLen(10, 20),
			State:   randomStrRangeLen(2, 2),
			Zip:     generateZip(),
			Tax:     randomFloatInRange(0.0000, 0.2000),
			Ytd:     300000.00,
		}
	}
	return warehouses
}

func generateZip() string {
	return ""
}

func (d DataGeneratorImpl) generateCustomers() []models.Customer {
	customers := make([]models.Customer, 3000*10*d.warehouseNumber)
	for w := 0; w < d.warehouseNumber; w++ {
		for d := 0; d < 10; d++ {
			for c := 0; c < 3000; c++ {
				customers[w*3000*10+d*3000+c] = models.Customer{
					ID:          c,
					DistrictId:  d,
					WarehouseId: w,
					First:       randomStrRangeLen(8, 16),
					Last:        generateLastName(c),
					Middle:      "OE",
					Street1:     randomStrRangeLen(10, 20),
					Street2:     randomStrRangeLen(10, 20),
					City:        randomStrRangeLen(10, 20),
					State:       randomStrRangeLen(2, 2),
					Zip:         generateZip(),
					Phone:       randomNStrRangeLen(16, 16),
					Since:       time.Now(),
					Credit:      generateCredit(),
					CreditLim:   50000.00,
					Discount:    randomFloatInRange(0.0000, 0.5000),
					Balance:     -10.00,
					YtdPayment:  10.00,
					PaymentCnt:  1,
					DeliveryCnt: 0,
					Data:        randomStrRangeLen(300, 500),
				}
			}
		}
	}
	return customers
}

func generateCredit() string {
	panic("implement me")
}

func randomNStrRangeLen(i int, i2 int) string {
	panic("implement me")
}

func generateLastName(c int) string {
	panic("implement me")
}

func (d DataGeneratorImpl) generateItems() []models.Item {
	items := make([]models.Item, 100000)
	for i := 0; i < 100000; i++ {
		items[i] = models.Item{
			Id:      i,
			ImageId: randomIntInRange(1, 100),
			Name:    randomStrRangeLen(14, 24),
			Price:   randomFloatInRange(1.00, 100.00),
			Data:    generateItemData(26, 50),
		}
	}
	return items
}

func generateItemData(i int, i2 int) string {
	return "0"
}

func randomFloatInRange(f float64, f2 float64) float64 {
	return 0
}

func randomStrRangeLen(i int, i2 int) string {
	return "0"
}

func randomIntInRange(i int, i2 int) int {
	return 0
}

func (d DataGeneratorImpl) generateStocks() []models.Stock {
	stocks := make([]models.Stock, 100000*d.warehouseNumber)
	for w := 0; w < d.warehouseNumber; w++ {
		for i := 0; i < 100000; i++ {
			stocks[w*100000+i] = models.Stock{
				Id:          i,
				WarehouseId: w,
				Quantity:    randomIntInRange(10, 100),
				Dist01:      randomStrRangeLen(24, 24),
				Dist02:      randomStrRangeLen(24, 24),
				Dist03:      randomStrRangeLen(24, 24),
				Dist04:      randomStrRangeLen(24, 24),
				Dist05:      randomStrRangeLen(24, 24),
				Dist06:      randomStrRangeLen(24, 24),
				Dist07:      randomStrRangeLen(24, 24),
				Dist08:      randomStrRangeLen(24, 24),
				Dist09:      randomStrRangeLen(24, 24),
				Dist10:      randomStrRangeLen(24, 24),
				Ytd:         0,
				OrderCnt:    0,
				RemoteCnt:   0,
				Data:        generateItemData(26, 50),
			}
		}
	}
	return stocks
}

func (d DataGeneratorImpl) generateOrders() []models.Order {
	//TODO implement me
	panic("implement me")
}

func (d DataGeneratorImpl) generateOrderLines() []models.OrderLine {
	//TODO implement me
	panic("implement me")
}

func (d DataGeneratorImpl) generateNewOrders() []models.NewOrder {
	//TODO implement me
	panic("implement me")
}

func (d DataGeneratorImpl) generateHistory() []models.History {
	//TODO implement me
	panic("implement me")
}

func (d DataGeneratorImpl) generateDistricts() []models.District {
	districts := make([]models.District, 10*d.warehouseNumber)
	for w := 0; w < d.warehouseNumber; w++ {
		for i := 0; i < 10; i++ {
			districts[w*10+i] = models.District{
				Id:          i,
				WarehouseId: w,
				Name:        randomStrRangeLen(6, 10),
				Street1:     randomStrRangeLen(10, 20),
				Street2:     randomStrRangeLen(10, 20),
				City:        randomStrRangeLen(10, 20),
				State:       randomStrRangeLen(2, 2),
				Zip:         generateZip(),
				Tax:         randomFloatInRange(0.0000, 0.2000),
				Ytd:         300000.00,
				NextOId:     3001,
			}
		}
	}
	return districts
}
