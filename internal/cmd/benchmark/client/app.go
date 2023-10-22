package client

import (
	"POCS_Projects/internal/cmd/benchmark/dataloaders/generators"
	"POCS_Projects/internal/services/order"
	"log"
)

type Client interface {
	CreateOrder() order.Response
}

const OrdersToGenerate = 1

type Impl struct {
	l               *log.Logger
	orderService    order.MonoService
	homeWarehouseId int
	numOfWarehouses int
	C               int
}

func (i Impl) CreateOrder() order.Response {
	com := i.generateNewOrder()
	return i.orderService.CreateOrder(com)
}

func (i Impl) generateNewOrder() order.Command {
	wId := i.homeWarehouseId
	dId := generators.RandomIntInRange(1, 10)
	cId := generators.NURand(1023, 1, 3000, i.C)
	lineCnt := generators.RandomIntInRange(5, 15)
	orderLines := i.generateOrderLines(lineCnt)
	return order.Command{WarehouseId: wId, DistrictId: dId, CustomerId: cId, Items: orderLines}
}

func (i Impl) generateOrderLines(lineCnt int) []order.CommandItems {
	orderLines := make([]order.CommandItems, lineCnt)
	for x := 0; x < lineCnt; x++ {
		itemId := generators.NURand(8191, 1, 100000, i.C)
		if x == lineCnt-1 && generators.RandomIntInRange(1, 100) == 1 {
			itemId = -1
		}
		supWId := i.homeWarehouseId
		if generators.RandomIntInRange(1, 100) == 1 {
			warehouses := generators.RandomPermutationInt(1, i.numOfWarehouses)
			supWId = warehouses[0]
			if supWId == i.homeWarehouseId {
				supWId = warehouses[1]
			}
		}
		quantity := generators.RandomIntInRange(1, 10)
		orderLines[x] = order.CommandItems{ItemId: itemId, SupplyWarehouseId: supWId, Quantity: quantity}
	}
	return orderLines
}

func (i Impl) Run() {
	for x := 0; x < OrdersToGenerate; x++ {
		i.l.Println("Creating Order: ", x)
		res := i.CreateOrder()
		i.l.Println("Order Created: ", res.ExecutionStatus)
	}
}

func NewClient(l *log.Logger, orderService order.MonoService, wId int, n int) Client {
	return &Impl{l: l, orderService: orderService, homeWarehouseId: wId, C: generators.RandomIntInRange(0, 1023), numOfWarehouses: n}
}
