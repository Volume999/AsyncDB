package order

import (
	"AsyncDB/asyncdb"
	"AsyncDB/internal/databases"
	"AsyncDB/internal/tpcc/models"
	"AsyncDB/internal/tpcc/stores/async"
	"log"
	"strings"
	"time"
)

type MonoService struct {
	l      *log.Logger
	db     *asyncdb.AsyncDB
	stores async.Stores
}

func NewMonoService(l *log.Logger, db *asyncdb.AsyncDB, stores async.Stores) *MonoService {
	return &MonoService{l: l, db: db, stores: stores}
}

func (s *MonoService) CreateOrder(command Command) Response {
	// Create a new transaction
	ctx, err := s.db.Connect()
	if err != nil {
		return Response{ExecutionStatus: "Error connecting to the database: " + err.Error()}
	}
	err = s.db.BeginTransaction(ctx)
	if err != nil {
		return Response{ExecutionStatus: "Can't Start Transaction: " + err.Error()}
	}
	numOfItems := len(command.Items)

	// Get Warehouse, District and Customer
	whCh := s.stores.Warehouse.Get(ctx, models.WarehousePK{Id: command.WarehouseId})
	dCh := s.stores.District.Get(ctx, models.DistrictPK{Id: command.DistrictId, WarehouseId: command.WarehouseId})
	cCh := s.stores.Customer.Get(ctx, models.CustomerPK{ID: command.CustomerId, DistrictId: command.DistrictId, WarehouseId: command.WarehouseId})

	// Insert Order and New Order
	dRes := <-dCh
	if dRes.Err != nil {
		return Response{ExecutionStatus: "Error getting district: " + dRes.Err.Error()}
	}
	d := dRes.Data.(models.District)
	orderId := d.NextOId
	noCh := s.stores.NewOrder.Put(ctx, models.NewOrder{OrderId: orderId, DistrictId: command.DistrictId, WarehouseId: command.WarehouseId})
	allLocal := 1
	for _, item := range command.Items {
		if item.SupplyWarehouseId != command.WarehouseId {
			allLocal = 0
		}
	}
	oCh := s.stores.Order.Put(ctx, models.Order{Id: orderId, DistrictId: command.DistrictId, WarehouseId: command.WarehouseId, CustomerId: command.CustomerId, EntryDate: time.Now(), AllLocal: allLocal, OrderLinesCnt: numOfItems})
	d.NextOId++
	dPutCh := s.stores.District.Put(ctx, d)

	// Insert Order Lines
	orderLineResponse := make([]<-chan databases.RequestResult, numOfItems)
	orderLines := make([]ResponseItems, numOfItems)
	for i, orderItem := range command.Items {
		itemChan := s.stores.Item.Get(ctx, models.ItemPK{Id: orderItem.ItemId})
		stockChan := s.stores.Stock.Get(ctx, models.StockPK{ItemId: orderItem.ItemId, WarehouseId: orderItem.SupplyWarehouseId})
		itemChanRes := <-itemChan
		if itemChanRes.Err != nil {
			// Abort Transaction
			// Todo: Maybe this shouldn't be here
			err := s.db.RollbackTransaction(ctx)
			if err != nil {
				s.l.Println("Error rolling back transaction: " + err.Error())
			}
			return Response{ExecutionStatus: "Error getting itemChanRes: " + itemChanRes.Err.Error()}
		}
		stockChanRes := <-stockChan
		if stockChanRes.Err != nil {
			return Response{ExecutionStatus: "Error getting stockChanRes: " + stockChanRes.Err.Error()}
		}
		item, stock := itemChanRes.Data.(models.Item), stockChanRes.Data.(models.Stock)
		orderLineAmount := float64(orderItem.Quantity) * item.Price
		brandGeneric := "G"
		if strings.Contains(item.Data, "ORIGINAL") && strings.Contains(stock.Data, "ORIGINAL") {
			brandGeneric = "B"
		}
		// Stock Information / Update
		distInfo := pickDistInfo(stock, command.DistrictId)
		if stock.Quantity >= orderItem.Quantity+10 {
			stock.Quantity -= orderItem.Quantity
		} else {
			stock.Quantity = stock.Quantity - orderItem.Quantity + 91
		}
		stock.Ytd += orderItem.Quantity
		stock.OrderCnt += 1
		// Update Stock
		stockCh := s.stores.Stock.Put(ctx, stock)
		stockRes := <-stockCh
		if stockRes.Err != nil {
			return Response{ExecutionStatus: "Error updating stock: " + stockRes.Err.Error()}
		}
		// Insert Order Line
		olCh := s.stores.OrderLine.Put(ctx, models.OrderLine{
			OrderId:           d.NextOId,
			DistrictId:        command.DistrictId,
			WarehouseId:       command.WarehouseId,
			LineNumber:        i,
			ItemId:            orderItem.ItemId,
			SupplyWarehouseId: orderItem.SupplyWarehouseId,
			Quantity:          orderItem.Quantity,
			Amount:            orderLineAmount,
			DistInfo:          distInfo})
		orderLineResponse[i] = olCh
		orderLines[i] = ResponseItems{
			SupplyWarehouseId: orderItem.SupplyWarehouseId,
			ItemId:            orderItem.ItemId,
			ItemName:          item.Name,
			LineQuantity:      orderItem.Quantity,
			StockQuantity:     stock.Quantity,
			BrandGeneric:      brandGeneric,
			ItemPrice:         item.Price,
			Amount:            orderLineAmount,
		}
	}
	totalAmount := 0.0
	for _, orderLine := range orderLines {
		totalAmount += orderLine.Amount
	}
	warehouseRes := <-whCh
	if warehouseRes.Err != nil {
		return Response{ExecutionStatus: "Error getting warehouse: " + warehouseRes.Err.Error()}
	}
	customerRes := <-cCh
	if customerRes.Err != nil {
		return Response{ExecutionStatus: "Error getting customer: " + customerRes.Err.Error()}
	}
	warehouse, customer := warehouseRes.Data.(models.Warehouse), customerRes.Data.(models.Customer)
	warehouseTax := warehouse.Tax
	districtTax := d.Tax
	customerDiscount := customer.Discount
	totalAmount = totalAmount * (1 + warehouseTax + districtTax) * (1 - customerDiscount)
	// Await all put functions
	noRes := <-noCh
	if noRes.Err != nil {
		return Response{ExecutionStatus: "Error inserting new order: " + noRes.Err.Error()}
	}
	oRes := <-oCh
	if oRes.Err != nil {
		return Response{ExecutionStatus: "Error inserting order: " + oRes.Err.Error()}
	}
	for _, olRes := range orderLineResponse {
		olRes := <-olRes
		if olRes.Err != nil {
			return Response{ExecutionStatus: "Error inserting order line: " + olRes.Err.Error()}
		}
	}
	dPutRes := <-dPutCh
	if dPutRes.Err != nil {
		return Response{ExecutionStatus: "Error updating district: " + dPutRes.Err.Error()}
	}
	// Commit Transaction
	err = s.db.CommitTransaction(ctx)
	if err != nil {
		return Response{ExecutionStatus: "Error committing transaction: " + err.Error()}
	}
	// prepare return value
	return Response{
		WarehouseId:      command.WarehouseId,
		DistrictId:       command.DistrictId,
		CustomerId:       command.CustomerId,
		CustomerLastName: customer.Last,
		CustomerCredit:   customer.Credit,
		CustomerDiscount: customerDiscount,
		WarehouseTax:     warehouseTax,
		DistrictTax:      districtTax,
		OrderLinesCount:  numOfItems,
		OrderId:          orderId,
		OrderEntryDate:   time.Now(),
		TotalAmount:      totalAmount,
		OrderLines:       orderLines,
		ExecutionStatus:  "OK",
	}
}

func pickDistInfo(stock models.Stock, dist int) string {
	return map[int]string{
		1:  stock.Dist01,
		2:  stock.Dist02,
		3:  stock.Dist03,
		4:  stock.Dist04,
		5:  stock.Dist05,
		6:  stock.Dist06,
		7:  stock.Dist07,
		8:  stock.Dist08,
		9:  stock.Dist09,
		10: stock.Dist10,
	}[dist]
}
