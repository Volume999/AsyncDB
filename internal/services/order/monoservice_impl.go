package order

import (
	"POCS_Projects/internal/benchmark/databases"
	"POCS_Projects/internal/benchmark/databases/pocsdb"
	"POCS_Projects/internal/models"
	"POCS_Projects/internal/services/order/cmd"
	"POCS_Projects/internal/stores/async"
	"log"
	"strings"
	"time"
)

type MonoService struct {
	l      *log.Logger
	db     *pocsdb.PocsDB
	stores async.Stores
}

func NewMonoService(l *log.Logger, db *pocsdb.PocsDB, stores async.Stores) *MonoService {
	return &MonoService{l: l, db: db, stores: stores}
}

func (s *MonoService) CreateOrder(command cmd.NewOrderCommand) cmd.NewOrderResponse {
	// Create a new transaction
	ctx, err := s.db.Connect()
	if err != nil {
		return cmd.NewOrderResponse{ExecutionStatus: "Error connecting to the database: " + err.Error()}
	}
	err = s.db.BeginTransaction(ctx)
	if err != nil {
		return cmd.NewOrderResponse{ExecutionStatus: "Can't Start Transaction: " + err.Error()}
	}
	numOfItems := len(command.Items)
	// Get warehouse and warehouse tax rate
	whCh := s.stores.Warehouse.Get(ctx, models.WarehousePK{Id: command.WarehouseId})
	// Get district and district tax rate
	dCh := s.stores.District.Get(ctx, models.DistrictPK{Id: command.DistrictId, WarehouseId: command.WarehouseId})
	// Get customer
	cCh := s.stores.Customer.Get(ctx, models.CustomerPK{ID: command.CustomerId, DistrictId: command.DistrictId, WarehouseId: command.WarehouseId})
	// Get District Data for Next Order ItemId
	dRes := <-dCh
	if dRes.Err != nil {
		return cmd.NewOrderResponse{ExecutionStatus: "Error getting district: " + dRes.Err.Error()}
	}
	d := dRes.Data.(models.District)
	// Insert Order and New Order
	noCh := s.stores.NewOrder.Put(ctx, models.NewOrder{OrderId: d.NextOId, DistrictId: command.DistrictId, WarehouseId: command.WarehouseId})
	allLocal := 1
	for _, item := range command.Items {
		if item.SupplyWarehouseId != command.WarehouseId {
			allLocal = 0
		}
	}
	orderId := d.NextOId
	oCh := s.stores.Order.Put(ctx, models.Order{Id: orderId, DistrictId: command.DistrictId, WarehouseId: command.WarehouseId, CustomerId: command.CustomerId, EntryDate: time.Now(), AllLocal: allLocal, OrderLinesCnt: numOfItems})
	// Update District Next Order ItemId
	d.NextOId++
	dCh = s.stores.District.Put(ctx, d)
	// Insert Order Lines
	// Get items of the order
	itemsChans := make([]<-chan databases.RequestResult, numOfItems)
	stockChans := make([]<-chan databases.RequestResult, numOfItems)
	orderLineResponse := make([]<-chan databases.RequestResult, numOfItems)
	orderLines := make([]cmd.OrderLine, numOfItems)
	for i, orderItem := range command.Items {
		itemChan := s.stores.Item.Get(ctx, models.ItemPK{Id: orderItem.ItemId})
		stockChan := s.stores.Stock.Get(ctx, models.StockPK{ItemId: orderItem.ItemId, WarehouseId: orderItem.SupplyWarehouseId})
		itemsChans[i], stockChans[i] = itemChan, stockChan
		itemChanRes := <-itemChan
		if itemChanRes.Err != nil {
			// Abort Transaction
			err := s.db.RollbackTransaction(ctx)
			if err != nil {
				s.l.Println("Error rolling back transaction: " + err.Error())
			}
			return cmd.NewOrderResponse{ExecutionStatus: "Error getting itemChanRes: " + itemChanRes.Err.Error()}
		}
		stockChanRes := <-stockChan
		if stockChanRes.Err != nil {
			return cmd.NewOrderResponse{ExecutionStatus: "Error getting stockChanRes: " + stockChanRes.Err.Error()}
		}
		item, stock := itemChanRes.Data.(models.Item), stockChanRes.Data.(models.Stock)
		orderLineAmount := float64(orderItem.Quantity) * item.Price
		brandGeneric := "G"
		if strings.Contains(item.Name, "ORIGINAL") && strings.Contains(stock.Dist01, "ORIGINAL") {
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
			return cmd.NewOrderResponse{ExecutionStatus: "Error updating stock: " + stockRes.Err.Error()}
		}
		// Insert Order Line
		olCh := s.stores.OrderLine.Put(ctx, models.OrderLine{OrderId: d.NextOId,
			DistrictId: command.DistrictId, WarehouseId: command.WarehouseId, LineNumber: i,
			ItemId: orderItem.ItemId, SupplyWarehouseId: orderItem.SupplyWarehouseId, Quantity: orderItem.Quantity,
			Amount: orderLineAmount, DistInfo: distInfo})
		orderLineResponse[i] = olCh
		orderLines[i] = cmd.OrderLine{
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
		return cmd.NewOrderResponse{ExecutionStatus: "Error getting warehouse: " + warehouseRes.Err.Error()}
	}
	districtRes := <-dCh
	if districtRes.Err != nil {
		return cmd.NewOrderResponse{ExecutionStatus: "Error getting district: " + districtRes.Err.Error()}
	}
	customerRes := <-cCh
	if customerRes.Err != nil {
		return cmd.NewOrderResponse{ExecutionStatus: "Error getting customer: " + customerRes.Err.Error()}
	}
	warehouse, district, customer := warehouseRes.Data.(models.Warehouse), districtRes.Data.(models.District), customerRes.Data.(models.Customer)
	warehouseTax := warehouse.Tax
	districtTax := district.Tax
	customerDiscount := customer.Discount
	totalAmount = totalAmount * (1 + warehouseTax + districtTax) * (1 - customerDiscount)
	// Await all put functions
	noRes := <-noCh
	if noRes.Err != nil {
		return cmd.NewOrderResponse{ExecutionStatus: "Error inserting new order: " + noRes.Err.Error()}
	}
	oRes := <-oCh
	if oRes.Err != nil {
		return cmd.NewOrderResponse{ExecutionStatus: "Error inserting order: " + oRes.Err.Error()}
	}
	for _, olRes := range orderLineResponse {
		olRes := <-olRes
		if olRes.Err != nil {
			return cmd.NewOrderResponse{ExecutionStatus: "Error inserting order line: " + olRes.Err.Error()}
		}
	}
	// prepare return value
	return cmd.NewOrderResponse{
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
	infoDistMap := map[int]string{
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
	}
	return infoDistMap[dist]
}
