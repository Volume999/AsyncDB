package pocsdb

import (
	"POCS_Projects/internal/cmd/benchmark/databases"
	"POCS_Projects/internal/cmd/benchmark/dataloaders"
	"POCS_Projects/internal/models"
	"fmt"
	"github.com/google/uuid"
)

const (
	Active = iota
	Committing
	Aborting
)

type ConnectionContext struct {
	ID   uuid.UUID
	Txn  *Txn
	Mode int // Active, Committing, Aborting
}

type PocsDB struct {
	data     dataloaders.GeneratedData
	tManager *TransactionManager
	lManager *LockManager
}

func NewPocsDB(tManager *TransactionManager, lManager *LockManager) *PocsDB {
	return &PocsDB{tManager: tManager, lManager: lManager}
}

func (p *PocsDB) LoadData(data dataloaders.GeneratedData) error {
	p.data = data
	return nil
}

func (p *PocsDB) Connect() (*ConnectionContext, error) {
	guid := uuid.New()
	return &ConnectionContext{ID: guid}, nil
}

func (p *PocsDB) Disconnect(context *ConnectionContext) error {
	// TODO implement me
	return nil
}

func (p *PocsDB) Put(ctx *ConnectionContext, dataType interface{}, key interface{}, value interface{}) <-chan databases.RequestResult {
	resultChan := make(chan databases.RequestResult)
	switch dataType.(type) {
	case models.Warehouse:
		go p.putWarehouse(ctx, key, value, resultChan)
	case models.Customer:
		go p.putCustomer(ctx, key, value, resultChan)
	case models.Item:
		go p.putItem(ctx, key, value, resultChan)
	case models.Stock:
		go p.putStock(ctx, key, value, resultChan)
	case models.Order:
		go p.putOrder(ctx, key, value, resultChan)
	case models.OrderLine:
		go p.putOrderLine(ctx, key, value, resultChan)
	case models.NewOrder:
		go p.putNewOrder(ctx, key, value, resultChan)
	case models.History:
		go p.putHistory(ctx, key, value, resultChan)
	case models.District:
		go p.putDistrict(ctx, key, value, resultChan)
	default:
		panic("implement me")
	}
	return resultChan
}

func (p *PocsDB) Get(ctx *ConnectionContext, dataType interface{}, key interface{}) <-chan databases.RequestResult {
	resultChan := make(chan databases.RequestResult)
	switch dataType.(type) {
	case models.Warehouse:
		go p.getWarehouse(ctx, key, resultChan)
	case models.Customer:
		go p.getCustomer(ctx, key, resultChan)
	case models.Item:
		go p.getItem(ctx, key, resultChan)
	case models.Stock:
		go p.getStock(ctx, key, resultChan)
	case models.Order:
		go p.getOrder(ctx, key, resultChan)
	case models.OrderLine:
		go p.getOrderLine(ctx, key, resultChan)
	case models.NewOrder:
		go p.getNewOrder(ctx, key, resultChan)
	case models.History:
		go p.getHistory(ctx, key, resultChan)
	case models.District:
		go p.getDistrict(ctx, key, resultChan)
	default:
		panic("implement me")
	}
	return resultChan
}

func (p *PocsDB) Delete(ctx *ConnectionContext, dataType interface{}, key interface{}) <-chan databases.RequestResult {
	resultChan := make(chan databases.RequestResult)
	switch dataType.(type) {
	case models.Warehouse:
		go p.deleteWarehouse(ctx, key, resultChan)
	case models.Customer:
		go p.deleteCustomer(ctx, key, resultChan)
	case models.Item:
		go p.deleteItem(ctx, key, resultChan)
	case models.Stock:
		go p.deleteStock(ctx, key, resultChan)
	case models.Order:
		go p.deleteOrder(ctx, key, resultChan)
	case models.OrderLine:
		go p.deleteOrderLine(ctx, key, resultChan)
	case models.NewOrder:
		go p.deleteNewOrder(ctx, key, resultChan)
	case models.History:
		go p.deleteHistory(ctx, key, resultChan)
	case models.District:
		go p.deleteDistrict(ctx, key, resultChan)
	default:
		panic("implement me")
	}
	return resultChan
}

func (p *PocsDB) BeginTransaction(ctx *ConnectionContext) error {
	txn, err := p.tManager.BeginTransaction(ctx.ID)
	if err != nil {
		return err
	}
	ctx.Txn = txn
	return nil
}

func (p *PocsDB) CommitTransaction(ctx *ConnectionContext) error {
	err := p.tManager.DeleteLog(ctx.ID)
	if err != nil {
		return err
	}
	return nil
}

func (p *PocsDB) RollbackTransaction(ctx *ConnectionContext) error {
	log, err := p.tManager.GetLog(ctx.ID)
	if err != nil {
		return err
	}
	ctx.Mode = Aborting
	for _, action := range log.actionList {
		err := p.reverseAction(ctx, action)
		if err != nil {
			return err
		}
	}
	err = p.tManager.DeleteLog(ctx.ID)
	if err != nil {
		return err
	}
	return nil
}

func (p *PocsDB) getDistrict(_ *ConnectionContext, key interface{}, resultChan chan<- databases.RequestResult) {
	resultChan <- databases.RequestResult{
		Data: p.data.Districts[key.(models.DistrictPK)],
		Err:  nil,
	}
}

func (p *PocsDB) getHistory(_ *ConnectionContext, key interface{}, resultChan chan<- databases.RequestResult) {
	resultChan <- databases.RequestResult{
		Data: p.data.History[key.(models.HistoryPK)],
		Err:  nil,
	}
}

func (p *PocsDB) getNewOrder(_ *ConnectionContext, key interface{}, resultChan chan<- databases.RequestResult) {
	resultChan <- databases.RequestResult{
		Data: p.data.NewOrders[key.(models.NewOrderPK)],
		Err:  nil,
	}
}

func (p *PocsDB) getOrderLine(_ *ConnectionContext, key interface{}, resultChan chan<- databases.RequestResult) {
	resultChan <- databases.RequestResult{
		Data: p.data.OrderLines[key.(models.OrderLinePK)],
		Err:  nil,
	}
}

func (p *PocsDB) getOrder(_ *ConnectionContext, key interface{}, resultChan chan<- databases.RequestResult) {
	resultChan <- databases.RequestResult{
		Data: p.data.Orders[key.(models.OrderPK)],
		Err:  nil,
	}
}

func (p *PocsDB) getStock(_ *ConnectionContext, key interface{}, resultChan chan<- databases.RequestResult) {
	resultChan <- databases.RequestResult{
		Data: p.data.Stocks[key.(models.StockPK)],
		Err:  nil,
	}
}

func (p *PocsDB) getItem(_ *ConnectionContext, key interface{}, resultChan chan<- databases.RequestResult) {
	resultChan <- databases.RequestResult{
		Data: p.data.Items[key.(models.ItemPK)],
		Err:  nil,
	}
}

func (p *PocsDB) getCustomer(_ *ConnectionContext, key interface{}, resultChan chan<- databases.RequestResult) {
	resultChan <- databases.RequestResult{
		Data: p.data.Customers[key.(models.CustomerPK)],
		Err:  nil,
	}
}

func (p *PocsDB) getWarehouse(_ *ConnectionContext, key interface{}, resultChan chan<- databases.RequestResult) {
	resultChan <- databases.RequestResult{
		Data: p.data.Warehouses[key.(models.WarehousePK)],
		Err:  nil,
	}
}

func (p *PocsDB) putDistrict(_ *ConnectionContext, key interface{}, value interface{}, errorChan chan<- databases.RequestResult) {
	p.data.Districts[key.(models.DistrictPK)] = value.(models.District)
	errorChan <- databases.RequestResult{
		Data: nil,
		Err:  nil,
	}
}

func (p *PocsDB) putHistory(_ *ConnectionContext, key interface{}, value interface{}, errorChan chan<- databases.RequestResult) {
	p.data.History[key.(models.HistoryPK)] = value.(models.History)
	errorChan <- databases.RequestResult{
		Data: nil,
		Err:  nil,
	}
}

func (p *PocsDB) putNewOrder(_ *ConnectionContext, key interface{}, value interface{}, errorChan chan<- databases.RequestResult) {
	p.data.NewOrders[key.(models.NewOrderPK)] = value.(models.NewOrder)
	errorChan <- databases.RequestResult{
		Data: nil,
		Err:  nil,
	}
}

func (p *PocsDB) putOrderLine(_ *ConnectionContext, key interface{}, value interface{}, errorChan chan<- databases.RequestResult) {
	p.data.OrderLines[key.(models.OrderLinePK)] = value.(models.OrderLine)
	errorChan <- databases.RequestResult{
		Data: nil,
		Err:  nil,
	}
}

func (p *PocsDB) putOrder(_ *ConnectionContext, key interface{}, value interface{}, errorChan chan<- databases.RequestResult) {
	p.data.Orders[key.(models.OrderPK)] = value.(models.Order)
	errorChan <- databases.RequestResult{
		Data: nil,
		Err:  nil,
	}
}

func (p *PocsDB) putStock(_ *ConnectionContext, key interface{}, value interface{}, errorChan chan<- databases.RequestResult) {
	p.data.Stocks[key.(models.StockPK)] = value.(models.Stock)
	errorChan <- databases.RequestResult{
		Data: nil,
		Err:  nil,
	}
}

func (p *PocsDB) putItem(_ *ConnectionContext, key interface{}, value interface{}, errorChan chan<- databases.RequestResult) {
	p.data.Items[key.(models.ItemPK)] = value.(models.Item)
	errorChan <- databases.RequestResult{
		Data: nil,
		Err:  nil,
	}
}

func (p *PocsDB) putCustomer(_ *ConnectionContext, key interface{}, value interface{}, errorChan chan<- databases.RequestResult) {
	p.data.Customers[key.(models.CustomerPK)] = value.(models.Customer)
	errorChan <- databases.RequestResult{
		Data: nil,
		Err:  nil,
	}
}

func (p *PocsDB) putWarehouse(_ *ConnectionContext, key interface{}, value interface{}, errorChan chan<- databases.RequestResult) {
	p.data.Warehouses[key.(models.WarehousePK)] = value.(models.Warehouse)
	errorChan <- databases.RequestResult{
		Data: nil,
		Err:  nil,
	}
}

func (p *PocsDB) deleteWarehouse(_ *ConnectionContext, key interface{}, errorChan chan<- databases.RequestResult) {
	if _, ok := p.data.Warehouses[key.(models.WarehousePK)]; ok {
		delete(p.data.Warehouses, key.(models.WarehousePK))
		errorChan <- databases.RequestResult{
			Data: nil,
			Err:  nil,
		}
	} else {
		errorChan <- databases.RequestResult{
			Data: nil,
			Err:  databases.ErrKeyNotFound,
		}
	}
}

func (p *PocsDB) deleteCustomer(_ *ConnectionContext, key interface{}, errorChan chan<- databases.RequestResult) {
	if _, ok := p.data.Customers[key.(models.CustomerPK)]; ok {
		delete(p.data.Customers, key.(models.CustomerPK))
		errorChan <- databases.RequestResult{
			Data: nil,
			Err:  nil,
		}
	} else {
		errorChan <- databases.RequestResult{
			Data: nil,
			Err:  databases.ErrKeyNotFound,
		}
	}
}

func (p *PocsDB) deleteItem(_ *ConnectionContext, key interface{}, errorChan chan<- databases.RequestResult) {
	if _, ok := p.data.Items[key.(models.ItemPK)]; ok {
		delete(p.data.Items, key.(models.ItemPK))
		errorChan <- databases.RequestResult{
			Data: nil,
			Err:  nil,
		}
	} else {
		errorChan <- databases.RequestResult{
			Data: nil,
			Err:  databases.ErrKeyNotFound,
		}
	}
}

func (p *PocsDB) deleteStock(_ *ConnectionContext, key interface{}, errorChan chan<- databases.RequestResult) {
	if _, ok := p.data.Stocks[key.(models.StockPK)]; ok {
		delete(p.data.Stocks, key.(models.StockPK))
		errorChan <- databases.RequestResult{
			Data: nil,
			Err:  nil,
		}
	} else {
		errorChan <- databases.RequestResult{
			Data: nil,
			Err:  databases.ErrKeyNotFound,
		}
	}
}

func (p *PocsDB) deleteOrder(_ *ConnectionContext, key interface{}, errorChan chan<- databases.RequestResult) {
	if _, ok := p.data.Orders[key.(models.OrderPK)]; ok {
		delete(p.data.Orders, key.(models.OrderPK))
		errorChan <- databases.RequestResult{
			Data: nil,
			Err:  nil,
		}
	} else {
		errorChan <- databases.RequestResult{
			Data: nil,
			Err:  databases.ErrKeyNotFound,
		}
	}
}

func (p *PocsDB) deleteOrderLine(_ *ConnectionContext, key interface{}, errorChan chan<- databases.RequestResult) {
	if _, ok := p.data.OrderLines[key.(models.OrderLinePK)]; ok {
		delete(p.data.OrderLines, key.(models.OrderLinePK))
		errorChan <- databases.RequestResult{
			Data: nil,
			Err:  nil,
		}
	} else {
		errorChan <- databases.RequestResult{
			Data: nil,
			Err:  databases.ErrKeyNotFound,
		}
	}
}

func (p *PocsDB) deleteNewOrder(_ *ConnectionContext, key interface{}, errorChan chan<- databases.RequestResult) {
	if _, ok := p.data.NewOrders[key.(models.NewOrderPK)]; ok {
		delete(p.data.NewOrders, key.(models.NewOrderPK))
		errorChan <- databases.RequestResult{
			Data: nil,
			Err:  nil,
		}
	} else {
		errorChan <- databases.RequestResult{
			Data: nil,
			Err:  databases.ErrKeyNotFound,
		}
	}
}

func (p *PocsDB) deleteHistory(_ *ConnectionContext, key interface{}, errorChan chan<- databases.RequestResult) {
	if _, ok := p.data.History[key.(models.HistoryPK)]; ok {
		delete(p.data.History, key.(models.HistoryPK))
		errorChan <- databases.RequestResult{
			Data: nil,
			Err:  nil,
		}
	} else {
		errorChan <- databases.RequestResult{
			Data: nil,
			Err:  databases.ErrKeyNotFound,
		}
	}
}

func (p *PocsDB) deleteDistrict(_ *ConnectionContext, key interface{}, errorChan chan<- databases.RequestResult) {
	if _, ok := p.data.Districts[key.(models.DistrictPK)]; ok {
		delete(p.data.Districts, key.(models.DistrictPK))
		errorChan <- databases.RequestResult{
			Data: nil,
			Err:  nil,
		}
	} else {
		errorChan <- databases.RequestResult{
			Data: nil,
			Err:  databases.ErrKeyNotFound,
		}
	}
}

func (p *PocsDB) reverseAction(ctx *ConnectionContext, action Action) error {
	var err error
	switch action.actionType {
	case LInsert:
		err = p.reverseInsert(ctx, action)
	case LDelete:
		err = p.reverseDelete(ctx, action)
	case LUpdate:
		err = p.reverseUpdate(ctx, action)
	}
	return err
}

func (p *PocsDB) reverseInsert(ctx *ConnectionContext, action Action) error {
	// Todo: Better type validation
	resChan := make(chan databases.RequestResult)
	switch action.dataType.(type) {
	case models.Warehouse:
		go p.deleteWarehouse(ctx, action.key, resChan)
	case models.Customer:
		go p.deleteCustomer(ctx, action.key, resChan)
	case models.Item:
		go p.deleteItem(ctx, action.key, resChan)
	case models.Stock:
		go p.deleteStock(ctx, action.key, resChan)
	case models.Order:
		go p.deleteOrder(ctx, action.key, resChan)
	case models.OrderLine:
		go p.deleteOrderLine(ctx, action.key, resChan)
	case models.NewOrder:
		go p.deleteNewOrder(ctx, action.key, resChan)
	case models.History:
		go p.deleteHistory(ctx, action.key, resChan)
	case models.District:
		go p.deleteDistrict(ctx, action.key, resChan)
	default:
		return fmt.Errorf("reverseInsert: invalid type %T", action.dataType)
	}
	val := <-resChan
	if val.Err != nil {
		return val.Err
	}
	return nil
}

func (p *PocsDB) reverseDelete(ctx *ConnectionContext, action Action) error {
	resChan := make(chan databases.RequestResult)
	switch action.dataType.(type) {
	case models.Warehouse:
		go p.putWarehouse(ctx, action.key, action.value, resChan)
	case models.Customer:
		go p.putCustomer(ctx, action.key, action.value, resChan)
	case models.Item:
		go p.putItem(ctx, action.key, action.value, resChan)
	case models.Stock:
		go p.putStock(ctx, action.key, action.value, resChan)
	case models.Order:
		go p.putOrder(ctx, action.key, action.value, resChan)
	case models.OrderLine:
		go p.putOrderLine(ctx, action.key, action.value, resChan)
	case models.NewOrder:
		go p.putNewOrder(ctx, action.key, action.value, resChan)
	case models.History:
		go p.putHistory(ctx, action.key, action.value, resChan)
	case models.District:
		go p.putDistrict(ctx, action.key, action.value, resChan)
	default:
		return fmt.Errorf("reverseDelete: invalid type %T", action.dataType)
	}
	val := <-resChan
	if val.Err != nil {
		return val.Err
	}
	return nil
}

func (p *PocsDB) reverseUpdate(ctx *ConnectionContext, action Action) error {
	resChan := make(chan databases.RequestResult)
	switch action.dataType.(type) {
	case models.Warehouse:
		go p.putWarehouse(ctx, action.key, action.prevValue, resChan)
	case models.Customer:
		go p.putCustomer(ctx, action.key, action.prevValue, resChan)
	case models.Item:
		go p.putItem(ctx, action.key, action.prevValue, resChan)
	case models.Stock:
		go p.putStock(ctx, action.key, action.prevValue, resChan)
	case models.Order:
		go p.putOrder(ctx, action.key, action.prevValue, resChan)
	case models.OrderLine:
		go p.putOrderLine(ctx, action.key, action.prevValue, resChan)
	case models.NewOrder:
		go p.putNewOrder(ctx, action.key, action.prevValue, resChan)
	case models.History:
		go p.putHistory(ctx, action.key, action.prevValue, resChan)
	case models.District:
		go p.putDistrict(ctx, action.key, action.prevValue, resChan)
	default:
		return fmt.Errorf("reverseUpdate: invalid type %T", action.dataType)
	}
	val := <-resChan
	if val.Err != nil {
		return val.Err
	}
	return nil
}
