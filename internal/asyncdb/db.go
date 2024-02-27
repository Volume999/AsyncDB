package asyncdb

import (
	"AsyncDB/internal/databases"
	"AsyncDB/internal/tpcc/dataloaders"
	"AsyncDB/internal/tpcc/models"
	"errors"
	"github.com/google/uuid"
)

const (
	Active = iota
	Committing
	Aborting
	Ready
)

type ConnectionContext struct {
	ID   uuid.UUID
	Txn  *Txn
	Mode int // Active, Committing, Aborting
}

//type ValueType interface{}
//type KeyType interface{}

var ErrTableExists = errors.New("table already exists")

type AsyncDB struct {
	data     map[uint64]Table
	tManager TransactionManager
	lManager LockManager
}

func NewAsyncDB(tManager *TransactionManagerImpl, lManager *LockManagerImpl) *AsyncDB {
	return &AsyncDB{tManager: tManager, lManager: lManager}
}

func (p *AsyncDB) LoadData(ctx *ConnectionContext, data dataloaders.GeneratedData) error {
	// Warehouses
	table := NewGenericTable[models.WarehousePK, models.Warehouse]("warehouses")
	_ = p.CreateTable(ctx, table)
	return nil
}

func (p *AsyncDB) Connect() (*ConnectionContext, error) {
	guid := uuid.New()
	return &ConnectionContext{ID: guid}, nil
}

func (p *AsyncDB) Disconnect(context *ConnectionContext) error {
	var rollbackErr, lockReleaseErr error
	if context.Txn != nil {
		rollbackErr = p.RollbackTransaction(context)
	}
	lockReleaseErr = p.lManager.ReleaseLocks(context.ID)
	return errors.Join(rollbackErr, lockReleaseErr)
}

func (p *AsyncDB) CreateTable(ctx *ConnectionContext, table Table) error {
	hash := table.Hash()
	if _, ok := p.data[hash]; ok {
		return ErrTableExists
	}
	p.data[hash] = table
	return nil
}

func (p *AsyncDB) Put(ctx *ConnectionContext, tableName string, key interface{}, value interface{}) <-chan databases.RequestResult {
	resultChan := make(chan databases.RequestResult)
	go func() {
		implTransaction := ctx.Txn == nil
		if implTransaction {
			txn, err := p.tManager.BeginTransaction(ctx.ID)
			if err != nil {
				resultChan <- databases.RequestResult{
					Data: nil,
					Err:  err,
				}
				return
			}
			ctx.Txn = txn
		}
		txn := ctx.Txn
		txn.tLogMutex.Lock()
		txn.tLog.addAction(Action{
			Op:       LPut,
			DataType: dataType,
			Key:      key,
			Value:    value,
		})
		txn.tLogMutex.Unlock()
		var err error
		if implTransaction {
			err = p.CommitTransaction(ctx)
		}
		resultChan <- databases.RequestResult{
			Data: nil,
			Err:  err,
		}
	}()
	return resultChan
}

func (p *AsyncDB) Get(ctx *ConnectionContext, tableName string, key interface{}) <-chan databases.RequestResult {
	resultChan := make(chan databases.RequestResult)
	go func() {
		implTransaction := ctx.Txn == nil
		if implTransaction {
			txn, err := p.tManager.BeginTransaction(ctx.ID)
			if err != nil {
				resultChan <- databases.RequestResult{
					Data: nil,
					Err:  err,
				}
				return
			}
			ctx.Txn = txn
		}
		var err error
		res := <-p.getValue(ctx, dataType, key)
		if implTransaction {
			err = p.CommitTransaction(ctx)
		}
		resultChan <- databases.RequestResult{
			Data: res.Data,
			Err:  errors.Join(err, res.Err),
		}
	}()
	return resultChan
}

func (p *AsyncDB) Delete(ctx *ConnectionContext, tableName string, key interface{}) <-chan databases.RequestResult {
	resultChan := make(chan databases.RequestResult)
	go func() {
		implTransaction := ctx.Txn == nil
		if implTransaction {
			txn, err := p.tManager.BeginTransaction(ctx.ID)
			if err != nil {
				resultChan <- databases.RequestResult{
					Data: nil,
					Err:  err,
				}
				return
			}
			ctx.Txn = txn
		}
		txn := ctx.Txn
		txn.tLogMutex.Lock()
		txn.tLog.addAction(Action{
			Op:       LDelete,
			DataType: dataType,
			Key:      key,
			Value:    nil,
		})
		txn.tLogMutex.Unlock()
		var err error
		if implTransaction {
			err = p.CommitTransaction(ctx)
		}
		resultChan <- databases.RequestResult{
			Data: nil,
			Err:  err,
		}
	}()
	return resultChan
}

func (p *AsyncDB) BeginTransaction(ctx *ConnectionContext) error {
	txn, err := p.tManager.BeginTransaction(ctx.ID)
	if err != nil {
		return err
	}
	ctx.Txn = txn
	ctx.Mode = Active
	return nil
}

func (p *AsyncDB) CommitTransaction(ctx *ConnectionContext) error {
	// Todo: Wait for concurrent queries to finish?
	ctx.Mode = Committing
	txn := ctx.Txn
	txn.tLogMutex.Lock()
	p.applyLogs(txn.tLog)
	txn.tLogMutex.Unlock()
	err := p.tManager.DeleteLog(ctx.ID)
	ctx.Txn = nil
	ctx.Mode = Ready
	return err
}

func (p *AsyncDB) RollbackTransaction(ctx *ConnectionContext) error {
	ctx.Mode = Aborting
	// Todo: Cancel concurrent queries?
	err := p.tManager.DeleteLog(ctx.ID)
	ctx.Mode = Ready
	return err
}

func (p *AsyncDB) applyLogs(log *TransactionLog) {
	applyLogToData[models.WarehousePK, models.Warehouse](p.data.Warehouses, log.WarehouseLog)
	applyLogToData[models.StockPK, models.Stock](p.data.Stocks, log.StockLog)
	applyLogToData[models.OrderPK, models.Order](p.data.Orders, log.OrderLog)
	applyLogToData[models.NewOrderPK, models.NewOrder](p.data.NewOrders, log.NewOrderLog)
	applyLogToData[models.DistrictPK, models.District](p.data.Districts, log.DistrictLog)
	applyLogToData[models.CustomerPK, models.Customer](p.data.Customers, log.CustomerLog)
	applyLogToData[models.ItemPK, models.Item](p.data.Items, log.ItemLog)
	applyLogToData[models.OrderLinePK, models.OrderLine](p.data.OrderLines, log.OrderLineLog)
	applyLogToData[models.HistoryPK, models.History](p.data.History, log.HistoryLog)
}

func applyLogToData[K comparable, V any](
	data map[K]V,
	logMap map[K]LogEntry[V],
) {
	for key, value := range logMap {
		switch value.Op {
		case LPut:
			data[key] = value.Value
		case LDelete:
			delete(data, key)
		}
	}
}

func (p *AsyncDB) putValue(ctx *ConnectionContext, dataType interface{}, key interface{}, value interface{}) <-chan databases.RequestResult {
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

func (p *AsyncDB) getValue(ctx *ConnectionContext, dataType interface{}, key interface{}) <-chan databases.RequestResult {
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

func (p *AsyncDB) deleteValue(ctx *ConnectionContext, dataType interface{}, key interface{}) <-chan databases.RequestResult {
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

func (p *AsyncDB) getDistrict(_ *ConnectionContext, key interface{}, resultChan chan<- databases.RequestResult) {
	resultChan <- databases.RequestResult{
		Data: p.data.Districts[key.(models.DistrictPK)],
		Err:  nil,
	}
}

func (p *AsyncDB) getHistory(_ *ConnectionContext, key interface{}, resultChan chan<- databases.RequestResult) {
	resultChan <- databases.RequestResult{
		Data: p.data.History[key.(models.HistoryPK)],
		Err:  nil,
	}
}

func (p *AsyncDB) getNewOrder(_ *ConnectionContext, key interface{}, resultChan chan<- databases.RequestResult) {
	resultChan <- databases.RequestResult{
		Data: p.data.NewOrders[key.(models.NewOrderPK)],
		Err:  nil,
	}
}

func (p *AsyncDB) getOrderLine(_ *ConnectionContext, key interface{}, resultChan chan<- databases.RequestResult) {
	resultChan <- databases.RequestResult{
		Data: p.data.OrderLines[key.(models.OrderLinePK)],
		Err:  nil,
	}
}

func (p *AsyncDB) getOrder(_ *ConnectionContext, key interface{}, resultChan chan<- databases.RequestResult) {
	resultChan <- databases.RequestResult{
		Data: p.data.Orders[key.(models.OrderPK)],
		Err:  nil,
	}
}

func (p *AsyncDB) getStock(_ *ConnectionContext, key interface{}, resultChan chan<- databases.RequestResult) {
	resultChan <- databases.RequestResult{
		Data: p.data.Stocks[key.(models.StockPK)],
		Err:  nil,
	}
}

func (p *AsyncDB) getItem(_ *ConnectionContext, key interface{}, resultChan chan<- databases.RequestResult) {
	resultChan <- databases.RequestResult{
		Data: p.data.Items[key.(models.ItemPK)],
		Err:  nil,
	}
}

func (p *AsyncDB) getCustomer(_ *ConnectionContext, key interface{}, resultChan chan<- databases.RequestResult) {
	resultChan <- databases.RequestResult{
		Data: p.data.Customers[key.(models.CustomerPK)],
		Err:  nil,
	}
}

func (p *AsyncDB) getWarehouse(_ *ConnectionContext, key interface{}, resultChan chan<- databases.RequestResult) {
	resultChan <- databases.RequestResult{
		Data: p.data.Warehouses[key.(models.WarehousePK)],
		Err:  nil,
	}
}

func (p *AsyncDB) putDistrict(_ *ConnectionContext, key interface{}, value interface{}, errorChan chan<- databases.RequestResult) {
	p.data.Districts[key.(models.DistrictPK)] = value.(models.District)
	errorChan <- databases.RequestResult{
		Data: nil,
		Err:  nil,
	}
}

func (p *AsyncDB) putHistory(_ *ConnectionContext, key interface{}, value interface{}, errorChan chan<- databases.RequestResult) {
	p.data.History[key.(models.HistoryPK)] = value.(models.History)
	errorChan <- databases.RequestResult{
		Data: nil,
		Err:  nil,
	}
}

func (p *AsyncDB) putNewOrder(_ *ConnectionContext, key interface{}, value interface{}, errorChan chan<- databases.RequestResult) {
	p.data.NewOrders[key.(models.NewOrderPK)] = value.(models.NewOrder)
	errorChan <- databases.RequestResult{
		Data: nil,
		Err:  nil,
	}
}

func (p *AsyncDB) putOrderLine(_ *ConnectionContext, key interface{}, value interface{}, errorChan chan<- databases.RequestResult) {
	p.data.OrderLines[key.(models.OrderLinePK)] = value.(models.OrderLine)
	errorChan <- databases.RequestResult{
		Data: nil,
		Err:  nil,
	}
}

func (p *AsyncDB) putOrder(_ *ConnectionContext, key interface{}, value interface{}, errorChan chan<- databases.RequestResult) {
	p.data.Orders[key.(models.OrderPK)] = value.(models.Order)
	errorChan <- databases.RequestResult{
		Data: nil,
		Err:  nil,
	}
}

func (p *AsyncDB) putStock(_ *ConnectionContext, key interface{}, value interface{}, errorChan chan<- databases.RequestResult) {
	p.data.Stocks[key.(models.StockPK)] = value.(models.Stock)
	errorChan <- databases.RequestResult{
		Data: nil,
		Err:  nil,
	}
}

func (p *AsyncDB) putItem(_ *ConnectionContext, key interface{}, value interface{}, errorChan chan<- databases.RequestResult) {
	p.data.Items[key.(models.ItemPK)] = value.(models.Item)
	errorChan <- databases.RequestResult{
		Data: nil,
		Err:  nil,
	}
}

func (p *AsyncDB) putCustomer(_ *ConnectionContext, key interface{}, value interface{}, errorChan chan<- databases.RequestResult) {
	p.data.Customers[key.(models.CustomerPK)] = value.(models.Customer)
	errorChan <- databases.RequestResult{
		Data: nil,
		Err:  nil,
	}
}

func (p *AsyncDB) putWarehouse(_ *ConnectionContext, key interface{}, value interface{}, errorChan chan<- databases.RequestResult) {
	p.data.Warehouses[key.(models.WarehousePK)] = value.(models.Warehouse)
	errorChan <- databases.RequestResult{
		Data: nil,
		Err:  nil,
	}
}

func (p *AsyncDB) deleteWarehouse(_ *ConnectionContext, key interface{}, errorChan chan<- databases.RequestResult) {
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

func (p *AsyncDB) deleteCustomer(_ *ConnectionContext, key interface{}, errorChan chan<- databases.RequestResult) {
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

func (p *AsyncDB) deleteItem(_ *ConnectionContext, key interface{}, errorChan chan<- databases.RequestResult) {
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

func (p *AsyncDB) deleteStock(_ *ConnectionContext, key interface{}, errorChan chan<- databases.RequestResult) {
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

func (p *AsyncDB) deleteOrder(_ *ConnectionContext, key interface{}, errorChan chan<- databases.RequestResult) {
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

func (p *AsyncDB) deleteOrderLine(_ *ConnectionContext, key interface{}, errorChan chan<- databases.RequestResult) {
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

func (p *AsyncDB) deleteNewOrder(_ *ConnectionContext, key interface{}, errorChan chan<- databases.RequestResult) {
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

func (p *AsyncDB) deleteHistory(_ *ConnectionContext, key interface{}, errorChan chan<- databases.RequestResult) {
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

func (p *AsyncDB) deleteDistrict(_ *ConnectionContext, key interface{}, errorChan chan<- databases.RequestResult) {
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
