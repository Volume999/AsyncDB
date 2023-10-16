package pocsdb

import (
	"POCS_Projects/internal/benchmark/databases"
	"POCS_Projects/internal/benchmark/dataloaders"
	"POCS_Projects/internal/models"
	"github.com/google/uuid"
)

type TransactionContext struct {
	changes map[string]interface{}
}

type PocsDB struct {
	data dataloaders.GeneratedData
}

func NewPocsDB() *PocsDB {
	return &PocsDB{}
}

func (p PocsDB) LoadData(data dataloaders.GeneratedData) error {
	p.data = data
	return nil
}

func (p PocsDB) Connect() (*databases.ConnectionContext, error) {
	guid := uuid.New()
	return &databases.ConnectionContext{ID: guid}, nil
}

func (p PocsDB) Disconnect(context *databases.ConnectionContext) error {
	// TODO implement me
	return nil
}

func (p PocsDB) Put(ctx *databases.ConnectionContext, dataType interface{}, key interface{}, value interface{}) error {
	switch dataType.(type) {
	case models.Warehouse:
		return p.putWarehouse(ctx, key, value)
	case models.Customer:
		return p.putCustomer(ctx, key, value)
	case models.Item:
		return p.putItem(ctx, key, value)
	case models.Stock:
		return p.putStock(ctx, key, value)
	case models.Order:
		return p.putOrder(ctx, key, value)
	case models.OrderLine:
		return p.putOrderLine(ctx, key, value)
	case models.NewOrder:
		return p.putNewOrder(ctx, key, value)
	case models.History:
		return p.putHistory(ctx, key, value)
	case models.District:
		return p.putDistrict(ctx, key, value)
	default:
		panic("implement me")
	}
}

func (p PocsDB) Get(ctx *databases.ConnectionContext, dataType interface{}, key interface{}) (interface{}, error) {
	switch dataType.(type) {
	case models.Warehouse:
		return p.getWarehouse(ctx, key)
	case models.Customer:
		return p.getCustomer(ctx, key)
	case models.Item:
		return p.getItem(ctx, key)
	case models.Stock:
		return p.getStock(ctx, key)
	case models.Order:
		return p.getOrder(ctx, key)
	case models.OrderLine:
		return p.getOrderLine(ctx, key)
	case models.NewOrder:
		return p.getNewOrder(ctx, key)
	case models.History:
		return p.getHistory(ctx, key)
	case models.District:
		return p.getDistrict(ctx, key)
	default:
		panic("implement me")
	}
}

func (p PocsDB) Delete(ctx *databases.ConnectionContext, dataType interface{}, key interface{}) error {
	switch dataType.(type) {
	case models.Warehouse:
		return p.deleteWarehouse(ctx, key)
	case models.Customer:
		return p.deleteCustomer(ctx, key)
	case models.Item:
		return p.deleteItem(ctx, key)
	case models.Stock:
		return p.deleteStock(ctx, key)
	case models.Order:
		return p.deleteOrder(ctx, key)
	case models.OrderLine:
		return p.deleteOrderLine(ctx, key)
	case models.NewOrder:
		return p.deleteNewOrder(ctx, key)
	case models.History:
		return p.deleteHistory(ctx, key)
	case models.District:
		return p.deleteDistrict(ctx, key)
	default:
		panic("implement me")
	}
}

func (p PocsDB) BeginTransaction(ctx *databases.ConnectionContext) error {
	//TODO implement me
	panic("implement me")
}

func (p PocsDB) CommitTransaction(ctx *databases.ConnectionContext) error {
	//TODO implement me
	panic("implement me")
}

func (p PocsDB) RollbackTransaction(ctx *databases.ConnectionContext) error {
	//TODO implement me
	panic("implement me")
}

func (p PocsDB) getDistrict(ctx *databases.ConnectionContext, key interface{}) (interface{}, error) {
	panic("implement me")
}

func (p PocsDB) getHistory(ctx *databases.ConnectionContext, key interface{}) (interface{}, error) {
	panic("implement me")
}

func (p PocsDB) getNewOrder(ctx *databases.ConnectionContext, key interface{}) (interface{}, error) {
	panic("implement me")
}

func (p PocsDB) getOrderLine(ctx *databases.ConnectionContext, key interface{}) (interface{}, error) {
	panic("implement me")
}

func (p PocsDB) getOrder(ctx *databases.ConnectionContext, key interface{}) (interface{}, error) {
	panic("implement me")
}

func (p PocsDB) getStock(ctx *databases.ConnectionContext, key interface{}) (interface{}, error) {
	panic("implement me")
}

func (p PocsDB) getItem(ctx *databases.ConnectionContext, key interface{}) (interface{}, error) {
	panic("implement me")
}

func (p PocsDB) getCustomer(ctx *databases.ConnectionContext, key interface{}) (interface{}, error) {
	panic("implement me")
}

func (p PocsDB) getWarehouse(ctx *databases.ConnectionContext, key interface{}) (interface{}, error) {
	panic("implement me")
}

func (p PocsDB) putDistrict(ctx *databases.ConnectionContext, key interface{}, value interface{}) error {
	panic("implement me")
}

func (p PocsDB) putHistory(ctx *databases.ConnectionContext, key interface{}, value interface{}) error {
	panic("implement me")
}

func (p PocsDB) putNewOrder(ctx *databases.ConnectionContext, key interface{}, value interface{}) error {
	panic("implement me")
}

func (p PocsDB) putOrderLine(ctx *databases.ConnectionContext, key interface{}, value interface{}) error {
	panic("implement me")
}

func (p PocsDB) putOrder(ctx *databases.ConnectionContext, key interface{}, value interface{}) error {
	panic("implement me")
}

func (p PocsDB) putStock(ctx *databases.ConnectionContext, key interface{}, value interface{}) error {
	panic("implement me")
}

func (p PocsDB) putItem(ctx *databases.ConnectionContext, key interface{}, value interface{}) error {
	panic("implement me")
}

func (p PocsDB) putCustomer(ctx *databases.ConnectionContext, key interface{}, value interface{}) error {
	panic("implement me")
}

func (p PocsDB) putWarehouse(ctx *databases.ConnectionContext, key interface{}, value interface{}) error {
	panic("implement me")
}

func (p PocsDB) deleteWarehouse(ctx *databases.ConnectionContext, key interface{}) error {
	panic("implement me")
}

func (p PocsDB) deleteCustomer(ctx *databases.ConnectionContext, key interface{}) error {
	panic("implement me")
}

func (p PocsDB) deleteItem(ctx *databases.ConnectionContext, key interface{}) error {
	panic("implement me")
}

func (p PocsDB) deleteStock(ctx *databases.ConnectionContext, key interface{}) error {
	panic("implement me")
}

func (p PocsDB) deleteOrder(ctx *databases.ConnectionContext, key interface{}) error {
	panic("implement me")
}

func (p PocsDB) deleteOrderLine(ctx *databases.ConnectionContext, key interface{}) error {
	panic("implement me")
}

func (p PocsDB) deleteNewOrder(ctx *databases.ConnectionContext, key interface{}) error {
	panic("implement me")
}

func (p PocsDB) deleteHistory(ctx *databases.ConnectionContext, key interface{}) error {
	panic("implement me")
}

func (p PocsDB) deleteDistrict(ctx *databases.ConnectionContext, key interface{}) error {
	panic("implement me")
}
