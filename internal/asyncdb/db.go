package asyncdb

import (
	"AsyncDB/internal/databases"
	"AsyncDB/internal/tpcc/dataloaders"
	"AsyncDB/internal/tpcc/models"
	"errors"
	"fmt"
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
var ErrTableNotFound = errors.New("table not found")

type AsyncDB struct {
	data     map[uint64]Table
	tManager TransactionManager
	lManager LockManager
}

func NewAsyncDB(tManager *TransactionManagerImpl, lManager *LockManagerImpl) *AsyncDB {
	return &AsyncDB{tManager: tManager, lManager: lManager, data: make(map[uint64]Table)}
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
			Op:        LPut,
			tableName: tableName,
			Key:       key,
			Value:     value,
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
		res := <-p.getValue(ctx, tableName, key)
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
			Op:        LDelete,
			tableName: tableName,
			Key:       key,
			Value:     nil,
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

func (p *AsyncDB) applyLogs(log *TransactionLog) {}

func (p *AsyncDB) putValue(ctx *ConnectionContext, tableName string, key interface{}, value interface{}) <-chan databases.RequestResult {
	resultChan := make(chan databases.RequestResult)
	go func() {
		hash := HashStringUint64(tableName)
		table, ok := p.data[hash]
		if !ok {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  fmt.Errorf("%w: %s", ErrTableNotFound, tableName),
			}
			return
		}
		err := table.Put(key, value)
		resultChan <- databases.RequestResult{
			Data: nil,
			Err:  err,
		}
	}()
	return resultChan
}

func (p *AsyncDB) getValue(ctx *ConnectionContext, tableName string, key interface{}) <-chan databases.RequestResult {
	resultChan := make(chan databases.RequestResult)
	go func() {
		hash := HashStringUint64(tableName)
		table, ok := p.data[hash]
		if !ok {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  fmt.Errorf("%w: %s", ErrTableNotFound, tableName),
			}
			return
		}
		value, err := table.Get(key)
		resultChan <- databases.RequestResult{
			Data: value,
			Err:  err,
		}
	}()
	return resultChan
}

func (p *AsyncDB) deleteValue(ctx *ConnectionContext, tableName string, key interface{}) <-chan databases.RequestResult {
	resultChan := make(chan databases.RequestResult)
	go func() {
		hash := HashStringUint64(tableName)
		table, ok := p.data[hash]
		if !ok {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  fmt.Errorf("%w: %s", ErrTableNotFound, tableName),
			}
			return
		}
		err := table.Delete(key)
		resultChan <- databases.RequestResult{
			Data: nil,
			Err:  err,
		}
	}()
	return resultChan
}
