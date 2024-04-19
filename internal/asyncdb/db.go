package asyncdb

import (
	"AsyncDB/internal/databases"
	"errors"
	"fmt"
	"github.com/dlsniper/debugger"
	"github.com/google/uuid"
)

const (
	Active = iota
	Committing
	Aborting
	Ready
)

type TransactInfo struct {
	tId  TransactId
	mode int
}

type ConnectionContext struct {
	ID  uuid.UUID
	Txn *TransactInfo
	//Txn  *Txn
	//Mode int // Active, Committing, Aborting
}

var ErrTableExists = errors.New("table already exists")
var ErrTableNotFound = errors.New("table not found")

type Hasher interface {
	HashStringUint64(string) uint64
}

type AsyncDB struct {
	data     *ThreadSafeMap[uint64, Table]
	tManager TransactionManager
	lManager LockManager
	hasher   Hasher
}

func NewAsyncDB(tManager TransactionManager, lManager LockManager, hasher Hasher) *AsyncDB {
	return &AsyncDB{tManager: tManager, lManager: lManager, data: NewThreadSafeMap[uint64, Table](), hasher: hasher}
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
	lockReleaseErr = p.lManager.ReleaseLocks(TransactId(context.ID))
	return errors.Join(rollbackErr, lockReleaseErr)
}

func (p *AsyncDB) CreateTable(_ *ConnectionContext, table Table) error {
	hash := p.hasher.HashStringUint64(table.Name())
	p.data.Lock()
	defer p.data.Unlock()
	if _, ok := p.data.GetUnsafe(hash); ok {
		return fmt.Errorf("%w - %s", ErrTableExists, table.Name())
	}
	p.data.PutUnsafe(hash, table)
	return nil
}

func (p *AsyncDB) ListTables(_ *ConnectionContext) []string {
	tables := p.data.Values()
	tableNames := make([]string, 0, len(tables))
	for _, table := range tables {
		tableNames = append(tableNames, table.Name())
	}
	return tableNames
}

func (p *AsyncDB) DropTable(_ *ConnectionContext, tableName string) error {
	hash := p.hasher.HashStringUint64(tableName)
	p.data.Lock()
	defer p.data.Unlock()
	if _, ok := p.data.GetUnsafe(hash); !ok {
		return fmt.Errorf("%w - %s", ErrTableNotFound, tableName)
	}
	// TODO: Here need to check if any transaction is using this table, Or, alternatively, we can check that on commit
	p.data.DeleteUnsafe(hash)
	return nil
}

func (p *AsyncDB) Put(ctx *ConnectionContext, tableName string, key interface{}, value interface{}) <-chan databases.RequestResult {
	resultChan := make(chan databases.RequestResult)
	go func() {
		hash := p.hasher.HashStringUint64(tableName)
		table, ok := p.data.Get(hash)
		if !ok {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  fmt.Errorf("%w - %s", ErrTableNotFound, tableName),
			}
			return
		}
		err := table.ValidateTypes(key, value)
		if err != nil {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  err,
			}
			return
		}
		implTransaction := false
		txnId, err := p.tManager.StartTransaction(ctx.ID)
		if err == nil {
			implTransaction = true
			ctx.Txn = &TransactInfo{
				tId:  txnId,
				mode: Active,
			}
		} else if !errors.Is(err, ErrConnInXact) {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  err,
			}
			return
		}
		tLog, err := p.tManager.GetLog(ctx.ID)
		// TODO: Want to handle some errors?
		tLog.addAction(Action{
			Op:      LPut,
			tableId: hash,
			Key:     key,
			Value:   value,
		})
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
		implTransaction := false
		txnId, err := p.tManager.StartTransaction(ctx.ID)
		if err == nil {
			implTransaction = true
			ctx.Txn = &TransactInfo{
				tId:  txnId,
				mode: Active,
			}
		} else if !errors.Is(err, ErrConnInXact) {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  err,
			}
			return
		}
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
	hash := p.hasher.HashStringUint64(tableName)
	go func() {
		var err error
		implTransaction := false
		txnId, err := p.tManager.StartTransaction(ctx.ID)
		if err == nil {
			implTransaction = true
			ctx.Txn = &TransactInfo{
				tId:  txnId,
				mode: Active,
			}
		} else if !errors.Is(err, ErrConnInXact) {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  err,
			}
			return
		}
		res := <-p.getValue(ctx, tableName, key)
		if res.Err != nil {
			err = p.RollbackTransaction(ctx)
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  res.Err,
			}
			return
		}
		tLog, err := p.tManager.GetLog(ctx.ID)
		tLog.addAction(Action{
			Op:      LDelete,
			tableId: hash,
			Key:     key,
			Value:   nil,
		})
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
	txn, err := p.tManager.StartTransaction(ctx.ID)
	if err != nil {
		return err
	}
	ctx.Txn = &TransactInfo{tId: txn, mode: Active}
	return nil
}

func (p *AsyncDB) CommitTransaction(ctx *ConnectionContext) error {
	// Todo: Check the current state of transaction?
	// Todo: Wait for concurrent queries to finish?
	ctx.Txn.mode = Committing
	tLog, err := p.tManager.GetLog(ctx.ID)
	// Todo: Error handling?
	// Todo: this is disgusting
	_ = p.applyLogs(tLog)
	err = p.tManager.EndTransaction(ctx.ID)
	ctx.Txn = &TransactInfo{tId: TransactId(uuid.Nil), mode: Ready}
	return err
}

func (p *AsyncDB) RollbackTransaction(ctx *ConnectionContext) error {
	ctx.Txn.mode = Aborting
	// Todo: Cancel concurrent queries?
	err := p.tManager.DeleteLog(ctx.ID)
	ctx.Txn.mode = Ready
	return err
}

// TODO: Implement this
func (p *AsyncDB) applyLogs(log *TransactionLog) error {
	// Todo: I should not do it this way, it is better to implement a Range function of some sort
	log.l.Lock()
	defer log.l.Unlock()
	for hash, actions := range log.l.m {
		table, ok := p.data.Get(hash)
		if !ok {
			return fmt.Errorf("%w - %d", ErrTableNotFound, hash)
		}
		for _, action := range actions {
			switch action.Op {
			case LPut:
				err := table.Put(action.Key, action.Value)
				if err != nil {
					return err
				}
			case LDelete:
				err := table.Delete(action.Key)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (p *AsyncDB) getValue(_ *ConnectionContext, tableName string, key interface{}) <-chan databases.RequestResult {
	resultChan := make(chan databases.RequestResult)
	go func() {
		debugger.SetLabels(func() []string {
			return []string{"asyncdb", "getValue", "tableName", tableName, "key", fmt.Sprintf("%v", key)}
		})

		hash := p.hasher.HashStringUint64(tableName)
		table, ok := p.data.Get(hash)
		if !ok {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  fmt.Errorf("%w - %s", ErrTableNotFound, tableName),
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
