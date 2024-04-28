package asyncdb

import (
	"AsyncDB/internal/databases"
	"errors"
	"fmt"
	"github.com/dlsniper/debugger"
	"github.com/google/uuid"
	"sync"
	"time"
)

const (
	Active = iota
	Committing
	Aborting
	Ready
)

type TransactInfo struct {
	tId  TransactId
	ts   int64
	mode int
}

type ConnectionContext struct {
	ID  uuid.UUID
	Txn *TransactInfo
}

var ErrTableExists = errors.New("table already exists")
var ErrTableNotFound = errors.New("table not found")
var ErrXactAborted = errors.New("transaction aborted")
var ErrXactInTerminalState = errors.New("transaction in terminal state")

type Hasher interface {
	HashStringUint64(string) uint64
}

type AsyncDB struct {
	data             *ThreadSafeMap[uint64, Table]
	tManager         TransactionManager
	lManager         LockManager
	hasher           Hasher
	currentProcesses *ThreadSafeMap[uuid.UUID, *sync.WaitGroup]
	withImplicitTxn  bool
}

func NewAsyncDB(tManager TransactionManager, lManager LockManager, hasher Hasher, options ...func(*AsyncDB)) *AsyncDB {
	db := &AsyncDB{
		tManager:         tManager,
		lManager:         lManager,
		data:             NewThreadSafeMap[uint64, Table](),
		hasher:           hasher,
		currentProcesses: NewThreadSafeMap[uuid.UUID, *sync.WaitGroup](),
		withImplicitTxn:  true,
	}

	for _, option := range options {
		option(db)
	}
	return db
}

func WithExplicitTxn() func(*AsyncDB) {
	return func(db *AsyncDB) {
		db.withImplicitTxn = false
	}
}

func (p *AsyncDB) Connect() (*ConnectionContext, error) {
	guid := uuid.New()
	p.currentProcesses.Put(guid, &sync.WaitGroup{})
	return &ConnectionContext{ID: guid, Txn: &TransactInfo{tId: TransactId(uuid.Nil), mode: Ready, ts: 0}}, nil
}

func (p *AsyncDB) Disconnect(context *ConnectionContext) error {
	var rollbackErr error
	// Todo: this is not good
	if context.Txn != nil {
		rollbackErr = p.RollbackTransaction(context)
	}
	return rollbackErr
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
	if ctx.Txn.mode == Committing || ctx.Txn.mode == Aborting {
		resultChan <- databases.RequestResult{
			Data: nil,
			Err:  ErrXactInTerminalState,
		}
		return resultChan
	}
	wg, _ := p.currentProcesses.Get(ctx.ID)
	wg.Add(1)
	go func() {
		hash := p.hasher.HashStringUint64(tableName)
		table, ok := p.data.Get(hash)
		if !ok {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  fmt.Errorf("%w - %s", ErrTableNotFound, tableName),
			}
			wg.Done()
			return
		}
		err := table.ValidateTypes(key, value)
		if err != nil {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  err,
			}
			wg.Done()
			return
		}
		implTransaction := false

		// If the connection is not in a transaction - start a transaction
		txnId, err := p.tManager.StartTransaction(ctx.ID)
		if err == nil {
			implTransaction = true
			ctx.Txn = &TransactInfo{
				tId:  txnId,
				mode: Active,
				ts:   time.Now().UnixNano(),
			}
		} else if !errors.Is(err, ErrConnInXact) {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  err,
			}
			wg.Done()
			return
		}
		tLog, err := p.tManager.GetLog(ctx.ID)
		err = p.lManager.Lock(WriteLock, ctx.Txn.tId, ctx.Txn.ts, TableId(hash), key)
		// TODO: Change this logic
		// Locks are released only when the transaction is aborted
		// This is temporary, in the future we need a better way of handling this
		if errors.Is(err, ErrLocksReleased) {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  ErrXactAborted,
			}
			wg.Done()
			return
		}
		if err != nil {
			wg.Done()
			p.abortTransaction(ctx)
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  err,
			}
			return
		}
		// TODO: Want to handle some errors?
		tLog.addAction(Action{
			Op:      LPut,
			tableId: hash,
			Key:     key,
			Value:   value,
		})
		wg.Done()
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
	if ctx.Txn.mode == Committing || ctx.Txn.mode == Aborting {
		resultChan <- databases.RequestResult{
			Data: nil,
			Err:  ErrXactInTerminalState,
		}
		return resultChan
	}
	wg, _ := p.currentProcesses.Get(ctx.ID)
	wg.Add(1)
	go func() {
		implTransaction := false
		txnId, err := p.tManager.StartTransaction(ctx.ID)
		if err == nil {
			implTransaction = true
			ctx.Txn = &TransactInfo{
				tId:  txnId,
				mode: Active,
				ts:   time.Now().UnixNano(),
			}
		} else if !errors.Is(err, ErrConnInXact) {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  err,
			}
			wg.Done()
			return
		}
		hash := p.hasher.HashStringUint64(tableName)
		err = p.lManager.Lock(WriteLock, ctx.Txn.tId, ctx.Txn.ts, TableId(hash), key)
		// TODO: Change this logic
		// Locks are released only when the transaction is aborted
		// This is temporary, in the future we need a better way of handling this
		if errors.Is(err, ErrLocksReleased) {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  ErrXactAborted,
			}
			wg.Done()
			return
		}
		if err != nil {
			wg.Done()
			p.abortTransaction(ctx)
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  err,
			}
			return
		}
		log, err := p.tManager.GetLog(ctx.ID)
		if err != nil {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  err,
			}
			wg.Done()
			return
		}
		if res, found := log.findLastValue(hash, key); found {
			resultChan <- databases.RequestResult{
				Data: res,
				Err:  nil,
			}
			wg.Done()
			return
		}
		res := <-p.getValue(ctx, tableName, key)
		wg.Done()
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
	if ctx.Txn.mode == Committing || ctx.Txn.mode == Aborting {
		resultChan <- databases.RequestResult{
			Data: nil,
			Err:  ErrXactInTerminalState,
		}
		return resultChan
	}
	hash := p.hasher.HashStringUint64(tableName)
	wg, _ := p.currentProcesses.Get(ctx.ID)
	wg.Add(1)
	go func() {
		var err error
		implTransaction := false
		txnId, err := p.tManager.StartTransaction(ctx.ID)
		if err == nil {
			implTransaction = true
			ctx.Txn = &TransactInfo{
				tId:  txnId,
				mode: Active,
				ts:   time.Now().UnixNano(),
			}
		} else if !errors.Is(err, ErrConnInXact) {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  err,
			}
			wg.Done()
			return
		}
		err = p.lManager.Lock(WriteLock, ctx.Txn.tId, ctx.Txn.ts, TableId(hash), key)
		// TODO: Change this logic
		// Locks are released only when the transaction is aborted
		// This is temporary, in the future we need a better way of handling this
		if errors.Is(err, ErrLocksReleased) {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  ErrXactAborted,
			}
			wg.Done()
			return
		}
		if err != nil {
			wg.Done()
			p.abortTransaction(ctx)
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  err,
			}
			return
		}
		res := <-p.getValue(ctx, tableName, key)
		if res.Err != nil {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  res.Err,
			}
			wg.Done()
			return
		}
		wg.Done()
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

	// Ideally would check if there are no running processes, but it's safer to just replace it
	p.currentProcesses.Put(ctx.ID, &sync.WaitGroup{})
	if err != nil {
		return err
	}
	ctx.Txn = &TransactInfo{tId: txn, mode: Active, ts: time.Now().UnixNano()}
	return nil
}

func (p *AsyncDB) CommitTransaction(ctx *ConnectionContext) error {
	// Todo: Check the current state of transaction?
	// Wait for concurrent queries to finish
	ctx.Txn.mode = Committing

	wg, _ := p.currentProcesses.Get(ctx.ID)
	wg.Wait()

	tLog, err := p.tManager.GetLog(ctx.ID)
	if err != nil {
		return err
	}
	// Todo: Error handling?
	// Todo: this is disgusting
	err = errors.Join(err, p.applyLogs(tLog))
	err = errors.Join(err, p.lManager.ReleaseLocks(ctx.Txn.tId))
	err = errors.Join(err, p.tManager.EndTransaction(ctx.ID))
	ctx.Txn = &TransactInfo{tId: TransactId(uuid.Nil), ts: 0, mode: Ready}
	return err
}

func (p *AsyncDB) abortTransaction(ctx *ConnectionContext) error {
	ctx.Txn.mode = Aborting
	wg, _ := p.currentProcesses.Get(ctx.ID)
	err := p.lManager.ReleaseLocks(ctx.Txn.tId)
	wg.Wait()
	err = errors.Join(err, p.tManager.DeleteLog(ctx.ID))
	ctx.Txn.mode = Active
	return err
}

func (p *AsyncDB) RollbackTransaction(ctx *ConnectionContext) error {
	ctx.Txn.mode = Aborting
	wg, _ := p.currentProcesses.Get(ctx.ID)
	// Todo: Figure out how to cancel queries, instead of waiting for them to finish
	err := p.lManager.ReleaseLocks(ctx.Txn.tId)
	wg.Wait()
	err = errors.Join(err, p.tManager.EndTransaction(ctx.ID))
	ctx.Txn = &TransactInfo{tId: TransactId(uuid.Nil), ts: 0, mode: Ready}
	return err
}

// TODO: Implement this
func (p *AsyncDB) applyLogs(log *TransactionLog) error {
	// Todo: Add validation for the transaction log before applying
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
