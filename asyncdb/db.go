package asyncdb

import (
	"errors"
	"fmt"
	"github.com/Volume999/AsyncDB/internal/databases"
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
	acts *sync.WaitGroup
}

// Debugging functions
func (t *TransactInfo) Timestamp() int64 {
	return t.ts
}

func (t *TransactInfo) SetTimestamp(ts int64) {
	t.ts = ts
}

type ConnectionContext struct {
	ID    uuid.UUID
	Txn   *TransactInfo
	TxnMu *sync.RWMutex
}

var ErrTableExists = errors.New("table already exists")
var ErrTableNotFound = errors.New("table not found")
var ErrXactAborted = errors.New("transaction aborted")
var ErrXactInTerminalState = errors.New("transaction in terminal state")
var ErrXactInProgress = errors.New("transaction in progress")

type Hasher interface {
	HashStringUint64(string) uint64
}

type AsyncDB struct {
	data            *ThreadSafeMap[uint64, Table]
	tManager        TransactionManager
	lManager        LockManager
	hasher          Hasher
	withImplicitTxn bool
}

func NewAsyncDB(tManager TransactionManager, lManager LockManager, hasher Hasher, options ...func(*AsyncDB)) *AsyncDB {
	db := &AsyncDB{
		tManager:        tManager,
		lManager:        lManager,
		data:            NewThreadSafeMap[uint64, Table](),
		hasher:          hasher,
		withImplicitTxn: true,
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
	return &ConnectionContext{ID: guid, Txn: nil, TxnMu: &sync.RWMutex{}}, nil
}

func (p *AsyncDB) Disconnect(ctx *ConnectionContext) error {
	// This function removes transaction info if there is any, and does not allow disconnect
	// if there is an active transaction. Previously it used to abort but
	// better to let the user do it
	if ctx.Txn != nil && ctx.Txn.mode != Ready {
		return ErrXactInProgress
	}
	ctx.Txn = nil
	return nil
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

func (p *AsyncDB) BeginTransaction(ctx *ConnectionContext) error {
	tId, err := p.tManager.StartTransaction(ctx.ID)

	if err != nil {
		return err
	}
	ctx.Txn = &TransactInfo{tId: tId, mode: Active, ts: time.Now().UnixNano(), acts: &sync.WaitGroup{}}
	return nil
}

func (p *AsyncDB) CommitTransaction(ctx *ConnectionContext) error {
	// Todo: Check the current state of transaction?
	// Wait for concurrent queries to finish
	ctx.TxnMu.Lock()
	defer ctx.TxnMu.Unlock()
	if ctx.Txn == nil {
		return ErrConnNotInXact
	}
	ctx.Txn.mode = Committing
	// Todo: Maybe wait can be outside of the locking scheme, because Status is locked by WLock
	ctx.Txn.acts.Wait()
	tLog, err := p.tManager.GetLog(ctx.ID)
	if err != nil {
		return err
	}
	// Todo: Error handling?
	// TODO: Log validation before applying
	err = errors.Join(err, p.applyLogs(tLog))

	// Currently, we do not expect errors from lock release
	_ = p.lManager.ReleaseLocks(ctx.Txn.tId)
	err = errors.Join(err, p.tManager.EndTransaction(ctx.ID))
	ctx.Txn = nil
	return err
}

func (p *AsyncDB) abortTransaction(ctx *ConnectionContext) error {
	ctx.TxnMu.Lock()
	defer ctx.TxnMu.Unlock()
	if ctx.Txn == nil {
		return ErrConnNotInXact
	}
	ctx.Txn.mode = Aborting

	err := p.lManager.ReleaseLocks(ctx.Txn.tId)

	//todo: maybe need a wait here?
	ctx.Txn.acts.Wait()

	ts := ctx.Txn.ts

	err = errors.Join(err, p.tManager.EndTransaction(ctx.ID))
	//ctx.Txn.mode = Active
	tId, xactErr := p.tManager.StartTransaction(ctx.ID)
	ctx.Txn = &TransactInfo{tId: tId, mode: Ready, ts: ts, acts: &sync.WaitGroup{}}
	return errors.Join(err, xactErr)
}

func (p *AsyncDB) RollbackTransaction(ctx *ConnectionContext) error {
	ctx.TxnMu.Lock()
	defer ctx.TxnMu.Unlock()
	if ctx.Txn == nil {
		return ErrConnNotInXact
	}
	ctx.Txn.mode = Aborting

	// Todo: Figure out how to cancel queries, instead of waiting for them to finish
	err := p.lManager.ReleaseLocks(ctx.Txn.tId)

	// Todo: maybe need a wait here?
	ctx.Txn.acts.Wait()

	err = errors.Join(err, p.tManager.EndTransaction(ctx.ID))
	ctx.Txn = nil
	return err
}

func (p *AsyncDB) Put(ctx *ConnectionContext, tableName string, key interface{}, value interface{}) <-chan databases.RequestResult {
	resultChan := make(chan databases.RequestResult, 1)
	go func() {
		// If the connection is not in a transaction and implicit transactions are allowed - start a transaction
		implTransaction := false
		transactionAborted := false
		ctx.TxnMu.RLock()
		if ctx.Txn == nil {
			if !p.withImplicitTxn {
				resultChan <- databases.RequestResult{
					Data: nil,
					Err:  ErrConnNotInXact,
				}
				return
			}
			txnId, err := p.tManager.StartTransaction(ctx.ID)
			if err != nil {
				resultChan <- databases.RequestResult{
					Data: nil,
					Err:  errors.Join(fmt.Errorf("error with implicit transaction"), err),
				}
				return
			}
			implTransaction = true
			ctx.Txn = &TransactInfo{
				tId:  txnId,
				mode: Active,
				ts:   time.Now().UnixNano(),
				acts: &sync.WaitGroup{},
			}
		}
		if ctx.Txn.mode == Committing || ctx.Txn.mode == Aborting {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  ErrXactInTerminalState,
			}
			return
		}
		ctx.Txn.acts.Add(1)
		defer func() {
			if !implTransaction && !transactionAborted {
				ctx.Txn.acts.Done()
			}
		}()
		ctx.TxnMu.RUnlock()
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

		tLog, err := p.tManager.GetLog(ctx.ID)
		err = p.lManager.Lock(WriteLock, ctx.Txn.tId, ctx.Txn.ts, TableId(hash), key)
		// TODO: Change this logic
		// Locks are released only when the transaction is aborted
		// This is temporary, in the future we need a better way of handling this
		if errors.Is(err, ErrLocksReleased) {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  ErrXactInTerminalState,
			}
			return
		}
		if err != nil {
			// Todo: Same as above, logging
			transactionAborted = true
			ctx.Txn.acts.Done()
			_ = p.abortTransaction(ctx)
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
		if implTransaction {
			ctx.Txn.acts.Done()
			_ = p.CommitTransaction(ctx)
		}
		resultChan <- databases.RequestResult{
			Data: nil,
			Err:  err,
		}
	}()
	return resultChan
}

func (p *AsyncDB) Get(ctx *ConnectionContext, tableName string, key interface{}) <-chan databases.RequestResult {
	resultChan := make(chan databases.RequestResult, 1)
	go func() {
		implTransaction := false
		transactionAborted := false
		ctx.TxnMu.RLock()
		if ctx.Txn == nil {
			if !p.withImplicitTxn {
				resultChan <- databases.RequestResult{
					Data: nil,
					Err:  ErrConnNotInXact,
				}
				return
			}
			txnId, err := p.tManager.StartTransaction(ctx.ID)
			if err != nil {
				resultChan <- databases.RequestResult{
					Data: nil,
					Err:  errors.Join(fmt.Errorf("error with implicit transaction"), err),
				}
				return
			}
			implTransaction = true
			ctx.Txn = &TransactInfo{
				tId:  txnId,
				mode: Active,
				ts:   time.Now().UnixNano(),
				acts: &sync.WaitGroup{},
			}
		}
		if ctx.Txn.mode == Committing || ctx.Txn.mode == Aborting {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  ErrXactInTerminalState,
			}
			return
		}
		ctx.Txn.acts.Add(1)
		defer func() {
			if !implTransaction && !transactionAborted {
				ctx.Txn.acts.Done()
			}
		}()
		ctx.TxnMu.RUnlock()

		hash := p.hasher.HashStringUint64(tableName)

		// Write lock even for Read operations because they are easier to reason about
		err := p.lManager.Lock(WriteLock, ctx.Txn.tId, ctx.Txn.ts, TableId(hash), key)
		// TODO: Change this logic
		// Locks are released only when the transaction is aborted
		// This is temporary, in the future we need a better way of handling this
		if errors.Is(err, ErrLocksReleased) {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  ErrXactInTerminalState,
			}
			return
		}
		if err != nil {
			// Todo: Same as above, logging
			transactionAborted = true
			ctx.Txn.acts.Done()
			_ = p.abortTransaction(ctx)
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
			return
		}
		if res, found := log.findLastValue(hash, key); found {
			resultChan <- databases.RequestResult{
				Data: res,
				Err:  nil,
			}
			return
		}
		res := <-p.getValue(ctx, tableName, key)

		if implTransaction {
			ctx.Txn.acts.Done()
			_ = p.CommitTransaction(ctx)
		}
		resultChan <- databases.RequestResult{
			Data: res.Data,
			Err:  errors.Join(err, res.Err),
		}
	}()
	return resultChan
}

func (p *AsyncDB) Delete(ctx *ConnectionContext, tableName string, key interface{}) <-chan databases.RequestResult {
	resultChan := make(chan databases.RequestResult, 1)
	go func() {
		implTransaction := false
		transactionAborted := false
		ctx.TxnMu.RLock()
		if ctx.Txn == nil {
			if !p.withImplicitTxn {
				resultChan <- databases.RequestResult{
					Data: nil,
					Err:  ErrConnNotInXact,
				}
				return
			}
			txnId, err := p.tManager.StartTransaction(ctx.ID)
			if err != nil {
				resultChan <- databases.RequestResult{
					Data: nil,
					Err:  errors.Join(fmt.Errorf("error with implicit transaction"), err),
				}
				return
			}
			implTransaction = true
			ctx.Txn = &TransactInfo{
				tId:  txnId,
				mode: Active,
				ts:   time.Now().UnixNano(),
				acts: &sync.WaitGroup{},
			}
		}
		hash := p.hasher.HashStringUint64(tableName)
		if ctx.Txn.mode == Committing || ctx.Txn.mode == Aborting {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  ErrXactInTerminalState,
			}
			return
		}
		ctx.Txn.acts.Add(1)
		defer func() {
			if !implTransaction && !transactionAborted {
				ctx.Txn.acts.Done()
			}
		}()
		ctx.TxnMu.RUnlock()
		tLog, err := p.tManager.GetLog(ctx.ID)
		err = p.lManager.Lock(WriteLock, ctx.Txn.tId, ctx.Txn.ts, TableId(hash), key)
		// TODO: Change this logic
		// Locks are released only when the transaction is aborted
		// This is temporary, in the future we need a better way of handling this
		if errors.Is(err, ErrLocksReleased) {
			resultChan <- databases.RequestResult{
				Data: nil,
				Err:  ErrXactInTerminalState,
			}
			return
		}
		if err != nil {
			// Todo: Same as above, logging
			transactionAborted = true
			ctx.Txn.acts.Done()
			_ = p.abortTransaction(ctx)
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

			return
		}

		tLog.addAction(Action{
			Op:      LDelete,
			tableId: hash,
			Key:     key,
			Value:   nil,
		})
		if implTransaction {
			ctx.Txn.acts.Done()
			_ = p.CommitTransaction(ctx)
		}
		resultChan <- databases.RequestResult{
			Data: nil,
			Err:  err,
		}
	}()
	return resultChan
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
