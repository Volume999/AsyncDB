package asyncdb

import (
	"errors"
	"github.com/google/uuid"
	"sync"
)

const (
	LPut = 1 + iota
	LDelete
)

var (
	ErrConnInXact   = errors.New("connection in transaction")
	ErrXactNotFound = errors.New("transaction not found")
)

type Txn struct {
	txnID     TransactId
	tLog      *TransactionLog
	tLogMutex *sync.Mutex
}

type Action struct {
	Op      int
	tableId uint64
	Key     interface{}
	Value   interface{}
}

type LogEntry struct {
	Op    int
	Key   interface{}
	Value interface{}
}

type TransactionLog struct {
	l *ThreadSafeMap[uint64, []LogEntry]
}

type TransactionManager interface {
	StartTransaction(ConnId uuid.UUID) (TransactId, error)
	DeleteLog(ConnId uuid.UUID) error
	GetLog(ConnId uuid.UUID) (*TransactionLog, error)
	EndTransaction(ConnId uuid.UUID) error
}

type TransactionManagerImpl struct {
	tLogs *ThreadSafeMap[uuid.UUID, *Txn]
}

func NewTransactionManager() *TransactionManagerImpl {
	return &TransactionManagerImpl{
		tLogs: NewThreadSafeMap[uuid.UUID, *Txn](),
	}
}

func (t *TransactionManagerImpl) StartTransaction(ConnId uuid.UUID) (TransactId, error) {
	t.tLogs.Lock()
	defer t.tLogs.Unlock()
	if _, ok := t.tLogs.GetUnsafe(ConnId); ok {
		return TransactId(uuid.Nil), ErrConnInXact
	}
	txnId := TransactId(uuid.New())
	txn := &Txn{
		txnID: txnId,
		tLog: &TransactionLog{
			l: NewThreadSafeMap[uint64, []LogEntry](),
		},
		tLogMutex: &sync.Mutex{},
	}
	t.tLogs.PutUnsafe(ConnId, txn)
	return txnId, nil
}

func (t *TransactionManagerImpl) EndTransaction(ConnId uuid.UUID) error {
	t.tLogs.Lock()
	defer t.tLogs.Unlock()
	if _, ok := t.tLogs.GetUnsafe(ConnId); !ok {
		return ErrXactNotFound
	}
	t.tLogs.DeleteUnsafe(ConnId)
	return nil
}

func (t *TransactionLog) addAction(a Action) {
	t.l.Lock()
	defer t.l.Unlock()
	entries, _ := t.l.GetUnsafe(a.tableId)
	entries = append(entries, LogEntry{Op: a.Op, Value: a.Value, Key: a.Key})
	t.l.PutUnsafe(a.tableId, entries)
}

func (t *TransactionManagerImpl) GetLog(ConnId uuid.UUID) (*TransactionLog, error) {
	tLog, ok := t.tLogs.Get(ConnId)
	if !ok {
		return nil, ErrXactNotFound
	}
	return tLog.tLog, nil
}

func (t *TransactionManagerImpl) DeleteLog(ConnId uuid.UUID) error {
	txn, ok := t.tLogs.Get(ConnId)
	if !ok {
		return ErrXactNotFound
	}
	txn.tLogMutex.Lock()
	defer txn.tLogMutex.Unlock()
	txn.tLog = &TransactionLog{
		l: NewThreadSafeMap[uint64, []LogEntry](),
	}
	return nil
}
