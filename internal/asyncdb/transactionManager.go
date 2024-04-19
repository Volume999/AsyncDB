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
	tLogs map[uuid.UUID]*Txn
}

func NewTransactionManager() *TransactionManagerImpl {
	return &TransactionManagerImpl{
		tLogs: make(map[uuid.UUID]*Txn),
	}
}

func (t *TransactionManagerImpl) StartTransaction(ConnId uuid.UUID) (TransactId, error) {
	if _, ok := t.tLogs[ConnId]; ok {
		return TransactId(uuid.Nil), ErrConnInXact
	}
	txnId := TransactId(uuid.New())
	txn := &Txn{
		txnID: txnId,
		tLog: &TransactionLog{
			//l: make(map[uint64][]LogEntry),
			l: NewThreadSafeMap[uint64, []LogEntry](),
		},
		tLogMutex: &sync.Mutex{},
	}
	t.tLogs[ConnId] = txn
	return txnId, nil
}

func (t *TransactionManagerImpl) EndTransaction(ConnId uuid.UUID) error {
	if _, ok := t.tLogs[ConnId]; !ok {
		return ErrXactNotFound
	}
	delete(t.tLogs, ConnId)
	return nil
}

func (t *TransactionLog) addAction(a Action) {
	//t.l[a.tableId] = append(t.l[a.tableId], LogEntry{Op: a.Op, Value: a.Value, Key: a.Key})
	t.l.Lock()
	defer t.l.Unlock()
	entries, _ := t.l.GetUnsafe(a.tableId)
	entries = append(entries, LogEntry{Op: a.Op, Value: a.Value, Key: a.Key})
	t.l.PutUnsafe(a.tableId, entries)
}

func (t *TransactionManagerImpl) GetLog(ConnId uuid.UUID) (*TransactionLog, error) {
	// TODO: Do we need this function?
	if _, ok := t.tLogs[ConnId]; !ok {
		return &TransactionLog{}, ErrXactNotFound
	}
	return t.tLogs[ConnId].tLog, nil
}

func (t *TransactionManagerImpl) DeleteLog(ConnId uuid.UUID) error {
	txn, ok := t.tLogs[ConnId]
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
