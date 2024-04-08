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
	txnID     uuid.UUID
	tLog      *TransactionLog
	tLogMutex *sync.Mutex
}

type Action struct {
	Op        int
	tableName string
	Key       interface{}
	Value     interface{}
}

type LogEntry struct {
	Op    int
	Key   interface{}
	Value interface{}
}

type TransactionLog struct {
	l map[uint64][]LogEntry
}

type TransactionManager interface {
	BeginTransaction(ConnId uuid.UUID) (*Txn, error)
	DeleteLog(ConnId uuid.UUID) error
	GetLog(ConnId uuid.UUID) (*TransactionLog, error)
}

type TransactionManagerImpl struct {
	tLogs map[uuid.UUID]*Txn
}

func NewTransactionManager() *TransactionManagerImpl {
	return &TransactionManagerImpl{
		tLogs: make(map[uuid.UUID]*Txn),
	}
}

func (t *TransactionManagerImpl) BeginTransaction(ConnId uuid.UUID) (*Txn, error) {
	if _, ok := t.tLogs[ConnId]; ok {
		return nil, ErrConnInXact
	}
	txn := &Txn{
		txnID: uuid.New(),
		tLog: &TransactionLog{
			l: make(map[uint64][]LogEntry),
		},
		tLogMutex: &sync.Mutex{},
	}
	t.tLogs[ConnId] = txn
	return txn, nil
}

func (t *TransactionManagerImpl) DeleteLog(ConnId uuid.UUID) error {
	if _, ok := t.tLogs[ConnId]; !ok {
		return ErrXactNotFound
	}
	delete(t.tLogs, ConnId)
	return nil
}

func (t *TransactionLog) addAction(a Action) {
	hash := HashStringUint64(a.tableName)
	t.l[hash] = append(t.l[hash], LogEntry{Op: a.Op, Value: a.Value, Key: a.Key})
}

func (t *TransactionManagerImpl) GetLog(ConnId uuid.UUID) (*TransactionLog, error) {
	// TODO: Do we need this function?
	if _, ok := t.tLogs[ConnId]; !ok {
		return &TransactionLog{}, ErrXactNotFound
	}
	return t.tLogs[ConnId].tLog, nil
}
