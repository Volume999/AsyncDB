package pocsdb

import (
	"errors"
	"github.com/google/uuid"
)

const (
	LInsert = 1 + iota
	LDelete
	LUpdate
)

var (
	ErrConnInXact   = errors.New("connection in transaction")
	ErrXactNotFound = errors.New("transaction not found")
)

type Txn struct {
	txnID uuid.UUID
	tLog  TransactionLog
}

type Action struct {
	actionType int
	dataType   interface{}
	key        interface{}
	value      interface{}
	prevValue  interface{}
}

type TransactionLog struct {
	actionList []Action
}

type TransactionManager struct {
	tLogs map[uuid.UUID]*Txn
}

func NewTransactionManager() *TransactionManager {
	return &TransactionManager{
		tLogs: make(map[uuid.UUID]*Txn),
	}
}

func (t *TransactionManager) BeginTransaction(ConnId uuid.UUID) (*Txn, error) {
	if _, ok := t.tLogs[ConnId]; ok {
		return nil, ErrConnInXact
	}
	txn := &Txn{
		txnID: uuid.New(),
		tLog:  TransactionLog{},
	}
	t.tLogs[ConnId] = txn
	return txn, nil
}

func (t *TransactionManager) DeleteLog(ConnId uuid.UUID) error {
	if _, ok := t.tLogs[ConnId]; !ok {
		return ErrXactNotFound
	}
	delete(t.tLogs, ConnId)
	return nil
}

func (t *TransactionManager) addAction(txn *Txn, a Action) {
	txn.tLog.actionList = append(txn.tLog.actionList, a)
}

func (t *TransactionManager) GetLog(ConnId uuid.UUID) (TransactionLog, error) {
	if _, ok := t.tLogs[ConnId]; !ok {
		return TransactionLog{}, ErrXactNotFound
	}
	return t.tLogs[ConnId].tLog, nil
}
