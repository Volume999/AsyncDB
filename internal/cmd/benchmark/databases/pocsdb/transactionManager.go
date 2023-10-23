package pocsdb

import (
	"POCS_Projects/internal/models"
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
	tLog      TransactionLog
	tLogMutex sync.Mutex
}

type Action struct {
	Op       int
	DataType interface{}
	Key      interface{}
	Value    interface{}
}

type LogEntry[T any] struct {
	Op    int
	Value T
}

type TransactionLog struct {
	WarehouseLog map[models.WarehousePK]LogEntry[models.Warehouse]
	StockLog     map[models.StockPK]LogEntry[models.Stock]
	OrderLog     map[models.OrderPK]LogEntry[models.Order]
	NewOrderLog  map[models.NewOrderPK]LogEntry[models.NewOrder]
	DistrictLog  map[models.DistrictPK]LogEntry[models.District]
	CustomerLog  map[models.CustomerPK]LogEntry[models.Customer]
	ItemLog      map[models.ItemPK]LogEntry[models.Item]
	OrderLineLog map[models.OrderLinePK]LogEntry[models.OrderLine]
	HistoryLog   map[models.HistoryPK]LogEntry[models.History]
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
	switch a.DataType.(type) {
	case models.Warehouse:
		txn.tLog.WarehouseLog[a.Key.(models.WarehousePK)] = LogEntry[models.Warehouse]{Op: a.Op, Value: a.Value.(models.Warehouse)}
	case models.Stock:
		txn.tLog.StockLog[a.Key.(models.StockPK)] = LogEntry[models.Stock]{Op: a.Op, Value: a.Value.(models.Stock)}
	case models.Order:
		txn.tLog.OrderLog[a.Key.(models.OrderPK)] = LogEntry[models.Order]{Op: a.Op, Value: a.Value.(models.Order)}
	case models.NewOrder:
		txn.tLog.NewOrderLog[a.Key.(models.NewOrderPK)] = LogEntry[models.NewOrder]{Op: a.Op, Value: a.Value.(models.NewOrder)}
	case models.District:
		txn.tLog.DistrictLog[a.Key.(models.DistrictPK)] = LogEntry[models.District]{Op: a.Op, Value: a.Value.(models.District)}
	case models.Customer:
		txn.tLog.CustomerLog[a.Key.(models.CustomerPK)] = LogEntry[models.Customer]{Op: a.Op, Value: a.Value.(models.Customer)}
	case models.Item:
		txn.tLog.ItemLog[a.Key.(models.ItemPK)] = LogEntry[models.Item]{Op: a.Op, Value: a.Value.(models.Item)}
	case models.OrderLine:
		txn.tLog.OrderLineLog[a.Key.(models.OrderLinePK)] = LogEntry[models.OrderLine]{Op: a.Op, Value: a.Value.(models.OrderLine)}
	case models.History:
		txn.tLog.HistoryLog[a.Key.(models.HistoryPK)] = LogEntry[models.History]{Op: a.Op, Value: a.Value.(models.History)}
	}
}

func (t *TransactionManager) GetLog(ConnId uuid.UUID) (TransactionLog, error) {
	if _, ok := t.tLogs[ConnId]; !ok {
		return TransactionLog{}, ErrXactNotFound
	}
	return t.tLogs[ConnId].tLog, nil
}
