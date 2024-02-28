package asyncdb

import (
	"AsyncDB/internal/tpcc/models"
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
			WarehouseLog: make(map[models.WarehousePK]LogEntry[models.Warehouse]),
			StockLog:     make(map[models.StockPK]LogEntry[models.Stock]),
			OrderLog:     make(map[models.OrderPK]LogEntry[models.Order]),
			NewOrderLog:  make(map[models.NewOrderPK]LogEntry[models.NewOrder]),
			DistrictLog:  make(map[models.DistrictPK]LogEntry[models.District]),
			CustomerLog:  make(map[models.CustomerPK]LogEntry[models.Customer]),
			ItemLog:      make(map[models.ItemPK]LogEntry[models.Item]),
			OrderLineLog: make(map[models.OrderLinePK]LogEntry[models.OrderLine]),
			HistoryLog:   make(map[models.HistoryPK]LogEntry[models.History]),
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
	switch a.tableName {
	case "Warehouse":
		if a.Value == nil {
			t.WarehouseLog[a.Key.(models.WarehousePK)] = LogEntry[models.Warehouse]{Op: a.Op}
			return
		} else {
			t.WarehouseLog[a.Key.(models.WarehousePK)] = LogEntry[models.Warehouse]{Op: a.Op, Value: a.Value.(models.Warehouse)}
		}
	case "Stock":
		if a.Value == nil {
			t.StockLog[a.Key.(models.StockPK)] = LogEntry[models.Stock]{Op: a.Op}
			return
		} else {
			t.StockLog[a.Key.(models.StockPK)] = LogEntry[models.Stock]{Op: a.Op, Value: a.Value.(models.Stock)}
		}
	case "Order":
		if a.Value == nil {
			t.OrderLog[a.Key.(models.OrderPK)] = LogEntry[models.Order]{Op: a.Op}
			return
		} else {
			t.OrderLog[a.Key.(models.OrderPK)] = LogEntry[models.Order]{Op: a.Op, Value: a.Value.(models.Order)}
		}
	case "NewOrder":
		if a.Value == nil {
			t.NewOrderLog[a.Key.(models.NewOrderPK)] = LogEntry[models.NewOrder]{Op: a.Op}
			return
		} else {
			t.NewOrderLog[a.Key.(models.NewOrderPK)] = LogEntry[models.NewOrder]{Op: a.Op, Value: a.Value.(models.NewOrder)}
		}
	case "District":
		if a.Value == nil {
			t.DistrictLog[a.Key.(models.DistrictPK)] = LogEntry[models.District]{Op: a.Op}
			return
		} else {
			t.DistrictLog[a.Key.(models.DistrictPK)] = LogEntry[models.District]{Op: a.Op, Value: a.Value.(models.District)}
		}
	case "Customer":
		if a.Value == nil {
			t.CustomerLog[a.Key.(models.CustomerPK)] = LogEntry[models.Customer]{Op: a.Op}
			return
		} else {
			t.CustomerLog[a.Key.(models.CustomerPK)] = LogEntry[models.Customer]{Op: a.Op, Value: a.Value.(models.Customer)}
		}
	case "Item":
		if a.Value == nil {
			t.ItemLog[a.Key.(models.ItemPK)] = LogEntry[models.Item]{Op: a.Op}
			return
		} else {
			t.ItemLog[a.Key.(models.ItemPK)] = LogEntry[models.Item]{Op: a.Op, Value: a.Value.(models.Item)}
		}
	case "OrderLine":
		if a.Value == nil {
			t.OrderLineLog[a.Key.(models.OrderLinePK)] = LogEntry[models.OrderLine]{Op: a.Op}
			return
		} else {
			t.OrderLineLog[a.Key.(models.OrderLinePK)] = LogEntry[models.OrderLine]{Op: a.Op, Value: a.Value.(models.OrderLine)}
		}
	case "History":
		if a.Value == nil {
			t.HistoryLog[a.Key.(models.HistoryPK)] = LogEntry[models.History]{Op: a.Op}
			return
		} else {
			t.HistoryLog[a.Key.(models.HistoryPK)] = LogEntry[models.History]{Op: a.Op, Value: a.Value.(models.History)}
		}
	}
}

func (t *TransactionManagerImpl) GetLog(ConnId uuid.UUID) (*TransactionLog, error) {
	// TODO: Do we need this function?
	if _, ok := t.tLogs[ConnId]; !ok {
		return &TransactionLog{}, ErrXactNotFound
	}
	return t.tLogs[ConnId].tLog, nil
}
