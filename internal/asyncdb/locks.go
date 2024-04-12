package asyncdb

import "github.com/google/uuid"

type ConnId uuid.UUID
type TransactId uuid.UUID
type TableId uint64

type TransactionStatusChecker interface {
	TransactionStatusCheck(tid TransactId) bool
}

type LockManager interface {
	Lock(tid TransactId, tableId TableId, key interface{}) error
	ReleaseLocks(tid TransactId) error
}

type LockInfo struct {
	tId TransactId
	ts  int
}

type LockWaiter struct {
	Info LockInfo
	Chan chan error
}

type LockTable struct {
	WLock LockInfo
	RLock []LockInfo
	Queue []LockWaiter
}

type LockManagerImpl struct {
	lockMap map[TableId]map[interface{}]LockInfo
}

func (m LockManagerImpl) Lock(tid TransactId, tableId TableId, key interface{}) error {
	return nil
}

func (m LockManagerImpl) ReleaseLocks(tid TransactId) error {
	return nil
}

func NewLockManager() *LockManagerImpl {
	return &LockManagerImpl{}
}
