package asyncdb

import (
	"errors"
	"github.com/google/uuid"
	"sync"
)

const (
	ReadLock = 1 + iota
	WriteLock
)

var ErrLockConflict = errors.New("lock conflict")

type ConnId uuid.UUID
type TransactId uuid.UUID
type TableId uint64

type TransactionStatusChecker interface {
	TransactionStatusCheck(tid TransactId) bool
}

type LockManager interface {
	Lock(lockType int, tid TransactId, ts int64, tableId TableId, key interface{}) error
	ReleaseLocks(tid TransactId) error
}

type LockInfo struct {
	tId TransactId
	ts  int64
}

type LockWaiter struct {
	Info LockInfo
	Chan chan error
}

type ObjectLock struct {
	WLock LockInfo
	RLock []LockInfo
	Queue []LockWaiter
}

type LockTable struct {
	Locks map[interface{}]ObjectLock
	m     *sync.Mutex
}

type LockManagerImpl struct {
	lockMap map[TableId]LockTable
	m       *sync.Mutex
}

func (lm *LockManagerImpl) Lock(lockType int, tid TransactId, ts int64, tableId TableId, key interface{}) error {
	if _, ok := lm.lockMap[tableId]; !ok {
		lm.m.Lock()
		if _, ok := lm.lockMap[tableId]; !ok {
			lm.lockMap[tableId] = LockTable{
				Locks: make(map[interface{}]ObjectLock),
			}
		}
		lm.m.Unlock()
	}
	res := make(chan error)
	go func() {
		if _, ok := lm.lockMap[tableId].Locks[key]; !ok {
			lm.lockMap[tableId].m.Lock()
			if _, ok := lm.lockMap[tableId].Locks[key]; !ok {
				lm.lockMap[tableId].Locks[key] = ObjectLock{}
			}
			lm.lockMap[tableId].m.Unlock()
		}
		if lockType == ReadLock {
		}
	}()
	return <-res
}

func (lm *LockManagerImpl) ReleaseLocks(tid TransactId) error {
	return nil
}

func NewLockManager() *LockManagerImpl {
	return &LockManagerImpl{
		lockMap: make(map[TableId]LockTable),
	}
}
