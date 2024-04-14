package asyncdb

import (
	"errors"
	"github.com/google/uuid"
	"math"
	"sync"
)

const (
	ReadLock = 1 + iota
	WriteLock
)

var ErrLockConflict = errors.New("lock conflict")
var ErrInvalidLockType = errors.New("invalid lock type")

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
	m     *sync.Mutex
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
	if _, ok := lm.lockMap[tableId].Locks[key]; !ok {
		lm.lockMap[tableId].m.Lock()
		if _, ok := lm.lockMap[tableId].Locks[key]; !ok {
			lm.lockMap[tableId].Locks[key] = ObjectLock{}
		}
		lm.lockMap[tableId].m.Unlock()
	}
	ol := lm.lockMap[tableId].Locks[key]
	ol.m.Lock()
	defer ol.m.Unlock()
	wl := ol.WLock
	rl := ol.RLock
	readTids := make(map[TransactId]bool)
	var readMinTs int64 = math.MaxInt64
	for _, r := range rl {
		readTids[r.tId] = true
		readMinTs = min(readMinTs, r.ts)
	}
	res := make(chan error)
	if lockType == ReadLock {
		if _, ok := readTids[tid]; ok {
			return nil
		}
		if wl.tId == TransactId(uuid.Nil) {
			rl = append(rl, LockInfo{tId: tid, ts: ts})
			return nil
		}
		if wl.ts < ts {
			ol.Queue = append(ol.Queue, LockWaiter{
				Info: LockInfo{tId: tid, ts: ts},
				Chan: res,
			})
			return <-res
		}
		return ErrLockConflict
	} else if lockType == WriteLock {
		if wl.tId == TransactId(uuid.Nil) {
			if len(rl) == 0 || (len(rl) == 1 && rl[0].tId == tid) {
				wl = LockInfo{tId: tid, ts: ts}
				return nil
			}
			if readMinTs >= ts {
				return ErrLockConflict
			}
			ol.Queue = append(ol.Queue, LockWaiter{
				Info: LockInfo{tId: tid, ts: ts},
				Chan: res,
			})
		}
		if wl.tId == tid {
			return nil
		}
		if wl.ts >= ts {
			return ErrLockConflict
		}
		ol.Queue = append(ol.Queue, LockWaiter{
			Info: LockInfo{tId: tid, ts: ts},
			Chan: res,
		})
	} else {
		return ErrInvalidLockType
	}
	panic("unreachable")
}

func (lm *LockManagerImpl) ReleaseLocks(tid TransactId) error {
	return nil
}

func NewLockManager() *LockManagerImpl {
	lm := &LockManagerImpl{
		lockMap: make(map[TableId]LockTable),
	}
	return lm
}
