package asyncdb

import (
	"errors"
	"github.com/google/uuid"
	"math"
	"slices"
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

type LockManager interface {
	Lock(lockType int, tid TransactId, ts int64, tableId TableId, key interface{}) error
	ReleaseLocks(tid TransactId) error
}

type LockInfo struct {
	key      interface{}
	lockType int
}

type TransactInfo struct {
	tId TransactId
	ts  int64
}

type LockWaiter struct {
	TInfo    *TransactInfo
	LockType int
	Chan     chan error
}

type ObjectLock struct {
	WLock *TransactInfo
	RLock []*TransactInfo
	Queue []*LockWaiter
	m     *sync.Mutex
}

type LockTable struct {
	Locks map[interface{}]*ObjectLock
	m     *sync.Mutex
}

type LockManagerImpl struct {
	lockMap     map[TableId]LockTable
	transactMap map[TransactId]map[TableId][]LockInfo
	m           *sync.Mutex
}

func NewLockManager() *LockManagerImpl {
	lm := &LockManagerImpl{
		lockMap:     make(map[TableId]LockTable),
		transactMap: make(map[TransactId]map[TableId][]LockInfo),
		m:           &sync.Mutex{},
	}
	return lm
}

func (lm *LockManagerImpl) AddLockInfo(tid TransactId, tableId TableId, info LockInfo) {
	lm.m.Lock()
	defer lm.m.Unlock()
	if _, ok := lm.transactMap[tid]; !ok {
		lm.transactMap[tid] = make(map[TableId][]LockInfo)
	}
	lm.transactMap[tid][tableId] = append(lm.transactMap[tid][tableId], info)
}

func (lm *LockManagerImpl) Lock(lockType int, tid TransactId, ts int64, tableId TableId, key interface{}) error {
	// Todo: Check if the transaction is already holding the lock
	if _, ok := lm.lockMap[tableId]; !ok {
		lm.m.Lock()
		if _, ok := lm.lockMap[tableId]; !ok {
			lm.lockMap[tableId] = LockTable{
				Locks: make(map[interface{}]*ObjectLock),
				m:     &sync.Mutex{},
			}
		}
		lm.m.Unlock()
	}
	if _, ok := lm.lockMap[tableId].Locks[key]; !ok {
		lm.lockMap[tableId].m.Lock()
		if _, ok := lm.lockMap[tableId].Locks[key]; !ok {
			lm.lockMap[tableId].Locks[key] = &ObjectLock{
				WLock: &TransactInfo{TransactId(uuid.Nil), 0},
				RLock: make([]*TransactInfo, 0),
				Queue: make([]*LockWaiter, 0),
				m:     &sync.Mutex{},
			}
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
			ol.RLock = append(rl, &TransactInfo{tId: tid, ts: ts})
			lm.AddLockInfo(tid, tableId, LockInfo{key: key, lockType: ReadLock})
			return nil
		}
		if wl.ts < ts {
			ol.Queue = append(ol.Queue, &LockWaiter{
				TInfo:    &TransactInfo{tId: tid, ts: ts},
				LockType: ReadLock,
				Chan:     res,
			})
			return <-res
		}
		return ErrLockConflict
	} else if lockType == WriteLock {
		if wl.tId == TransactId(uuid.Nil) {
			if len(rl) == 0 || (len(rl) == 1 && rl[0].tId == tid) {
				wl = &TransactInfo{tId: tid, ts: ts}
				lm.AddLockInfo(tid, tableId, LockInfo{key: key, lockType: WriteLock})
				return nil
			}
			if readMinTs >= ts {
				return ErrLockConflict
			}
			ol.Queue = append(ol.Queue, &LockWaiter{
				TInfo:    &TransactInfo{tId: tid, ts: ts},
				LockType: WriteLock,
				Chan:     res,
			})
		}
		if wl.tId == tid {
			return nil
		}
		if wl.ts >= ts {
			return ErrLockConflict
		}
		ol.Queue = append(ol.Queue, &LockWaiter{
			TInfo:    &TransactInfo{tId: tid, ts: ts},
			LockType: WriteLock,
			Chan:     res,
		})
	} else {
		return ErrInvalidLockType
	}
	panic("unreachable")
}

func (lm *LockManagerImpl) ReleaseLocks(tid TransactId) error {
	lm.m.Lock()
	defer lm.m.Unlock()
	if _, ok := lm.transactMap[tid]; !ok {
		return nil
	}
	transactLocks := lm.transactMap[tid]
	for tableId, locks := range transactLocks {
		if _, ok := lm.lockMap[tableId]; !ok {
			continue
		}
		table := lm.lockMap[tableId]
		table.m.Lock()
		for _, lock := range locks {
			if _, ok := table.Locks[lock.key]; !ok {
				continue
			}
			ol := table.Locks[lock.key]
			ol.m.Lock()
			if ol.WLock.tId == tid {
				ol.WLock = &TransactInfo{
					TransactId(uuid.Nil),
					0,
				}
			}
			for i, r := range ol.RLock {
				if r.tId == tid {
					// Optimized way to remove element from slice
					ol.RLock[i] = ol.RLock[len(ol.RLock)-1]
					ol.RLock = ol.RLock[:len(ol.RLock)-1]
				}
			}
			for i, waiter := range ol.Queue {
				if waiter.TInfo.tId == tid {
					slices.Delete(ol.Queue, i, i+1)
				}
			}

			if len(ol.Queue) > 0 {
				waiter := ol.Queue[0]
				if waiter.LockType == ReadLock {
					wlock := ol.WLock
					if wlock.tId == TransactId(uuid.Nil) {
						ol.RLock = append(ol.RLock, waiter.TInfo)
						ol.Queue = ol.Queue[1:]
						lm.AddLockInfo(waiter.TInfo.tId, tableId, LockInfo{key: lock.key, lockType: ReadLock})
						waiter.Chan <- nil
					} else if wlock.ts < waiter.TInfo.ts {
						ol.Queue = ol.Queue[1:]
						waiter.Chan <- ErrLockConflict
					}
				} else {
					wlock := ol.WLock
					rlock := ol.RLock
					var minReadTs int64 = math.MaxInt64
					for _, r := range rlock {
						minReadTs = min(minReadTs, r.ts)
					}
					if wlock.tId == TransactId(uuid.Nil) {
						if len(rlock) == 0 || (len(rlock) == 1 && rlock[0].tId == waiter.TInfo.tId) {
							ol.WLock = waiter.TInfo
							ol.Queue = ol.Queue[1:]
							lm.AddLockInfo(waiter.TInfo.tId, tableId, LockInfo{key: lock.key, lockType: WriteLock})
							waiter.Chan <- nil
						} else if waiter.TInfo.ts < minReadTs {
							ol.Queue = ol.Queue[1:]
							waiter.Chan <- ErrLockConflict
						}
					} else {
						if wlock.ts < waiter.TInfo.ts {
							ol.Queue = ol.Queue[1:]
							waiter.Chan <- ErrLockConflict
						}
					}
				}
			}
			ol.m.Unlock()
		}
		table.m.Unlock()
	}
	return nil
}
