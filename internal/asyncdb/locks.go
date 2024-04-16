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
var ErrLocksReleased = errors.New("locks released")

type ConnId uuid.UUID
type TransactId uuid.UUID
type TableId uint64

type LockManager interface {
	Lock(lockType int, tid TransactId, ts int64, tableId TableId, key interface{}) error
	ReleaseLocks(tid TransactId) error
}

type Transaction struct {
	tId TransactId
	ts  int64
}

func (t *Transaction) isOlderThan(other *Transaction) bool {
	return t.ts < other.ts
}

type LockWaiter struct {
	xact     *Transaction
	LockType int
	Chan     chan error
}

type ObjectLock struct {
	WLock *Transaction
	RLock []*Transaction
	Queue []*LockWaiter
	m     *sync.Mutex
}

type LockTable struct {
	Locks map[interface{}]*ObjectLock
	m     *sync.Mutex
}

type LockInfo struct {
	key      interface{}
	lockType int
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

func (lm *LockManagerImpl) addLockInfo(tid TransactId, tableId TableId, info LockInfo) {
	lm.m.Lock()
	defer lm.m.Unlock()
	if _, ok := lm.transactMap[tid]; !ok {
		lm.transactMap[tid] = make(map[TableId][]LockInfo)
	}
	lm.transactMap[tid][tableId] = append(lm.transactMap[tid][tableId], info)
}

func (lm *LockManagerImpl) addTableIfNotExists(tableId TableId) {
	if _, ok := lm.lockMap[tableId]; ok {
		return
	}
	lm.m.Lock()
	defer lm.m.Unlock()
	lm.lockMap[tableId] = LockTable{
		Locks: make(map[interface{}]*ObjectLock),
		m:     &sync.Mutex{},
	}
}

func (lm *LockManagerImpl) addTableKeyIfNotExists(tableId TableId, key interface{}) {
	if _, ok := lm.lockMap[tableId].Locks[key]; ok {
		return
	}
	lm.lockMap[tableId].m.Lock()
	defer lm.lockMap[tableId].m.Unlock()
	lm.lockMap[tableId].Locks[key] = &ObjectLock{
		WLock: &Transaction{TransactId(uuid.Nil), 0},
		RLock: make([]*Transaction, 0),
		Queue: make([]*LockWaiter, 0),
		m:     &sync.Mutex{},
	}
}

func (lm *LockManagerImpl) Lock(lockType int, tid TransactId, ts int64, tableId TableId, key interface{}) error {
	// Todo: Check if the transaction is already holding the lock
	lm.addTableIfNotExists(tableId)
	lm.addTableKeyIfNotExists(tableId, key)
	lm.addLockInfo(tid, tableId, LockInfo{key: key, lockType: lockType})
	xact := &Transaction{tId: tid, ts: ts}
	ol := lm.lockMap[tableId].Locks[key]
	ol.m.Lock()
	wl := ol.WLock
	rl := ol.RLock
	readTids := make(map[TransactId]bool)
	youngerReadExists := false
	for _, r := range rl {
		readTids[r.tId] = true
		youngerReadExists = youngerReadExists || xact.isOlderThan(r)
	}
	res := make(chan error)
	if lockType == ReadLock {
		if _, ok := readTids[tid]; ok {
			ol.m.Unlock()
			return nil
		}
		if wl.tId == TransactId(uuid.Nil) {
			ol.RLock = append(rl, xact)
			ol.m.Unlock()
			return nil
		}
		if xact.isOlderThan(wl) {
			ol.Queue = append(ol.Queue, &LockWaiter{
				xact:     xact,
				LockType: ReadLock,
				Chan:     res,
			})
			ol.m.Unlock()
			return <-res
		}
		ol.m.Unlock()
		return ErrLockConflict
	} else if lockType == WriteLock {
		if wl.tId == TransactId(uuid.Nil) {
			if len(rl) == 0 || (len(rl) == 1 && rl[0].tId == tid) {
				ol.WLock = xact
				ol.m.Unlock()
				return nil
			}
			if youngerReadExists {
				ol.Queue = append(ol.Queue, &LockWaiter{
					xact:     xact,
					LockType: WriteLock,
					Chan:     res,
				})
				ol.m.Unlock()
				return <-res
			}
			ol.m.Unlock()
			return ErrLockConflict
		}
		if wl.tId == tid {
			ol.m.Unlock()
			return nil
		}
		if xact.isOlderThan(wl) {
			ol.Queue = append(ol.Queue, &LockWaiter{
				xact:     &Transaction{tId: tid, ts: ts},
				LockType: WriteLock,
				Chan:     res,
			})
			ol.m.Unlock()
			return <-res
		}
		ol.m.Unlock()
		return ErrLockConflict
	} else {
		return ErrInvalidLockType
	}
}

func (lm *LockManagerImpl) ReleaseLocks(tid TransactId) error {
	lm.m.Lock()
	if _, ok := lm.transactMap[tid]; !ok {
		return nil
	}
	transactLocks := lm.transactMap[tid]
	lm.m.Unlock()
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
				ol.WLock = &Transaction{
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
				if waiter.xact.tId == tid {
					waiter.Chan <- ErrLocksReleased
					ol.Queue = slices.Delete(ol.Queue, i, i+1)
				}
			}

			if len(ol.Queue) > 0 {
				waiter := ol.Queue[0]
				if waiter.LockType == ReadLock {
					wlock := ol.WLock
					if wlock.tId == TransactId(uuid.Nil) {
						ol.RLock = append(ol.RLock, waiter.xact)
						ol.Queue = ol.Queue[1:]
						waiter.Chan <- nil
					} else if wlock.ts > waiter.xact.ts {
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
						if len(rlock) == 0 || (len(rlock) == 1 && rlock[0].tId == waiter.xact.tId) {
							ol.WLock = waiter.xact
							ol.Queue = ol.Queue[1:]
							waiter.Chan <- nil
						} else if waiter.xact.ts < minReadTs {
							ol.Queue = ol.Queue[1:]
							waiter.Chan <- ErrLockConflict
						}
					} else {
						if wlock.ts > waiter.xact.ts {
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
