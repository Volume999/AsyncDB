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

//type LockTable struct {
//	Locks map[interface{}]*ObjectLock
//	m     *sync.Mutex
//}

type LockTable struct {
	Locks *ThreadSafeMap[interface{}, *ObjectLock]
	m     *sync.Mutex
}

type LockInfo struct {
	key      interface{}
	lockType int
}

//type LockManagerImpl struct {
//	lockMap     map[TableId]LockTable
//	transactMap map[TransactId]map[TableId][]LockInfo
//	m           *sync.Mutex
//}

type LockManagerImpl struct {
	lockMap          *ThreadSafeMap[TableId, *LockTable]
	transactMap      *ThreadSafeMap[TransactId, *ThreadSafeMap[TableId, []LockInfo]]
	transactReleased *ThreadSafeMap[TransactId, bool]
}

//func NewLockManager() *LockManagerImpl {
//	lm := &LockManagerImpl{
//		lockMap:     make(map[TableId]LockTable),
//		transactMap: make(map[TransactId]map[TableId][]LockInfo),
//		m:           &sync.Mutex{},
//	}
//	return lm
//}

func NewLockManager() *LockManagerImpl {
	lm := &LockManagerImpl{
		lockMap:          NewThreadSafeMap[TableId, *LockTable](),
		transactMap:      NewThreadSafeMap[TransactId, *ThreadSafeMap[TableId, []LockInfo]](),
		transactReleased: NewThreadSafeMap[TransactId, bool](),
	}
	return lm
}

func (lm *LockManagerImpl) addLockInfoIfNotExists(tid TransactId, tableId TableId, info LockInfo) {
	lm.transactMap.Lock()
	if _, ok := lm.transactMap.GetUnsafe(tid); !ok {
		lm.transactMap.PutUnsafe(tid, NewThreadSafeMap[TableId, []LockInfo]())
	}
	defer lm.transactMap.Unlock()
	//if !slices.Contains(lm.transactMap.m[tid][tableId], info) {
	//	lm.transactMap[tid][tableId] = append(lm.transactMap[tid][tableId], info)
	//}
	lm.transactMap.m[tid].Lock()
	infos, _ := lm.transactMap.m[tid].GetUnsafe(tableId)
	if !slices.Contains(infos, info) {
		infos = append(infos, info)
		lm.transactMap.m[tid].PutUnsafe(tableId, infos)
	}
	lm.transactMap.m[tid].Unlock()
}

func (lm *LockManagerImpl) addTableIfNotExists(tableId TableId) {
	lm.lockMap.Lock()
	defer lm.lockMap.Unlock()
	if _, ok := lm.lockMap.GetUnsafe(tableId); ok {
		return
	}
	lm.lockMap.PutUnsafe(tableId, &LockTable{
		Locks: NewThreadSafeMap[interface{}, *ObjectLock](),
		m:     &sync.Mutex{},
	})
}

func (lm *LockManagerImpl) addTableKeyIfNotExists(tableId TableId, key interface{}) {
	// We assume that the table already exists
	table, _ := lm.lockMap.Get(tableId)
	locks := table.Locks
	locks.Lock()
	defer locks.Unlock()
	if _, ok := locks.GetUnsafe(key); ok {
		return
	}
	locks.PutUnsafe(key, &ObjectLock{
		WLock: &Transaction{TransactId(uuid.Nil), 0},
		RLock: make([]*Transaction, 0),
		Queue: make([]*LockWaiter, 0),
		m:     &sync.Mutex{},
	})
}

func (lm *LockManagerImpl) Lock(lockType int, tid TransactId, ts int64, tableId TableId, key interface{}) error {
	// Todo: Check if the transaction is already holding the lock
	lm.addTableIfNotExists(tableId)
	lm.addTableKeyIfNotExists(tableId, key)
	lm.addLockInfoIfNotExists(tid, tableId, LockInfo{key: key, lockType: lockType})
	if _, ok := lm.transactReleased.Get(tid); ok {
		return ErrLocksReleased
	}
	xact := &Transaction{tId: tid, ts: ts}
	table, _ := lm.lockMap.Get(tableId)
	//ol := lm.lockMap[tableId].Locks[key]
	ol, _ := table.Locks.Get(key)
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
		if _, ok := lm.transactReleased.Get(wl.tId); ok || wl.tId == TransactId(uuid.Nil) {
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
				xact:     xact,
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
	//lm.lockMap.Lock()
	//if _, ok := lm.transactMap[tid]; !ok {
	//	return nil
	//}
	//transactLocks := lm.transactMap[tid]
	//delete(lm.transactMap, tid)
	//lm.m.Unlock()
	lm.transactReleased.Put(tid, true)
	lm.transactMap.Lock()
	transactLocks, ok := lm.transactMap.GetUnsafe(tid)
	lm.transactMap.DeleteUnsafe(tid)
	lm.transactMap.Unlock()
	if !ok {
		return nil
	}
	transactLocks.Lock()
	for tableId, locks := range transactLocks.m {
		//if _, ok := lm.lockMap[tableId]; !ok {
		//	continue
		//}
		//table := lm.lockMap[tableId]
		table, ok := lm.lockMap.Get(tableId)
		if !ok {
			continue
		}
		table.Locks.Lock()
		for _, lock := range locks {
			ol, ok := table.Locks.GetUnsafe(lock.key)
			if !ok {
				continue
			}
			ol.m.Lock()
			if ol.WLock.tId == tid {
				ol.WLock = &Transaction{
					TransactId(uuid.Nil),
					0,
				}
			}
			// Todo: Not the right way to delete from a slice
			for i, r := range ol.RLock {
				if r.tId == tid {
					// Optimized way to remove element from slice
					ol.RLock[i] = ol.RLock[len(ol.RLock)-1]
					ol.RLock = ol.RLock[:len(ol.RLock)-1]
				}
			}
			newQueue := make([]*LockWaiter, 0)
			for i, waiter := range ol.Queue {
				if waiter.xact.tId == tid {
					waiter.Chan <- ErrLocksReleased
				} else {
					newQueue = append(newQueue, ol.Queue[i])
				}
			}
			ol.Queue = newQueue

			// Check the queue for the lock
			if len(ol.Queue) > 0 {
				waiter := ol.Queue[0]
				if waiter.LockType == ReadLock {
					wlock := ol.WLock
					if wlock.tId == TransactId(uuid.Nil) {
						ol.RLock = append(ol.RLock, waiter.xact)
						ol.Queue = ol.Queue[1:]
						waiter.Chan <- nil
					} else if wlock.isOlderThan(waiter.xact) {
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
					if _, ok := lm.transactReleased.Get(wlock.tId); ok || wlock.tId == TransactId(uuid.Nil) {
						if len(rlock) == 0 || (len(rlock) == 1 && rlock[0].tId == waiter.xact.tId) {
							ol.WLock = waiter.xact
							ol.Queue = ol.Queue[1:]
							waiter.Chan <- nil
						} else if waiter.xact.ts < minReadTs {
							ol.Queue = ol.Queue[1:]
							waiter.Chan <- ErrLockConflict
						}
					} else {
						if wlock.isOlderThan(waiter.xact) {
							ol.Queue = ol.Queue[1:]
							waiter.Chan <- ErrLockConflict
						}
					}
				}
			}
			ol.m.Unlock()
		}
		table.Locks.Unlock()
	}
	transactLocks.Unlock()
	return nil
}
