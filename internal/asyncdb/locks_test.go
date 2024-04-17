package asyncdb

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

const lockConflictErr = "lock conflict"
const locksReleasedErr = "locks released"

func TestLockManagerImpl_Lock_Single_Lock_Should_Succeed(t *testing.T) {
	lm := NewLockManager()
	err := lm.Lock(WriteLock, TransactId(uuid.New()), 1, TableId(1), 1)
	assert.Nil(t, err)
}

func TestLockManagerImpl_Lock_Upgrade_Lock_Should_Succeed(t *testing.T) {
	lm := NewLockManager()
	tid := TransactId(uuid.New())
	_ = lm.Lock(ReadLock, tid, 1, TableId(1), 1)
	err := lm.Lock(WriteLock, tid, 1, TableId(1), 1)
	assert.Nil(t, err)
}

func TestLockManagerImpl_Lock_Double_Lock_Is_Noop(t *testing.T) {
	lm := NewLockManager()
	tid := TransactId(uuid.New())
	_ = lm.Lock(WriteLock, tid, 1, TableId(1), 1)
	err := lm.Lock(WriteLock, tid, 1, TableId(1), 1)
	assert.Nil(t, err)
}

func TestLockManagerImpl_Lock_Conflicting_Read_Lock_Should_Succeed(t *testing.T) {
	lm := NewLockManager()
	_ = lm.Lock(ReadLock, TransactId(uuid.New()), 2, TableId(1), 1)
	err := lm.Lock(ReadLock, TransactId(uuid.New()), 1, TableId(1), 1)
	assert.Nil(t, err)
}

func TestLockManagerImpl_Lock_When_Conflict_With_Older_Lock_Should_Fail(t *testing.T) {
	lm := NewLockManager()
	tid1 := TransactId(uuid.New())
	tid2 := TransactId(uuid.New())
	_ = lm.Lock(ReadLock, tid1, 1, TableId(1), 1)
	err := lm.Lock(WriteLock, tid2, 2, TableId(1), 1)
	assert.EqualError(t, err, lockConflictErr)
}

func TestLockManagerImpl_ReleaseLocks_Should_Enable_Write(t *testing.T) {
	lm := NewLockManager()
	tid := TransactId(uuid.New())
	_ = lm.Lock(ReadLock, tid, 1, TableId(1), 1)
	err := lm.ReleaseLocks(tid)
	assert.Nil(t, err)
	err = lm.Lock(WriteLock, TransactId(uuid.New()), 1, TableId(1), 1)
	assert.Nil(t, err)
}

func TestLockManagerImpl_ReleaseLocks_Waiters_Should_Be_Released(t *testing.T) {
	lm := NewLockManager()
	tid := TransactId(uuid.New())
	tid2 := TransactId(uuid.New())
	_ = lm.Lock(WriteLock, tid2, 2, TableId(1), 1)
	waiterErr := make(chan error)
	go func() {
		err := lm.Lock(WriteLock, tid, 1, TableId(1), 1)
		waiterErr <- err
	}()
	time.Sleep(10 * time.Millisecond)
	_ = lm.ReleaseLocks(tid)
	assert.Eventually(t, func() bool {
		select {
		case err := <-waiterErr:
			assert.EqualError(t, err, locksReleasedErr)
			return true
		default:
			return false
		}
	}, 10*time.Millisecond, 1*time.Millisecond)
}

func TestLockManagerImpl_Data_Consistency(t *testing.T) {
	// GIVEN routineCount concurrent goroutines that execute iterCount transactions that increment a counter
	// WHEN each routineCount completes
	// THEN the counter should be equal to routineCount * iterCount
	routineCount := 8
	iterCount := 1000
	lm := NewLockManager()
	counter := 0
	wg := sync.WaitGroup{}
	wg.Add(routineCount)
	aborts := 0
	f := func() {
		defer wg.Done()
		for i := range iterCount {
			tid := TransactId(uuid.New())
			err := lm.Lock(WriteLock, tid, int64(i), TableId(1), 1)
			for err != nil {
				err = lm.Lock(WriteLock, tid, int64(i), TableId(1), 1)
			}
			counter++
			_ = lm.ReleaseLocks(tid)
		}
	}
	for i := 0; i < routineCount; i++ {
		go f()
	}
	wg.Wait()
	assert.Equal(t, iterCount*routineCount-aborts, counter)
}

// This test is meant to test upgradable locks, and will be enabled once I get around to implementing that

//func TestLockManagerImpl_Data_Consistency_With_Upgradable_Lock(t *testing.T) {
//	// GIVEN routineCount concurrent goroutines that execute iterCount transactions
//	// that GET a counter, increment a counter, then PUT it back
//	// WHEN each routineCount completes
//	// THEN the counter should be equal to routineCount * iterCount
//	routineCount := 8
//	iterCount := 1000
//	lm := NewLockManager()
//	counter := 0
//	wg := sync.WaitGroup{}
//	wg.Add(routineCount)
//	xact := func(tid TransactId, ts int64) error {
//		if err := lm.Lock(ReadLock, tid, ts, TableId(1), 1); err != nil {
//			return err
//		}
//		val := counter
//		if err := lm.Lock(WriteLock, tid, ts, TableId(1), 1); err != nil {
//			return err
//		}
//		counter = val + 1
//		return lm.ReleaseLocks(tid)
//	}
//	f := func() {
//		defer wg.Done()
//		for i := range iterCount {
//			tid := TransactId(uuid.New())
//			err := xact(tid, int64(i))
//			for err != nil {
//				lm.ReleaseLocks(tid)
//				err = xact(tid, int64(i))
//			}
//		}
//	}
//	for i := 0; i < routineCount; i++ {
//		go f()
//	}
//	assert.Eventually(t, func() bool {
//		wg.Wait()
//		return assert.Equal(t, iterCount*routineCount, counter)
//	}, 1*time.Second, 10*time.Millisecond, "Deadlock/Live-lock detected")
//}
