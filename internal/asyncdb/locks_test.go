package asyncdb

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
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
	_ = lm.Lock(ReadLock, tid1, 2, TableId(1), 1)
	err := lm.Lock(WriteLock, tid2, 1, TableId(1), 1)
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
	_ = lm.Lock(WriteLock, tid2, 1, TableId(1), 1)
	waiterErr := make(chan error)
	go func() {
		err := lm.Lock(WriteLock, tid, 2, TableId(1), 1)
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
