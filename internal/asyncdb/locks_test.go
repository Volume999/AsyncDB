package asyncdb

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

const lockConflictErr = "lock conflict"

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
	err := lm.Lock(ReadLock, tid1, 2, TableId(1), 1)
	assert.Nil(t, err)
	err = lm.Lock(WriteLock, tid2, 1, TableId(1), 1)
	assert.EqualError(t, err, lockConflictErr)
}
