package asyncdb

import "github.com/google/uuid"

type TransactionStatusChecker interface {
	TransactionStatusCheck(tid uuid.UUID) bool
}

type LockManager interface {
	Lock(tableId interface{}, key interface{}) error
	ReleaseLocks(tid uuid.UUID) error
}

type LockManagerImpl struct {
}

func (m LockManagerImpl) Lock(tableId interface{}, key interface{}) error {
	return nil
}

func (m LockManagerImpl) ReleaseLocks(id uuid.UUID) error {
	return nil
}

func NewLockManager() *LockManagerImpl {
	return &LockManagerImpl{}
}
