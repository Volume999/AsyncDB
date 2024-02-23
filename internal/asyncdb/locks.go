package asyncdb

import "github.com/google/uuid"

type LockManager interface {
	ReleaseLocks(id uuid.UUID) error
}

type LockManagerImpl struct {
}

func (m LockManagerImpl) ReleaseLocks(id uuid.UUID) error {
	return nil
}

func NewLockManager() *LockManagerImpl {
	return &LockManagerImpl{}
}
