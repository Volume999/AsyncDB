package pocsdb

import "github.com/google/uuid"

type LockManager struct {
}

func (m LockManager) ReleaseLocks(id uuid.UUID) error {
	return nil
}

func NewLockManager() *LockManager {
	return &LockManager{}
}
