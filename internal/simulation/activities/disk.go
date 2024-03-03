package activities

import (
	"AsyncDB/pkg/util"
	"sync"
)

type DiskAccessSimulator interface {
	SimulateDiskAccess()
}

type UnsafeDiskAccessSimulator struct{}

func NewUnsafeDiskAccessSimulator() *UnsafeDiskAccessSimulator {
	return &UnsafeDiskAccessSimulator{}
}

func (u *UnsafeDiskAccessSimulator) SimulateDiskAccess() {
	util.SimulateSyncIoLoad()
}

type ThreadSafeDiskAccessSimulator struct {
	lock *sync.Mutex
}

func NewThreadSafeDiskAccessSimulator() *ThreadSafeDiskAccessSimulator {
	return &ThreadSafeDiskAccessSimulator{
		lock: &sync.Mutex{},
	}
}

func (t *ThreadSafeDiskAccessSimulator) SimulateDiskAccess() {
	t.lock.Lock()
	defer t.lock.Unlock()
	util.SimulateSyncIoLoad()
}
