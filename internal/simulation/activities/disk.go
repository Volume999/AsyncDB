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
	util.SimulateSyncIoLoad()
	t.lock.Lock()
	// Writing to the log file
	util.SimulateCpuLoad(10)
	t.lock.Unlock()
}
