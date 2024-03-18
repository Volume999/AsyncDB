package activities

import (
	"AsyncDB/pkg/util"
	"sync"
)

type DiskAccessSimulator interface {
	SimulateDiskAccess()
}

type UnsafeDiskAccessSimulator struct {
	accessTimeMs int
}

func NewUnsafeDiskAccessSimulator(accessTimeMs int) *UnsafeDiskAccessSimulator {
	return &UnsafeDiskAccessSimulator{
		accessTimeMs: accessTimeMs,
	}
}

func (u *UnsafeDiskAccessSimulator) SimulateDiskAccess() {
	util.SimulateSyncIoLoad(u.accessTimeMs)
}

type ThreadSafeDiskAccessSimulator struct {
	lock         *sync.Mutex
	accessTimeMs int
}

func NewThreadSafeDiskAccessSimulator(accessTimeMs int) *ThreadSafeDiskAccessSimulator {
	return &ThreadSafeDiskAccessSimulator{
		lock:         &sync.Mutex{},
		accessTimeMs: accessTimeMs,
	}
}

func (t *ThreadSafeDiskAccessSimulator) SimulateDiskAccess() {
	util.SimulateSyncIoLoad(t.accessTimeMs)
	t.lock.Lock()
	// Writing to the log file
	util.SimulateCpuLoad(10)
	t.lock.Unlock()
}
