package util

import "time"

const (
	IoTimeLowerBoundMs int = 2
	IoTimeUpperBoundMs int = 20
)

func SimulateCpuLoad(cpuLoadCycles int) {
	for range cpuLoadCycles {
	}
}

func SimulateAsyncIoLoad() chan struct{} {
	done := make(chan struct{})
	go func() {
		sleepTime := time.Duration(IoTimeLowerBoundMs + (IoTimeUpperBoundMs-IoTimeLowerBoundMs)/2)
		time.Sleep(sleepTime * time.Millisecond)
		done <- struct{}{}
	}()
	return done
}

func SimulateSyncIoLoad() {
	sleepTime := time.Duration(IoTimeLowerBoundMs + (IoTimeUpperBoundMs-IoTimeLowerBoundMs)/2)
	time.Sleep(sleepTime * time.Millisecond)
}
