package util

import "time"

const (
	CpuLoadCycles      int = 10000000
	IoTimeLowerBoundMs int = 2
	IoTimeUpperBoundMs int = 20
)

func simulateCpuLoad() {
	for range CpuLoadCycles {
	}
}

func simulateAsyncIoLoad() chan struct{} {
	done := make(chan struct{})
	go func() {
		sleepTime := time.Duration(IoTimeLowerBoundMs + (IoTimeUpperBoundMs-IoTimeLowerBoundMs)/2)
		time.Sleep(sleepTime * time.Millisecond)
		done <- struct{}{}
	}()
	return done
}

func simulateSyncIoLoad() {
	sleepTime := time.Duration(IoTimeLowerBoundMs + (IoTimeUpperBoundMs-IoTimeLowerBoundMs)/2)
	time.Sleep(sleepTime * time.Millisecond)
}
