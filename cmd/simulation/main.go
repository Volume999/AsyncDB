package main

import (
	"AsyncDB/simulation/activities"
	"AsyncDB/simulation/workflows"
	"fmt"
	"sync"
)

func main() {
	disk := activities.NewThreadSafeDiskAccessSimulator(300)
	simulator := activities.NewSequentialSimulator(activities.RandomConfig(), disk)
	workflow := workflows.NewSequentialWorkflow(simulator)
	limit := workflows.NewLimitedConnectionsWorkflow(workflow, 10)
	wg := &sync.WaitGroup{}
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func() {
			defer wg.Done()
			fmt.Println("Launch workflow, i: ", i)
			limit.Execute()
		}()
	}
	wg.Wait()
}
