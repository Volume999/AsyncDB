package main

import (
	"AsyncDB/simulation"
	"AsyncDB/simulation/simulator"
	"AsyncDB/simulation/workflows"
	"AsyncDB/simulation/workload"
	"fmt"
	"sync"
)

func main() {
	disk := workload.NewThreadSafeDiskAccessSimulator(300)
	simulator := simulator.NewSequentialSimulator(simulation.RandomConfig(), disk)
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
