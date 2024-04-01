package simulation

import (
	"AsyncDB/internal/simulation/activities"
	"AsyncDB/internal/simulation/workflows"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

var config = activities.RandomConfig()

func diskByType(diskType string, accessTimeMs int) activities.DiskAccessSimulator {
	switch diskType {
	case "unsafe":
		return activities.NewUnsafeDiskAccessSimulator(accessTimeMs)
	case "thread-safe":
		return activities.NewThreadSafeDiskAccessSimulator(accessTimeMs)
	default:
		panic("Invalid disk type")
	}
}

func simulatorByType(simulatorType string, config *activities.Config, disk activities.DiskAccessSimulator) activities.Simulator {
	switch simulatorType {
	case "sequential":
		return activities.NewSequentialSimulator(config, disk)
	case "async":
		return activities.NewAsyncSimulator(config, disk)
	default:
		panic("Invalid simulator type")
	}
}

func workflowByType(workflowType string, simulator activities.Simulator) workflows.Workflow {
	switch workflowType {
	case "sequential":
		return workflows.NewSequentialWorkflow(simulator)
	case "async":
		return workflows.NewAsyncWorkflow(simulator)
	default:
		panic("Invalid workflow type")
	}
}

func BenchmarkWorkflows(b *testing.B) {
	disks := []string{"thread-safe"}
	simulators := []string{"sequential", "async"}
	workflowTypes := []string{"sequential", "async"}
	//parallelisms := []int{1, 10, 100, 1000, 2500, 5000, 10000, 20000, 40000, 80000, 120000}
	parallelisms := []int{1, 10, 100, 1000, 2500, 5000, 10000, 20000}
	//parallelisms := []int{1, 1000, 2500, 5000}
	limitConnections := []int{-1, 200, 5000, 36000}
	diskAccessTimesMs := []int{2, 5, 10, 20, 40, 100}
	//diskAccessTimesMs := []int{70, 100}
	//diskAccessTimesMs := []int{5}
	for _, limitConnectionsT := range limitConnections {
		for _, diskT := range disks {
			for _, diskAccessTime := range diskAccessTimesMs {
				for _, simulatorT := range simulators {
					for _, workflowT := range workflowTypes {
						for _, parallelismT := range parallelisms {
							b.Run("disk="+diskT+"/accessTime(ms)="+strconv.Itoa(diskAccessTime)+"/simulator="+simulatorT+"/workflow="+workflowT+"/parallelism="+strconv.Itoa(parallelismT*runtime.NumCPU())+"/limitConnections="+strconv.Itoa(limitConnectionsT), func(b *testing.B) {
								b.SetParallelism(parallelismT)
								disk := diskByType(diskT, diskAccessTime)
								simulator := simulatorByType(simulatorT, config, disk)
								workflow := workflowByType(workflowT, simulator)
								if limitConnectionsT > 0 {
									workflow = workflows.NewLimitedConnectionsWorkflow(workflow, limitConnectionsT)
								}
								benchStart := time.Now()
								totalFunctionTime := int64(0)
								b.ResetTimer()
								b.RunParallel(func(pb *testing.PB) {
									for pb.Next() {
										fnStart := time.Now()
										workflow.Execute()
										atomic.AddInt64(&totalFunctionTime, time.Since(fnStart).Milliseconds())
									}
								})
								// Avg Exec Time and Time Per Individual Function
								b.ReportMetric(0, "ns/op")
								b.ReportMetric(float64(time.Since(benchStart).Milliseconds())/float64(b.N), "ms/op1")
								b.ReportMetric(float64(totalFunctionTime)/float64(b.N), "ms/op2")
							})
						}
					}
				}
			}
		}
	}
}

//func BenchmarkDummy(b *testing.B) {
//	//parallelisms := []int{1, 10, 100, 1000, 10000, 100000}
//	b.SetParallelism(1)
//	i := 0
//	//b.SetParallelism(parallelism)
//	//now := time.Now()
//	count := int64(0)
//	totalTime := int64(0)
//	b.ResetTimer()
//	b.RunParallel(func(pb *testing.PB) {
//		atomic.AddInt64(&count, 1)
//		for pb.Next() {
//			now := time.Now()
//			util.SimulateSyncIoLoad()
//			atomic.AddInt64(&totalTime, int64(time.Since(now).Milliseconds()))
//		}
//	})
//	//b.ReportMetric(float64(time.Since(now).Nanoseconds())/float64(b.N), "ns/op.2")
//	b.ReportMetric(float64(i), "i")
//	b.ReportMetric(float64(count), "count")
//	b.ReportMetric(float64(totalTime)/float64(b.N), "totalTime(ms)")
//}
