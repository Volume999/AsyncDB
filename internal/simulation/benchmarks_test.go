package simulation

import (
	"AsyncDB/internal/simulation/activities"
	"AsyncDB/internal/simulation/workflows"
	"strconv"
	"testing"
)

func benchmarkWorkflow(setup func() workflows.Workflow, b *testing.B, async bool) {
	parallelisms := []int{1, 10, 100, 1000, 10000, 100000}
	f := func() {
		w := setup()
		w.Execute()
	}
	b.ResetTimer()
	for _, parallelism := range parallelisms {
		b.SetParallelism(parallelism)
		b.Run("parallelism="+strconv.Itoa(parallelism), func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					f()
				}
			})
		})
	}
}

func BenchmarkSequentialWorkflowUnsafeIO(b *testing.B) {
	setup := func() workflows.Workflow {
		disk := activities.NewUnsafeDiskAccessSimulator()
		config := activities.RandomConfig()
		simulator := activities.NewSequentialSimulator(config, disk)
		return workflows.NewSequentialWorkflow(simulator)
	}
	benchmarkWorkflow(setup, b, false)
}

func BenchmarkAsyncWorkflowSequentialActivitiesUnsafeIO(b *testing.B) {
	setup := func() workflows.Workflow {
		disk := activities.NewUnsafeDiskAccessSimulator()
		config := activities.RandomConfig()
		simulator := activities.NewSequentialSimulator(config, disk)
		return workflows.NewAsyncWorkflow(simulator)
	}
	benchmarkWorkflow(setup, b, false)
}

func BenchmarkAsyncWorkflowAsyncActivitiesUnsafeIO(b *testing.B) {
	setup := func() workflows.Workflow {
		disk := activities.NewUnsafeDiskAccessSimulator()
		config := activities.RandomConfig()
		simulator := activities.NewAsyncSimulator(config, disk)
		return workflows.NewAsyncWorkflow(simulator)
	}
	benchmarkWorkflow(setup, b, true)
}

func BenchmarkSequentialWorkflowThreadSafeIO(b *testing.B) {
	setup := func() workflows.Workflow {
		disk := activities.NewThreadSafeDiskAccessSimulator()
		config := activities.RandomConfig()
		simulator := activities.NewSequentialSimulator(config, disk)
		return workflows.NewSequentialWorkflow(simulator)
	}
	benchmarkWorkflow(setup, b, false)
}

func BenchmarkAsyncWorkflowSequentialActivitiesThreadSafeIO(b *testing.B) {
	setup := func() workflows.Workflow {
		disk := activities.NewThreadSafeDiskAccessSimulator()
		config := activities.RandomConfig()
		simulator := activities.NewSequentialSimulator(config, disk)
		return workflows.NewAsyncWorkflow(simulator)
	}
	benchmarkWorkflow(setup, b, false)
}

func BenchmarkAsyncWorkflowAsyncActivitiesThreadSafeIO(b *testing.B) {
	setup := func() workflows.Workflow {
		disk := activities.NewThreadSafeDiskAccessSimulator()
		config := activities.RandomConfig()
		simulator := activities.NewAsyncSimulator(config, disk)
		return workflows.NewAsyncWorkflow(simulator)
	}
	benchmarkWorkflow(setup, b, true)
}
